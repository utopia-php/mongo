<?php

namespace Utopia\Mongo;

use MongoDB\BSON\Document;
use MongoDB\BSON\Int64;
use MongoDB\Driver\Exception\InvalidArgumentException;
use Ramsey\Uuid\Uuid;
use stdClass;
use Swoole\Client as SwooleClient;
use Swoole\Coroutine;
use Swoole\Coroutine\Client as CoroutineClient;

class Client
{
    /**
     * Unique identifier for socket connection.
     */
    private string $id;

    /**
     * Socket (sync or async) client.
     */
    private SwooleClient|CoroutineClient $client;

    /**
     * Active sessions with transaction state tracking.
     */
    private array $sessions = [];

    /**
     * Current cluster time for causal consistency.
     */
    private ?object $clusterTime = null;

    /**
     * Current operation time for causal consistency.
     */
    private ?object $operationTime = null;

    /**
     * Connection status flag.
     */
    private bool $isConnected = false;

    /**
     * Defines commands Mongo uses over wire protocol.
     */

    public const COMMAND_CREATE = "create";
    public const COMMAND_DELETE = "delete";
    public const COMMAND_FIND = "find";
    public const COMMAND_FIND_AND_MODIFY = "findAndModify";
    public const COMMAND_GET_LAST_ERROR = "getLastError";
    public const COMMAND_GET_MORE = "getMore";
    public const COMMAND_INSERT = "insert";
    public const COMMAND_RESET_ERROR = "resetError";
    public const COMMAND_UPDATE = "update";
    public const COMMAND_COUNT = "count";
    public const COMMAND_AGGREGATE = "aggregate";
    public const COMMAND_DISTINCT = "distinct";
    public const COMMAND_MAP_REDUCE = "mapReduce";
    public const COMMAND_START_SESSION = "startSession";
    public const COMMAND_COMMIT_TRANSACTION = "commitTransaction";
    public const COMMAND_ABORT_TRANSACTION = "abortTransaction";
    public const COMMAND_END_SESSIONS = "endSessions";
    public const COMMAND_LIST_INDEXES = "listIndexes";
    public const COMMAND_COLLMOD = "collMod";
    public const COMMAND_KILL_CURSORS = "killCursors";
    // Connection and performance settings
    private int $defaultMaxTimeMS = 30000; // 30 seconds default

    // Transaction error codes for retry logic
    public const TRANSIENT_TRANSACTION_ERROR = 'TransientTransactionError';
    public const UNKNOWN_TRANSACTION_COMMIT_RESULT = 'UnknownTransactionCommitResult';
    public const TRANSACTION_TIMEOUT_ERROR = 50;
    public const TRANSACTION_ABORTED_ERROR = 251;

    // Transaction states
    public const TRANSACTION_NONE = 'none';
    public const TRANSACTION_STARTING = 'starting';
    public const TRANSACTION_IN_PROGRESS = 'in_progress';
    public const TRANSACTION_COMMITTED = 'committed';
    public const TRANSACTION_ABORTED = 'aborted';

    // Read concerns
    public const READ_CONCERN_LOCAL = 'local';
    public const READ_CONCERN_AVAILABLE = 'available';
    public const READ_CONCERN_MAJORITY = 'majority';
    public const READ_CONCERN_LINEARIZABLE = 'linearizable';
    public const READ_CONCERN_SNAPSHOT = 'snapshot';

    // Read preferences
    public const READ_PREFERENCE_PRIMARY = 'primary';
    public const READ_PREFERENCE_SECONDARY = 'secondary';
    public const READ_PREFERENCE_PRIMARY_PREFERRED = 'primaryPreferred';
    public const READ_PREFERENCE_SECONDARY_PREFERRED = 'secondaryPreferred';
    public const READ_PREFERENCE_NEAREST = 'nearest';

    /**
     *  Commands that don't support  readConcern
     */
    private array $readConcernNotSupporteedCommands = [
        self::COMMAND_GET_MORE,
        self::COMMAND_KILL_CURSORS
    ];


    /**
     * Authentication for connection
     */
    private Auth $auth;

    /**
     * Default Database Name
     */
    private string $database;

    /**
     * Database $host
     */
    private string $host;

    /**
     * Database $port
     */
    private int $port;

    /**
     * Create a Mongo connection.
     * @param string $database
     * @param string $host
     * @param int $port
     * @param string $user
     * @param string $password
     * @param Boolean $useCoroutine
     * @throws \Exception
     */
    public function __construct(
        string $database,
        string $host,
        int $port,
        string $user,
        string $password,
        bool $useCoroutine = false
    ) {
        if (empty($database)) {
            throw new \InvalidArgumentException('Database name cannot be empty');
        }
        if (empty($host)) {
            throw new \InvalidArgumentException('Host cannot be empty');
        }
        if ($port <= 0 || $port > 65535) {
            throw new \InvalidArgumentException('Port must be between 1 and 65535');
        }
        if (empty($user)) {
            throw new \InvalidArgumentException('Username cannot be empty');
        }
        if (empty($password)) {
            throw new \InvalidArgumentException('Password cannot be empty');
        }

        $this->id = uniqid('utopia.mongo.client');
        $this->database = $database;
        $this->host = $host;
        $this->port = $port;

        // Only use coroutines if explicitly requested and we're in a coroutine context
        if ($useCoroutine) {
            try {
                $cid = Coroutine::getCid();
                if ($cid <= 0) {
                    $useCoroutine = false;
                }
            } catch (\Throwable) {
                $useCoroutine = false;
            }
        }

        $this->client = $useCoroutine
            ? new CoroutineClient(SWOOLE_SOCK_TCP | SWOOLE_KEEP)
            : new SwooleClient(SWOOLE_SOCK_TCP | SWOOLE_KEEP);

        // Set socket options to prevent hanging
        $this->client->set([
            'open_tcp_keepalive' => true,
            'tcp_keepidle' => 4,     // Start keepalive after 4s idle
            'tcp_keepinterval' => 3, // Keepalive interval 3s
            'tcp_keepcount' => 2,    // Close after 2 failed keepalives
            'timeout' => 30          // 30 second connection timeout
        ]);

        $this->auth = new Auth([
            'authcid' => $user,
            'secret' => Auth::encodeCredentials($user, $password)
        ]);
    }

    /**
     * Connect to MongoDB using TCP/IP and Wire Protocol.
     * @throws Exception
     */
    public function connect(): self
    {
        if ($this->client->isConnected()) {
            return $this;
        }
        if (empty($this->host)) {
            throw new Exception('MongoDB host cannot be empty');
        }
        if ($this->port <= 0 || $this->port > 65535) {
            throw new Exception('MongoDB port must be between 1 and 65535');
        }
        if (!$this->client->connect($this->host, $this->port)) {
            throw new Exception("Failed to connect to MongoDB at {$this->host}:{$this->port}");
        }

        $this->isConnected = true;

        [$payload, $db] = $this->auth->start();

        $res = $this->query($payload, $db);

        [$payload, $db] = $this->auth->continue($res);

        $this->query($payload, $db);

        return $this;
    }

    /**
     * Create a UUID using UUID7 standard for MongoDB _id field.
     *
     * @return string
     */
    public function createUuid(): string
    {
        return Uuid::uuid7()->toString();
    }

    /**
     * Send a BSON packed query to connection with comprehensive session, causal consistency, and transaction support.
     *
     * @param array<string, mixed> $command Command to execute
     * @param string|null $db Database name
     * @return stdClass|array|int Query result
     * @throws Exception
     */
    public function query(array $command, ?string $db = null): stdClass|array|int
    {
        // Validate connection state before each operation
        $this->validateConnection();

        $sessionId = null;

        // Extract and process session from options if provided
        if (isset($command['session'])) {
            $sessionData = $command['session'];
            unset($command['session']);

            // Handle different session formats
            if (is_array($sessionData) && isset($sessionData['id'])) {
                $command['lsid'] = $sessionData['id'];
                $rawId = $sessionData['id']->id ?? null;
            } else {
                $command['lsid'] = $sessionData;
                $rawId = $sessionData->id ?? null;
            }

            $sessionId = $rawId instanceof \MongoDB\BSON\Binary
                ? bin2hex($rawId->getData())
                : $rawId;

            // Add transaction parameters if session is in transaction
            if ($sessionId && isset($this->sessions[$sessionId]) &&
                $this->sessions[$sessionId]['state'] === self::TRANSACTION_IN_PROGRESS) {
                $command['txnNumber'] = new Int64($this->sessions[$sessionId]['txnNumber']);
                $command['autocommit'] = false;

                // Check if this is the first operation
                $isFirstOperation = !isset($this->sessions[$sessionId]['firstOperationDone']);

                // Add the first operation flag for the first operation in the transaction
                if ($isFirstOperation) {
                    $command['startTransaction'] = true;
                    $this->sessions[$sessionId]['firstOperationDone'] = true;

                    // Add transaction options from startTransaction
                    if (isset($this->sessions[$sessionId]['transactionOptions'])) {
                        $txnOpts = $this->sessions[$sessionId]['transactionOptions'];
                        if (isset($txnOpts['readConcern']) && !isset($command['readConcern'])) {
                            $command['readConcern'] = $txnOpts['readConcern'];
                        }
                        if (isset($txnOpts['writeConcern']) && !isset($command['writeConcern'])) {
                            $command['writeConcern'] = $txnOpts['writeConcern'];
                        }
                    }
                }

                // IMPORTANT: Do NOT add causal consistency readConcern for ANY operation in a transaction
                // MongoDB transactions provide their own consistency guarantees, and readConcern can only
                // be specified on the first operation (which is handled above via transactionOptions)
                // Attempting to add readConcern to subsequent operations will cause E72 InvalidOptions error

                // Remove any readConcern that might have been added before this point for non-first operations
                if (!$isFirstOperation && isset($command['readConcern'])) {
                    unset($command['readConcern']);
                }
            } else {
                // Not in a transaction - can add causal consistency readConcern freely
                if ($this->operationTime !== null && !isset($command['readConcern']['afterClusterTime'])) {
                    $command['readConcern'] = $command['readConcern'] ?? [];
                    $command['readConcern']['afterClusterTime'] = $this->operationTime;
                }
            }
        } else {
            // No session - can add causal consistency readConcern freely (unless explicitly skipped)
            if (!isset($command['skipReadConcern']) &&
                $this->operationTime !== null &&
                !isset($command['readConcern']['afterClusterTime'])) {
                $command['readConcern'] = $command['readConcern'] ?? [];
                $command['readConcern']['afterClusterTime'] = $this->operationTime;
            }
        }

        // Remove internal flag before sending to MongoDB
        unset($command['skipReadConcern']);

        // CRITICAL: Remove readConcern from any non-first operation in a transaction
        // MongoDB will reject commands with readConcern that have txnNumber but not startTransaction
        // Or if the command is in the readConcernNotSupporteedCommands array
        if (((isset($command['txnNumber']) &&  !isset($command['startTransaction']) &&  isset($command['readConcern']))
        if (
            ((isset($command['txnNumber']) && !isset($command['startTransaction']) &&  isset($command['readConcern']))
            || \in_array(array_key_first($command) ?? '', $this->readConcernNotSupporteedCommands))
        ) {
            unset($command['readConcern']);
        }

        // Add cluster time for causal consistency
        if ($this->clusterTime !== null) {
            $command['$clusterTime'] = $this->clusterTime;
        }

        $params = array_merge($command, [
            '$db' => $db ?? $this->database,
        ]);

        $sections = Document::fromPHP($params);
        $message = pack('V*', 21 + strlen($sections), $this->id, 0, 2013, 0) . "\0" . $sections;
        $result = $this->send($message);

        $this->updateCausalConsistency($result);

        // Update session last use time if session was provided
        if ($sessionId && isset($this->sessions[$sessionId])) {
            $this->sessions[$sessionId]['lastUse'] = time();
        }

        return $result;
    }


    /**
     * Send a message to connection.
     *
     * @param mixed $data
     * @return stdClass|array|int
     * @throws Exception
     */
    public function send(mixed $data): stdClass|array|int
    {
        // Check if connection is alive, connect if not
        if (!$this->client->isConnected()) {
            $this->connect();
        }

        $result = $this->client->send($data);

        // If send fails, try to reconnect once
        if ($result === false) {
            $this->close();
            $this->connect();
            $result = $this->client->send($data);
            if ($result === false) {
                throw new Exception('Failed to send data to MongoDB after reconnection attempt');
            }
        }

        return $this->receive();
    }

    /**
     * Receive a message from connection.
     * @throws Exception
     */
    private function receive(): stdClass|array|int
    {
        $chunks = [];
        $receivedLength = 0;
        $responseLength = null;
        $attempts = 0;
        $maxAttempts = 10000;
        $sleepTime = 100;

        do {
            $chunk = $this->client->recv();

            if ($chunk === false || $chunk === '') {
                $attempts++;
                if ($attempts >= $maxAttempts) {
                    throw new Exception('Receive timeout: no data received within reasonable time');
                }

                // Adaptive backoff: shorter delays for coroutines, longer for sync
                if ($this->client instanceof CoroutineClient) {
                    Coroutine::sleep(0.001); // 1ms for coroutines
                } else {
                    \usleep((int)$sleepTime); // Microsecond precision for sync client
                    $sleepTime = (int)\min($sleepTime * 1.2, 10000); // Cap at 10ms for faster checking
                }
                continue;
            }

            // Reset attempts counter when we receive data
            $attempts = 0;
            $sleepTime = 100; // Reset to 0.1ms

            $chunkLen = \strlen($chunk);
            $receivedLength += $chunkLen;
            $chunks[] = $chunk;

            // Parse message length from first 4 bytes
            if ($responseLength === null && $receivedLength >= 4) {
                $firstData = $chunks[0];

                if (\strlen($firstData) < 4) {
                    $firstData = \implode('', $chunks);
                }

                $responseLength = \unpack('Vl', substr($firstData, 0, 4))['l'];

                // Validate response length before allocating memory to prevent memory exhaustion
                if ($responseLength > 16777216) { // 16MB limit
                    throw new Exception('Response too large: ' . $responseLength . ' bytes');
                }

                // Validate for negative or tiny values
                if ($responseLength < 21) { // Minimum MongoDB message size
                    throw new Exception('Invalid response length: ' . $responseLength . ' bytes');
                }
            }

            if ($responseLength !== null && $receivedLength >= $responseLength) {
                break;
            }
        } while (true);

        $res = \implode('', $chunks);

        return $this->parseResponse($res, $responseLength);
    }

    /**
     * Selects a collection.
     *
     * Note: Since Mongo creates on the fly, this just returns
     * an instances of self.
     */
    public function selectDatabase(): self
    {
        return $this;
    }

    /**
     * Creates a collection.
     *
     * Note: Since Mongo creates on the fly, this just returns
     * an instances of self.
     */
    public function createDatabase(): self
    {
        return $this;
    }

    /**
     * Get a list of databases.
     */
    public function listDatabaseNames(): stdClass
    {
        return $this->query([
            'listDatabases' => 1,
            'nameOnly' => true,
        ], 'admin');
    }

    /**
     * Drop (remove) a database.
     * https://docs.mongodb.com/manual/reference/command/dropDatabase/#mongodb-dbcommand-dbcmd.dropDatabase
     *
     * @param array $options
     * @param string|null $db
     * @return bool
     * @throws Exception
     */
    public function dropDatabase(array $options = [], ?string $db = null): bool
    {
        $db ??= $this->database;
        $res = $this->query(array_merge(["dropDatabase" => 1], $options), $db);
        return $res->ok === 1.0;
    }

    public function selectCollection($name): self
    {
        return $this;
    }

    /**
     * Create a collection.
     * https://docs.mongodb.com/manual/reference/command/create/#mongodb-dbcommand-dbcmd.create
     *
     * @param string $name
     * @param array $options
     * @return bool
     * @throws Exception
     */
    public function createCollection(string $name, array $options = []): bool
    {
        $list = $this->listCollectionNames(["name" => $name], $options);

        if (\count($list->cursor->firstBatch) > 0) {
            throw new Exception('Collection Exists');
        }

        $res = $this->query(array_merge([
            'create' => $name,
        ], $options));

        return $res->ok === 1.0;
    }

    /**
     * Drop a collection.
     * https://docs.mongodb.com/manual/reference/command/drop/#mongodb-dbcommand-dbcmd.drop
     *
     * @param string $name
     * @param array $options
     * @return bool
     * @throws Exception
     */
    public function dropCollection(string $name, array $options = []): bool
    {
        $res = $this->query(array_merge([
            'drop' => $name,
        ], $options));

        return $res->ok === 1.0;
    }

    /**
     * List collections (name only).
     * https://docs.mongodb.com/manual/reference/command/listCollections/#listcollections
     *
     * @param array $filter
     * @param array $options
     *
     * @return stdClass
     * @throws Exception
     */
    public function listCollectionNames(array $filter = [], array $options = []): stdClass
    {
        $qry = array_merge(
            [
                "listCollections" => 1.0,
                "nameOnly" => true,
                "authorizedCollections" => true,
                "filter" => $this->toObject($filter)
            ],
            $options
        );

        return $this->query($qry);
    }

    /**
     * Create indexes.
     * https://docs.mongodb.com/manual/reference/command/createIndexes/#createindexes
     *
     * @param string $collection
     * @param array $indexes
     * @param array $options
     *
     * @return boolean
     * @throws Exception
     */
    public function createIndexes(string $collection, array $indexes, array $options = []): bool
    {
        foreach ($indexes as $key => $index) {
            if ($index['unique'] ?? false) {
                /**
                 * TODO: Unique Indexes are now sparse indexes, which results into incomplete indexes.
                 * However, if partialFilterExpression is present, we can't use sparse.
                 */
                if (!\array_key_exists('partialFilterExpression', $index)) {
                    $indexes[$key] = \array_merge($index, ['sparse' => true]);
                }
            }
        }

        $qry = array_merge(
            [
                'createIndexes' => $collection,
                'indexes' => $indexes
            ],
            $options
        );

        $this->query($qry);

        return true;
    }

    /**
     * Drop indexes from a collection.
     * https://docs.mongodb.com/manual/reference/command/dropIndexes/#dropindexes
     *
     * @param string $collection
     * @param array $indexes
     * @param array $options
     *
     * @return Client
     * @throws Exception
     */
    public function dropIndexes(string $collection, array $indexes, array $options = []): self
    {
        $this->query(
            array_merge([
                'dropIndexes' => $collection,
                'index' => $indexes,
            ], $options)
        );

        return $this;
    }

    /**
     * Insert document with full transaction and session support.
     * https://docs.mongodb.com/manual/reference/command/insert/#mongodb-dbcommand-dbcmd.insert
     *
     * @param string $collection Collection name
     * @param array $document Document to insert
     * @param array $options Options array supporting:
     *   - session: Session object for transactions
     *   - writeConcern: Write concern specification
     *   - readConcern: Read concern specification
     *   - readPreference: Read preference
     *   - maxTimeMS: Operation timeout in milliseconds
     *   - ordered: Whether to stop on first error (default: true)
     *
     * @return array Inserted document with _id
     * @throws Exception
     */
    public function insert(string $collection, array $document, array $options = []): array
    {
        $docObj = new stdClass();

        foreach ($document as $key => $value) {
            $docObj->{$key} = $value;
        }

        if (!isset($docObj->_id) || $docObj->_id === '') {
            $docObj->_id = $this->createUuid();
        }

        // Build command with session and concerns
        $command = [
            self::COMMAND_INSERT => $collection,
            'documents' => [$docObj],
        ];

        // Add session if provided
        if (isset($options['session'])) {
            $command['session'] = $options['session'];
        }

        // Add write concern if provided with validation
        if (isset($options['writeConcern'])) {
            $command['writeConcern'] = $this->createWriteConcern($options['writeConcern']);
        }

        // Add read concern if provided with validation (skip for non-first transaction operations)
        if (isset($options['readConcern']) && !$this->shouldSkipReadConcern($options)) {
            $command['readConcern'] = $this->createReadConcern($options['readConcern']);
        }

        // Add other options (excluding those we've already handled)
        $otherOptions = array_diff_key($options, array_flip(['session', 'writeConcern', 'readConcern']));
        $command = array_merge($command, $otherOptions);

        $this->query($command);

        return $this->toArray($docObj);
    }

    /**
     * Insert multiple documents with improved batching for MongoDB 8+ performance.
     * Automatically handles large datasets by batching operations with full transaction support.
     *
     * @param string $collection Collection name
     * @param array $documents Array of documents to insert
     * @param array $options Options array supporting:
     *   - session: Session object for transactions
     *   - writeConcern: Write concern specification
     *   - readConcern: Read concern specification
     *   - readPreference: Read preference
     *   - maxTimeMS: Operation timeout in milliseconds
     *   - ordered: Whether to stop on first error (default: true)
     *   - batchSize: Number of documents per batch (default: 1000)
     * @return array Array of inserted documents with generated _ids
     * @throws Exception
     */
    public function insertMany(string $collection, array $documents, array $options = []): array
    {
        if (empty($documents)) {
            return [];
        }

        $batchSize = $options['batchSize'] ?? 1000;
        $ordered = $options['ordered'] ?? true;
        $insertedDocs = [];

        // Process documents in batches for better performance
        $batches = array_chunk($documents, $batchSize);

        foreach ($batches as $batch) {
            $docObjs = [];

            foreach ($batch as $document) {
                $docObj = new stdClass();

                foreach ($document as $key => $value) {
                    $docObj->{$key} = $value;
                }

                if (!isset($docObj->_id) || $docObj->_id === '') {
                    $docObj->_id = $this->createUuid();
                }

                $docObjs[] = $docObj;
            }

            // Build command with session and concerns
            $command = [
                self::COMMAND_INSERT => $collection,
                'documents' => $docObjs,
                'ordered' => $ordered,
            ];

            // Add session if provided
            if (isset($options['session'])) {
                $command['session'] = $options['session'];
            }

            // Add write concern if provided with validation
            if (isset($options['writeConcern'])) {
                $command['writeConcern'] = $this->createWriteConcern($options['writeConcern']);
            }

            // Add read concern if provided with validation (skip for non-first transaction operations)
            if (isset($options['readConcern']) && !$this->shouldSkipReadConcern($options)) {
                $command['readConcern'] = $this->createReadConcern($options['readConcern']);
            }

            // Add other options (excluding those we've already handled)
            $otherOptions = array_diff_key($options, array_flip(['session', 'writeConcern', 'readConcern', 'batchSize']));
            $command = array_merge($command, $otherOptions);

            $this->query($command);

            $insertedDocs = array_merge($insertedDocs, $this->toArray($docObjs));
        }

        return $insertedDocs;
    }

    /**
     * Retrieve the last lastDocument
     *
     * @param string $collection
     *
     * @return array
     * @throws Exception
     */

    public function lastDocument(string $collection): array
    {
        $result = $this->find($collection, [], [
            'sort' => ['_id' => -1],
            'limit' => 1
        ])->cursor->firstBatch[0];

        return $this->toArray($result);
    }

    /**
     * Update document(s) with full transaction and session support.
     * https://docs.mongodb.com/manual/reference/command/update/#syntax
     *
     * @param string $collection Collection name
     * @param array $where Filter criteria
     * @param array $updates Update operations
     * @param array $options Options array supporting:
     *   - session: Session object for transactions
     *   - writeConcern: Write concern specification
     *   - readConcern: Read concern specification
     *   - readPreference: Read preference
     *   - maxTimeMS: Operation timeout in milliseconds
     *   - upsert: Whether to insert if no match found
     *   - arrayFilters: Array filters for updates
     * @param bool $multi Whether to update multiple documents
     *
     * @return int Number of modified documents
     * @throws Exception
     */
    public function update(string $collection, array $where = [], array $updates = [], array $options = [], bool $multi = false): int
    {
        // Build command with session and concerns
        $command = [
            self::COMMAND_UPDATE => $collection,
            'updates' => [
                [
                    'q' => $this->toObject($where),
                    'u' => $this->toObject($updates),
                    'multi' => $multi,
                    'upsert' => $options['upsert'] ?? false
                ]
            ]
        ];

        // Add session if provided
        if (isset($options['session'])) {
            $command['session'] = $options['session'];
        }

        // Add write concern if provided with validation
        if (isset($options['writeConcern'])) {
            $command['writeConcern'] = $this->createWriteConcern($options['writeConcern']);
        }

        // Add read concern if provided with validation (skip for non-first transaction operations)
        if (isset($options['readConcern']) && !$this->shouldSkipReadConcern($options)) {
            $command['readConcern'] = $this->createReadConcern($options['readConcern']);
        }

        // Add other options (excluding those we've already handled)
        $otherOptions = array_diff_key($options, array_flip(['session', 'writeConcern', 'readConcern', 'upsert']));
        $command = array_merge($command, $otherOptions);

        return $this->query($command);
    }

    /**
     * Insert, or update, document(s) with support for bulk operations.
     * https://docs.mongodb.com/manual/reference/command/update/#syntax
     *
     * @param string $collection
     * @param array $operations Array of operations, each with 'filter' and 'update' keys
     * @param array $options
     *
     * @return int Number of modified documents
     * @throws Exception
     */
    public function upsert(string $collection, array $operations, array $options = []): int
    {
        $updates = [];

        foreach ($operations as $op) {
            $updateOperation = [
                'q' => $op['filter'],
                'u' => $this->toObject($op['update']),
                'upsert' => true,
                'multi' => isset($op['multi']) ? $op['multi'] : false,
            ];

            $updates[] = $updateOperation;
        }

        return $this->query(
            array_merge(
                [
                    self::COMMAND_UPDATE => $collection,
                    'updates' => $updates,
                ],
                $options
            )
        );
    }


    /**
     * Find document(s) with full transaction and session support.
     * https://docs.mongodb.com/manual/reference/command/find/#mongodb-dbcommand-dbcmd.find
     *
     * @param string $collection Collection name
     * @param array $filters Query filters
     * @param array $options Options array supporting:
     *   - session: Session object for transactions
     *   - readConcern: Read concern specification
     *   - readPreference: Read preference
     *   - maxTimeMS: Operation timeout in milliseconds
     *   - limit: Maximum number of documents to return
     *   - skip: Number of documents to skip
     *   - sort: Sort specification
     *   - projection: Field projection specification
     *   - hint: Index hint
     *   - allowPartialResults: Allow partial results from sharded clusters
     * @return stdClass Query result
     * @throws Exception
     */
    public function find(string $collection, array $filters = [], array $options = []): stdClass
    {
        $filters = $this->cleanFilters($filters);

        // Build command with session and concerns
        $command = [
            self::COMMAND_FIND => $collection,
            'filter' => $this->toObject($filters),
        ];

        // Add session if provided
        if (isset($options['session'])) {
            $command['session'] = $options['session'];
        }

        // Add read concern if provided with validation (skip for non-first transaction operations)
        if (isset($options['readConcern']) && !$this->shouldSkipReadConcern($options)) {
            $command['readConcern'] = $this->createReadConcern($options['readConcern']);
        }

        // Add read preference if provided
        if (isset($options['readPreference'])) {
            $command['$readPreference'] = $options['readPreference'];
        }

        // Add other options (excluding those we've already handled)
        $otherOptions = array_diff_key($options, array_flip(['session', 'readConcern', 'readPreference']));
        $command = array_merge($command, $otherOptions);

        return $this->query($command);
    }

    /**
     * Aggregate a collection pipeline with full transaction and session support.
     *
     * @param string $collection Collection name
     * @param array $pipeline Aggregation pipeline
     * @param array $options Options array supporting:
     *   - session: Session object for transactions
     *   - readConcern: Read concern specification
     *   - readPreference: Read preference
     *   - maxTimeMS: Operation timeout in milliseconds
     *   - allowDiskUse: Allow using disk for large result sets
     *   - batchSize: Batch size for cursor
     *   - hint: Index hint
     *   - explain: Return query execution plan
     *
     * @return stdClass Aggregation result
     * @throws Exception
     */
    public function aggregate(string $collection, array $pipeline, array $options = []): stdClass
    {
        // Build command with session and concerns
        $command = [
            self::COMMAND_AGGREGATE => $collection,
            'pipeline' => $pipeline,
            'cursor' => $this->toObject([]),
        ];

        // Add session if provided
        if (isset($options['session'])) {
            $command['session'] = $options['session'];
        }

        // Add read concern if provided with validation (skip for non-first transaction operations)
        if (isset($options['readConcern']) && !$this->shouldSkipReadConcern($options)) {
            $command['readConcern'] = $this->createReadConcern($options['readConcern']);
        }

        // Add read preference if provided
        if (isset($options['readPreference'])) {
            $command['$readPreference'] = $options['readPreference'];
        }

        // Add other options (excluding those we've already handled)
        $otherOptions = array_diff_key($options, array_flip(['session', 'readConcern', 'readPreference']));
        $command = array_merge($command, $otherOptions);

        return $this->query($command);
    }

    /**
     * Find and modify document/s.
     * https://docs.mongodb.com/manual/reference/command/findAndModify/#mongodb-dbcommand-dbcmd.findAndModify
     *
     * @param string $collection
     * @param array $update
     * @param boolean $remove
     * @param array $filters
     * @param array $options
     *
     * @return stdClass
     * @throws Exception
     */
    public function findAndModify(string $collection, array $update, bool $remove = false, array $filters = [], array $options = []): stdClass
    {
        return $this->query(
            array_merge([
                self::COMMAND_FIND_AND_MODIFY => $collection,
                'filter' => $this->toObject($filters),
                'remove' => $remove,
                'update' => $update,
            ], $options)
        );
    }

    /**
     * Use in conjunction with commands that return a cursor.
     * https://www.mongodb.com/docs/v5.0/reference/command/getMore/#getmore
     *
     * @param int $cursorId
     * @param string $collection
     * @param int $batchSize
     * @return  stdClass
     * @throws Exception
     */
    public function getMore(int $cursorId, string $collection, int $batchSize = 25): stdClass
    {
        return $this->query([
            self::COMMAND_GET_MORE => $cursorId,
            'collection' => $collection,
            'batchSize' => $batchSize
        ]);
    }

    /**
     * Delete document(s) with full transaction and session support.
     * https://docs.mongodb.com/manual/reference/command/delete/#mongodb-dbcommand-dbcmd.delete
     *
     * @param string $collection Collection name
     * @param array $filters Delete filters
     * @param int $limit Maximum number of documents to delete
     * @param array $deleteOptions Delete operation options
     * @param array $options Options array supporting:
     *   - session: Session object for transactions
     *   - writeConcern: Write concern specification
     *   - readConcern: Read concern specification
     *   - readPreference: Read preference
     *   - maxTimeMS: Operation timeout in milliseconds
     *   - hint: Index hint
     *
     * @return int Number of deleted documents
     * @throws Exception
     */
    public function delete(string $collection, array $filters = [], int $limit = 1, array $deleteOptions = [], array $options = []): int
    {
        // Build command with session and concerns
        $command = [
            self::COMMAND_DELETE => $collection,
            'deletes' => [
                $this->toObject(
                    array_merge(
                        [
                            'q' => $this->toObject($filters),
                            'limit' => $limit,
                        ],
                        $deleteOptions
                    )
                ),
            ]
        ];

        // Add session if provided
        if (isset($options['session'])) {
            $command['session'] = $options['session'];
        }

        // Add write concern if provided with validation
        if (isset($options['writeConcern'])) {
            $command['writeConcern'] = $this->createWriteConcern($options['writeConcern']);
        }

        // Add read concern if provided with validation (skip for non-first transaction operations)
        if (isset($options['readConcern']) && !$this->shouldSkipReadConcern($options)) {
            $command['readConcern'] = $this->createReadConcern($options['readConcern']);
        }

        // Add other options (excluding those we've already handled)
        $otherOptions = array_diff_key($options, array_flip(['session', 'writeConcern', 'readConcern']));
        $command = array_merge($command, $otherOptions);

        return $this->query($command);
    }

    /**
     * Count documents with full transaction and session support.
     *
     * @param string $collection Collection name
     * @param array $filters Query filters
     * @param array $options Options array supporting:
     *   - session: Session object for transactions
     *   - readConcern: Read concern specification
     *   - readPreference: Read preference
     *   - maxTimeMS: Operation timeout in milliseconds
     *   - limit: Maximum number of documents to count
     *   - skip: Number of documents to skip
     *   - hint: Index hint
     *
     * @return int Number of matching documents
     * @throws Exception
     */
    public function count(string $collection, array $filters, array $options): int
    {
        $filters = $this->cleanFilters($filters);

        // Use MongoDB's native count command with the working format instead of running find and count the results
        $command = [
            self::COMMAND_COUNT => $collection,
            'query' => $this->toObject($filters),
        ];

        // Add session if provided
        if (isset($options['session'])) {
            $command['session'] = $options['session'];
        }

        // Add read concern if provided with validation (skip for non-first transaction operations)
        if (isset($options['readConcern']) && !$this->shouldSkipReadConcern($options)) {
            $command['readConcern'] = $this->createReadConcern($options['readConcern']);
        }

        // Add read preference if provided
        if (isset($options['readPreference'])) {
            $command['$readPreference'] = $options['readPreference'];
        }

        // Add limit if specified
        if (isset($options['limit'])) {
            $command['limit'] = (int)$options['limit'];
        }

        // Add skip if specified
        if (isset($options['skip'])) {
            $command['skip'] = (int)$options['skip'];
        }

        // Add maxTimeMS if specified
        if (isset($options['maxTimeMS'])) {
            $command['maxTimeMS'] = (int)$options['maxTimeMS'];
        }

        // Add other options (excluding those we've already handled)
        $otherOptions = array_diff_key($options, array_flip(['session', 'readConcern', 'readPreference', 'limit', 'skip', 'maxTimeMS']));
        $command = array_merge($command, $otherOptions);

        try {
            $result = $this->query($command);
            return (int)$result;
        } catch (Exception $e) {
            return 0;
        }
    }


    /**
     * Start a new logical session with comprehensive state tracking for transactions.
     *
     * @param array $options Session options supporting:
     *   - causalConsistency: Enable causal consistency (default: true)
     *   - defaultTransactionOptions: Default transaction options
     * @return array Session object with comprehensive state tracking
     * @throws Exception
     */
    public function startSession(array $options = []): array
    {
        $sessionOptions = [
            'causalConsistency' => $options['causalConsistency'] ?? true
        ];

        if (isset($options['defaultTransactionOptions'])) {
            $sessionOptions['defaultTransactionOptions'] = $options['defaultTransactionOptions'];
        }

        $result = $this->query([
            self::COMMAND_START_SESSION => 1,
            'options' => $sessionOptions
        ], 'admin');

        // Convert BSON\Binary to string for use as array key
        $sessionId = $result->id->id instanceof \MongoDB\BSON\Binary
            ? bin2hex($result->id->id->getData())
            : (string)$result->id->id;

        // Initialize session state tracking
        $this->sessions[$sessionId] = [
            'id' => $result->id,
            'state' => self::TRANSACTION_NONE,
            'txnNumber' => 0,
            'lastUse' => time(),
            'operationTime' => null,
            'clusterTime' => null,
            'options' => $sessionOptions,
            'retryableWriteNumber' => 0
        ];

        return ['id' => $result->id, 'sessionId' => $sessionId];
    }

    /**
     * Start a new transaction on a session with comprehensive state management.
     *
     * @param array $session Session from startSession()
     * @param array $options Transaction options supporting:
     *   - readConcern: Read concern specification
     *   - writeConcern: Write concern specification
     *   - readPreference: Read preference
     *   - maxCommitTimeMS: Maximum time to allow for commit
     * @return bool Success status
     * @throws Exception
     */
    public function startTransaction(array $session, array $options = []): bool
    {
        $rawId = $session['sessionId'] ?? ($session['id']->id ?? null);
        $sessionId = $rawId instanceof \MongoDB\BSON\Binary
            ? bin2hex($rawId->getData())
            : $rawId;

        if (!$sessionId || !isset($this->sessions[$sessionId])) {
            throw new Exception('Invalid session provided to startTransaction');
        }

        $sessionState = &$this->sessions[$sessionId];

        // Check current transaction state
        if ($sessionState['state'] === self::TRANSACTION_IN_PROGRESS) {
            throw new Exception('Session already has a transaction in progress');
        }

        // Increment transaction number for new transaction
        $sessionState['txnNumber']++;

        // In MongoDB, transactions are started implicitly with the first operation
        // We just need to update the session state and store the options
        $sessionState['state'] = self::TRANSACTION_IN_PROGRESS;
        $sessionState['lastUse'] = time();

        // Reset the firstOperationDone flag for the new transaction
        unset($sessionState['firstOperationDone']);

        // Store transaction options for use with actual operations
        $sessionState['transactionOptions'] = [];

        // Store read/write concerns if provided
        if (isset($options['readConcern'])) {
            $sessionState['transactionOptions']['readConcern'] = $options['readConcern'];
        }
        if (isset($options['writeConcern'])) {
            $sessionState['transactionOptions']['writeConcern'] = $options['writeConcern'];
        }
        if (isset($options['readPreference'])) {
            $sessionState['transactionOptions']['readPreference'] = $options['readPreference'];
        }
        if (isset($options['maxCommitTimeMS'])) {
            $sessionState['transactionOptions']['maxCommitTimeMS'] = $options['maxCommitTimeMS'];
        }

        return true;
    }

    /**
     * Commit a transaction with comprehensive state management and retry support.
     *
     * @param array $session Session from startSession()
     * @param array $options Commit options supporting:
     *   - writeConcern: Write concern specification
     *   - maxTimeMS: Maximum time for commit operation
     * @return mixed Commit result
     * @throws Exception
     */
    public function commitTransaction(array $session, array $options = [])
    {
        $rawId = $session['sessionId'] ?? ($session['id']->id ?? null);
        $sessionId = $rawId instanceof \MongoDB\BSON\Binary
            ? bin2hex($rawId->getData())
            : $rawId;

        if (!$sessionId || !isset($this->sessions[$sessionId])) {
            throw new Exception('Invalid session provided to commitTransaction');
        }

        $sessionState = &$this->sessions[$sessionId];

        // Check current transaction state
        if ($sessionState['state'] !== self::TRANSACTION_IN_PROGRESS) {
            throw new Exception('No active transaction to commit');
        }

        $command = [
            self::COMMAND_COMMIT_TRANSACTION => 1,
            'lsid' => $sessionState['id'],
            'txnNumber' => new Int64($sessionState['txnNumber']),
            'autocommit' => false,
            'skipReadConcern' => true  // Internal flag to prevent adding readConcern
        ];

        // Add write concern if provided
        if (isset($options['writeConcern'])) {
            $command['writeConcern'] = $options['writeConcern'];
        }

        // Add maxTimeMS if provided
        if (isset($options['maxTimeMS'])) {
            $command['maxTimeMS'] = $options['maxTimeMS'];
        }

        try {
            $result = $this->query($command, 'admin');

            if ($result->ok === 1.0) {
                $sessionState['state'] = self::TRANSACTION_COMMITTED;
                $sessionState['lastUse'] = time();
                unset($sessionState['firstOperationDone']); // Reset for next transaction
            } else {
                $sessionState['state'] = self::TRANSACTION_ABORTED;
            }

            return $result;
        } catch (Exception $e) {
            // Handle specific commit errors
            if ($this->isTransientTransactionError($e) || $this->isUnknownTransactionCommitResult($e)) {
                // Keep transaction state for retry
                throw $e;
            }

            $sessionState['state'] = self::TRANSACTION_ABORTED;
            throw $e;
        }
    }

    /**
     * Abort (rollback) a transaction with comprehensive state management.
     *
     * @param array $session Session from startSession()
     * @param array $options Abort options supporting:
     *   - maxTimeMS: Maximum time for abort operation
     * @return mixed Abort result
     * @throws Exception
     */
    public function abortTransaction(array $session, array $options = [])
    {
        $rawId = $session['sessionId'] ?? ($session['id']->id ?? null);
        $sessionId = $rawId instanceof \MongoDB\BSON\Binary
            ? bin2hex($rawId->getData())
            : $rawId;

        if (!$sessionId || !isset($this->sessions[$sessionId])) {
            throw new Exception('Invalid session provided to abortTransaction');
        }

        $sessionState = &$this->sessions[$sessionId];

        // Check current transaction state
        if ($sessionState['state'] !== self::TRANSACTION_IN_PROGRESS &&
            $sessionState['state'] !== self::TRANSACTION_STARTING) {
            throw new Exception('No active transaction to abort');
        }

        $command = [
            self::COMMAND_ABORT_TRANSACTION => 1,
            'lsid' => $sessionState['id'],
            'txnNumber' => new Int64($sessionState['txnNumber']),
            'autocommit' => false,
            'skipReadConcern' => true  // Internal flag to prevent adding readConcern
        ];

        // Add maxTimeMS if provided
        if (isset($options['maxTimeMS'])) {
            $command['maxTimeMS'] = $options['maxTimeMS'];
        }

        try {
            $result = $this->query($command, 'admin');
            $sessionState['state'] = self::TRANSACTION_ABORTED;
            $sessionState['lastUse'] = time();
            unset($sessionState['firstOperationDone']); // Reset for next transaction
            return $result;
        } catch (Exception $e) {
            // Even if abort fails, mark transaction as aborted
            $sessionState['state'] = self::TRANSACTION_ABORTED;
            unset($sessionState['firstOperationDone']); // Reset for next transaction
            throw $e;
        }
    }

    /**
     * End sessions with proper state cleanup and validation.
     *
     * @param array $sessions Array of session objects from startSession()
     * @param array $options End session options
     * @return mixed Result of end sessions command
     * @throws Exception
     */
    public function endSessions(array $sessions, array $options = [])
    {
        $sessionIds = [];

        foreach ($sessions as $session) {
            $rawId = $session['sessionId'] ?? ($session['id']->id ?? null);
            $sessionId = $rawId instanceof \MongoDB\BSON\Binary
                ? bin2hex($rawId->getData())
                : $rawId;

            if ($sessionId && isset($this->sessions[$sessionId])) {
                $sessionState = $this->sessions[$sessionId];

                // Warn about active transactions
                if ($sessionState['state'] === self::TRANSACTION_IN_PROGRESS) {
                    \error_log("Warning: Ending session with active transaction. Transaction will be aborted.");
                }

                $sessionIds[] = $sessionState['id'];

                // Clean up local session state
                unset($this->sessions[$sessionId]);
            } else {
                // Handle legacy format
                $sessionId = $session['id'] ?? $session;
                $sessionIds[] = $sessionId;
            }
        }

        if (empty($sessionIds)) {
            return new \stdClass(); // Return empty result if no valid sessions
        }

        return $this->query(
            array_merge(
                [
                    self::COMMAND_END_SESSIONS => $sessionIds,
                ],
                $options
            ),
            'admin'
        );
    }

    /**
     * Convert an assoc array to an object (stdClass).
     *
     * @param array $dict
     *
     * @return stdClass
     */
    public function toObject(array $dict): stdClass
    {
        $obj = new stdClass();

        foreach ($dict as $k => $v) {
            $obj->{$k} = $v;
        }

        return $obj;
    }

    /**
     * Convert an object (stdClass) to an assoc array.
     *
     * @param mixed $obj
     * @return array|null
     */
    public function toArray(mixed $obj): ?array
    {
        if (\is_null($obj)) {
            return null;
        }

        if (is_object($obj) || is_array($obj)) {
            $ret = (array)$obj;
            foreach ($ret as $item) {
                $item = $this->toArray($item);
            }

            return $ret;
        }

        return [$obj];
    }

    private function cleanFilters($filters): array
    {
        $cleanedFilters = [];

        foreach ($filters as $k => $v) {
            $value = $v;

            if (in_array($k, ['$and', '$or', '$nor']) && is_array($v)) {
                $values = [];
                foreach ($v as $item) {
                    $values[] = is_array($item) ? $this->toObject($item) : $item;
                }

                $value = $values;
            }

            $cleanedFilters[$k] = $value;
        }

        return $cleanedFilters;
    }

    private ?bool $replicaSet = null;

    /**
     * Check if MongoDB is running as a replica set.
     *
     * @return bool True if this is a replica set, false if standalone
     * @throws Exception
     */
    public function isReplicaSet(): bool
    {
        if ($this->replicaSet !== null) {
            return $this->replicaSet;
        }

        $result = $this->query([
            'isMaster' => 1,
        ], 'admin');

        $this->replicaSet = property_exists($result, 'setName');
        return $this->replicaSet;
    }

    /**
     * Close connection and clean up resources including active sessions.
     */
    public function close(): void
    {
        // End any active sessions before closing connection
        if (!empty($this->sessions)) {
            try {
                $activeSessions = [];
                foreach ($this->sessions as $sessionId => $sessionData) {
                    $activeSessions[] = ['id' => $sessionData['id'], 'sessionId' => $sessionId];
                }

                $this->endSessions($activeSessions);
            } catch (Exception $e) {
                // Silently ignore if connection is already lost during cleanup
                if (!str_contains($e->getMessage(), 'Connection to MongoDB has been lost')) {
                    \error_log("Error ending sessions during close: " . $e->getMessage());
                }
            }
        }

        if ($this->client->isConnected()) {
            $this->client->close();
        }

        $this->isConnected = false;
        $this->sessions = [];
        $this->clusterTime = null;
        $this->operationTime = null;
    }

    /**
     * Parse MongoDB wire protocol response and handle BSON decoding.
     *
     * @param string $response Raw response data
     * @param int $responseLength Expected response length
     * @return stdClass|array|int Parsed response
     * @throws Exception
     */
    private function parseResponse(string $response, int $responseLength): stdClass|array|int
    {
        /*
         * The first 21 bytes of the MongoDB wire protocol response consist of:
         * - 16 bytes: Standard message header, which includes:
         *     - messageLength (4 bytes): Total size of the message, including the header.
         *     - requestID (4 bytes): Identifier for this message.
         *     - responseTo (4 bytes): The requestID that this message is responding to.
         *     - opCode (4 bytes): The operation code for the message type (e.g., OP_MSG).
         * - 4 bytes: flagBits, which provide additional information about the message.
         * - 1 byte: payloadType, indicating the type of the following payload (usually 0 for a BSON document).
         *
         * These 21 bytes are protocol metadata and precede the actual BSON-encoded document in the response.
         */

        if (\strlen($response) < 21) {
            throw new Exception('Invalid response: too short');
        }

        // Extract message header
        $header = \substr($response, 0, 16);
        $headerData = \unpack('VmessageLength/VrequestID/VresponseTo/VopCode', $header);

        // Validate message length
        if ($headerData['messageLength'] !== $responseLength) {
            throw new Exception('Response length mismatch');
        }

        // Extract flag bits and payload type
        $flagBits = \unpack('V', \substr($response, 16, 4))[1];
        $payloadType = \ord(\substr($response, 20, 1));

        // Extract BSON document (skip header + flagBits + payloadType)
        $bsonString = \substr($response, 21, $responseLength - 21);

        if (empty($bsonString)) {
            return new \stdClass();
        }

        try {
            // Parse BSON document
            $result = Document::fromBSON($bsonString)->toPHP();

            // Convert array to stdClass if needed
            if (\is_array($result)) {
                $result = (object)$result;
            }

            // Check for write errors (duplicate key, etc.)
            if (\property_exists($result, 'writeErrors') && !empty($result->writeErrors)) {
                throw new Exception(
                    $result->writeErrors[0]->errmsg,
                    $result->writeErrors[0]->code
                );
            }

            // Check for general MongoDB errors
            if (\property_exists($result, 'errmsg')) {
                throw new Exception(
                    'E' . $result->code . ' ' . $result->codeName . ': ' . $result->errmsg,
                    $result->code
                );
            }

            // Check for operation success
            if (\property_exists($result, 'n') && $result->ok === 1.0) {
                return $result->n;
            }

            if (\property_exists($result, 'nonce') && $result->ok === 1.0) {
                return $result;
            }

            if ($result->ok === 1.0) {
                return $result;
            }

            return $result->cursor->firstBatch;
        } catch (InvalidArgumentException $e) {
            throw new Exception('Failed to parse BSON response: ' . $e->getMessage());
        } catch (\Exception $e) {
            if ($e instanceof Exception) {
                throw $e;
            }
            throw new Exception('Error parsing response: ' . $e->getMessage());
        }
    }

    /**
     * Check if an exception represents a transient transaction error.
     *
     * @param Exception $exception Exception to check
     * @return bool True if transient transaction error
     */
    public function isTransientTransactionError(Exception $exception): bool
    {
        $message = $exception->getMessage();
        $code = $exception->getCode();

        // MongoDB transient error codes
        $transientCodes = [
            self::TRANSACTION_TIMEOUT_ERROR,
            self::TRANSACTION_ABORTED_ERROR,
            6, // HostUnreachable
            7, // HostNotFound
            89, // NetworkTimeout
            91, // ShutdownInProgress
            189, // PrimarySteppedDown
            262, // ExceededTimeLimit
            9001, // SocketException
            10107, // NotMaster
            11600, // InterruptedAtShutdown
            11602, // InterruptedDueToReplStateChange
            13435, // NotMasterNoSlaveOk
            13436, // NotMasterOrSecondary
        ];

        return \in_array($code, $transientCodes) ||
            \str_contains($message, self::TRANSIENT_TRANSACTION_ERROR) ||
            \str_contains($message, 'connection') ||
            \str_contains($message, 'timeout') ||
            \str_contains($message, 'network');
    }

    /**
     * Check if an exception represents an unknown transaction commit result.
     *
     * @param Exception $exception Exception to check
     * @return bool True if unknown commit result
     */
    public function isUnknownTransactionCommitResult(Exception $exception): bool
    {
        $message = $exception->getMessage();
        $code = $exception->getCode();

        $unknownCommitCodes = [
            self::TRANSACTION_TIMEOUT_ERROR,
            91, // ShutdownInProgress
            189, // PrimarySteppedDown
            262, // ExceededTimeLimit
            9001, // SocketException
            10107, // NotMaster
            11600, // InterruptedAtShutdown
            11602, // InterruptedDueToReplStateChange
            13435, // NotMasterNoSlaveOk
            13436, // NotMasterOrSecondary
        ];

        return \in_array($code, $unknownCommitCodes) ||
            \str_contains($message, self::UNKNOWN_TRANSACTION_COMMIT_RESULT);
    }

    /**
     * Execute a callback within a transaction with automatic retry logic.
     *
     * @param array $session Session from startSession()
     * @param callable $callback Transaction callback that receives the session
     * @param array $options Transaction options supporting:
     *   - readConcern: Read concern specification
     *   - writeConcern: Write concern specification
     *   - readPreference: Read preference
     *   - maxCommitTimeMS: Maximum time to allow for commit
     *   - maxRetries: Maximum number of retries (default: 3)
     *   - retryDelayMs: Delay between retries in milliseconds (default: 100)
     * @return mixed Result from callback
     * @throws Exception
     */
    public function withTransaction(array $session, callable $callback, array $options = [])
    {
        $maxRetries = $options['maxRetries'] ?? 3;
        $retryDelayMs = $options['retryDelayMs'] ?? 100;
        $attempt = 0;

        while ($attempt <= $maxRetries) {
            try {
                // Start transaction
                $this->startTransaction($session, $options);

                try {
                    // Execute user callback
                    $result = $callback($session);

                    // Attempt to commit
                    $commitAttempt = 0;
                    $maxCommitRetries = 3;

                    while ($commitAttempt <= $maxCommitRetries) {
                        try {
                            $this->commitTransaction($session, $options);
                            return $result;
                        } catch (Exception $e) {
                            if ($this->isUnknownTransactionCommitResult($e) && $commitAttempt < $maxCommitRetries) {
                                $commitAttempt++;
                                if ($retryDelayMs > 0) {
                                    usleep($retryDelayMs * 1000);
                                }
                                continue;
                            }
                            throw $e;
                        }
                    }
                } catch (Exception $e) {
                    // Abort transaction on any error in callback or commit
                    try {
                        $this->abortTransaction($session);
                    } catch (Exception $abortError) {
                        // Log abort error but don't mask original error
                        \error_log("Error aborting transaction: " . $abortError->getMessage());
                    }
                    throw $e;
                }
            } catch (Exception $e) {
                if ($this->isTransientTransactionError($e) && $attempt < $maxRetries) {
                    $attempt++;
                    if ($retryDelayMs > 0) {
                        \usleep($retryDelayMs * 1000);
                    }
                    continue;
                }
                throw $e;
            }
        }

        throw new Exception('Transaction failed after maximum retries');
    }

    /**
     * Update causal consistency timestamps from operation results.
     *
     * @param mixed $result Operation result
     */
    private function updateCausalConsistency($result): void
    {
        if (is_object($result)) {
            if (property_exists($result, 'operationTime')) {
                $this->operationTime = $result->operationTime;
            }
            if (property_exists($result, '$clusterTime')) {
                $this->clusterTime = $result->{'$clusterTime'};
            }
        }
    }

    /**
     * Get the current operation time for causal consistency.
     *
     * @return object|null Current operation time
     */
    public function getOperationTime(): ?object
    {
        return $this->operationTime;
    }

    /**
     * Get the current cluster time for causal consistency.
     *
     * @return object|null Current cluster time
     */
    public function getClusterTime(): ?object
    {
        return $this->clusterTime;
    }

    /**
     * Get session state information for debugging.
     *
     * @param array $session Session to inspect
     * @return array Session state information
     */
    public function getSessionState(array $session): array
    {
        $rawId = $session['sessionId'] ?? ($session['id']->id ?? null);
        $sessionId = $rawId instanceof \MongoDB\BSON\Binary
            ? bin2hex($rawId->getData())
            : $rawId;

        if (!$sessionId || !isset($this->sessions[$sessionId])) {
            return ['error' => 'Session not found'];
        }

        return [
            'sessionId' => $sessionId,
            'state' => $this->sessions[$sessionId]['state'],
            'txnNumber' => $this->sessions[$sessionId]['txnNumber'],
            'lastUse' => $this->sessions[$sessionId]['lastUse'],
            'retryableWriteNumber' => $this->sessions[$sessionId]['retryableWriteNumber']
        ];
    }

    /**
     * Validate connection before operation with comprehensive checks.
     *
     * @throws Exception If connection is invalid
     */
    private function validateConnection(): void
    {
        if (!$this->isConnected) {
            throw new Exception('Client is not connected to MongoDB');
        }

        if (!$this->client->isConnected()) {
            $this->isConnected = false;
            throw new Exception('Connection to MongoDB has been lost');
        }
    }

    /**
     * Create a write concern object with validation.
     *
     * @param array|string|int $writeConcern Write concern specification
     * @return array Validated write concern
     * @throws Exception If write concern is invalid
     */
    public function createWriteConcern(array|string|int $writeConcern): array
    {
        if (is_string($writeConcern)) {
            return ['w' => $writeConcern];
        }

        if (is_int($writeConcern)) {
            if ($writeConcern < 0) {
                throw new Exception('Write concern w value must be >= 0');
            }
            return ['w' => $writeConcern];
        }

        $concern = [];

        if (isset($writeConcern['w'])) {
            if (is_int($writeConcern['w']) && $writeConcern['w'] < 0) {
                throw new Exception('Write concern w value must be >= 0');
            }
            $concern['w'] = $writeConcern['w'];
        }

        if (isset($writeConcern['j'])) {
            $concern['j'] = (bool)$writeConcern['j'];
        }

        if (isset($writeConcern['wtimeout'])) {
            if (!is_int($writeConcern['wtimeout']) || $writeConcern['wtimeout'] < 0) {
                throw new Exception('Write concern wtimeout must be a non-negative integer');
            }
            $concern['wtimeout'] = $writeConcern['wtimeout'];
        }

        return $concern;
    }

    /**
     * Check if readConcern should be skipped for a transaction operation
     *
     * @param array $options The options array containing session
     * @return bool True if readConcern should be skipped
     */
    private function shouldSkipReadConcern(array $options): bool
    {
        if (!isset($options['session'])) {
            return false;
        }

        $sessionData = $options['session'];

        // Use the same extraction logic as in query() method
        $sessionId = null;
        if (is_array($sessionData) && isset($sessionData['id'])) {
            $rawId = $sessionData['id']->id ?? null;
            $sessionId = $rawId instanceof \MongoDB\BSON\Binary
                ? bin2hex($rawId->getData())
                : $rawId;
        } else {
            $rawId = $sessionData->id ?? null;
            $sessionId = $rawId instanceof \MongoDB\BSON\Binary
                ? bin2hex($rawId->getData())
                : $rawId;
        }

        // If in transaction and not first operation, skip readConcern
        if ($sessionId && isset($this->sessions[$sessionId]) &&
            $this->sessions[$sessionId]['state'] === self::TRANSACTION_IN_PROGRESS &&
            isset($this->sessions[$sessionId]['firstOperationDone'])) {
            return true;
        }

        return false;
    }

    /**
     * Create a read concern object with validation.
     *
     * @param array|string $readConcern Read concern specification
     * @return array Validated read concern
     * @throws Exception If read concern is invalid
     */
    public function createReadConcern(array|string $readConcern): array
    {
        if (is_string($readConcern)) {
            $validLevels = [
                self::READ_CONCERN_LOCAL,
                self::READ_CONCERN_AVAILABLE,
                self::READ_CONCERN_MAJORITY,
                self::READ_CONCERN_LINEARIZABLE,
                self::READ_CONCERN_SNAPSHOT
            ];

            if (!in_array($readConcern, $validLevels)) {
                throw new Exception('Invalid read concern level: ' . $readConcern);
            }

            return ['level' => $readConcern];
        }

        $concern = [];

        if (isset($readConcern['level'])) {
            $validLevels = [
                self::READ_CONCERN_LOCAL,
                self::READ_CONCERN_AVAILABLE,
                self::READ_CONCERN_MAJORITY,
                self::READ_CONCERN_LINEARIZABLE,
                self::READ_CONCERN_SNAPSHOT
            ];

            if (!in_array($readConcern['level'], $validLevels)) {
                throw new Exception('Invalid read concern level: ' . $readConcern['level']);
            }

            $concern['level'] = $readConcern['level'];
        }

        if (isset($readConcern['afterClusterTime'])) {
            $concern['afterClusterTime'] = $readConcern['afterClusterTime'];
        }

        return $concern;
    }

    /**
     * Get connection status information.
     *
     * @return array Connection status details
     */
    public function getConnectionInfo(): array
    {
        return [
            'connected' => $this->isConnected,
            'host' => $this->host,
            'port' => $this->port,
            'database' => $this->database,
            'activeSessions' => count($this->sessions),
            'clusterTimeSet' => $this->clusterTime !== null,
            'operationTimeSet' => $this->operationTime !== null
        ];
    }

    /**
     * Clean up stale sessions (older than 30 minutes).
     */
    public function cleanupStaleSessions(): void
    {
        $cutoff = time() - 1800; // 30 minutes
        $staleSessions = [];

        foreach ($this->sessions as $sessionId => $sessionData) {
            if ($sessionData['lastUse'] < $cutoff) {
                $staleSessions[] = ['id' => $sessionData['id'], 'sessionId' => $sessionId];
            }
        }

        if (!empty($staleSessions)) {
            try {
                $this->endSessions($staleSessions);
            } catch (Exception $e) {
                \error_log("Error cleaning up stale sessions: " . $e->getMessage());
            }
        }
    }

    /**
     * Destructor to ensure proper cleanup.
     */
    public function __destruct()
    {
        $this->close();
    }
}
