<?php

namespace Utopia\Mongo;

use MongoDB\BSON\Document;
use Swoole\Client as SwooleClient;
use Swoole\Coroutine\Client as CoroutineClient;
use Ramsey\Uuid\Uuid;
use stdClass;
use Swoole\Coroutine;

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
     */
    public function __construct(
        string $database,
        string $host,
        int $port,
        string $user,
        string $password,
        bool $useCoroutine = false
    ) {
        $this->id = uniqid('utopia.mongo.client');
        $this->database = $database;
        $this->host = $host;
        $this->port = $port;

        // Only use coroutines if explicitly requested and we're in a coroutine context
        if ($useCoroutine) {
            try {
                $cid = \Swoole\Coroutine::getCid();
                if ($cid === false || $cid < 0) {
                    $useCoroutine = false;
                }
            } catch (\Throwable $e) {
                $useCoroutine = false;
            }
        }

        $this->client = $useCoroutine
            ? new CoroutineClient(SWOOLE_SOCK_TCP | SWOOLE_KEEP)
            : new SwooleClient(SWOOLE_SOCK_TCP | SWOOLE_KEEP);

        $this->auth = new Auth([
            'authcid' => $user,
            'secret' => Auth::encodeCredentials($user, $password)
        ]);
    }

    /**
     * Connect to Mongo using TCP/IP
     * and Wire Protocol.
     * @throws Exception
     */
    public function connect(): self
    {
        if ($this->client->isConnected()) {
            return $this;
        }
        if (!$this->client->connect($this->host, $this->port)) {
            throw new Exception("Failed to connect to MongoDB at {$this->host}:{$this->port}");
        }

        [$payload, $db] = $this->auth->start();

        $res = $this->query($payload, $db);

        [$payload, $db] = $this->auth->continue($res);

        $res = $this->query($payload, $db);

        return $this;
    }

    /**
     * Create a UUID.
     * @return string
     */
    public function createUuid(): string
    {
        return  Uuid::uuid7()->toString();
    }

    /**
     * Send a raw string query to connection.
     * @param string $qry
     * @return mixed
     */
    public function raw_query(string $qry): mixed
    {
        return $this->send($qry);
    }

    /**
     * Send a BSON packed query to connection.
     *
     * @param array<string, mixed> $command
     * @param string|null $db
     * @return stdClass|array|int
     * @throws Exception
     */
    public function query(array $command, ?string $db = null): stdClass|array|int
    {
        $params = array_merge($command, [
            '$db' => $db ?? $this->database,
        ]);

        $sections = Document::fromPHP($params);
        $message = pack('V*', 21 + strlen($sections), $this->id, 0, 2013, 0) . "\0" . $sections;
        return $this->send($message);
    }

    /**
     * Send a synchronous command to connection.
     */
    public function blocking(string $cmd): stdClass|array|int
    {
        $this->client->send($cmd . PHP_EOL);

        $result = '';

        while ($result = $this->client->recv()) {
            sleep(1);
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
        $this->client->send($data);
        return $this->receive();
    }

    /**
     * Receive a message from connection.
     * @throws Exception
     */
    private function receive(): stdClass|array|int
    {
        $receivedLength = 0;
        $responseLength = null;
        $res = '';

        do {
            if (($chunk = $this->client->recv()) === false) {
                sleep(1); // Prevent excessive CPU Load, test lower.
                continue;
            }

            $receivedLength += strlen($chunk);
            $res .= $chunk;

            if ((!isset($responseLength)) && (strlen($res) >= 4)) {
                $responseLength = unpack('Vl', substr($res, 0, 4))['l'];
            }
        } while (
            (!isset($responseLength)) || ($receivedLength < $responseLength)
        );

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

        $bsonString = substr($res, 21, $responseLength - 21);
        $result = Document::fromBSON($bsonString)->toPHP();
        if (is_array($result)) {
            $result = (object) $result;
        }
        if (property_exists($result, "writeErrors")) {
            // Throws Utopia\Mongo\Exception
            throw new Exception(
                $result->writeErrors[0]->errmsg,
                $result->writeErrors[0]->code
            );
        }

        if (property_exists($result, 'errmsg')) {
            // Throws Utopia\Mongo\Exception
            throw new Exception(
                'E'.$result->code.' '.$result->codeName.': '.$result->errmsg,
                $result->code
            );
        }

        if (property_exists($result, "n") && $result->ok === 1.0) {
            return $result->n;
        }

        if (property_exists($result, "nonce") && $result->ok === 1.0) {
            return $result;
        }

        if ($result->ok === 1.0) {
            return $result;
        }

        return $result->cursor->firstBatch;
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
        $list = $this->listCollectionNames(["name" => $name]);

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
            if (\array_key_exists('unique', $index) && $index['unique'] == true) {
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
     * Insert a document/s.
     * https://docs.mongodb.com/manual/reference/command/insert/#mongodb-dbcommand-dbcmd.insert
     *
     * @param string $collection
     * @param array $document
     * @param array $options
     *
     * @return array
     * @throws Exception
     */
    public function insert(string $collection, array $document, array $options = []): array
    {
        $docObj = new stdClass();

        foreach ($document as $key => $value) {
            $docObj->{$key} = $value;
        }

        if (!isset($docObj->_id) || $docObj->_id === '' || $docObj->_id === null) {
            $docObj->_id = $this->createUuid();
        }

        $this->query(array_merge([
            self::COMMAND_INSERT => $collection,
            'documents' => [$docObj],
        ], $options));

        return $this->toArray($docObj);
    }

    public function insertMany(string $collection, array $documents, array $options = []): array
    {
        $docObjs = [];

        foreach ($documents as $document) {
            $docObj = new stdClass();

            foreach ($document as $key => $value) {
                $docObj->{$key} = $value;
            }

            if (!isset($docObj->_id) || $docObj->_id === '' || $docObj->_id === null) {
                $docObj->_id = $this->createUuid();
            }

            $docObjs[] = $docObj;
        }

        $this->query(array_merge([
            self::COMMAND_INSERT => $collection,
            'documents' => $docObjs,
        ], $options));

        return $this->toArray($docObjs);
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
     * Update a document/s.
     * https://docs.mongodb.com/manual/reference/command/update/#syntax
     *
     * @param string $collection
     * @param array $where
     * @param array $updates
     * @param array $options
     * @param bool $multi
     *
     * @return Client
     * @throws Exception
     */
    public function update(string $collection, array $where = [], array $updates = [], array $options = [], bool $multi = false): self
    {
        $this->query(
            array_merge([
                self::COMMAND_UPDATE => $collection,
                'updates' => [
                    [
                        'q' => $this->toObject($where),
                        'u' => $this->toObject($updates),
                        'multi' => $multi,
                        'upsert' => false
                    ]
                ]
            ], $options)
        );

        return $this;
    }

    /**
     * Insert, or update, document(s) with support for bulk operations.
     * https://docs.mongodb.com/manual/reference/command/update/#syntax
     *
     * @param string $collection
     * @param array $operations Array of operations, each with 'filter' and 'update' keys
     * @param array $options
     *
     * @return self
     * @throws Exception
     */
    public function upsert(string $collection, array $operations, array $options = []): self
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

        $this->query(
            array_merge(
                [
                    self::COMMAND_UPDATE => $collection,
                    'updates' => $updates,
                ],
                $options
            )
        );
        return $this;
    }



    /**
     * Find a document/s.
     * https://docs.mongodb.com/manual/reference/command/find/#mongodb-dbcommand-dbcmd.find
     *
     * @param string $collection
     * @param array $filters
     * @param array $options
     *
     * @return stdClass
     * @throws Exception
     */
    public function find(string $collection, array $filters = [], array $options = []): stdClass
    {
        $filters = $this->cleanFilters($filters);

        return $this->query(
            array_merge([
                self::COMMAND_FIND => $collection,
                'filter' => $this->toObject($filters),
            ], $options)
        );
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
     * Delete a document/s.
     * https://docs.mongodb.com/manual/reference/command/delete/#mongodb-dbcommand-dbcmd.delete
     *
     * @param string $collection
     * @param array $filters
     * @param int $limit
     * @param array $deleteOptions
     * @param array $options
     *
     * @return int
     * @throws Exception
     */
    public function delete(string $collection, array $filters = [], int $limit = 1, array $deleteOptions = [], array $options = []): int
    {
        return $this->query(
            array_merge(
                [
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
                ],
                $options
            )
        );
    }

    /**
     * Count documents.
     *
     * @param string $collection
     * @param array $filters
     * @param array $options
     *
     * @return int
     * @throws Exception
     */
    public function count(string $collection, array $filters, array $options): int
    {
        $filters = $this->cleanFilters($filters);

        // Use MongoDB's native count command with the working format instad of running find and count the results
        $command = [
            self::COMMAND_COUNT => $collection,
            'query' => $this->toObject($filters),
        ];

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

        try {
            $result = $this->query($command);
            return (int)$result;
        } catch (Exception $e) {
            return 0;
        }
    }

    /**
     * Aggregate a collection pipeline.
     *
     * @param string $collection
     * @param array $pipeline
     *
     * @return stdClass
     * @throws Exception
     */
    public function aggregate(string $collection, array $pipeline): stdClass
    {
        return $this->query([
            self::COMMAND_AGGREGATE => $collection,
            'pipeline' => $pipeline,
            'cursor' => $this->toObject([]),
        ]);
    }

    /**
     * Start a new logical session. Returns the session id object..
     *
     * @return object
     * @throws Exception
     */
    public function startSession(): object
    {
        $result = $this->query([
            self::COMMAND_START_SESSION => 1
        ], 'admin');

        return $result->id->id;
    }

    /**
     * Commit a transaction.
     *
     * @param array $lsid
     * @param int $txnNumber
     * @param bool $autocommit
     * @return mixed
     * @throws Exception
     */
    public function commitTransaction(array $lsid, int $txnNumber, bool $autocommit = false)
    {
        $txnNumber =  new \MongoDB\BSON\Int64($txnNumber);

        $result = $this->query([
            self::COMMAND_COMMIT_TRANSACTION => 1,
            'lsid' => $lsid,
            'txnNumber' => $txnNumber,
            'autocommit' => $autocommit
        ], 'admin');

        // End the session after successful commit
        $this->endSessions([$lsid]);

        return $result;
    }

    /**
     * Abort (rollback) a transaction.
     *
     * @param array $lsid
     * @param int $txnNumber
     * @param bool $autocommit
     * @return mixed
     * @throws Exception
     */
    public function abortTransaction(array $lsid, int $txnNumber, bool $autocommit = false)
    {
        $txnNumber = new \MongoDB\BSON\Int64($txnNumber);

        $result = $this->query([
            self::COMMAND_ABORT_TRANSACTION => 1,
            'lsid' => $lsid,
            'txnNumber' => $txnNumber,
            'autocommit' => $autocommit
        ], 'admin');

        // End the session after successful rollback
        $this->endSessions([$lsid]);

        return $result;
    }

    /**
     * End sessions.
     *
     * @param array $lsids
     * @param array $options
     * @return mixed
     * @throws Exception
     */
    public function endSessions(array $lsids, array $options = [])
    {
        // Extract session IDs from the format ['id' => sessionId] and format as objects
        $sessionIds = array_map(function ($lsid) {
            $sessionId = $lsid['id'] ?? $lsid;
            return ['id' => $sessionId];
        }, $lsids);

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
            $ret = (array) $obj;
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
}
