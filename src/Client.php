<?php

namespace Utopia\Mongo;

use Exception;
use MongoDB\BSON;
use Swoole\Client as SwooleClient;
use Swoole\Coroutine\Client as CoroutineClient;
use stdClass;
use Utopia\Mongo\Exception\Duplicate;

class Client
{
    /**
     * Unique identifier for socket connection.
     */
    private string $id;

    /**
     * Options for connection.
     */
    private ClientOptions $options;

    /**
     * Socket (sync or async) client.
     */
    private SwooleClient|CoroutineClient $client;

    /**
     * Authentication for connection
     */
    private Auth $auth;

    /**
     * Create a Mongo connection.
     * @param ClientOptions $options
     * @param Boolean $useCoroutine
     */
    public function __construct(ClientOptions $options, bool $useCoroutine = true)
    {
        $this->id = uniqid('utopia.mongo.client');
        $this->options = $options;

        $this->client = $useCoroutine
            ? new CoroutineClient(SWOOLE_SOCK_TCP | SWOOLE_KEEP)
            : new SwooleClient(SWOOLE_SOCK_TCP | SWOOLE_KEEP);

        $this->auth = new Auth([
            'authcid' => $options->username,
            'secret' => Auth::encodeCredentials($options->username, $options->password)
        ]);
    }

    /**
     * Connect to Mongo using TCP/IP
     * and Wire Protocol.
     */
    public function connect(): Client
    {
        if($this->client->isConnected()) return $this;

        $this->client->connect($this->options->host, $this->options->port);
        [$payload, $db] = $this->auth->start();

        $res = $this->query($payload, $db);

        [$payload, $db] = $this->auth->continue($res);

        $res = $this->query($payload, $db);

        return $this;
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
     * @param array $command
     * @param string|null $db
     * @return stdClass|array|int
     */
    public function query(array $command, ?string $db = null): stdClass|array|int
    {
        $params = array_merge($command, [
            '$db' => $db ?? $this->options->name,
        ]);

        $sections = BSON\fromPHP($params);
        $message = pack('V*', 21 + strlen($sections), $this->id, 0, 2013, 0) . "\0" . $sections;
        return $this->send($message);
    }

    /**
     * Send a syncronous command to connection.
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
     * @throws Duplicate
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

        $result = BSON\toPHP(substr($res, 21, $responseLength - 21));

        if (property_exists($result, "writeErrors")) {
            throw new Duplicate($result->writeErrors[0]->errmsg);
        }

        if (property_exists($result, "n") && $result->ok === 1.0) {
            return $result->n;
        }

        if (property_exists($result, "nonce") && $result->ok === 1.0) {
            return $result;
        }

        if (property_exists($result, 'errmsg')) {
            throw new Exception($result->errmsg);
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
    public function selectDatabase(): Client
    {
        return $this;
    }

    /**
     * Creates a collection.
     *
     * Note: Since Mongo creates on the fly, this just returns
     * an instances of self.
     */
    public function createDatabase(): Client
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
     */
    public function dropDatabase(array $options = [], ?string $db = null): bool
    {
        $db ??= $this->options->name;
        $res = $this->query(array_merge(["dropDatabase" => 1], $options), $db);
        return $res->ok === 1.0;
    }

    public function selectCollection($name): Client
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
     * @throws Duplicate
     */
    public function createCollection(string $name, array $options = []): bool
    {
        $list = $this->listCollectionNames(["name" => $name]);

        if (\count($list->cursor->firstBatch) > 0) {
            throw new Duplicate('Collection Exists');
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
     */
    public function createIndexes(string $collection, array $indexes, array $options = []): bool
    {
        foreach ($indexes as $key => $index) {
            if (\array_key_exists('unique', $index) && $index['unique'] == true) {
                /**
                 * TODO: Unique Indexes are now sparse indexes, which results into incomplete indexes.
                 */
                $indexes[$key] = \array_merge($index, ['sparse' => true]);
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
     * @return array
     */
    public function dropIndexes(string $collection, array $indexes, array $options = []): Client
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
            if(\is_null($value)) continue;

            $docObj->{$key} = $value;
        }

        $docObj->_id ??= new BSON\ObjectId();

        $this->query(array_merge([
            Command::INSERT => $collection,
            'documents' => [$docObj],
        ], $options));

        return $this->lastInsertedDocument($collection);
    }

    /**
     * Retreive the last inserted document.
     *
     * @param string $collection
     *
     * @return array
     */

    public function lastInsertedDocument(string $collection): array
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
     *
     * @return Client
     */
    public function update(string $collection, array $where = [], array $updates = [], array $options = []): Client
    {

        $cleanUpdates = [];

        foreach($updates as $k => $v) {
            if(\is_null($v)) continue;
            $cleanUpdates[$k] = $v;
        }

        $this->query(
            array_merge([
                Command::UPDATE => $collection,
                'updates' => [
                    [
                        'q' => $this->toObject($where),
                        'u' => $this->toObject($cleanUpdates),
                        'multi' => false,
                        'upsert' => false
                    ]
                ]
            ], $options)
        );

        return $this;
    }

    /**
     * Insert, or update, a document/s.
     * https://docs.mongodb.com/manual/reference/command/update/#syntax
     *
     * @param string $collection
     * @param array $where
     * @param array $updates
     * @param array $options
     *
     * @return Client
     */

    public function upsert(string $collection, array $where = [], array $updates = [], array $options = []): Client
    {
        $cleanUpdates = [];

        foreach($updates as $k => $v) {
            if(\is_null($v)) continue;
            $cleanUpdates[$k] = $v;
        }


        $this->query(
            array_merge(
                [
                    'update' => $collection,
                    'updates' => [
                        [
                            'q' => ['_uid' => $where['_uid']],
                            'u' => ['$set' => $cleanUpdates],
                        ]
                    ],
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
     */
    public function find(string $collection, array $filters = [], array $options = []): stdClass
    {
        $result =  $this->query(
            array_merge([
                Command::FIND => $collection,
                'filter' => $this->toObject($filters),
            ], $options)
        );

        return $result;

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
     */
    public function findAndModify(string $collection, array $update, bool $remove = false, array $filters = [], array $options = []): stdClass
    {
        return $this->query(
            array_merge([
                Command::FIND_AND_MODIFY => $collection,
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
     * @param int batchSize
     *
     * @return  stdClass
     */
    public function getMore(int $cursorId, string $collection, int $batchSize = 25): stdClass
    {
        return $this->query([
            Command::GET_MORE => $cursorId,
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
     */
    public function delete(string $collection, array $filters = [], int $limit = 1, array $deleteOptions = [], array $options = []): int
    {
        return $this->query(
            array_merge(
                [
                    Command::DELETE => $collection,
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
     */
    public function count(string $collection, array $filters, array $options): int
    {
        $result = $this->find($collection, $filters, $options);
        $list = $result->cursor->firstBatch;

        return \count($list);
    }

    /**
     * Aggregate a collection pipeline.
     *
     * @param string $collection
     * @param array $pipeline
     *
     * @return stdClass
     */
    public function aggregate(string $collection, array $pipeline): stdClass
    {
        return $this->query([
            Command::AGGREGATE => $collection,
            'pipeline' => $pipeline,
            'cursor' => $this->toObject([]),
        ]);
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
     * @param stdClass|array|string|null $obj
     *
     * @return array|null
     */
    public function toArray(stdClass|array|string|null $obj): array|null
    {
        if(\is_null($obj)) return null;

        if (is_object($obj) || is_array($obj)) {
            $ret = (array) $obj;
            foreach ($ret as $item) {
                $item = $this->toArray($item);
            }

            return $ret;
        }

        return [$obj];
    }
}