<?php

namespace Utopia\Tests;

use MongoDB\BSON\ObjectId;
use PHPUnit\Framework\TestCase;
use Utopia\Mongo\Client;
use Utopia\Mongo\Exception;

class MongoTest extends TestCase
{
    public static ?Client $db = null;

    /**
     * @throws Exception
     */
    public static function getDatabase(): Client
    {
        if (!is_null(self::$db)) {
            return self::$db;
        }

        $client = new Client('testing', 'mongo', 27017, 'root', 'example', false);
        $client->connect();

        self::$db = $client;
        return self::$db;
    }

    public function testDeleteDatabase()
    {
        self::assertTrue($this->getDatabase()->dropDatabase([]));
    }


    /**
     * @throws Exception
     */
    public function testCreateCollection()
    {
        self::assertTrue($this->getDatabase()->createCollection('movies'));
        self::assertTrue($this->getDatabase()->dropCollection('movies'));
        self::assertTrue($this->getDatabase()->createCollection('movies'));
        self::expectException(Exception::class);
        self::assertTrue($this->getDatabase()->createCollection('movies'));
    }

    public function testListDatabases()
    {
        self::assertCount(4, $this->getDatabase()->listDatabaseNames()->databases);
    }

    public function testCreateDocument()
    {
        $doc = $this->getDatabase()->insert(
            'movies',
            [
            'name' => 'Armageddon',
            'country' => 'USA',
            'language' => 'English'
            ]
        );

        $id = (string)$doc['_id'];
        self::assertEquals(24, strlen($id));

        $doc = $this->getDatabase()->find('movies', ['name' => 'Armageddon'])->cursor->firstBatch ?? [];
        self::assertCount(1, $doc);

        $doc = $this->getDatabase()->find('movies', ['_id' => new ObjectId($id)])->cursor->firstBatch ?? [];
        self::assertCount(1, $doc);

        $doc = $this->getDatabase()->insert('movies', ['9 Monkeys']);
        $id = (string)$doc['_id'];
        self::assertEquals(24, strlen($id));

        $doc = $this->getDatabase()->insert(
            'movies',
            [
                'name' => 300,
                'country' => 'USA',
                'language' => 'English'
            ]
        );

        $doc = $this->getDatabase()->insert(
            'movies',
            [
                '_id' => 999,
                'array' => ['USA', 'UK', 'India'],
                'language' => 'English',
                'float' => 9.9,
                'integer' => 9,
                'is_open' => true,
                'date_string' => (new \DateTime())->format('Y-m-d\TH:i:s.vP'),
                'date_object' => new \DateTime()
            ]
        );

        $doc = $this->getDatabase()->find('movies', ['_id' => 999])->cursor->firstBatch ?? [];
        $doc = $doc[0];

        self::assertTrue($doc->is_open);
        self::assertIsFloat($doc->float);
        self::assertIsInt($doc->integer);
        self::assertIsArray($doc->array);
        self::assertIsString($doc->date_string);
        self::assertIsObject($doc->date_object); // Todo: This is not working can't retrieve the object back
    }

    public function testDriverException()
    {
        self::expectException('MongoDB\Driver\Exception\InvalidArgumentException');
        self::expectExceptionCode(0);
        new ObjectId('triggerAnError');
    }

    public function testDuplicationException()
    {
        self::expectException(Exception::class);
        self::expectExceptionCode(11000);
        $this->getDatabase()->insert('movies', ['_id' => 999, 'b' => 'Duplication']);
    }

    public function testExceedTimeException()
    {
        self::expectException(Exception::class);
        self::expectExceptionCode(50);

        $this->getDatabase()->find(
                'movies',
                ['$where' => "sleep(1000) || true"],
                ['maxTimeMS'=> 1]
            )->cursor->firstBatch ?? [];
    }
}
