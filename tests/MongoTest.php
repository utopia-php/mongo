<?php

namespace Utopia\Tests;

use Mongo\Exception\Duplicate;
use MongoDB\BSON\ObjectId;
use PHPUnit\Framework\TestCase;
use Utopia\Mongo\MongoClient;
use Utopia\Mongo\MongoClientOptions;

class MongoTest extends TestCase
{
    static ?MongoClient $db = null;

    static function getDatabase(): MongoClient
    {
        if (!is_null(self::$db)) {
            return self::$db;
        }

        $options = new MongoClientOptions(
            'utopia_testing',
            'mongo',
            27017,
            'root',
            'example'
        );

        $client = new MongoClient($options, false);
        $client->connect();

        self::$db = $client;
        return self::$db;
    }

    public function testDeleteDatabase()
    {
        self::assertTrue($this->getDatabase()->dropDatabase([]));
    }

    /**
     * @throws Duplicate
     */
    public function testCreateCollection()
    {
        self::assertTrue($this->getDatabase()->createCollection('movies'));
        self::expectExceptionMessage(Duplicate::class);
        self::assertTrue($this->getDatabase()->createCollection('movies'));
    }

    public function testListDatabases()
    {
        self::assertCount(4, $this->getDatabase()->listDatabaseNames()->databases);
    }

    public function testCreateDocument()
    {
        $doc = $this->getDatabase()->insert('movies', [
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

        $doc = $this->getDatabase()->insert('movies', [
                'name' => 300,
                'country' => 'USA',
                'language' => 'English'
            ]
        );

        $id = 999;
        $doc = $this->getDatabase()->insert('movies', [
                '_id' => $id,
                'array' => ['USA', 'UK', 'India'],
                'language' => 'English',
                'float' => 9.9,
                'integer' => 9,
                'is_open' => true,
                'date_string' => (new \DateTime())->format('Y-m-d\TH:i:s.vP'),
                'date_object' => new \DateTime()
            ]
        );

        $doc = $this->getDatabase()->find('movies', ['_id' => $id])->cursor->firstBatch ?? [];
        $doc = $doc[0];

        self::assertTrue($doc->is_open);
        self::assertIsFloat($doc->float);
        self::assertIsInt($doc->integer);
        self::assertIsArray($doc->array);
        self::assertIsObject($doc->date_object); // Todo: This is not working can'tretrive the object value

    }




}
