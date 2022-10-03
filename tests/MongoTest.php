<?php

namespace Utopia\Tests;

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
        self::assertTrue(!!$this->getDatabase()->dropDatabase([]));
    }

    public function testCreateCollection()
    {
        self::assertTrue(!!$this->getDatabase()->createCollection('movies'));
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

        var_dump($doc['_id']);

        $this->getDatabase()->insert('movies', ['9 Monkeys']);

    }




}
