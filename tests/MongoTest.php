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
        self::$db = $client;
        return self::$db;
    }

    public function testListDatabases()
    {
        $this->getDatabase()->listDatabaseNames();
    }

    public function testDeleteDatabase()
    {
        $this->getDatabase()->dropDatabase([]);
    }

    public function testCreateCollection()
    {
        $this->getDatabase()->createCollection('movies');
    }

    public function testCreateDocument()
    {
        $this->getDatabase()->insert('movies', [
            'name' => 'Armageddon',
            'country' => 'USA',
            'language' => 'English'
            ]
        );

        $this->getDatabase()->insert('movies', ['9 Monkeys']);

    }


    public function testCreateExistsDelete()
    {
        var_dump($this->getDatabase()->selectDatabase());

    }

}
