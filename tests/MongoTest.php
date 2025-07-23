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

    public function testCreateDocuments(): array
    {
        $docs = $this->getDatabase()->insertMany(
            'movies',
            [
                [
                    'name' => 'Armageddon',
                    'country' => 'USA',
                    'language' => 'English'
                ],
                [
                    'name' => '9 Monkeys',
                    'country' => 'USA',
                    'language' => 'English'
                ],
                [
                    'name' => 300,
                    'country' => 'USA',
                    'language' => 'English'
                ]
            ]
        );

        self::assertCount(3, $docs);

        return $docs;
    }

    public function testUpdateDocument(): void
    {
        $this->getDatabase()->insert(
            'movies',
            [
                'name' => 'Armageddon',
                'country' => 'AUS',
                'language' => 'English'
            ]
        );

        $this->getDatabase()->update(
            'movies',
            ['name' => 'Armageddon'],
            ['$set' => ['name' => 'Armageddon 2']]
        );

        $doc = $this->getDatabase()->find(
            'movies',
            ['name' => 'Armageddon 2']
        )->cursor->firstBatch ?? [];

        self::assertCount(1, $doc);
    }

    /**
     * @depends testCreateDocuments
     * @return void
     * @throws Exception
     */
    public function testUpdateDocuments(array $documents): void
    {
        $this->getDatabase()->update(
            'movies',
            updates: ['$set' => ['name' => 'Armageddon 2']],
            multi: true
        );

        $docs = $this->getDatabase()->find(
            'movies',
            filters: ['name' => 'Armageddon 2']
        )->cursor->firstBatch ?? [];

        self::assertCount(8, $docs);
    }

    public function testUpdateMultipleDocuments(): void
    {
        $this->getDatabase()->update(
            'movies',
            ['name' => 'Armageddon 2'],
            ['$rename' => ['name' => 'title']],
            multi: true
        );

        $docs = $this->getDatabase()->find(
            'movies',
            ['title' => 'Armageddon 2']
        )->cursor->firstBatch ?? [];

        self::assertCount(8, $docs);
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
        $this->getDatabase()->insert('movies', ['_id' => 999]);
    }

    public function testExceedTimeException()
    {
        self::expectException(Exception::class);
        self::expectExceptionCode(50);

        $this->getDatabase()->find(
            'movies',
            ['$where' => 'sleep(1000) || true'],
            ['maxTimeMS'=> 1]
        )->cursor->firstBatch ?? [];
    }


    public function testUpsert()
    {
        $this->getDatabase()->insert(
            'movies_upsert',
            [
                'name' => 'Gone with the wind',
                'language' => 'English',
                'country' => 'UK',
                'counter' => 1
            ]
        );

        $this->getDatabase()->upsert('movies_upsert', [
            [
                'filter' => ['name' => 'Gone with the wind'],
                'update' => [
                    '$set' => ['country' => 'USA'],
                    '$inc' => ['counter' => 3]
                ]
            ],
            [
                'filter' => ['name' => 'The godfather'],
                'update' => [
                    '$set' => ['name' => 'The godfather 2', 'country' => 'USA', 'language' => 'English']
                ]
            ],
        ]);

        $documents = $this->getDatabase()->find('movies_upsert')->cursor->firstBatch ?? [];
        self::assertCount(2, $documents);
        self::assertEquals(4, $documents[0]->counter);
        self::assertEquals('The godfather 2', $documents[1]->name);
        self::assertEquals('USA', $documents[1]->country);
        self::assertEquals('English', $documents[1]->language);
    }
}
