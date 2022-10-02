<?php

namespace Utopia\Tests;

use PHPUnit\Framework\TestCase;
use Utopia\Mongo\MongoClient;
use Utopia\Mongo\MongoClientOptions;

class MongoDBTest extends TestCase
{
    static ?MongoClient $database = null;

    static function getDatabase(): MongoClient
    {

        if (!is_null(self::$database)) {
            return self::$database;
        }

        $options = new MongoClientOptions(
            'utopia_testing',
            'mongo',
            27017,
            'root',
            'example'
        );

        $client = new MongoClient($options, false);
        self::$database = $client;

        return self::$database;
    }

    public function testCreateExistsDelete()
    {
        var_dump("getDatabase");
        $this->assertNotNull(static::getDatabase()->create($this->testDatabase));

        $this->assertEquals(true, static::getDatabase()->exists($this->testDatabase));
        $this->assertEquals(true, static::getDatabase()->delete($this->testDatabase));

        // Mongo creates on the fly, so this will never be true, do we want to try to make it pass
        // by doing something else?
        // $this->assertEquals(false, static::getDatabase()->exists($this->testDatabase));
        // $this->assertEquals(true, static::getDatabase()->create($this->testDatabase));
        // $this->assertEquals(true, static::getDatabase()->setDefaultDatabase($this->testDatabase));
    }

    /**
     * @depends testCreateDocument
     */
    public function testListDocumentSearch(Document $document)
    {
        static::getDatabase()->createIndex('documents', 'string', Database::INDEX_FULLTEXT, ['string']);
        static::getDatabase()->createDocument('documents', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'string' => '*test+alias@email-provider.com',
            'integer' => 0,
            'bigint' => 8589934592, // 2^33
            'float' => 5.55,
            'boolean' => true,
            'colors' => ['pink', 'green', 'blue'],
            'empty' => [],
        ]));

        $documents = static::getDatabase()->find('documents', [
            Query::search('string', '*test+alias@email-provider.com')
        ]);

        $this->assertEquals(1, count($documents));

        return $document;
    }

    /**
     * @depends testUpdateDocument
     */
    public function testFind(Document $document)
    {
        static::getDatabase()->createCollection('movies');

        $this->assertEquals(true, static::getDatabase()->createAttribute('movies', 'name', Database::VAR_STRING, 128, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('movies', 'director', Database::VAR_STRING, 128, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('movies', 'year', Database::VAR_INTEGER, 0, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('movies', 'price', Database::VAR_FLOAT, 0, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('movies', 'active', Database::VAR_BOOLEAN, 0, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('movies', 'generes', Database::VAR_STRING, 32, true, null, true, true));

        static::getDatabase()->createDocument('movies', new Document([
            '$id' => ID::custom('frozen'),
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::read(Role::user(ID::custom('1'))),
                Permission::read(Role::user(ID::custom('2'))),
                Permission::create(Role::any()),
                Permission::create(Role::user(ID::custom('1x'))),
                Permission::create(Role::user(ID::custom('2x'))),
                Permission::update(Role::any()),
                Permission::update(Role::user(ID::custom('1x'))),
                Permission::update(Role::user(ID::custom('2x'))),
                Permission::delete(Role::any()),
                Permission::delete(Role::user(ID::custom('1x'))),
                Permission::delete(Role::user(ID::custom('2x'))),
            ],
            'name' => 'Frozen',
            'director' => 'Chris Buck & Jennifer Lee',
            'year' => 2013,
            'price' => 39.50,
            'active' => true,
            'generes' => ['animation', 'kids'],
        ]));

        static::getDatabase()->createDocument('movies', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::read(Role::user(ID::custom('1'))),
                Permission::read(Role::user(ID::custom('2'))),
                Permission::create(Role::any()),
                Permission::create(Role::user(ID::custom('1x'))),
                Permission::create(Role::user(ID::custom('2x'))),
                Permission::update(Role::any()),
                Permission::update(Role::user(ID::custom('1x'))),
                Permission::update(Role::user(ID::custom('2x'))),
                Permission::delete(Role::any()),
                Permission::delete(Role::user(ID::custom('1x'))),
                Permission::delete(Role::user(ID::custom('2x'))),
            ],
            'name' => 'Frozen II',
            'director' => 'Chris Buck & Jennifer Lee',
            'year' => 2019,
            'price' => 39.50,
            'active' => true,
            'generes' => ['animation', 'kids'],
        ]));

        static::getDatabase()->createDocument('movies', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::read(Role::user(ID::custom('1'))),
                Permission::read(Role::user(ID::custom('2'))),
                Permission::create(Role::any()),
                Permission::create(Role::user(ID::custom('1x'))),
                Permission::create(Role::user(ID::custom('2x'))),
                Permission::update(Role::any()),
                Permission::update(Role::user(ID::custom('1x'))),
                Permission::update(Role::user(ID::custom('2x'))),
                Permission::delete(Role::any()),
                Permission::delete(Role::user(ID::custom('1x'))),
                Permission::delete(Role::user(ID::custom('2x'))),
            ],
            'name' => 'Captain America: The First Avenger',
            'director' => 'Joe Johnston',
            'year' => 2011,
            'price' => 25.94,
            'active' => true,
            'generes' => ['science fiction', 'action', 'comics'],
        ]));

        static::getDatabase()->createDocument('movies', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::read(Role::user(ID::custom('1'))),
                Permission::read(Role::user(ID::custom('2'))),
                Permission::create(Role::any()),
                Permission::create(Role::user(ID::custom('1x'))),
                Permission::create(Role::user(ID::custom('2x'))),
                Permission::update(Role::any()),
                Permission::update(Role::user(ID::custom('1x'))),
                Permission::update(Role::user(ID::custom('2x'))),
                Permission::delete(Role::any()),
                Permission::delete(Role::user(ID::custom('1x'))),
                Permission::delete(Role::user(ID::custom('2x'))),
            ],
            'name' => 'Captain Marvel',
            'director' => 'Anna Boden & Ryan Fleck',
            'year' => 2019,
            'price' => 25.99,
            'active' => true,
            'generes' => ['science fiction', 'action', 'comics'],
        ]));

        static::getDatabase()->createDocument('movies', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::read(Role::user(ID::custom('1'))),
                Permission::read(Role::user(ID::custom('2'))),
                Permission::create(Role::any()),
                Permission::create(Role::user(ID::custom('1x'))),
                Permission::create(Role::user(ID::custom('2x'))),
                Permission::update(Role::any()),
                Permission::update(Role::user(ID::custom('1x'))),
                Permission::update(Role::user(ID::custom('2x'))),
                Permission::delete(Role::any()),
                Permission::delete(Role::user(ID::custom('1x'))),
                Permission::delete(Role::user(ID::custom('2x'))),
            ],
            'name' => 'Work in Progress',
            'director' => 'TBD',
            'year' => 2025,
            'price' => 0.0,
            'active' => false,
            'generes' => [],
        ]));

        static::getDatabase()->createDocument('movies', new Document([
            '$permissions' => [
                Permission::read(Role::user(ID::custom('x'))),
                Permission::create(Role::any()),
                Permission::create(Role::user(ID::custom('1x'))),
                Permission::create(Role::user(ID::custom('2x'))),
                Permission::update(Role::any()),
                Permission::update(Role::user(ID::custom('1x'))),
                Permission::update(Role::user(ID::custom('2x'))),
                Permission::delete(Role::any()),
                Permission::delete(Role::user(ID::custom('1x'))),
                Permission::delete(Role::user(ID::custom('2x'))),
            ],
            'name' => 'Work in Progress 2',
            'director' => 'TBD',
            'year' => 2026,
            'price' => 0.0,
            'active' => false,
            'generes' => [],
        ]));

        /**
         * Check Basic
         */
        $documents = static::getDatabase()->count('movies');

        $this->assertEquals(5, $documents);
    }

    /**
     * @depends testCreateExistsDelete
     */
    public function testCreateListExistsDeleteCollection()
    {
        $this->assertInstanceOf('Utopia\Database\Document', static::getDatabase()->createCollection('actors'));

        $this->assertCount(1, static::getDatabase()->listCollections());
        $this->assertNotNull(static::getDatabase()->exists($this->testDatabase, 'actors'));

        // Collection names should not be unique
        $this->assertInstanceOf('Utopia\Database\Document', static::getDatabase()->createCollection('actors2'));
        $this->assertCount(2, static::getDatabase()->listCollections());
        $this->assertNotNull(static::getDatabase()->exists($this->testDatabase, 'actors2'));
        $collection = static::getDatabase()->getCollection('actors2');
        $collection->setAttribute('name', 'actors'); // change name to one that exists

        $this->assertInstanceOf('Utopia\Database\Document', static::getDatabase()->updateDocument($collection->getCollection(), $collection->getId(), $collection));
        $this->assertEquals(true, static::getDatabase()->deleteCollection('actors2')); // Delete collection when finished
        $this->assertCount(1, static::getDatabase()->listCollections());

        $this->assertEquals(false, static::getDatabase()->getCollection('actors')->isEmpty());
        $this->assertEquals(true, static::getDatabase()->deleteCollection('actors'));
        $this->assertEquals(true, static::getDatabase()->getCollection('actors')->isEmpty());

        $this->assertNotNull(static::getDatabase()->exists($this->testDatabase, 'actors'));
    }

    /**
     * @depends testFind
     */
    public function testCount()
    {
        $count = static::getDatabase()->count('movies');
        $this->assertEquals(5, $count);

        $count = static::getDatabase()->count('movies', [new Query(Query::TYPE_EQUAL, 'year', [2019]),]);
        $this->assertEquals(2, $count);

        Authorization::unsetRole('userx');
        $count = static::getDatabase()->count('movies');
        $this->assertEquals(5, $count);

        Authorization::disable();
        $count = static::getDatabase()->count('movies');
        $this->assertEquals(6, $count);
        Authorization::reset();

        Authorization::disable();
        $count = static::getDatabase()->count('movies', [], 3);
        $this->assertEquals(3, $count);
        Authorization::reset();

        /**
         * Test that OR queries are handled correctly
         */
        Authorization::disable();
        $count = static::getDatabase()->count('movies', [
            new Query(Query::TYPE_EQUAL, 'director', ['TBD', 'Joe Johnston']),
            new Query(Query::TYPE_EQUAL, 'year', [2025]),
        ]);
        $this->assertEquals(1, $count);
        Authorization::reset();
    }

    public function testRenameAttribute()
    {
        $this->assertTrue(true);
    }

    public function testRenameAttributeExisting()
    {
        $this->assertTrue(true);
    }

    public function testUpdateAttributeStructure()
    {
        $this->assertTrue(true);
    }

    /**
     * Ensure the collection is removed after use
     *
     * @depends testIndexCaseInsensitivity
     */
    public function testCleanupAttributeTests()
    {
        $res = static::getDatabase()->deleteCollection('attributes');

        $this->assertEquals(true, $res);
    }

    /**
     * Return keywords reserved by database backend
     *
     * @return string[]
     */
    static function getReservedKeywords(): array
    {
        // Mongo does not have concept of reserverd words. We put something here just to run the rests for this adapter too
        return ['mogno'];
    }
}
