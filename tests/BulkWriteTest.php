<?php

namespace Utopia\Mongo\Tests;

use Utopia\Mongo\Client;
use Utopia\Mongo\Exception;

class BulkWriteTest
{
    private Client $client;

    public function __construct()
    {
        $this->client = new Client(
            'test_db',
            'localhost',
            27017,
            'test_user',
            'test_password',
            true
        );
    }

    public function testBulkWrite()
    {
        try {
            // Connect to MongoDB
            $this->client->connect();

            $collection = 'users';

            // Example bulk write operations
            $operations = [
                // Insert operations
                [
                    'operationType' => 'insertOne',
                    'document' => [
                        'name' => 'John Doe',
                        'email' => 'john@example.com',
                        'age' => 30
                    ]
                ],
                [
                    'operationType' => 'insertOne',
                    'document' => [
                        'name' => 'Jane Smith',
                        'email' => 'jane@example.com',
                        'age' => 25
                    ]
                ],

                // Update operations
                [
                    'operationType' => 'updateOne',
                    'filter' => ['email' => 'john@example.com'],
                    'update' => ['$set' => ['age' => 31]],
                    'upsert' => false
                ],
                [
                    'operationType' => 'updateMany',
                    'filter' => ['age' => ['$lt' => 30]],
                    'update' => ['$set' => ['status' => 'young']],
                    'upsert' => false
                ],

                // Replace operation
                [
                    'operationType' => 'replaceOne',
                    'filter' => ['email' => 'jane@example.com'],
                    'replacement' => [
                        'name' => 'Jane Smith Updated',
                        'email' => 'jane.updated@example.com',
                        'age' => 26,
                        'status' => 'updated'
                    ],
                    'upsert' => false
                ],

                // Delete operations
                [
                    'operationType' => 'deleteOne',
                    'filter' => ['email' => 'nonexistent@example.com']
                ],
                [
                    'operationType' => 'deleteMany',
                    'filter' => ['status' => 'old']
                ]
            ];

            // Execute bulk write with ordered operations (default)
            $result = $this->client->bulkWrite($collection, $operations, [
                'ordered' => true
            ]);

            echo "Bulk write completed successfully!\n";
            echo "Result: " . json_encode($result, JSON_PRETTY_PRINT) . "\n";

        } catch (Exception $e) {
            echo "Error: " . $e->getMessage() . "\n";
        }
    }

    public function testUnorderedBulkWrite()
    {
        try {
            $collection = 'products';

            $operations = [
                [
                    'operationType' => 'insertOne',
                    'document' => [
                        'name' => 'Product 1',
                        'price' => 100,
                        'category' => 'electronics'
                    ]
                ],
                [
                    'operationType' => 'insertOne',
                    'document' => [
                        'name' => 'Product 2',
                        'price' => 200,
                        'category' => 'clothing'
                    ]
                ],
                [
                    'operationType' => 'updateMany',
                    'filter' => ['category' => 'electronics'],
                    'update' => ['$set' => ['discount' => 0.1]],
                    'upsert' => false
                ]
            ];

            // Execute bulk write with unordered operations
            $result = $this->client->bulkWrite($collection, $operations, [
                'ordered' => false
            ]);

            echo "Unordered bulk write completed successfully!\n";
            echo "Result: " . json_encode($result, JSON_PRETTY_PRINT) . "\n";

        } catch (Exception $e) {
            echo "Error: " . $e->getMessage() . "\n";
        }
    }
}

// Example usage
if (php_sapi_name() === 'cli') {
    $test = new BulkWriteTest();
    $test->testBulkWrite();
    $test->testUnorderedBulkWrite();
} 