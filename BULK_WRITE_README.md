# MongoDB Bulk Write Support

This document describes the new `bulkWrite` function added to the Utopia MongoDB client, which allows you to perform multiple write operations in a single request.

## Overview

The `bulkWrite` function supports the following operation types:
- `insertOne` - Insert a single document
- `updateOne` - Update a single document
- `updateMany` - Update multiple documents
- `replaceOne` - Replace a single document
- `deleteOne` - Delete a single document
- `deleteMany` - Delete multiple documents

## Function Signature

```php
public function bulkWrite(string $collection, array $operations, array $options = []): stdClass
```

### Parameters

- `$collection` (string): The name of the collection to perform operations on
- `$operations` (array): Array of operations to perform
- `$options` (array): Optional parameters for the bulk write operation

### Options

- `ordered` (boolean): Whether operations should be executed in order. Default: `true`

## Usage Examples

### Basic Bulk Write

```php
$operations = [
    [
        'operationType' => 'insertOne',
        'document' => [
            'name' => 'John Doe',
            'email' => 'john@example.com',
            'age' => 30
        ]
    ],
    [
        'operationType' => 'updateOne',
        'filter' => ['email' => 'john@example.com'],
        'update' => ['$set' => ['age' => 31]],
        'upsert' => false
    ],
    [
        'operationType' => 'deleteOne',
        'filter' => ['email' => 'old@example.com']
    ]
];

$result = $client->bulkWrite('users', $operations);
```

### Insert Operations

```php
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
    ]
];
```

### Update Operations

```php
$operations = [
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
    ]
];
```

### Replace Operations

```php
$operations = [
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
    ]
];
```

### Delete Operations

```php
$operations = [
    [
        'operationType' => 'deleteOne',
        'filter' => ['email' => 'john@example.com']
    ],
    [
        'operationType' => 'deleteMany',
        'filter' => ['status' => 'inactive']
    ]
];
```

### Ordered vs Unordered Operations

#### Ordered Operations (Default)
Operations are executed in the order they appear in the array. If an operation fails, subsequent operations are not executed.

```php
$result = $client->bulkWrite('users', $operations, [
    'ordered' => true
]);
```

#### Unordered Operations
Operations are executed in parallel. If one operation fails, others may still succeed.

```php
$result = $client->bulkWrite('users', $operations, [
    'ordered' => false
]);
```

## Error Handling

The function throws `Utopia\Mongo\Exception` for various error conditions:

- Missing `operationType` in operation
- Missing required fields for specific operations
- Unsupported operation types
- MongoDB server errors

## Operation Types Reference

### insertOne
```php
[
    'operationType' => 'insertOne',
    'document' => [
        'field1' => 'value1',
        'field2' => 'value2'
    ]
]
```

### updateOne
```php
[
    'operationType' => 'updateOne',
    'filter' => ['field' => 'value'],
    'update' => ['$set' => ['field' => 'new_value']],
    'upsert' => false
]
```

### updateMany
```php
[
    'operationType' => 'updateMany',
    'filter' => ['field' => ['$lt' => 100]],
    'update' => ['$set' => ['status' => 'updated']],
    'upsert' => false
]
```

### replaceOne
```php
[
    'operationType' => 'replaceOne',
    'filter' => ['email' => 'user@example.com'],
    'replacement' => [
        'name' => 'New Name',
        'email' => 'new@example.com'
    ],
    'upsert' => false
]
```

### deleteOne
```php
[
    'operationType' => 'deleteOne',
    'filter' => ['email' => 'user@example.com']
]
```

### deleteMany
```php
[
    'operationType' => 'deleteMany',
    'filter' => ['status' => 'inactive']
]
```

## Performance Benefits

Using `bulkWrite` instead of individual operations provides several benefits:

1. **Reduced Network Overhead**: Multiple operations are sent in a single request
2. **Atomic Operations**: All operations succeed or fail together (when ordered)
3. **Better Performance**: Especially useful for large datasets
4. **Transaction Support**: Can be used within transactions

## Testing

Run the test file to see examples in action:

```bash
php tests/BulkWriteTest.php
```

## Notes

- The function automatically generates `_id` fields for insert operations if not provided
- Null values are automatically filtered out from documents
- The function follows the same patterns as existing methods in the client
- All operations are validated before being sent to MongoDB 