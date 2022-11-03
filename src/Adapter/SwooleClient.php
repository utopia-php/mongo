<?php

namespace Utopia\Mongo\Adapter;

use Swoole\Client;
use Utopia\Mongo\Adapter;

class SwooleClient extends Adapter
{

    public function isConnected(): bool
    {
        // TODO: Implement isConnected() method.
    }

    public function connect(string $host, int $port = null, float $timeout = null): bool
    {
        // TODO: Implement connect() method.
    }

    public function send(mixed $data): int|bool
    {
        // TODO: Implement send() method.
    }

    public function recv(int $timeout = null): string|bool
    {
        // TODO: Implement recv() method.
    }
}
