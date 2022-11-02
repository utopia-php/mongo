<?php

namespace Utopia\Mongo;

abstract class Adapter
{
    abstract public function isConnected(): bool;
    abstract public function connect(
        string $host,
        int $port = null,
        float $timeout = null
    ): bool;
    abstract public function send(mixed $data): int|bool;
    abstract public function recv(int $timeout = null): string|bool;
}
