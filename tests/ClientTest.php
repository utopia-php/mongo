<?php

namespace Utopia\Tests;

use MongoDB\BSON\Document;
use PHPUnit\Framework\TestCase;
use ReflectionMethod;
use ReflectionProperty;
use Swoole\Client as SwooleClient;
use Swoole\Coroutine\Client as CoroutineClient;
use Utopia\Mongo\Auth;
use Utopia\Mongo\Client;
use Utopia\Mongo\Exception;

trait TransportDouble
{
    public bool $open = true;
    public array $connects = [];
    public array $events = [];
    public array $receives = [];
    public array $sends = [];
    public array $writes = [];

    private function connectTransport(string $host, int $port, float $timeout, int $flags): bool
    {
        $this->events[] = ['connect', $host, $port, $timeout, $flags];
        $this->connects[] = [$host, $port, $timeout, $flags];
        $this->open = true;

        return true;
    }

    private function receiveTransport(): string|false
    {
        $this->events[] = ['receive'];
        $receive = array_shift($this->receives) ?? ['result' => '', 'error' => 0];
        $this->errCode = $receive['error'];

        return $receive['result'];
    }

    private function sendTransport(string $data): int|false
    {
        $this->events[] = ['send', $data];
        $this->writes[] = $data;
        $send = array_shift($this->sends) ?? ['result' => strlen($data), 'error' => 0];
        $this->errCode = $send['error'];

        return $send['result'];
    }
}

final class SyncTransportDouble extends SwooleClient
{
    use TransportDouble;

    public array $closes = [];

    public function __construct()
    {
    }

    public function close(bool $force = false): bool
    {
        $this->events[] = ['close', $force];
        $this->closes[] = [$force];
        $this->open = false;

        return true;
    }

    public function connect(string $host, int $port = 0, float $timeout = 0.5, int $sock_flag = 0): bool
    {
        return $this->connectTransport($host, $port, $timeout, $sock_flag);
    }

    public function isConnected(): bool
    {
        return $this->open;
    }

    public function recv(int $size = 65535, int $flag = 0): string|false
    {
        return $this->receiveTransport();
    }

    public function send(string $data, int $flag = 0): int|false
    {
        return $this->sendTransport($data);
    }
}

final class CoroutineTransportDouble extends CoroutineClient
{
    use TransportDouble;

    public int $closes = 0;

    public function __construct()
    {
    }

    public function close(): bool
    {
        $this->events[] = ['close'];
        $this->closes++;
        $this->open = false;

        return true;
    }

    public function connect(string $host, int $port = 0, float $timeout = 0.5, int $sock_flag = 0): bool
    {
        return $this->connectTransport($host, $port, $timeout, $sock_flag);
    }

    public function isConnected(): bool
    {
        return $this->open;
    }

    public function recv(float $timeout = 0): string|false
    {
        return $this->receiveTransport();
    }

    public function send(string $data, float $timeout = 0): int|false
    {
        return $this->sendTransport($data);
    }
}

final class AuthenticationDouble extends Auth
{
    public function __construct()
    {
    }

    public function continue($data): array
    {
        return [['ping' => 1], 'admin'];
    }

    public function start(): array
    {
        return [['ping' => 1], 'admin'];
    }
}

final class ReconnectingClient extends Client
{
    public SwooleClient|CoroutineClient $transport;

    public function connect(): self
    {
        $this->transport->connect('mongo', 27017, 30.0);
        $connection = new ReflectionProperty(Client::class, 'isConnected');
        $connection->setAccessible(true);
        $connection->setValue($this, true);

        return $this;
    }
}

final class ClientTest extends TestCase
{
    public function testConnectPassesConfiguredTimeout(): void
    {
        $transport = new SyncTransportDouble();
        $transport->open = false;
        $transport->receives = [
            ['result' => $this->frame(['ok' => 1.0]), 'error' => 0],
            ['result' => $this->frame(['ok' => 1.0]), 'error' => 0],
        ];
        $client = $this->client($transport, timeout: 7.5);
        $this->set($client, 'auth', new AuthenticationDouble());

        $client->connect();

        $this->assertSame([['mongo', 27017, 7.5, 0]], $transport->connects);
    }

    public function testSyncReceiveFailureHardClosesAndClearsState(): void
    {
        $transport = new SyncTransportDouble();
        $transport->receives = [['result' => false, 'error' => 11]];
        $client = $this->client($transport);
        $this->seedState($client);

        $exception = $this->receiveException($client);

        $this->assertSame(11601, $exception->getCode());
        $this->assertStringContainsString('errCode=11', $exception->getMessage());
        $this->assertSame([[true]], $transport->closes);
        $this->assertSame([], $transport->connects);
        $this->assertStateCleared($client);
    }

    public function testCoroutineReceiveFailureHardClosesAndClearsState(): void
    {
        $transport = new CoroutineTransportDouble();
        $transport->receives = [['result' => false, 'error' => 11]];
        $client = $this->client($transport);
        $this->seedState($client);

        $exception = $this->receiveException($client);

        $this->assertSame(11601, $exception->getCode());
        $this->assertStringContainsString('errCode=11', $exception->getMessage());
        $this->assertSame(1, $transport->closes);
        $this->assertSame([], $transport->connects);
        $this->assertStateCleared($client);
    }

    public function testTransientEmptyReceiveContinuesWithoutClosing(): void
    {
        $transport = new SyncTransportDouble();
        $transport->receives = [
            ['result' => '', 'error' => 0],
            ['result' => $this->frame(['ok' => 1.0]), 'error' => 0],
        ];
        $client = $this->client($transport);

        $result = $this->receive($client);

        $this->assertSame(1.0, $result->ok);
        $this->assertSame([], $transport->closes);
    }

    public function testErroredEmptyReceiveHardClosesTransport(): void
    {
        $transport = new SyncTransportDouble();
        $transport->receives = [['result' => '', 'error' => 11]];
        $client = $this->client($transport);
        $this->seedState($client);

        $exception = $this->receiveException($client);

        $this->assertSame(11601, $exception->getCode());
        $this->assertStringContainsString('errCode=11', $exception->getMessage());
        $this->assertSame([[true]], $transport->closes);
        $this->assertStateCleared($client);
    }

    public function testReceiveDeadlineHardClosesTransport(): void
    {
        $transport = new SyncTransportDouble();
        $client = $this->client($transport, timeout: 0.0001);

        $exception = $this->receiveException($client);

        $this->assertSame(11601, $exception->getCode());
        $this->assertSame([[true]], $transport->closes);
    }

    public function testSyncSendFailureHardClosesBeforeFreshFullMessageRetry(): void
    {
        $transport = new SyncTransportDouble();
        $transport->sends = [['result' => false, 'error' => 11]];
        $transport->receives = [['result' => $this->frame(['ok' => 1.0]), 'error' => 0]];
        $client = $this->reconnectingClient($transport);
        $this->seedState($client);

        $result = $client->send('complete-message');

        $this->assertSame(1.0, $result->ok);
        $this->assertSame(['complete-message', 'complete-message'], $transport->writes);
        $this->assertSame(
            ['send', 'close', 'connect', 'send', 'receive'],
            array_column($transport->events, 0)
        );
        $this->assertSame([[true]], $transport->closes);
        $this->assertContextCleared($client);
        $this->assertTrue($this->get($client, 'isConnected'));
    }

    public function testCoroutineSendFailureHardClosesBeforeFreshFullMessageRetry(): void
    {
        $transport = new CoroutineTransportDouble();
        $transport->sends = [['result' => false, 'error' => 11]];
        $transport->receives = [['result' => $this->frame(['ok' => 1.0]), 'error' => 0]];
        $client = $this->reconnectingClient($transport);

        $result = $client->send('complete-message');

        $this->assertSame(1.0, $result->ok);
        $this->assertSame(['complete-message', 'complete-message'], $transport->writes);
        $this->assertSame(
            ['send', 'close', 'connect', 'send', 'receive'],
            array_column($transport->events, 0)
        );
        $this->assertSame(1, $transport->closes);
    }

    public function testSyncShortSendHardClosesBeforeRetry(): void
    {
        $transport = new SyncTransportDouble();
        $transport->sends = [['result' => 4, 'error' => 0]];
        $transport->receives = [['result' => $this->frame(['ok' => 1.0]), 'error' => 0]];
        $client = $this->reconnectingClient($transport);

        $client->send('complete-message');

        $this->assertSame([[true]], $transport->closes);
        $this->assertSame(['complete-message', 'complete-message'], $transport->writes);
    }

    public function testCoroutineShortSendHardClosesBeforeRetry(): void
    {
        $transport = new CoroutineTransportDouble();
        $transport->sends = [['result' => 4, 'error' => 0]];
        $transport->receives = [['result' => $this->frame(['ok' => 1.0]), 'error' => 0]];
        $client = $this->reconnectingClient($transport);

        $client->send('complete-message');

        $this->assertSame(1, $transport->closes);
        $this->assertSame(['complete-message', 'complete-message'], $transport->writes);
    }

    public function testRetryFailureHardClosesReplacementTransport(): void
    {
        $transport = new SyncTransportDouble();
        $transport->sends = [
            ['result' => false, 'error' => 11],
            ['result' => false, 'error' => 0],
        ];
        $client = $this->reconnectingClient($transport);

        try {
            $client->send('complete-message');
            $this->fail('Expected send failure');
        } catch (Exception $exception) {
            $this->assertSame(11, $exception->getCode());
        }

        $this->assertSame([[true], [true]], $transport->closes);
    }

    public function testRetryShortSendHardClosesReplacementTransport(): void
    {
        $transport = new CoroutineTransportDouble();
        $transport->sends = [
            ['result' => false, 'error' => 11],
            ['result' => 4, 'error' => 0],
        ];
        $transport->receives = [['result' => $this->frame(['ok' => 1.0]), 'error' => 0]];
        $client = $this->reconnectingClient($transport);

        try {
            $client->send('complete-message');
            $this->fail('Expected send failure');
        } catch (Exception $exception) {
            $this->assertSame(11, $exception->getCode());
        }

        $this->assertSame(2, $transport->closes);
    }

    public function testImpossibleFrameLengthHardClosesTransport(): void
    {
        $transport = new SyncTransportDouble();
        $transport->receives = [['result' => pack('V', 20), 'error' => 0]];
        $client = $this->client($transport);

        $exception = $this->receiveException($client);

        $this->assertStringContainsString('Invalid response length', $exception->getMessage());
        $this->assertSame([[true]], $transport->closes);
    }

    public function testOverlongFrameHardClosesTransport(): void
    {
        $transport = new SyncTransportDouble();
        $transport->receives = [['result' => pack('V*', 21, 1, 0, 2013, 0) . "\0x", 'error' => 0]];
        $client = $this->client($transport);

        $exception = $this->receiveException($client);

        $this->assertStringContainsString('Response length mismatch', $exception->getMessage());
        $this->assertSame([[true]], $transport->closes);
    }

    public function testInvalidBsonHardClosesCoroutineTransport(): void
    {
        $transport = new CoroutineTransportDouble();
        $transport->receives = [['result' => pack('V*', 26, 1, 0, 2013, 0) . "\0abcde", 'error' => 0]];
        $client = $this->client($transport);

        $exception = $this->receiveException($client);

        $this->assertStringContainsString('BSON', $exception->getMessage());
        $this->assertSame(1, $transport->closes);
    }

    public function testDecodedMongoCommandErrorDoesNotCloseTransport(): void
    {
        $transport = new SyncTransportDouble();
        $transport->receives = [[
            'result' => $this->frame([
                'ok' => 0.0,
                'errmsg' => 'duplicate key',
                'code' => 11000,
                'codeName' => 'DuplicateKey',
            ]),
            'error' => 0,
        ]];
        $client = $this->client($transport);

        $exception = $this->receiveException($client);

        $this->assertSame(11000, $exception->getCode());
        $this->assertSame([], $transport->closes);
        $this->assertTrue($transport->open);
    }

    private function assertStateCleared(Client $client): void
    {
        $this->assertFalse($this->get($client, 'isConnected'));
        $this->assertContextCleared($client);
    }

    private function assertContextCleared(Client $client): void
    {
        $this->assertSame([], $this->get($client, 'sessions'));
        $this->assertNull($this->get($client, 'clusterTime'));
        $this->assertNull($this->get($client, 'operationTime'));
        $this->assertNull($this->get($client, 'replicaSet'));
    }

    private function client(SwooleClient|CoroutineClient $transport, float $timeout = 0.05): Client
    {
        $client = new Client('testing', 'mongo', 27017, 'root', 'example', timeout: $timeout);
        $this->set($client, 'client', $transport);

        return $client;
    }

    private function frame(array $document): string
    {
        $bson = (string) Document::fromPHP($document);

        return pack('V*', 21 + strlen($bson), 1, 0, 2013, 0) . "\0" . $bson;
    }

    private function get(Client $client, string $property): mixed
    {
        $reflection = new ReflectionProperty(Client::class, $property);
        $reflection->setAccessible(true);

        return $reflection->getValue($client);
    }

    private function receive(Client $client): mixed
    {
        $reflection = new ReflectionMethod(Client::class, 'receive');
        $reflection->setAccessible(true);

        return $reflection->invoke($client);
    }

    private function receiveException(Client $client): Exception
    {
        try {
            $this->receive($client);
            $this->fail('Expected receive failure');
        } catch (Exception $exception) {
            return $exception;
        }
    }

    private function reconnectingClient(SwooleClient|CoroutineClient $transport): ReconnectingClient
    {
        $client = new ReconnectingClient('testing', 'mongo', 27017, 'root', 'example');
        $client->transport = $transport;
        $this->set($client, 'client', $transport);

        return $client;
    }

    private function seedState(Client $client): void
    {
        $this->set($client, 'isConnected', true);
        $this->set($client, 'sessions', ['session' => ['id' => 'session']]);
        $this->set($client, 'clusterTime', (object) ['time' => 1]);
        $this->set($client, 'operationTime', (object) ['time' => 2]);
        $this->set($client, 'replicaSet', true);
    }

    private function set(Client $client, string $property, mixed $value): void
    {
        $reflection = new ReflectionProperty(Client::class, $property);
        $reflection->setAccessible(true);
        $reflection->setValue($client, $value);
    }
}
