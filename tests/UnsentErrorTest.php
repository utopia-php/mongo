<?php

namespace Utopia\Tests;

use PHPUnit\Framework\TestCase;
use Utopia\Mongo\Exception;

/**
 * Connection validation runs ahead of the send, so every failure it raises
 * leaves the command unsent and nothing applied - the one class of failure a
 * caller may safely replay. That distinction was previously legible only in
 * the message text, which a caller cannot classify on safely, and a cloud
 * pool consequently surfaced a replayable refusal to the client as a 500.
 */
final class UnsentErrorTest extends TestCase
{
    public function testAnUnsentFailureIsDistinguishableByCode(): void
    {
        $unsent = new Exception('Client is not connected to MongoDB', Exception::HOST_UNREACHABLE);

        $this->assertTrue($unsent->isUnsentError());
        $this->assertFalse($unsent->isTimeoutError(), 'An unsent failure must not be confused with a post-send timeout');
    }

    public function testAPostSendTimeoutIsNotReportedAsUnsent(): void
    {
        $timeout = new Exception('Receive timeout: no data received within reasonable time', 11601);

        $this->assertTrue($timeout->isTimeoutError());
        $this->assertFalse(
            $timeout->isUnsentError(),
            'The command was already on the wire, so its outcome is unknown and it must never be replayed blindly',
        );
    }

    public function testAnUncodedFailureIsNotAssumedUnsent(): void
    {
        $this->assertFalse(new Exception('something went wrong')->isUnsentError());
    }
}
