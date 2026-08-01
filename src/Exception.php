<?php

namespace Utopia\Mongo;

/**
 * Base MongoDB exception class with enhanced error categorization.
 *
 * Provides better error handling and categorization for MongoDB 8+ operations.
 */
class Exception extends \Exception
{
    /**
     * The connection was gone before the command left the client.
     *
     * MongoDB's own HostUnreachable. Carried by every throw raised out of
     * connection validation, which runs ahead of the send: the command was
     * never on the wire, so nothing was applied and a caller may safely
     * replay it. Distinguishing this from a post-send failure used to be
     * possible only by matching the message text, which callers cannot do
     * safely - a message quotes caller-chosen values, a code cannot be
     * spelled by a caller.
     */
    public const int HOST_UNREACHABLE = 6;

    protected array $errorLabels = [];
    protected ?array $writeErrors = null;
    protected ?array $writeConcernErrors = null;
    protected ?string $operationType = null;

    /**
     * @param string $message Error message
     * @param int $code Error code
     * @param \Throwable|null $previous Previous exception
     * @param array $errorLabels MongoDB error labels
     * @param array|null $writeErrors Write errors if applicable
     * @param array|null $writeConcernErrors Write concern errors if applicable
     * @param string|null $operationType Operation type that caused the error
     */
    public function __construct(
        string $message = '',
        int $code = 0,
        ?\Throwable $previous = null,
        array $errorLabels = [],
        ?array $writeErrors = null,
        ?array $writeConcernErrors = null,
        ?string $operationType = null
    ) {
        parent::__construct($message, $code, $previous);
        $this->errorLabels = $errorLabels;
        $this->writeErrors = $writeErrors;
        $this->writeConcernErrors = $writeConcernErrors;
        $this->operationType = $operationType;
    }

    /**
     * Check if this is a transient error that can be retried.
     *
     * @return bool
     */
    public function isTransientError(): bool
    {
        return in_array('TransientTransactionError', $this->errorLabels) ||
            in_array('UnknownTransactionCommitResult', $this->errorLabels) ||
            $this->isNetworkError() ||
            $this->isRetryableWrite();
    }

    /**
     * Check if this is a network-related error.
     *
     * @return bool
     */
    public function isNetworkError(): bool
    {
        $networkErrorCodes = [
            11600, 11601, 11602, // Socket errors
            89,    // NetworkTimeout
            9001,  // SocketException
            6,     // HostUnreachable
            7,     // HostNotFound
        ];

        return in_array($this->code, $networkErrorCodes);
    }

    /**
     * Check if this is a retryable write error.
     *
     * @return bool
     */
    public function isRetryableWrite(): bool
    {
        return in_array('RetryableWriteError', $this->errorLabels);
    }

    /**
     * Check if this is a duplicate key error.
     *
     * @return bool
     */
    public function isDuplicateKeyError(): bool
    {
        return $this->code === 11000 || $this->code === 11001;
    }

    /**
     * Check if this is a write concern error.
     *
     * @return bool
     */
    public function isWriteConcernError(): bool
    {
        return !empty($this->writeConcernErrors);
    }

    /**
     * Check if this is a timeout error.
     *
     * @return bool
     */
    /**
     * Whether the command was still unsent when this failure was raised, so
     * nothing was applied and the caller may replay it.
     */
    public function isUnsentError(): bool
    {
        return $this->code === self::HOST_UNREACHABLE;
    }

    public function isTimeoutError(): bool
    {
        $timeoutCodes = [
            50,    // MaxTimeMSExpired
            89,    // NetworkTimeout
            11601, // SocketTimeout
        ];

        return in_array($this->code, $timeoutCodes);
    }

    /**
     * Get error labels.
     *
     * @return array
     */
    public function getErrorLabels(): array
    {
        return $this->errorLabels;
    }

    /**
     * Get write errors.
     *
     * @return array|null
     */
    public function getWriteErrors(): ?array
    {
        return $this->writeErrors;
    }

    /**
     * Get write concern errors.
     *
     * @return array|null
     */
    public function getWriteConcernErrors(): ?array
    {
        return $this->writeConcernErrors;
    }

    /**
     * Get the operation type that caused this error.
     *
     * @return string|null
     */
    public function getOperationType(): ?string
    {
        return $this->operationType;
    }

    /**
     * Get a human-readable error category.
     *
     * @return string
     */
    public function getErrorCategory(): string
    {
        if ($this->isNetworkError()) {
            return 'Network Error';
        }

        if ($this->isTimeoutError()) {
            return 'Timeout Error';
        }

        if ($this->isDuplicateKeyError()) {
            return 'Duplicate Key Error';
        }

        if ($this->isWriteConcernError()) {
            return 'Write Concern Error';
        }

        if ($this->isTransientError()) {
            return 'Transient Error';
        }

        return 'MongoDB Error';
    }

    /**
     * Create exception from MongoDB error response.
     *
     * @param \stdClass $errorResponse MongoDB error response
     * @param string|null $operationType Operation type
     * @return self
     */
    public static function fromResponse(\stdClass $errorResponse, ?string $operationType = null): self
    {
        $message = $errorResponse->errmsg ?? 'Unknown MongoDB error';
        $code = $errorResponse->code ?? 0;
        $errorLabels = $errorResponse->errorLabels ?? [];
        $writeErrors = $errorResponse->writeErrors ?? null;
        $writeConcernErrors = $errorResponse->writeConcernErrors ?? null;

        return new self(
            $message,
            $code,
            null,
            $errorLabels,
            $writeErrors,
            $writeConcernErrors,
            $operationType
        );
    }
}

/**
 * Connection-related exception.
 */
class ConnectionException extends Exception
{
}

/**
 * Authentication-related exception.
 */
class AuthenticationException extends Exception
{
}

/**
 * Transaction-related exception.
 */
class TransactionException extends Exception
{
}

/**
 * Bulk write operation exception.
 */
class BulkWriteException extends Exception
{
    private array $result;

    public function __construct(string $message, array $result, int $code = 0, ?\Throwable $previous = null)
    {
        parent::__construct($message, $code, $previous);
        $this->result = $result;
    }

    /**
     * Get the bulk write result.
     *
     * @return array
     */
    public function getResult(): array
    {
        return $this->result;
    }
}
