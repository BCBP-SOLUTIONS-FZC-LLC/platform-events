package domain

import "errors"

// Sentinel errors used across the platform-events library.
var (
	ErrEnvelopeIDRequired     = errors.New("events: envelope ID is required")
	ErrEnvelopeTypeRequired   = errors.New("events: envelope type is required")
	ErrEnvelopeSourceRequired = errors.New("events: envelope source is required")
	ErrKeyTooShort            = errors.New("events: HMAC key must be at least 32 bytes")
	ErrInvalidSignature       = errors.New("events: invalid HMAC signature")
)

// ErrRetryable is a sentinel used to distinguish transient infrastructure
// errors (e.g. SNS throttling) from permanent failures. Publishers wrap
// retryable AWS API errors with RetryableError; OutboxService checks for it
// to avoid counting transient failures toward maxAttempts.
var ErrRetryable = errors.New("retryable error")

// RetryableError wraps a transient error so the outbox service can detect it
// without importing AWS SDK types in the core layer.
type RetryableError struct{ Cause error }

func (e *RetryableError) Error() string { return e.Cause.Error() }
func (e *RetryableError) Unwrap() error { return e.Cause }
func (e *RetryableError) Is(target error) bool {
	return target == ErrRetryable
}
