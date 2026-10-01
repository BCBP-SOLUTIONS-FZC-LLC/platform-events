package domain

import "fmt"

// BatchFailure describes a single failed message within a PublishBatch call.
type BatchFailure struct {
	ID      string
	Code    string
	Message string
	// Retryable reports a transient failure (throttling, service-side error,
	// timeout) that says nothing about the message itself: the outbox retries
	// it without counting an attempt. Code "TransportError" is also treated as
	// retryable, for publishers that predate this field.
	Retryable bool
}

// BatchError collects per-message errors from a PublishBatch call.
// Returned when some messages in the batch fail while others succeed.
type BatchError struct {
	Failures []BatchFailure
}

func (e *BatchError) Error() string {
	return fmt.Sprintf("events: %d message(s) failed in batch", len(e.Failures))
}
