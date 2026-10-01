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
	return FormatBatchError("events", len(e.Failures), first(e.Failures))
}

func first(fs []BatchFailure) *BatchFailure {
	if len(fs) == 0 {
		return nil
	}
	return &fs[0]
}

// FormatBatchError renders a batch error message with the first failure, so
// a log line shows why the batch failed — not only how many messages did.
func FormatBatchError(prefix string, n int, f *BatchFailure) string {
	if f == nil {
		return fmt.Sprintf("%s: %d message(s) failed in batch", prefix, n)
	}
	return fmt.Sprintf("%s: %d message(s) failed in batch (first: %s %s: %s)", prefix, n, f.ID, f.Code, f.Message)
}
