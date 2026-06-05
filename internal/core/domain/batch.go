package domain

import "fmt"

// BatchFailure describes a single failed message within a PublishBatch call.
type BatchFailure struct {
	ID      string
	Code    string
	Message string
}

// BatchError collects per-message errors from a PublishBatch call.
// Returned when some messages in the batch fail while others succeed.
type BatchError struct {
	Failures []BatchFailure
}

func (e *BatchError) Error() string {
	return fmt.Sprintf("events: %d message(s) failed in batch", len(e.Failures))
}
