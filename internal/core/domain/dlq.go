package domain

import (
	"errors"
	"strings"
)

// Sentinel error kinds for forwarding a message to a queue's dead-letter
// queue. Every error returned by the DLQ publisher is a *DLQError whose Kind
// is one of these, so callers branch with errors.Is.
//
// ErrDLQNotConfigured, ErrDLQInvalidRedrivePolicy and ErrDLQInvalidMessage are
// permanent: retrying without a configuration or code change cannot succeed.
// ErrDLQUnresolved and ErrDLQSendFailed may be either — the error additionally
// matches ErrRetryable when the underlying AWS failure is transient
// (throttling, service unavailable, network timeout).
var (
	ErrDLQNotConfigured        = errors.New("events: source queue has no RedrivePolicy")
	ErrDLQInvalidRedrivePolicy = errors.New("events: malformed RedrivePolicy")
	ErrDLQUnresolved           = errors.New("events: dead-letter queue could not be resolved")
	ErrDLQSendFailed           = errors.New("events: send to dead-letter queue failed")
	ErrDLQInvalidMessage       = errors.New("events: invalid dead-letter message")
)

// DLQError is returned by every failing DLQ publisher call. Kind is one of the
// ErrDLQ* sentinels; Cause, when non-nil, is the underlying error (an AWS API
// error, possibly wrapped in *RetryableError). Both are reachable through
// errors.Is / errors.As.
type DLQError struct {
	Kind        error
	SourceQueue string
	Cause       error
}

func (e *DLQError) Error() string {
	var b strings.Builder
	b.WriteString(e.Kind.Error())
	if e.SourceQueue != "" {
		b.WriteString(" (source queue ")
		b.WriteString(e.SourceQueue)
		b.WriteString(")")
	}
	if e.Cause != nil {
		b.WriteString(": ")
		b.WriteString(e.Cause.Error())
	}
	return b.String()
}

// Unwrap exposes both the kind sentinel and the cause chain to errors.Is/As.
func (e *DLQError) Unwrap() []error {
	if e.Cause == nil {
		return []error{e.Kind}
	}
	return []error{e.Kind, e.Cause}
}
