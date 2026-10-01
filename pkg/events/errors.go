package events

import "github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/internal/core/domain"

// Sentinel errors for envelope validation. Use errors.Is to branch in consuming services.
var (
	ErrEnvelopeIDRequired     = domain.ErrEnvelopeIDRequired
	ErrEnvelopeTypeRequired   = domain.ErrEnvelopeTypeRequired
	ErrEnvelopeSourceRequired = domain.ErrEnvelopeSourceRequired
	ErrKeyTooShort            = domain.ErrKeyTooShort
	// ErrInvalidSignature is exported for completeness but is not currently returned
	// by any function in this library. Verify and VerifyEnvelope return bool, not error.
	// Kept for forward-compatibility if a future release adds a stricter Verify variant.
	ErrInvalidSignature = domain.ErrInvalidSignature
)

// ErrRetryable matches errors caused by a transient failure — AWS throttling,
// 5xx / service unavailable, network or timeout errors — returned (wrapped) by
// the SNS publisher and DLQPublisher. The outbox retries such failures without
// counting toward MaxAttempts. A custom Codec (or Publisher) marks its own
// transient errors by wrapping this sentinel with %w, e.g.
// fmt.Errorf("glue: %w: %v", events.ErrRetryable, err).
var ErrRetryable = domain.ErrRetryable

// DLQ error kinds. Every DLQPublisher error is a *DLQError matching exactly
// one of these via errors.Is. ErrDLQNotConfigured, ErrDLQInvalidRedrivePolicy
// and ErrDLQInvalidMessage are permanent; ErrDLQUnresolved and
// ErrDLQSendFailed additionally match ErrRetryable when the cause is transient.
var (
	ErrDLQNotConfigured        = domain.ErrDLQNotConfigured
	ErrDLQInvalidRedrivePolicy = domain.ErrDLQInvalidRedrivePolicy
	ErrDLQUnresolved           = domain.ErrDLQUnresolved
	ErrDLQSendFailed           = domain.ErrDLQSendFailed
	ErrDLQInvalidMessage       = domain.ErrDLQInvalidMessage
)

// DLQError is the error type returned by DLQPublisher. Kind is one of the
// ErrDLQ* sentinels; Cause is the underlying error, if any.
type DLQError = domain.DLQError
