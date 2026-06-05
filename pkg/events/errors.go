package events

import "github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/domain"

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
