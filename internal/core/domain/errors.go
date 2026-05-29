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
