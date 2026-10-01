// Package service contains the use-case layer implementations.
package service

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/internal/core/domain"
)

// Sign returns the hex-encoded HMAC-SHA256 signature of payload using key.
// Returns domain.ErrKeyTooShort if len(key) < 32.
func Sign(key, payload []byte) (string, error) {
	if len(key) < 32 {
		return "", domain.ErrKeyTooShort
	}
	mac := hmac.New(sha256.New, key)
	mac.Write(payload)
	return hex.EncodeToString(mac.Sum(nil)), nil
}

// Verify uses constant-time comparison to check sig against payload.
// Returns false if the key is too short, the signature is malformed hex, or
// the signature does not match. Never returns an error — callers should treat
// false as an authentication failure.
func Verify(key, payload []byte, sig string) bool {
	if len(key) < 32 {
		return false
	}
	sigBytes, err := hex.DecodeString(sig)
	if err != nil {
		return false
	}
	mac := hmac.New(sha256.New, key)
	mac.Write(payload)
	return hmac.Equal(sigBytes, mac.Sum(nil))
}
