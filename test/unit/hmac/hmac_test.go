package hmac_test

import (
	"crypto/rand"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/events"
)

func key32() []byte {
	k := make([]byte, 32)
	_, _ = rand.Read(k)
	return k
}

func TestSign_ValidKey(t *testing.T) {
	key := key32()
	sig, err := events.Sign(key, []byte("hello world"))
	require.NoError(t, err)
	assert.NotEmpty(t, sig)
}

func TestSign_ShortKey(t *testing.T) {
	key := []byte("tooshort")
	_, err := events.Sign(key, []byte("hello world"))
	require.Error(t, err)
}

func TestVerify_Valid(t *testing.T) {
	key := key32()
	payload := []byte("test payload")
	sig, err := events.Sign(key, payload)
	require.NoError(t, err)
	assert.True(t, events.Verify(key, payload, sig))
}

func TestVerify_InvalidSignature(t *testing.T) {
	key := key32()
	payload := []byte("test payload")
	assert.False(t, events.Verify(key, payload, "invalidsignature"))
}

func TestVerify_WrongKey(t *testing.T) {
	key1 := key32()
	key2 := key32()
	payload := []byte("test payload")
	sig, err := events.Sign(key1, payload)
	require.NoError(t, err)
	assert.False(t, events.Verify(key2, payload, sig))
}

func TestVerify_WrongPayload(t *testing.T) {
	key := key32()
	payload := []byte("original payload")
	sig, err := events.Sign(key, payload)
	require.NoError(t, err)
	assert.False(t, events.Verify(key, []byte("tampered payload"), sig))
}

func TestVerify_ShortKey(t *testing.T) {
	assert.False(t, events.Verify([]byte("short"), []byte("payload"), "sig"))
}

func TestSignEnvelope_VerifyEnvelope(t *testing.T) {
	key := key32()
	env := events.NewEnvelope("test.event", "svc", json.RawMessage(`{"x":1}`),
		events.WithTenantID("acme"),
	)

	sig, err := events.SignEnvelope(key, env)
	require.NoError(t, err)
	assert.NotEmpty(t, sig)

	ok, err := events.VerifyEnvelope(key, env, sig)
	require.NoError(t, err)
	assert.True(t, ok)
}

func TestVerifyEnvelope_WrongSig(t *testing.T) {
	key := key32()
	env := events.NewEnvelope("test.event", "svc", json.RawMessage(`{}`))

	sig, err := events.SignEnvelope(key, env)
	require.NoError(t, err)

	// Tamper the key.
	wrongKey := key32()
	ok, err := events.VerifyEnvelope(wrongKey, env, sig)
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestSignEnvelope_ShortKey(t *testing.T) {
	key := []byte("tooshort") // < 32 bytes
	env := events.NewEnvelope("test.event", "svc", json.RawMessage(`{}`))
	_, err := events.SignEnvelope(key, env)
	require.Error(t, err)
}

func TestVerifyEnvelope_ShortKey(t *testing.T) {
	key := []byte("tooshort") // < 32 bytes
	env := events.NewEnvelope("test.event", "svc", json.RawMessage(`{}`))
	ok, err := events.VerifyEnvelope(key, env, "deadbeef")
	// Short key: VerifyEnvelope now returns (false, error) so callers can
	// distinguish a configuration error from a legitimate signature mismatch.
	require.Error(t, err, "short key must return an error, not a silent false")
	assert.False(t, ok)
}

func TestVerify_EmptySig(t *testing.T) {
	key := key32()
	payload := []byte("test")
	assert.False(t, events.Verify(key, payload, ""))
}

func TestVerify_NonHexSig(t *testing.T) {
	key := key32()
	payload := []byte("test")
	assert.False(t, events.Verify(key, payload, "notahexstring!!!"))
}

func TestSign_ReturnsHexString(t *testing.T) {
	key := key32()
	sig, err := events.Sign(key, []byte("hello"))
	require.NoError(t, err)
	assert.Regexp(t, "^[0-9a-f]+$", sig)
}

func TestSignEnvelope_InvalidJSONPayload_Error(t *testing.T) {
	key := key32()
	// json.RawMessage with non-JSON bytes causes json.Marshal to return an error
	// when encoding the hmacEnvelope (compact validates the bytes).
	env := events.NewEnvelope("test.event", "svc", json.RawMessage(`invalid-json`))
	_, err := events.SignEnvelope(key, env)
	require.Error(t, err, "invalid JSON payload must cause a marshal error")
	assert.Contains(t, err.Error(), "SignEnvelope marshal failed")
}

func TestVerifyEnvelope_InvalidJSONPayload_Error(t *testing.T) {
	key := key32()
	env := events.NewEnvelope("test.event", "svc", json.RawMessage(`invalid-json`))
	ok, err := events.VerifyEnvelope(key, env, "anysignature")
	require.Error(t, err, "invalid JSON payload must cause a marshal error")
	assert.Contains(t, err.Error(), "VerifyEnvelope marshal failed")
	assert.False(t, ok)
}
