package glue_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/events"
)

func framed(version, compression byte, payload string) []byte {
	b := make([]byte, 18, 18+len(payload))
	b[0], b[1] = version, compression
	return append(b, payload...)
}

func TestGlueDecodeCodec(t *testing.T) {
	c := events.GlueDecodeCodec{}
	ctx := context.Background()

	got, err := c.Decode(ctx, "T", framed(0x03, 0x00, `{"a":1}`))
	require.NoError(t, err)
	assert.JSONEq(t, `{"a":1}`, string(got))

	for name, in := range map[string][]byte{
		"short":      {0x03},
		"version":    framed(0x02, 0x00, `{}`),
		"compressed": framed(0x03, 0x05, `{}`),
		"avro":       framed(0x03, 0x00, "\x02\x06abc"),
		"empty":      framed(0x03, 0x00, ""),
	} {
		_, err := c.Decode(ctx, "T", in)
		assert.Error(t, err, name)
	}
	_, _, err = c.Encode(ctx, "T", nil)
	assert.Error(t, err)
}
