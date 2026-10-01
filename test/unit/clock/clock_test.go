package clock_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/internal/core/port"
)

func TestRealClock_Now(t *testing.T) {
	c := port.RealClock{}
	before := time.Now().UTC()
	got := c.Now()
	after := time.Now().UTC()
	assert.False(t, got.Before(before), "Now() should be >= before")
	assert.False(t, got.After(after), "Now() should be <= after")
}

func TestRealClock_IsUTC(t *testing.T) {
	c := port.RealClock{}
	got := c.Now()
	assert.Equal(t, time.UTC, got.Location())
}

func TestRealClock_MonotonicallyIncreasing(t *testing.T) {
	c := port.RealClock{}
	t1 := c.Now()
	time.Sleep(time.Millisecond)
	t2 := c.Now()
	assert.True(t, t2.After(t1) || t2.Equal(t1))
}
