package fixtures

import (
	"sync"
	"time"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/internal/core/port"
)

// FakeClock implements port.Clock with a fixed, advanceable time for testing.
type FakeClock struct {
	mu  sync.Mutex
	now time.Time
}

// NewFakeClock creates a FakeClock set to t.
func NewFakeClock(t time.Time) *FakeClock {
	return &FakeClock{now: t}
}

// Now returns the current fake time.
func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Advance moves the fake clock forward by d.
func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// Set sets the fake clock to t.
func (c *FakeClock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = t
}

// Ensure FakeClock satisfies port.Clock at compile time.
var _ port.Clock = (*FakeClock)(nil)
