package port

import "time"

// Clock provides the current time. Injectable for deterministic testing.
type Clock interface {
	Now() time.Time
}

// RealClock returns real wall-clock time in UTC.
type RealClock struct{}

// Now returns the current UTC time.
func (RealClock) Now() time.Time { return time.Now().UTC() }
