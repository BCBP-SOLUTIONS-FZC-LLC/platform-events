// Package fixtures provides shared test helpers.
package fixtures

import (
	"sync"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/internal/core/port"
)

// LogEntry represents a single log call captured by MockLogger.
type LogEntry struct {
	Level   string
	Message string
	Fields  map[string]interface{}
}

// MockLogger implements port.Logger and records all log calls for assertions.
type MockLogger struct {
	mu      sync.Mutex
	entries []LogEntry
}

// Debug records a DEBUG level log entry.
func (l *MockLogger) Debug(msg string, fields map[string]interface{}) {
	l.record("DEBUG", msg, fields)
}

// Info records an INFO level log entry.
func (l *MockLogger) Info(msg string, fields map[string]interface{}) {
	l.record("INFO", msg, fields)
}

// Warn records a WARN level log entry.
func (l *MockLogger) Warn(msg string, fields map[string]interface{}) {
	l.record("WARN", msg, fields)
}

// Error records an ERROR level log entry.
func (l *MockLogger) Error(msg string, fields map[string]interface{}) {
	l.record("ERROR", msg, fields)
}

func (l *MockLogger) record(level, msg string, fields map[string]interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	cp := make(map[string]interface{}, len(fields))
	for k, v := range fields {
		cp[k] = v
	}
	l.entries = append(l.entries, LogEntry{Level: level, Message: msg, Fields: cp})
}

// Entries returns a copy of all recorded log entries.
func (l *MockLogger) Entries() []LogEntry {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]LogEntry, len(l.entries))
	copy(out, l.entries)
	return out
}

// Reset clears all recorded log entries.
func (l *MockLogger) Reset() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = nil
}

// Ensure MockLogger satisfies port.Logger at compile time.
var _ port.Logger = (*MockLogger)(nil)
