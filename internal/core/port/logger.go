// Package port defines the interfaces (ports) owned by the use-case layer.
// Implementations live in internal/adapter/outbound.
package port

// Logger is a structured logging port.
// The map-based signature is compatible with platform-gincommon's ZapLogger —
// consuming services can pass gincommon's ZapLogger directly without an adapter.
type Logger interface {
	Debug(msg string, fields map[string]interface{})
	Info(msg string, fields map[string]interface{})
	Warn(msg string, fields map[string]interface{})
	Error(msg string, fields map[string]interface{})
}
