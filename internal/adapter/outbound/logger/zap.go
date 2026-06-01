// Package logger provides a Zap-backed implementation of port.Logger.
package logger

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/port"
)

// ZapLogger wraps a *zap.Logger and implements port.Logger.
type ZapLogger struct {
	log *zap.Logger
}

// NewLogger creates a ZapLogger configured for the given environment.
// env "production"/"prod" uses a production JSON encoder; anything else uses development mode.
func NewLogger(env string) (*ZapLogger, error) {
	var cfg zap.Config
	if env == "production" || env == "prod" {
		cfg = zap.NewProductionConfig()
	} else {
		cfg = zap.NewDevelopmentConfig()
		cfg.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	}
	l, err := cfg.Build()
	if err != nil {
		return nil, err
	}
	return &ZapLogger{log: l}, nil
}

// NewLoggerFromZap wraps an existing *zap.Logger.
func NewLoggerFromZap(l *zap.Logger) *ZapLogger {
	return &ZapLogger{log: l}
}

// Debug logs at DEBUG level.
func (z *ZapLogger) Debug(msg string, fields map[string]interface{}) {
	z.log.Debug(msg, toZapFields(fields)...)
}

// Info logs at INFO level.
func (z *ZapLogger) Info(msg string, fields map[string]interface{}) {
	z.log.Info(msg, toZapFields(fields)...)
}

// Warn logs at WARN level.
func (z *ZapLogger) Warn(msg string, fields map[string]interface{}) {
	z.log.Warn(msg, toZapFields(fields)...)
}

// Error logs at ERROR level.
func (z *ZapLogger) Error(msg string, fields map[string]interface{}) {
	z.log.Error(msg, toZapFields(fields)...)
}

// Sync flushes any buffered log entries. Call on shutdown.
func (z *ZapLogger) Sync() error {
	return z.log.Sync()
}

// toZapFields converts a map of fields to []zap.Field.
func toZapFields(fields map[string]interface{}) []zap.Field {
	if len(fields) == 0 {
		return nil
	}
	zf := make([]zap.Field, 0, len(fields))
	for k, v := range fields {
		zf = append(zf, zap.Any(k, v))
	}
	return zf
}

// Ensure ZapLogger satisfies port.Logger at compile time.
var _ port.Logger = (*ZapLogger)(nil)
