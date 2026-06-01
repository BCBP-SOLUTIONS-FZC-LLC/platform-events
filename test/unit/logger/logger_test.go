package logger_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/adapter/outbound/logger"
)

func TestNewLogger_Dev(t *testing.T) {
	l, err := logger.NewLogger("dev")
	require.NoError(t, err)
	assert.NotNil(t, l)
	l.Info("test info", map[string]interface{}{"key": "val"})
	l.Warn("test warn", nil)
	l.Error("test error", map[string]interface{}{"error": "oops"})
	l.Debug("test debug", map[string]interface{}{"n": 1})
}

func TestNewLogger_Development(t *testing.T) {
	l, err := logger.NewLogger("development")
	require.NoError(t, err)
	assert.NotNil(t, l)
}

func TestNewLogger_Local(t *testing.T) {
	l, err := logger.NewLogger("local")
	require.NoError(t, err)
	assert.NotNil(t, l)
}

func TestNewLogger_Prod(t *testing.T) {
	l, err := logger.NewLogger("production")
	require.NoError(t, err)
	assert.NotNil(t, l)
	l.Info("prod info", map[string]interface{}{"key": "val"})
}

func TestNewLogger_ProdShort(t *testing.T) {
	l, err := logger.NewLogger("prod")
	require.NoError(t, err)
	assert.NotNil(t, l)
}

func TestNewLoggerFromZap(t *testing.T) {
	z, err := zap.NewDevelopment()
	require.NoError(t, err)
	l := logger.NewLoggerFromZap(z)
	assert.NotNil(t, l)
	l.Info("from zap", map[string]interface{}{"k": "v"})
	l.Warn("warn", nil)
	l.Error("error", map[string]interface{}{"err": "x"})
	l.Debug("debug", nil)
}

func TestNewLoggerFromZap_NilFields(t *testing.T) {
	z, _ := zap.NewDevelopment()
	l := logger.NewLoggerFromZap(z)
	// All methods with nil fields should not panic.
	l.Info("no fields", nil)
	l.Warn("no fields", nil)
	l.Error("no fields", nil)
	l.Debug("no fields", nil)
}

func TestZapLogger_Sync(t *testing.T) {
	l, err := logger.NewLogger("dev")
	require.NoError(t, err)
	// Sync may return an error on stderr — just ensure no panic.
	_ = l.Sync()
}
