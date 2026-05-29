// Package testenv loads environment variables from .env-example for tests.
package testenv

import (
	"os"
	"path/filepath"
	"runtime"

	"github.com/joho/godotenv"
)

// Load reads .env-example from the repository root into the process environment.
// Safe to call multiple times; ignores errors if the file does not exist
// (CI may set env vars directly).
func Load() {
	_, file, _, _ := runtime.Caller(0)
	// Navigate up from test/testenv/ to the repo root.
	root := filepath.Join(filepath.Dir(file), "..", "..")
	envFile := filepath.Join(root, ".env-example")
	if _, err := os.Stat(envFile); err == nil {
		_ = godotenv.Load(envFile)
	}
}
