package test

import (
	"log/slog"
	"strings"
	"testing"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// An io.Writer implementation that provides access to the test logger
type tWriter struct {
	t *testing.T
}

// Write writes the provided byte array as text to the test logger
func (w *tWriter) Write(p []byte) (int, error) {
	w.t.Helper()
	w.t.Log(strings.TrimRight(string(p), "\n"))
	return len(p), nil
}

// NewTestLogger returns a service.Logger that writes to test output
func NewTestLogger(t *testing.T) *service.Logger {
	t.Helper()
	h := slog.NewTextHandler(&tWriter{t}, &slog.HandlerOptions{Level: slog.LevelDebug})
	return service.NewLoggerFromSlog(slog.New(h))
}
