// Package envflag reads defaults for command-line flags from environment
// variables, so the same binary can be configured by flags locally and by
// environment in Docker Compose.
package envflag

import (
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"
)

// String returns the value of the environment variable name, or def.
func String(name, def string) string {
	if v, ok := os.LookupEnv(name); ok && v != "" {
		return v
	}
	return def
}

// Duration parses the environment variable name as a time.Duration, or
// returns def. It exits the program on a malformed value.
func Duration(name string, def time.Duration) time.Duration {
	v, ok := os.LookupEnv(name)
	if !ok || v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid %s=%q: %v\n", name, v, err)
		os.Exit(2)
	}
	return d
}

// Logger returns a text logger at the level named by level
// (debug, info, warn, error; default info).
func Logger(level string) *slog.Logger {
	var l slog.Level
	switch strings.ToLower(level) {
	case "debug":
		l = slog.LevelDebug
	case "warn":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default:
		l = slog.LevelInfo
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: l}))
}
