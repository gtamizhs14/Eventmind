package logger

import (
	"os"
	"time"

	"github.com/rs/zerolog"
)

// New returns a configured zerolog.Logger.
// LOG_LEVEL env controls level (debug/info/warn/error), defaults to info.
func New() zerolog.Logger {
	level := zerolog.InfoLevel
	if l, err := zerolog.ParseLevel(os.Getenv("LOG_LEVEL")); err == nil {
		level = l
	}

	zerolog.SetGlobalLevel(level)
	zerolog.TimeFieldFormat = time.RFC3339

	var out = zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}
	if os.Getenv("ENV") == "production" {
		// JSON in prod, human-readable in dev
		return zerolog.New(os.Stdout).With().Timestamp().Logger()
	}
	return zerolog.New(out).With().Timestamp().Logger()
}
