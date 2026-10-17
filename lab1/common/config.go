package common

import (
	"log/slog"
	"os"
	"strconv"
	"time"
)

// Env întoarce valoarea variabilei de mediu sau valoarea implicită.
func Env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// EnvInt este ca Env, dar pentru numere întregi.
func EnvInt(key string, def int) int {
	if n, err := strconv.Atoi(os.Getenv(key)); err == nil {
		return n
	}
	return def
}

// NewLogger creează un logger structurat (o linie JSON per eveniment) cu timestamp UTC
// și câmpul "component".
func NewLogger(component string) *slog.Logger {
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey {
				a.Value = slog.StringValue(a.Value.Time().UTC().Format(time.RFC3339Nano))
			}
			return a
		},
	})
	return slog.New(h).With("component", component)
}
