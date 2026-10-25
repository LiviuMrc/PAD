package common

import (
	"bufio"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

// Env întoarce valoarea variabilei de mediu sau valoarea implicită.
func Env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// EnvAllowEmpty este ca Env, dar o variabilă setată explicit la "" întoarce "",
// nu valoarea implicită (de exemplu BROKER_DATA_DIR= dezactivează persistența).
func EnvAllowEmpty(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
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

// EnvBool este ca Env, dar pentru true/false (1/0).
func EnvBool(key string, def bool) bool {
	if b, err := strconv.ParseBool(os.Getenv(key)); err == nil {
		return b
	}
	return def
}

// LoadDotEnv citește fișierul .env (linii KEY=VALUE, # pentru comentarii) și setează
// variabilele care nu sunt deja în mediu: mediul real are prioritate față de fișier.
// Un fișier lipsă nu e o eroare. Întoarce cheile setate din fișier.
func LoadDotEnv(path string) ([]string, error) {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var set []string
	sc := bufio.NewScanner(f)
	for n := 1; sc.Scan(); n++ {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, val, ok := strings.Cut(strings.TrimPrefix(line, "export "), "=")
		key = strings.TrimSpace(key)
		if !ok || key == "" {
			return set, fmt.Errorf("%s:%d: lipsește KEY=VALUE", path, n)
		}
		val = strings.TrimSpace(val)
		if len(val) >= 2 && (val[0] == '"' || val[0] == '\'') && val[len(val)-1] == val[0] {
			val = val[1 : len(val)-1]
		}
		if _, exists := os.LookupEnv(key); exists {
			continue
		}
		if err := os.Setenv(key, val); err != nil {
			return set, err
		}
		set = append(set, key)
	}
	return set, sc.Err()
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

// Setup încarcă .env din directorul curent și creează loggerul componentei.
// Fiecare main o apelează înainte de a citi flag-urile, ca valorile implicite să vină din .env.
func Setup(component string) *slog.Logger {
	log := NewLogger(component)
	keys, err := LoadDotEnv(".env")
	if err != nil {
		log.Error(".env invalid", "event", "config", "result", "failed", "error", err.Error())
		os.Exit(1)
	}
	if len(keys) > 0 {
		log.Info("configurare din .env", "event", "config", "keys", keys)
	}
	return log
}
