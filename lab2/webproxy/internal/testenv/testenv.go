// Package testenv leagă testele de infrastructura reală (Cassandra și Redis din
// docker-compose.yml). Nu există implementări false: dacă serviciile nu rulează, testele
// care au nevoie de ele sunt sărite cu un mesaj clar (pornește-le cu scripts/infra-up.sh).
package testenv

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"lab2/webproxy/internal/store"
)

func CassandraHosts() []string {
	if h := os.Getenv("CASSANDRA_HOSTS"); h != "" {
		return []string{h}
	}
	return []string{"127.0.0.1:19042"}
}

func RedisAddr() string {
	if a := os.Getenv("REDIS_ADDR"); a != "" {
		return a
	}
	return "127.0.0.1:16379"
}

var (
	mu     sync.Mutex
	stores = map[string]*store.Store{}
)

// Store întoarce un store pe keyspace-ul dat (unul per pachet de teste), cu tabelele golite.
func Store(t testing.TB, keyspace, dw string) *store.Store {
	t.Helper()
	mu.Lock()
	defer mu.Unlock()
	key := keyspace + "/" + dw
	st := stores[key]
	if st == nil {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		var err error
		st, err = store.Connect(ctx, store.Config{Hosts: CassandraHosts(), Keyspace: keyspace, DW: dw, Timeout: 20 * time.Second})
		if err != nil {
			t.Skipf("Cassandra nu rulează (%v); pornește infrastructura cu webproxy/scripts/infra-up.sh", err)
		}
		stores[key] = st
	}
	return st
}

// Reset golește tabelele keyspace-ului de test.
func Reset(t testing.TB, st *store.Store) {
	t.Helper()
	if err := st.Truncate(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// Redis întoarce un client pe baza de date dată (una per pachet de teste), golită.
func Redis(t testing.TB, db int) *redis.Client {
	t.Helper()
	c := redis.NewClient(&redis.Options{Addr: RedisAddr(), DB: db})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := c.Ping(ctx).Err(); err != nil {
		c.Close()
		t.Skipf("Redis nu rulează (%v); pornește infrastructura cu webproxy/scripts/infra-up.sh", err)
	}
	if err := c.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}
