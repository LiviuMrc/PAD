// proxy: reverse proxy-ul din fața nodurilor DW (smart proxy, cache în Redis, load balancing).
//
//	go run ./webproxy/proxy -backends http://127.0.0.1:9101,http://127.0.0.1:9102
package main

import (
	"context"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"lab2/common"
	"lab2/webproxy/internal/proxy"
)

func main() {
	log := common.Setup("proxy")
	id := flag.String("id", common.Env("PROXY_ID", "proxy1"), "numele instanței proxy")
	addr := flag.String("addr", common.Env("PROXY_ADDR", "127.0.0.1:9100"), "adresa HTTP")
	backends := flag.String("backends", common.Env("DW_BACKENDS", "http://127.0.0.1:9101,http://127.0.0.1:9102,http://127.0.0.1:9103"), "nodurile DW, separate prin virgulă")
	algorithm := flag.String("lb", common.Env("LB_ALGORITHM", "round-robin"), "round-robin sau least-connections")
	redisAddr := flag.String("redis", common.Env("REDIS_ADDR", "127.0.0.1:16379"), "adresa Redis")
	ttl := flag.Duration("cache-ttl", time.Duration(common.EnvInt("CACHE_TTL_SEC", 60))*time.Second, "TTL maxim în cache")
	health := flag.Duration("health-interval", time.Duration(common.EnvInt("HEALTH_INTERVAL_MS", 2000))*time.Millisecond, "intervalul health check-urilor")
	flag.Parse()
	log = log.With("proxy", *id)

	rdb := redis.NewClient(&redis.Options{Addr: *redisAddr})
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	err := rdb.Ping(pctx).Err()
	cancel()
	if err != nil {
		log.Error("nu mă pot conecta la Redis", "event", "proxy.start", "result", "failed", "error", err.Error())
		os.Exit(1)
	}

	p, err := proxy.New(proxy.Config{ID: *id, Backends: strings.Split(*backends, ","), Algorithm: *algorithm,
		CacheTTL: *ttl, HealthInterval: *health, Redis: rdb}, log)
	if err != nil {
		log.Error("configurare invalidă", "event", "proxy.start", "result", "failed", "error", err.Error())
		os.Exit(1)
	}
	go p.Run(ctx)
	hs := p.Server(*addr)
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		hs.Shutdown(sctx)
	}()
	log.Info("proxy pornit", "event", "proxy.started", "addr", *addr, "backends", *backends, "lb", *algorithm, "redis", *redisAddr)
	if err := hs.ListenAndServe(); err != http.ErrServerClosed {
		log.Error("server oprit cu eroare", "event", "proxy.serve", "error", err.Error())
		os.Exit(1)
	}
	rdb.Close()
	log.Info("proxy oprit", "event", "proxy.stopped")
}
