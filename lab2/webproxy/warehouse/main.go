// warehouse: nodul informativ (DW). Pornește un server HTTP cu procesare concurentă a
// cererilor, cu datele în Cassandra (aceeași instanță pentru toate nodurile DW).
//
//	go run ./webproxy/warehouse -id dw1 -addr 127.0.0.1:9101
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

	"lab2/common"
	"lab2/webproxy/internal/dw"
	"lab2/webproxy/internal/store"
)

func main() {
	log := common.Setup("warehouse")
	id := flag.String("id", common.Env("DW_ID", "dw1"), "numele instanței DW (X-Served-By)")
	addr := flag.String("addr", common.Env("DW_ADDR", "127.0.0.1:9101"), "adresa HTTP")
	hosts := flag.String("cassandra", common.Env("CASSANDRA_HOSTS", "127.0.0.1:19042"), "adresa Cassandra")
	keyspace := flag.String("keyspace", common.Env("CASSANDRA_KEYSPACE", "lab2_dw"), "keyspace-ul Cassandra")
	maxAge := flag.Duration("max-age", time.Duration(common.EnvInt("CACHE_MAX_AGE_SEC", 30))*time.Second, "Cache-Control: max-age pentru GET")
	flag.Parse()
	log = log.With("dw", *id)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	st, err := store.Connect(ctx, store.Config{Hosts: strings.Split(*hosts, ","), Keyspace: *keyspace, DW: *id})
	if err != nil {
		log.Error("nu mă pot conecta la Cassandra", "event", "dw.start", "result", "failed", "error", err.Error())
		os.Exit(1)
	}
	defer st.Close()

	srv := dw.New(dw.Config{ID: *id, MaxAge: *maxAge}, st, log)
	hs := &http.Server{Addr: *addr, Handler: srv, ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 120 * time.Second}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		hs.Shutdown(sctx) // așteaptă cererile în curs
	}()
	log.Info("DW pornit", "event", "dw.started", "addr", *addr, "cassandra", *hosts, "keyspace", *keyspace)
	if err := hs.ListenAndServe(); err != http.ErrServerClosed {
		log.Error("server oprit cu eroare", "event", "dw.serve", "error", err.Error())
		os.Exit(1)
	}
	srv.Close()
	log.Info("DW oprit", "event", "dw.stopped")
}
