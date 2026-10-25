// source: nod de stocare. Trimite datele lui la DW (PUT, prin proxy) și preia modificările
// prin pull (GET /update/employees/) și push (callback POST /push).
//
//	go run ./webproxy/source -id src1 -seed webproxy/data/src1.json -format xml -addr 127.0.0.1:9111
package main

import (
	"context"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"lab2/common"
	"lab2/webproxy/internal/model"
	"lab2/webproxy/internal/source"
)

func main() {
	log := common.Setup("source")
	id := flag.String("id", common.Env("SOURCE_ID", "src1"), "numele nodului de stocare")
	proxyURL := flag.String("proxy", common.Env("PROXY_URL", "http://127.0.0.1:9100"), "proxy-ul (toate cererile spre DW trec prin el)")
	addr := flag.String("addr", common.Env("SOURCE_ADDR", "127.0.0.1:9111"), "adresa callback-ului push și a datelor locale")
	data := flag.String("data", common.Env("SOURCE_DATA", ""), "fișierul local (implicit webproxy/var/<id>.json)")
	seed := flag.String("seed", common.Env("SOURCE_SEED", ""), "datele inițiale (JSON), dacă fișierul local nu există")
	format := flag.String("format", common.Env("SOURCE_FORMAT", "json"), "formatul datelor trimise: json sau xml")
	poll := flag.Duration("poll", time.Duration(common.EnvInt("SOURCE_POLL_MS", 2000))*time.Millisecond, "intervalul pull (0 = fără pull)")
	push := flag.Bool("push", common.EnvBool("SOURCE_PUSH", true), "abonare la notificări push")
	flag.Parse()
	log = log.With("source", *id)
	if *data == "" {
		*data = "webproxy/var/" + *id + ".json"
	}
	cfg := source.Config{ID: *id, ProxyURL: *proxyURL, DataFile: *data, SeedFile: *seed,
		Format: model.Format(*format), PollInterval: *poll}
	if *push {
		cfg.PushURL = "http://" + *addr + "/push"
	}
	n, err := source.New(cfg, log)
	if err != nil {
		log.Error("nu pot porni", "event", "source.start", "result", "failed", "error", err.Error())
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	hs := &http.Server{Addr: *addr, Handler: n.Handler(), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		hs.Shutdown(context.Background())
	}()
	go func() {
		if err := hs.ListenAndServe(); err != http.ErrServerClosed {
			log.Error("callback-ul push nu a pornit", "event", "source.serve", "error", err.Error())
			stop()
		}
	}()
	log.Info("nod de stocare pornit", "event", "source.started", "addr", *addr, "proxy", *proxyURL, "format", *format, "file", *data)
	if err := n.SyncAll(ctx); err != nil {
		if ctx.Err() == nil {
			log.Error("trimiterea datelor a eșuat", "event", "source.sync", "error", err.Error())
			os.Exit(1)
		}
		return
	}
	if err := n.Subscribe(ctx); err != nil && ctx.Err() == nil {
		log.Error("abonarea push a eșuat", "event", "source.subscribe", "error", err.Error())
	}
	n.RunPull(ctx)
	<-ctx.Done()
	log.Info("nod de stocare oprit", "event", "source.stopped")
}
