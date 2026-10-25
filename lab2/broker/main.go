// Broker gRPC: aceeași arhitectură ca în Lab 1 (rutare pe topic, coadă per receiver),
// dar comunicarea se face prin RPC-uri definite în contracts/broker.proto.
// Logica e în internal/broker. Aici se citește doar configurarea (.env, variabile, flag-uri).
package main

import (
	"flag"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"lab2/common"
	"lab2/internal/broker"
)

func main() {
	log := common.Setup("broker")
	addr := flag.String("addr", common.Env("BROKER_ADDR", "127.0.0.1:50061"), "adresa gRPC")
	dataDir := flag.String("data", common.EnvAllowEmpty("BROKER_DATA_DIR", "data"), "director pentru jurnal (gol = fără persistență)")
	fsync := flag.Bool("fsync", common.EnvBool("BROKER_FSYNC", true), "fsync după fiecare scriere în jurnal")
	ttl := flag.Duration("ttl", time.Duration(common.EnvInt("RECEIVER_TTL_SEC", 600))*time.Second, "după cât timp offline e uitat un receiver (0 = niciodată)")
	maxQueue := flag.Int("max-queue", common.EnvInt("MAX_QUEUE", 1000), "mesaje neconfirmate per receiver")
	cert := flag.String("tls-cert", common.Env("BROKER_TLS_CERT", ""), "certificat TLS (PEM)")
	key := flag.String("tls-key", common.Env("BROKER_TLS_KEY", ""), "cheia TLS (PEM)")
	drain := flag.Duration("drain", time.Duration(common.EnvInt("SHUTDOWN_DRAIN_MS", 200))*time.Millisecond, "cât timp e NOT_SERVING înainte de oprire")
	flag.Parse()

	srv, err := broker.NewServer(broker.ServerConfig{
		Broker:     broker.Config{MaxQueue: *maxQueue, ReceiverTTL: *ttl},
		DataDir:    *dataDir,
		Fsync:      *fsync,
		Token:      common.Env("BROKER_TOKEN", ""), // doar din mediu, ca să nu apară în lista de procese
		TLSCert:    *cert,
		TLSKey:     *key,
		DrainDelay: *drain,
	}, log)
	if err != nil {
		log.Error("nu pot porni brokerul", "event", "broker.start", "result", "failed", "error", err.Error())
		os.Exit(1)
	}
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Error("nu pot porni brokerul", "event", "broker.start", "result", "failed", "error", err.Error())
		os.Exit(1)
	}

	go func() {
		stop := make(chan os.Signal, 1)
		signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
		sig := <-stop
		log.Info("semnal primit", "event", "broker.signal", "signal", sig.String())
		srv.Shutdown()
	}()

	if err := srv.Serve(ln); err != nil {
		log.Error("server oprit cu eroare", "event", "broker.serve", "error", err.Error())
		os.Exit(1)
	}
	log.Info("broker oprit", "event", "broker.stopped")
}
