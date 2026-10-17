// Broker: primește PUBLISH/SUBSCRIBE pe TCP, rutează după topic, păstrează o coadă
// în memorie per receiver și trimite topicul "broadcast" prin UDP multicast.
package main

import (
	"flag"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"lab1/common"
)

func main() {
	addr := flag.String("addr", common.Env("BROKER_ADDR", "127.0.0.1:7000"), "adresa TCP")
	healthAddr := flag.String("health", common.Env("HEALTH_ADDR", "127.0.0.1:7001"), "adresa HTTP pentru health check")
	mcastAddr := flag.String("multicast", common.Env("MULTICAST_ADDR", "239.0.0.1:9999"), "grupul UDP multicast")
	flag.Parse()

	log := common.NewLogger("broker")

	var mcast common.Transport
	if m, err := common.NewMulticastSender(*mcastAddr); err != nil {
		log.Warn("multicast indisponibil, topicul broadcast merge pe TCP", "event", "multicast.unavailable", "error", err.Error())
	} else {
		mcast = m
		log.Info("multicast pregătit", "event", "multicast.ready", "group", *mcastAddr)
	}

	b := NewBroker(log, mcast)
	if _, err := b.Listen(*addr); err != nil {
		log.Error("nu pot porni brokerul", "event", "broker.start", "result", "failed", "error", err.Error())
		os.Exit(1)
	}

	go func() {
		if err := http.ListenAndServe(*healthAddr, b.HealthHandler()); err != nil {
			log.Warn("health check indisponibil", "event", "health.start", "error", err.Error())
		}
	}()
	log.Info("health check pornit", "event", "health.started", "addr", *healthAddr)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	log.Info("broker oprit", "event", "broker.stopped")
	b.Close()
}
