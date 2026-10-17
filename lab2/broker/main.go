// Broker gRPC: aceeași arhitectură ca în Lab 1 (rutare pe topic, coadă în memorie
// per receiver), dar comunicarea se face prin RPC-uri definite în contracts/broker.proto.
package main

import (
	"flag"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"lab2/common"
	"lab2/gen/brokerpb"
)

func main() {
	addr := flag.String("addr", common.Env("BROKER_ADDR", "127.0.0.1:50061"), "adresa gRPC")
	flag.Parse()

	log := common.NewLogger("broker")
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Error("nu pot porni brokerul", "event", "broker.start", "result", "failed", "error", err.Error())
		os.Exit(1)
	}

	srv := grpc.NewServer()
	brokerpb.RegisterBrokerServer(srv, NewBroker(log))

	// health check standard gRPC: NOT_SERVING la pornire și oprire, SERVING când acceptă trafic
	hs := health.NewServer()
	healthpb.RegisterHealthServer(srv, hs)
	hs.SetServingStatus(brokerpb.Broker_ServiceDesc.ServiceName, healthpb.HealthCheckResponse_SERVING)

	go func() {
		stop := make(chan os.Signal, 1)
		signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
		<-stop
		hs.Shutdown() // toate serviciile devin NOT_SERVING
		srv.Stop()
	}()

	log.Info("broker pornit", "event", "broker.started", "addr", ln.Addr().String())
	if err := srv.Serve(ln); err != nil {
		log.Error("server oprit cu eroare", "event", "broker.serve", "error", err.Error())
	}
	log.Info("broker oprit", "event", "broker.stopped")
}
