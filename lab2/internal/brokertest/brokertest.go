// Package brokertest pornește un broker real (broker.NewServer) pentru testele
// sender-ului și ale receiverului.
package brokertest

import (
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"

	"lab2/internal/broker"
)

type Broker struct {
	*broker.Server
	Addr string
	done chan error
	once sync.Once
}

// Start pornește brokerul pe addr ("127.0.0.1:0" = port liber) și îl oprește la finalul testului.
func Start(t *testing.T, addr string, cfg broker.ServerConfig) *Broker {
	t.Helper()
	srv, err := broker.NewServer(cfg, slog.New(slog.NewJSONHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	b := &Broker{Server: srv, Addr: ln.Addr().String(), done: make(chan error, 1)}
	go func() { b.done <- srv.Serve(ln) }()
	t.Cleanup(b.Stop)
	return b
}

// Stop oprește brokerul și așteaptă închiderea (o singură dată).
func (b *Broker) Stop() {
	b.once.Do(func() {
		b.Shutdown()
		<-b.done
	})
}
