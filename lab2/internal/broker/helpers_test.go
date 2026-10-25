package broker

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	"lab2/common"
	"lab2/gen/brokerpb"
)

// Testele pornesc serverul real (NewServer, aceeași funcție ca broker/main.go) pe
// localhost și folosesc clienți gRPC reali, creați cu common.Dial ca sender/receiver.

// logBuf adună log-urile brokerului ca testele să poată verifica evenimentele.
type logBuf struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *logBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *logBuf) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

type env struct {
	srv  *Server
	b    *Broker
	addr string
	conn *grpc.ClientConn
	logs *logBuf
	done chan error
}

func start(t *testing.T, cfg ServerConfig) *env {
	t.Helper()
	return startOn(t, "127.0.0.1:0", cfg, common.ClientOptions{Token: cfg.Token})
}

func startOn(t *testing.T, addr string, cfg ServerConfig, client common.ClientOptions) *env {
	t.Helper()
	logs := &logBuf{}
	var w io.Writer = logs
	if testing.Verbose() {
		w = io.MultiWriter(logs, testWriter{t})
	}
	srv, err := NewServer(cfg, slog.New(slog.NewJSONHandler(w, nil)))
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	e := &env{srv: srv, b: srv.Broker, addr: ln.Addr().String(), logs: logs, done: make(chan error, 1)}
	go func() { e.done <- srv.Serve(ln) }()
	t.Cleanup(e.stop)

	e.conn = dial(t, e.addr, client)
	return e
}

// stop oprește serverul (o singură dată) și așteaptă să se închidă.
func (e *env) stop() {
	e.srv.Shutdown()
	<-e.done
	e.done <- nil // apelurile următoare nu se blochează
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Log(strings.TrimSpace(string(p)))
	return len(p), nil
}

func dial(t *testing.T, addr string, o common.ClientOptions) *grpc.ClientConn {
	t.Helper()
	conn, err := common.Dial(addr, o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

type sub struct {
	id     string
	stream grpc.ServerStreamingClient[brokerpb.Message]
	cancel context.CancelFunc
}

func subscribe(t *testing.T, conn *grpc.ClientConn, id string, topics ...string) *sub {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	stream, err := brokerpb.NewBrokerClient(conn).Subscribe(ctx, &brokerpb.SubscribeRequest{
		ReceiverId: id, Topics: topics, CorrelationId: common.NewID(),
	})
	if err != nil {
		t.Fatal(err)
	}
	md, err := stream.Header() // confirmarea abonării
	if err != nil || len(md.Get("subscribed")) == 0 {
		_, rerr := stream.Recv()
		t.Fatalf("abonare eșuată: %v %v", err, rerr)
	}
	return &sub{id: id, stream: stream, cancel: cancel}
}

// recv citește următorul mesaj, cu deadline, ca testul să nu se blocheze.
func (s *sub) recv(t *testing.T) *brokerpb.Message {
	t.Helper()
	m, err := s.tryRecv(2 * time.Second)
	if err != nil {
		t.Fatalf("%s: %v", s.id, err)
	}
	return m
}

var errNothing = context.DeadlineExceeded

// tryRecv întoarce errNothing dacă nu vine nimic în d.
func (s *sub) tryRecv(d time.Duration) (*brokerpb.Message, error) {
	type res struct {
		m   *brokerpb.Message
		err error
	}
	ch := make(chan res, 1)
	go func() { m, err := s.stream.Recv(); ch <- res{m, err} }()
	select {
	case r := <-ch:
		return r.m, r.err
	case <-time.After(d):
		return nil, errNothing
	}
}

// expectNothing verifică că nu mai vine niciun mesaj în d.
func (s *sub) expectNothing(t *testing.T, d time.Duration) {
	t.Helper()
	if m, err := s.tryRecv(d); err != errNothing {
		t.Fatalf("%s: nu trebuia să primească nimic, a primit %q (%v)", s.id, m.GetPayload(), err)
	}
}

func publish(t *testing.T, conn *grpc.ClientConn, topic, payload string) *brokerpb.Message {
	t.Helper()
	m := common.NewMessage("", "sender-1", topic, payload)
	reply := publishMsg(t, conn, m)
	if reply.Duplicate {
		t.Fatalf("mesaj nou raportat ca duplicat: %+v", reply)
	}
	return m
}

func publishMsg(t *testing.T, conn *grpc.ClientConn, m *brokerpb.Message) *brokerpb.PublishReply {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	reply, err := brokerpb.NewBrokerClient(conn).Publish(ctx, &brokerpb.PublishRequest{Message: m})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if reply.CorrelationId != m.CorrelationId || reply.MessageId != m.MessageId {
		t.Fatalf("confirmare pentru alt mesaj: %+v", reply)
	}
	return reply
}

func ack(t *testing.T, conn *grpc.ClientConn, receiverID string, m *brokerpb.Message) bool {
	t.Helper()
	reply, err := brokerpb.NewBrokerClient(conn).Ack(context.Background(),
		&brokerpb.AckRequest{ReceiverId: receiverID, MessageId: m.MessageId})
	if err != nil {
		t.Fatalf("Ack: %v", err)
	}
	return reply.Removed
}

// eventually reîncearcă cond până devine true (cel mult 2s).
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for i := 0; i < 200; i++ {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("nu s-a întâmplat în 2s: %s", what)
}

func waitOnline(t *testing.T, b *Broker, want int) {
	t.Helper()
	eventually(t, "receivere online", func() bool { return b.Online() == want })
}
