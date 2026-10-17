package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"lab2/common"
	"lab2/gen/brokerpb"
)

// Teste de integrare: server gRPC real pe localhost, clienți gRPC reali.

func startBroker(t *testing.T) (*Broker, *grpc.ClientConn) {
	t.Helper()
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))
	if testing.Verbose() {
		log = common.NewLogger("broker")
	}
	b := NewBroker(log)
	srv := grpc.NewServer()
	brokerpb.RegisterBrokerServer(srv, b)
	hs := health.NewServer()
	healthpb.RegisterHealthServer(srv, hs)
	hs.SetServingStatus(brokerpb.Broker_ServiceDesc.ServiceName, healthpb.HealthCheckResponse_SERVING)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve(ln)
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return b, conn
}

type sub struct {
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
	if _, err := stream.Header(); err != nil { // confirmarea abonării
		t.Fatalf("abonare eșuată: %v", err)
	}
	return &sub{stream: stream, cancel: cancel}
}

// recv citește următorul mesaj, cu deadline, ca testul să nu se blocheze.
func (s *sub) recv(t *testing.T) *brokerpb.Message {
	t.Helper()
	type res struct {
		m   *brokerpb.Message
		err error
	}
	ch := make(chan res, 1)
	go func() { m, err := s.stream.Recv(); ch <- res{m, err} }()
	select {
	case r := <-ch:
		if r.err != nil {
			t.Fatalf("Recv: %v", r.err)
		}
		return r.m
	case <-time.After(2 * time.Second):
		t.Fatal("niciun mesaj în 2s")
		return nil
	}
}

func publish(t *testing.T, conn *grpc.ClientConn, topic, payload string) *brokerpb.Message {
	t.Helper()
	m := common.NewMessage("", "sender-1", topic, payload)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	reply, err := brokerpb.NewBrokerClient(conn).Publish(ctx, &brokerpb.PublishRequest{Message: m})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if reply.CorrelationId != m.CorrelationId || reply.MessageId != m.MessageId {
		t.Fatalf("confirmare pentru alt mesaj: %+v", reply)
	}
	return m
}

func waitOnline(t *testing.T, b *Broker, want int) {
	t.Helper()
	for i := 0; i < 100; i++ {
		if b.Online() == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("brokerul nu a ajuns la %d receivere online (are %d)", want, b.Online())
}

func TestPublishReachesBothReceivers(t *testing.T) {
	_, conn := startBroker(t)
	r1 := subscribe(t, conn, "receiver-1", "news")
	r2 := subscribe(t, conn, "receiver-2", "news")

	sent := publish(t, conn, "news", "salut")

	for _, r := range []*sub{r1, r2} {
		got := r.recv(t)
		if got.MessageId != sent.MessageId || got.CorrelationId != sent.CorrelationId || got.Payload != "salut" {
			t.Fatalf("mesaj diferit: %+v", got)
		}
	}
}

func TestRoutingByTopic(t *testing.T) {
	_, conn := startBroker(t)
	news := subscribe(t, conn, "receiver-1", "news")
	alerts := subscribe(t, conn, "receiver-2", "alerts")

	publish(t, conn, "alerts", "doar pentru alerts")
	marker := publish(t, conn, "news", "doar pentru news")

	if got := alerts.recv(t); got.Topic != "alerts" {
		t.Fatalf("topic greșit: %s", got.Topic)
	}
	// primul mesaj primit de receiver-1 trebuie să fie cel de pe news, nu cel de pe alerts
	if got := news.recv(t); got.MessageId != marker.MessageId {
		t.Fatalf("receiver-1 (news) a primit mesajul de pe %s", got.Topic)
	}
}

// Scenariu de eșec: un receiver cade, între timp se publică mesaje, iar la
// reconectare le primește pe toate, în ordine.
func TestOfflineReceiverGetsQueuedMessagesOnReconnect(t *testing.T) {
	b, conn := startBroker(t)
	r1 := subscribe(t, conn, "receiver-1", "news")
	waitOnline(t, b, 1)

	r1.cancel() // injectarea eșecului: receiverul închide conexiunea
	waitOnline(t, b, 0)

	var sent []*brokerpb.Message
	for i := 1; i <= 3; i++ {
		sent = append(sent, publish(t, conn, "news", fmt.Sprintf("mesaj %d", i)))
	}

	again := subscribe(t, conn, "receiver-1", "news") // recuperarea: același id
	for i, want := range sent {
		if got := again.recv(t); got.MessageId != want.MessageId {
			t.Fatalf("mesajul %d: primit %q, așteptat %q", i+1, got.Payload, want.Payload)
		}
	}
}

func TestInvalidMessagesAreRejected(t *testing.T) {
	_, conn := startBroker(t)
	client := brokerpb.NewBrokerClient(conn)

	valid := func() *brokerpb.Message { return common.NewMessage("", "sender-1", "news", "x") }
	cases := map[string]*brokerpb.Message{
		"fără mesaj":        nil,
		"schemaVersion 2.0": func() *brokerpb.Message { m := valid(); m.SchemaVersion = "2.0"; return m }(),
		"topic gol":         func() *brokerpb.Message { m := valid(); m.Topic = ""; return m }(),
		"topic cu spații":   func() *brokerpb.Message { m := valid(); m.Topic = "nu e valid"; return m }(),
		"dată non-UTC":      func() *brokerpb.Message { m := valid(); m.OccurredAt = "2026-10-17T13:00:00+03:00"; return m }(),
		"fără messageId":    func() *brokerpb.Message { m := valid(); m.MessageId = ""; return m }(),
		"payload prea mare": func() *brokerpb.Message { m := valid(); m.Payload = string(make([]byte, 5000)); return m }(),
	}
	for name, m := range cases {
		_, err := client.Publish(context.Background(), &brokerpb.PublishRequest{Message: m})
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s: cod %v, așteptat InvalidArgument", name, status.Code(err))
		} else {
			t.Logf("%s -> %s", name, status.Convert(err).Message())
		}
	}

	stream, _ := client.Subscribe(context.Background(), &brokerpb.SubscribeRequest{ReceiverId: "r 1", Topics: []string{"news"}})
	if _, err := stream.Recv(); status.Code(err) != codes.InvalidArgument {
		t.Errorf("Subscribe cu id invalid: cod %v, așteptat InvalidArgument", status.Code(err))
	}
}

func TestPublishRespectsDeadline(t *testing.T) {
	_, conn := startBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	time.Sleep(time.Millisecond)
	_, err := brokerpb.NewBrokerClient(conn).Publish(ctx, &brokerpb.PublishRequest{Message: common.NewMessage("", "s", "news", "x")})
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("cod %v, așteptat DeadlineExceeded", status.Code(err))
	}
}

// Procesare concurentă: mai mulți senderi publică în paralel, iar receiverul primește
// fiecare mesaj exact o dată.
func TestConcurrentPublishers(t *testing.T) {
	_, conn := startBroker(t)
	r := subscribe(t, conn, "receiver-1", "news")

	const senders, each = 10, 20
	var wg sync.WaitGroup
	for s := 0; s < senders; s++ {
		wg.Add(1)
		go func(s int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				publish(t, conn, "news", fmt.Sprintf("s%d-%d", s, i))
			}
		}(s)
	}
	wg.Wait()

	seen := map[string]bool{}
	for i := 0; i < senders*each; i++ {
		m := r.recv(t)
		if seen[m.MessageId] {
			t.Fatalf("mesaj duplicat: %s", m.Payload)
		}
		seen[m.MessageId] = true
	}
}

func TestSameReceiverIdTakesOver(t *testing.T) {
	_, conn := startBroker(t)
	old := subscribe(t, conn, "receiver-1", "news")
	newer := subscribe(t, conn, "receiver-1", "news")

	if _, err := old.stream.Recv(); status.Code(err) != codes.Canceled {
		t.Fatalf("sesiunea veche: cod %v, așteptat Canceled", status.Code(err))
	}
	sent := publish(t, conn, "news", "pentru sesiunea nouă")
	if got := newer.recv(t); got.MessageId != sent.MessageId {
		t.Fatal("sesiunea nouă nu a primit mesajul")
	}
}

func TestHealth(t *testing.T) {
	_, conn := startBroker(t)
	resp, err := healthpb.NewHealthClient(conn).Check(context.Background(),
		&healthpb.HealthCheckRequest{Service: brokerpb.Broker_ServiceDesc.ServiceName})
	if err != nil || resp.Status != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("health: %v %v", resp, err)
	}
}
