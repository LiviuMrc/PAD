package main

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"lab2/common"
	"lab2/gen/brokerpb"
	"lab2/internal/broker"
	"lab2/internal/brokertest"
)

// Testele rulează receiverul (aceeași funcție run ca main) împotriva unui broker real.

type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) { s.mu.Lock(); defer s.mu.Unlock(); return s.b.Write(p) }
func (s *syncBuf) String() string              { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }

type harness struct {
	r    *receiver
	logs *syncBuf
	out  *syncBuf
	done chan error
	stop context.CancelFunc
}

func newReceiver(t *testing.T, addr, id string, retries int, interval time.Duration, topics ...string) *harness {
	t.Helper()
	conn, err := common.Dial(addr, common.ClientOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	h := &harness{logs: &syncBuf{}, out: &syncBuf{}, done: make(chan error, 1)}
	h.r = &receiver{
		conn: conn, client: brokerpb.NewBrokerClient(conn), req: &brokerpb.SubscribeRequest{ReceiverId: id, Topics: topics},
		maxRetries: retries, connectTimeout: interval, interval: interval, out: h.out,
		log: slog.New(slog.NewJSONHandler(h.logs, nil)).With("receiver", id),
	}
	return h
}

func (h *harness) start(t *testing.T) *harness {
	ctx, cancel := context.WithCancel(context.Background())
	h.stop = cancel
	t.Cleanup(cancel)
	go func() { h.done <- h.r.run(ctx) }()
	return h
}

func (h *harness) wait(t *testing.T, d time.Duration) error {
	t.Helper()
	select {
	case err := <-h.done:
		return err
	case <-time.After(d):
		t.Fatalf("receiverul nu s-a oprit în %v; log:\n%s", d, h.logs.String())
		return nil
	}
}

func (h *harness) waitFor(t *testing.T, what, substr string) {
	t.Helper()
	for i := 0; i < 300; i++ {
		if strings.Contains(h.logs.String()+h.out.String(), substr) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("lipsește %s (%q); log:\n%s", what, substr, h.logs.String())
}

func publish(t *testing.T, addr, topic, payload string) *brokerpb.Message {
	t.Helper()
	conn, _ := common.Dial(addr, common.ClientOptions{})
	defer conn.Close()
	m := common.NewMessage("", "sender-1", topic, payload)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := brokerpb.NewBrokerClient(conn).Publish(ctx, &brokerpb.PublishRequest{Message: m}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	return m
}

func freeAddr(t *testing.T) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

// #5: brokerul nu rulează. Receiverul reîncearcă exact de maxRetries ori, la interval, apoi renunță.
func TestGivesUpAfterMaxRetries(t *testing.T) {
	h := newReceiver(t, freeAddr(t), "r", 3, 50*time.Millisecond, "news")
	start := time.Now()
	err := h.start(t).wait(t, 3*time.Second)
	if !errors.Is(err, errGaveUp) {
		t.Fatalf("eroare %v, așteptat errGaveUp", err)
	}
	logs := h.logs.String()
	for _, a := range []string{`"attempt":1`, `"attempt":2`, `"attempt":3`} {
		if !strings.Contains(logs, a) {
			t.Fatalf("lipsește %s; log:\n%s", a, logs)
		}
	}
	if strings.Contains(logs, `"attempt":4`) || !strings.Contains(logs, `"result":"gave_up"`) {
		t.Fatalf("număr greșit de reîncercări; log:\n%s", logs)
	}
	if el := time.Since(start); el < 300*time.Millisecond { // 3 × (connectTimeout + interval)
		t.Fatalf("a renunțat după %v, sub 3 × interval", el)
	}
}

func TestStopsImmediatelyOnInvalidArgument(t *testing.T) {
	b := brokertest.Start(t, "127.0.0.1:0", broker.ServerConfig{})
	h := newReceiver(t, b.Addr, "r", 5, time.Second, "nu e valid")
	err := h.start(t).wait(t, time.Second)
	if status.Code(err) != codes.InvalidArgument || strings.Contains(h.logs.String(), "reconnect") {
		t.Fatalf("eroare %v; log:\n%s", err, h.logs.String())
	}
}

func TestStopsWhenTakenOver(t *testing.T) {
	b := brokertest.Start(t, "127.0.0.1:0", broker.ServerConfig{})
	first := newReceiver(t, b.Addr, "r", 5, time.Second, "news").start(t)
	first.waitFor(t, "abonarea", `"event":"subscribe","result":"ok"`)
	second := newReceiver(t, b.Addr, "r", 5, time.Second, "news").start(t)
	if err := first.wait(t, time.Second); status.Code(err) != codes.Canceled {
		t.Fatalf("primul receiver: %v, așteptat Canceled", err)
	}
	m := publish(t, b.Addr, "news", "pentru al doilea")
	second.waitFor(t, "mesajul", m.Payload)
}

// Recuperare completă: brokerul cade de două ori. Receiverul se reconectează de fiecare
// dată (contorul se resetează după fiecare abonare reușită, altfel ar fi renunțat la a
// doua cădere) și primește mesajele publicate cât a fost deconectat, salvate în jurnal.
func TestReconnectsAcrossBrokerRestarts(t *testing.T) {
	addr, dir := freeAddr(t), t.TempDir()
	cfg := broker.ServerConfig{DataDir: dir, Fsync: true}
	b := brokertest.Start(t, addr, cfg)
	h := newReceiver(t, addr, "r", 3, 100*time.Millisecond, "news").start(t)
	h.waitFor(t, "abonarea", `"result":"ok"`)
	h.waitFor(t, "m1", publish(t, addr, "news", "m1").Payload)

	for round := 1; round <= 2; round++ {
		b.Stop()
		h.waitFor(t, "reîncercarea 2", `"attempt":2`)
		// mesajul e publicat când receiverul e deconectat; brokerul pornește din jurnal
		b = brokertest.Start(t, addr, cfg)
		m := publish(t, addr, "news", "după repornirea "+string(rune('0'+round)))
		h.waitFor(t, m.Payload, m.Payload)
		if n := b.Broker.Pending("r"); n != 0 {
			// Ack-ul ajunge imediat după afișare
			time.Sleep(100 * time.Millisecond)
			if n = b.Broker.Pending("r"); n != 0 {
				t.Fatalf("runda %d: %d mesaje neconfirmate", round, n)
			}
		}
		h.logs.mu.Lock()
		h.logs.b.Reset() // ca "attempt":2 din runda următoare să fie unul nou
		h.logs.mu.Unlock()
	}
	select {
	case err := <-h.done:
		t.Fatalf("receiverul s-a oprit: %v", err)
	default:
	}
	h.stop()
	if err := h.wait(t, time.Second); err != nil {
		t.Fatalf("oprire prin context: %v", err)
	}
}

// At-least-once: primul Ack eșuează, deci brokerul retrimite mesajul la reconectare.
// Receiverul îl recunoaște după messageId: îl afișează o singură dată, dar îl confirmă.
func TestRedeliveryIsProcessedOnce(t *testing.T) {
	var failed sync.Once
	failFirstAck := grpc.ChainUnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		var err error
		if info.FullMethod == brokerpb.Broker_Ack_FullMethodName {
			failed.Do(func() { err = status.Error(codes.Unavailable, "ack pierdut (injectat)") })
		}
		if err != nil {
			return nil, err
		}
		return h(ctx, req)
	})
	b := brokertest.Start(t, "127.0.0.1:0", broker.ServerConfig{ExtraOptions: []grpc.ServerOption{failFirstAck}})
	h := newReceiver(t, b.Addr, "r", 3, 50*time.Millisecond, "news").start(t)
	h.waitFor(t, "abonarea", `"result":"ok"`)
	m := publish(t, b.Addr, "news", "o singură procesare")

	h.waitFor(t, "retrimiterea recunoscută", `"result":"duplicate"`)
	for i := 0; i < 100 && b.Broker.Pending("r") != 0; i++ {
		time.Sleep(10 * time.Millisecond)
	}
	if n := b.Broker.Pending("r"); n != 0 {
		t.Fatalf("%d mesaje neconfirmate", n)
	}
	if c := strings.Count(h.out.String(), m.Payload); c != 1 {
		t.Fatalf("mesajul a fost procesat de %d ori", c)
	}
}

func TestUnsubscribeFlag(t *testing.T) {
	b := brokertest.Start(t, "127.0.0.1:0", broker.ServerConfig{})
	h := newReceiver(t, b.Addr, "r", 3, time.Second, "news").start(t)
	h.waitFor(t, "abonarea", `"result":"ok"`)
	h.stop()
	h.wait(t, time.Second)
	publish(t, b.Addr, "news", "rămâne în coadă")

	u := newReceiver(t, b.Addr, "r", 0, 0)
	if err := u.r.unsubscribe(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(u.logs.String(), `"dropped":1`) || b.Broker.Pending("r") != -1 {
		t.Fatalf("dezabonare: pending=%d log=%s", b.Broker.Pending("r"), u.logs.String())
	}
}
