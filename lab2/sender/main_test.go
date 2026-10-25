package main

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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"lab2/common"
	"lab2/gen/brokerpb"
	"lab2/internal/broker"
	"lab2/internal/brokertest"
)

func newPublisher(t *testing.T, addr string, logs io.Writer, timeout time.Duration, retries int) publisher {
	t.Helper()
	conn, err := common.Dial(addr, common.ClientOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return publisher{conn: conn, client: brokerpb.NewBrokerClient(conn), timeout: timeout, retries: retries,
		interval: 50 * time.Millisecond, log: slog.New(slog.NewJSONHandler(logs, nil))}
}

func subscribe(t *testing.T, addr, id string) grpc.ServerStreamingClient[brokerpb.Message] {
	t.Helper()
	conn, _ := common.Dial(addr, common.ClientOptions{})
	t.Cleanup(func() { conn.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	s, err := brokerpb.NewBrokerClient(conn).Subscribe(ctx, &brokerpb.SubscribeRequest{ReceiverId: id, Topics: []string{"news"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Header(); err != nil {
		t.Fatal(err)
	}
	return s
}

func recvWithin(s grpc.ServerStreamingClient[brokerpb.Message], d time.Duration) *brokerpb.Message {
	ch := make(chan *brokerpb.Message, 1)
	go func() { m, _ := s.Recv(); ch <- m }()
	select {
	case m := <-ch:
		return m
	case <-time.After(d):
		return nil
	}
}

// Primul răspuns întârzie peste deadline-ul senderului, deși brokerul a salvat mesajul.
// Senderul reîncearcă cu același messageId, brokerul răspunde duplicate=true, iar
// receiverul primește mesajul o singură dată.
func TestRetryAfterDeadlineIsDeduplicated(t *testing.T) {
	var once sync.Once
	slowReply := grpc.ChainUnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		resp, err := h(ctx, req)
		if info.FullMethod == brokerpb.Broker_Publish_FullMethodName {
			once.Do(func() { time.Sleep(300 * time.Millisecond) }) // răspunsul la primul apel „se pierde”
		}
		return resp, err
	})
	b := brokertest.Start(t, "127.0.0.1:0", broker.ServerConfig{ExtraOptions: []grpc.ServerOption{slowReply}})
	s := subscribe(t, b.Addr, "r")

	var logs bytes.Buffer
	p := newPublisher(t, b.Addr, &logs, 100*time.Millisecond, 3)
	m := common.NewMessage("", "sender-1", "news", "exact o dată")
	reply, err := p.publish(m)
	if err != nil {
		t.Fatalf("publish: %v\n%s", err, logs.String())
	}
	if !reply.Duplicate || !strings.Contains(logs.String(), `"code":"DeadlineExceeded"`) {
		t.Fatalf("așteptat: prima încercare DeadlineExceeded, a doua duplicate=true\n%s", logs.String())
	}
	if got := recvWithin(s, time.Second); got.GetMessageId() != m.MessageId {
		t.Fatal("receiverul nu a primit mesajul")
	}
	if extra := recvWithin(s, 300*time.Millisecond); extra != nil {
		t.Fatalf("mesajul a ajuns de două ori: %q", extra.Payload)
	}
	t.Log(logs.String())
}

// Brokerul pornește abia după ce senderul a început să trimită: reîncercările îl prind.
func TestRetryUntilBrokerIsUp(t *testing.T) {
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := ln.Addr().String()
	ln.Close()

	var logs bytes.Buffer
	p := newPublisher(t, addr, &logs, 200*time.Millisecond, 10)
	go func() {
		time.Sleep(200 * time.Millisecond)
		brokertest.Start(t, addr, broker.ServerConfig{})
	}()
	if _, err := p.publish(common.NewMessage("", "sender-1", "news", "x")); err != nil {
		t.Fatalf("publish: %v\n%s", err, logs.String())
	}
	if !strings.Contains(logs.String(), `"result":"retry","code":"Unavailable"`) {
		t.Fatalf("nu s-a reîncercat la Unavailable:\n%s", logs.String())
	}
}

func TestInvalidMessageIsNotRetried(t *testing.T) {
	b := brokertest.Start(t, "127.0.0.1:0", broker.ServerConfig{})
	var logs bytes.Buffer
	p := newPublisher(t, b.Addr, &logs, time.Second, 3)
	m := common.NewMessage("", "sender-1", "news", "x")
	m.SchemaVersion = "2.0"
	if _, err := p.publish(m); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("cod %v", status.Code(err))
	}
	if strings.Contains(logs.String(), `"result":"retry"`) {
		t.Fatalf("INVALID_ARGUMENT a fost reîncercat:\n%s", logs.String())
	}
}
