package main

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"lab2/common"
	"lab2/gen/brokerpb"
	"lab2/internal/broker"
	"lab2/internal/brokertest"
)

// Comanda dlq împotriva unui broker real: listare, redrive, purge.
func TestListRedrivePurge(t *testing.T) {
	b := brokertest.Start(t, "127.0.0.1:0", broker.ServerConfig{Broker: broker.Config{MaxQueue: 1}})
	conn, _ := common.Dial(b.Addr, common.ClientOptions{})
	defer conn.Close()
	c := brokerpb.NewBrokerClient(conn)
	ctx := context.Background()

	ctxSub, cancel := context.WithCancel(ctx)
	s, _ := c.Subscribe(ctxSub, &brokerpb.SubscribeRequest{ReceiverId: "r", Topics: []string{"news"}})
	s.Header()
	cancel()
	for i := 0; i < 50 && b.Broker.Online() != 0; i++ {
		time.Sleep(10 * time.Millisecond)
	}
	for i := 1; i <= 3; i++ {
		if _, err := c.Publish(ctx, &brokerpb.PublishRequest{Message: common.NewMessage("", "s", "news", fmt.Sprintf("m%d", i))}); err != nil {
			t.Fatal(err)
		}
	}

	var out bytes.Buffer
	if err := run(ctx, c, &out, "", nil, false, false, 0); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "DLQ: 2 mesaje") || !strings.Contains(out.String(), "queue_overflow") ||
		!strings.Contains(out.String(), "news: m1") || !strings.Contains(out.String(), "news: m2") {
		t.Fatalf("listare:\n%s", out.String())
	}
	t.Log("\n" + out.String())

	out.Reset()
	if err := run(ctx, c, &out, "", nil, true, false, 0); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("redrive fără -receiver: %v", err)
	}
	if err := run(ctx, c, &out, "r", nil, true, false, 0); err != nil || !strings.Contains(out.String(), "retrimise în coada lui r: 2") {
		t.Fatalf("redrive: %v\n%s", err, out.String())
	}
	out.Reset()
	if err := run(ctx, c, &out, "", nil, false, true, 0); err != nil || !strings.Contains(out.String(), "șterse din DLQ: 2") {
		t.Fatalf("purge: %v\n%s", err, out.String())
	}
	if b.Broker.DeadLetters() != 0 {
		t.Fatal("DLQ nu e goală după purge")
	}
}
