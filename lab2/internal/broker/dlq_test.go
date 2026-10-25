package broker

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"lab2/common"
	"lab2/gen/brokerpb"
)

func listDLQ(t *testing.T, conn *grpc.ClientConn, receiverID string) *brokerpb.ListDeadLettersReply {
	t.Helper()
	reply, err := brokerpb.NewBrokerClient(conn).ListDeadLetters(context.Background(),
		&brokerpb.ListDeadLettersRequest{ReceiverId: receiverID})
	if err != nil {
		t.Fatalf("ListDeadLetters: %v", err)
	}
	return reply
}

func payloads(dl []*brokerpb.DeadLetter) []string {
	var out []string
	for _, d := range dl {
		out = append(out, d.Message.Payload)
	}
	return out
}

// Coada lui r are limita 3. Din 5 mesaje publicate cât e offline, cele mai vechi 2 nu se
// pierd: ajung în DLQ cu motivul queue_overflow, iar redrive le livrează după celelalte.
func TestOverflowMovesOldestToDLQ(t *testing.T) {
	e := start(t, ServerConfig{Broker: Config{MaxQueue: 3}})
	subscribe(t, e.conn, "r", "news").cancel()
	waitOnline(t, e.b, 0)
	var sent []*brokerpb.Message
	for i := 1; i <= 5; i++ {
		sent = append(sent, publish(t, e.conn, "news", fmt.Sprintf("m%d", i)))
	}

	dlq := listDLQ(t, e.conn, "r")
	if dlq.Total != 2 || strings.Join(payloads(dlq.DeadLetters), ",") != "m1,m2" {
		t.Fatalf("DLQ: %v (total %d), așteptat m1,m2", payloads(dlq.DeadLetters), dlq.Total)
	}
	for i, d := range dlq.DeadLetters {
		if d.ReceiverId != "r" || d.Reason != "queue_overflow" || d.Message.MessageId != sent[i].MessageId {
			t.Fatalf("dead letter %d: %+v", i, d)
		}
		if _, err := time.Parse(time.RFC3339, d.DeadAt); err != nil || !strings.HasSuffix(d.DeadAt, "Z") {
			t.Fatalf("dead_at invalid: %q", d.DeadAt)
		}
	}
	if c := strings.Count(e.logs.String(), `"event":"dlq.add","reason":"queue_overflow"`); c != 2 {
		t.Fatalf("dlq.add de %d ori, așteptat 2", c)
	}

	r := subscribe(t, e.conn, "r", "news")
	for _, want := range sent[2:] {
		got := r.recv(t)
		if got.MessageId != want.MessageId {
			t.Fatalf("primit %q, așteptat %q", got.Payload, want.Payload)
		}
		ack(t, e.conn, "r", got)
	}
	red, err := brokerpb.NewBrokerClient(e.conn).RedriveDeadLetters(context.Background(), &brokerpb.RedriveRequest{ReceiverId: "r"})
	if err != nil || red.Redriven != 2 || red.Skipped != 0 {
		t.Fatalf("redrive: %v %v", red, err)
	}
	for _, want := range sent[:2] { // receiverul e online: le primește imediat, în ordinea din DLQ
		got := r.recv(t)
		if got.MessageId != want.MessageId {
			t.Fatalf("după redrive: primit %q, așteptat %q", got.Payload, want.Payload)
		}
		ack(t, e.conn, "r", got)
	}
	if n := e.b.DeadLetters(); n != 0 {
		t.Fatalf("DLQ are încă %d mesaje", n)
	}
	r.expectNothing(t, 200*time.Millisecond)
}

// Un receiver offline peste TTL e uitat, dar mesajele lui neconfirmate trec în DLQ.
// Redrive cere ca receiverul să fie abonat din nou.
func TestExpiredReceiverMessagesGoToDLQ(t *testing.T) {
	var mu sync.Mutex
	now := time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { mu.Lock(); defer mu.Unlock(); return now }

	e := start(t, ServerConfig{Broker: Config{ReceiverTTL: time.Hour, Now: clock}})
	subscribe(t, e.conn, "gone", "news").cancel()
	waitOnline(t, e.b, 0)
	m1 := publish(t, e.conn, "news", "a")
	m2 := publish(t, e.conn, "news", "b")
	mu.Lock()
	now = now.Add(2 * time.Hour)
	mu.Unlock()
	if n := e.b.Expire(); n != 1 {
		t.Fatalf("expirate: %d", n)
	}

	dlq := listDLQ(t, e.conn, "gone")
	if dlq.Total != 2 || dlq.DeadLetters[0].Reason != "receiver_expired" || dlq.DeadLetters[0].DeadAt != "2026-10-25T14:00:00Z" {
		t.Fatalf("DLQ: %+v", dlq.DeadLetters)
	}
	client := brokerpb.NewBrokerClient(e.conn)
	if _, err := client.RedriveDeadLetters(context.Background(), &brokerpb.RedriveRequest{ReceiverId: "gone"}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("redrive pentru receiver neabonat: %v, așteptat FailedPrecondition", err)
	}

	r := subscribe(t, e.conn, "gone", "news")
	if red, err := client.RedriveDeadLetters(context.Background(), &brokerpb.RedriveRequest{ReceiverId: "gone", MessageIds: []string{m2.MessageId}}); err != nil || red.Redriven != 1 {
		t.Fatalf("redrive selectiv: %v %v", red, err)
	}
	if got := r.recv(t); got.MessageId != m2.MessageId {
		t.Fatalf("primit %q, așteptat b", got.Payload)
	}
	if left := listDLQ(t, e.conn, ""); left.Total != 1 || left.DeadLetters[0].Message.MessageId != m1.MessageId {
		t.Fatalf("în DLQ trebuia să rămână doar a: %v", payloads(left.DeadLetters))
	}
}

// Redrive nu pune înapoi mesaje de pe un topic la care receiverul nu mai e abonat
// (abonarea următoare le-ar șterge); ele rămân în DLQ. Purge le șterge definitiv.
func TestRedriveSkipsUnsubscribedTopicsAndPurge(t *testing.T) {
	e := start(t, ServerConfig{Broker: Config{MaxQueue: 1}})
	subscribe(t, e.conn, "r", "news", "alerts").cancel()
	waitOnline(t, e.b, 0)
	publish(t, e.conn, "alerts", "alertă veche")
	publish(t, e.conn, "news", "știre")      // alerta trece în DLQ
	publish(t, e.conn, "news", "știre nouă") // „știre” trece în DLQ

	subscribe(t, e.conn, "r", "news").cancel() // r renunță la alerts
	waitOnline(t, e.b, 0)
	client := brokerpb.NewBrokerClient(e.conn)
	red, err := client.RedriveDeadLetters(context.Background(), &brokerpb.RedriveRequest{ReceiverId: "r"})
	if err != nil || red.Redriven != 1 || red.Skipped != 1 {
		t.Fatalf("redrive: %v %v, așteptat 1 retrimis și 1 sărit", red, err)
	}
	// MaxQueue = 1: „știre” revine în coadă și o împinge pe „știre nouă” în DLQ
	if left := payloads(listDLQ(t, e.conn, "r").DeadLetters); strings.Join(left, ",") != "alertă veche,știre nouă" {
		t.Fatalf("DLQ după redrive: %v", left)
	}
	p, err := client.PurgeDeadLetters(context.Background(), &brokerpb.PurgeDeadLettersRequest{ReceiverId: "r"})
	if err != nil || p.Purged != 2 || e.b.DeadLetters() != 0 {
		t.Fatalf("purge: %v %v (rămase %d)", p, err, e.b.DeadLetters())
	}
}

// Un mesaj deja trimis receiverului, apoi împins în DLQ de coada plină, e confirmat de
// receiver: a fost procesat, deci iese și din DLQ (nu va fi retrimis la redrive).
func TestAckRemovesInFlightMessageFromDLQ(t *testing.T) {
	e := start(t, ServerConfig{Broker: Config{MaxQueue: 1}})
	r := subscribe(t, e.conn, "r", "news")
	first := publish(t, e.conn, "news", "în zbor")
	got := r.recv(t) // trimis, dar încă neconfirmat
	publish(t, e.conn, "news", "următorul")
	if e.b.DeadLetters() != 1 {
		t.Fatalf("DLQ: %d, așteptat 1", e.b.DeadLetters())
	}
	if got.MessageId != first.MessageId || !ack(t, e.conn, "r", got) {
		t.Fatal("Ack pe mesajul din DLQ trebuia să întoarcă removed=true")
	}
	if e.b.DeadLetters() != 0 {
		t.Fatal("mesajul confirmat a rămas în DLQ")
	}
}

// DLQ e și ea limitată: peste DLQMax, cel mai vechi mesaj din DLQ e eliminat și logat ca eroare.
func TestDLQIsBounded(t *testing.T) {
	e := start(t, ServerConfig{Broker: Config{MaxQueue: 1, DLQMax: 2}})
	subscribe(t, e.conn, "r", "news").cancel()
	waitOnline(t, e.b, 0)
	for i := 1; i <= 4; i++ {
		publish(t, e.conn, "news", fmt.Sprintf("m%d", i))
	}
	if got := strings.Join(payloads(listDLQ(t, e.conn, "r").DeadLetters), ","); got != "m2,m3" {
		t.Fatalf("DLQ: %s, așteptat m2,m3", got)
	}
	if !strings.Contains(e.logs.String(), `"level":"ERROR","msg":"DLQ plină`) {
		t.Fatal("eliminarea din DLQ plină trebuia logată ca eroare")
	}
}

// DLQ supraviețuiește repornirii (cu același dead_at) și compactării jurnalului.
func TestDLQSurvivesRestartAndCompaction(t *testing.T) {
	dir := t.TempDir()
	cfg := ServerConfig{DataDir: dir, Broker: Config{MaxQueue: 2, DLQMax: 5}}
	first := start(t, cfg)
	first.srv.journal.compactAt = 20
	subscribe(t, first.conn, "r", "news").cancel()
	waitOnline(t, first.b, 0)
	for i := 1; i <= 30; i++ {
		publish(t, first.conn, "news", fmt.Sprintf("m%d", i))
	}
	before := listDLQ(t, first.conn, "r")
	if before.Total != 5 || before.DeadLetters[0].Message.Payload != "m24" || !strings.Contains(first.logs.String(), `"event":"journal.compact","result":"ok"`) {
		t.Fatalf("DLQ %d (așteptat 5, de la m24), compactat=%v", before.Total, strings.Contains(first.logs.String(), "journal.compact"))
	}
	first.stop()

	second := start(t, cfg)
	after := listDLQ(t, second.conn, "r")
	if after.Total != before.Total {
		t.Fatalf("după repornire DLQ are %d, înainte avea %d", after.Total, before.Total)
	}
	for i := range before.DeadLetters {
		b, a := before.DeadLetters[i], after.DeadLetters[i]
		if b.Message.MessageId != a.Message.MessageId || b.DeadAt != a.DeadAt || b.Reason != a.Reason {
			t.Fatalf("dead letter %d diferit după repornire:\n%+v\n%+v", i, b, a)
		}
	}
	if !strings.Contains(second.logs.String(), `"deadLetters":5`) {
		t.Fatal("refacerea nu raportează DLQ-ul")
	}
	r := subscribe(t, second.conn, "r", "news")
	for _, want := range []string{"m29", "m30"} {
		got := r.recv(t)
		if got.Payload != want {
			t.Fatalf("primit %q, așteptat %q", got.Payload, want)
		}
		ack(t, second.conn, "r", got)
	}
	red, err := brokerpb.NewBrokerClient(second.conn).RedriveDeadLetters(context.Background(),
		&brokerpb.RedriveRequest{ReceiverId: "r", MessageIds: []string{before.DeadLetters[0].Message.MessageId}})
	if err != nil || red.Redriven != 1 {
		t.Fatalf("redrive după repornire: %v %v", red, err)
	}
	if got := r.recv(t); got.Payload != "m24" {
		t.Fatalf("după redrive: %q", got.Payload)
	}
}

func TestDLQValidation(t *testing.T) {
	e := start(t, ServerConfig{})
	c := brokerpb.NewBrokerClient(e.conn)
	ctx := context.Background()
	if _, err := c.RedriveDeadLetters(ctx, &brokerpb.RedriveRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("redrive fără receiver: %v", err)
	}
	if _, err := c.PurgeDeadLetters(ctx, &brokerpb.PurgeDeadLettersRequest{MessageIds: []string{"nu e valid"}}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("purge cu id invalid: %v", err)
	}
	if _, err := c.ListDeadLetters(ctx, &brokerpb.ListDeadLettersRequest{ReceiverId: "a b"}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("list cu receiver invalid: %v", err)
	}
	if r, err := c.ListDeadLetters(ctx, &brokerpb.ListDeadLettersRequest{}); err != nil || r.Total != 0 {
		t.Errorf("list pe DLQ goală: %v %v", r, err)
	}
	_ = common.NewID
}
