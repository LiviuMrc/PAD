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

func TestPublishReachesBothReceivers(t *testing.T) {
	e := start(t, ServerConfig{})
	r1 := subscribe(t, e.conn, "receiver-1", "news")
	r2 := subscribe(t, e.conn, "receiver-2", "news")

	sent := publish(t, e.conn, "news", "salut")

	for _, r := range []*sub{r1, r2} {
		got := r.recv(t)
		if got.MessageId != sent.MessageId || got.CorrelationId != sent.CorrelationId || got.Payload != "salut" {
			t.Fatalf("mesaj diferit: %+v", got)
		}
	}
}

func TestRoutingByTopic(t *testing.T) {
	e := start(t, ServerConfig{})
	news := subscribe(t, e.conn, "receiver-1", "news")
	alerts := subscribe(t, e.conn, "receiver-2", "alerts")

	publish(t, e.conn, "alerts", "doar pentru alerts")
	marker := publish(t, e.conn, "news", "doar pentru news")

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
	e := start(t, ServerConfig{})
	r1 := subscribe(t, e.conn, "receiver-1", "news")
	waitOnline(t, e.b, 1)

	r1.cancel() // injectarea eșecului: receiverul închide conexiunea
	waitOnline(t, e.b, 0)

	var sent []*brokerpb.Message
	for i := 1; i <= 3; i++ {
		sent = append(sent, publish(t, e.conn, "news", fmt.Sprintf("mesaj %d", i)))
	}

	again := subscribe(t, e.conn, "receiver-1", "news") // recuperarea: același id
	for i, want := range sent {
		if got := again.recv(t); got.MessageId != want.MessageId {
			t.Fatalf("mesajul %d: primit %q, așteptat %q", i+1, got.Payload, want.Payload)
		}
	}
}

func TestInvalidMessagesAreRejected(t *testing.T) {
	e := start(t, ServerConfig{})
	client := brokerpb.NewBrokerClient(e.conn)

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
	if _, err := client.Ack(context.Background(), &brokerpb.AckRequest{ReceiverId: "r1", MessageId: "nu e valid"}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("Ack cu messageId invalid: cod %v", status.Code(err))
	}
	if _, err := client.Ack(context.Background(), &brokerpb.AckRequest{ReceiverId: "necunoscut", MessageId: "m1"}); status.Code(err) != codes.NotFound {
		t.Errorf("Ack pentru receiver necunoscut: cod %v, așteptat NotFound", status.Code(err))
	}
	if _, err := client.Unsubscribe(context.Background(), &brokerpb.UnsubscribeRequest{ReceiverId: "necunoscut"}); status.Code(err) != codes.NotFound {
		t.Errorf("Unsubscribe pentru receiver necunoscut: cod %v, așteptat NotFound", status.Code(err))
	}
}

// Clientul renunță singur când termenul expiră.
func TestPublishRespectsDeadline(t *testing.T) {
	e := start(t, ServerConfig{})
	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	time.Sleep(time.Millisecond)
	_, err := brokerpb.NewBrokerClient(e.conn).Publish(ctx, &brokerpb.PublishRequest{Message: common.NewMessage("", "s", "news", "x")})
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("cod %v, așteptat DeadlineExceeded", status.Code(err))
	}
}

// #2: cererea ajunge la broker după ce termenul ei a expirat (aici întârziată de un
// interceptor). Brokerul nu trebuie să pună în cozi un mesaj pentru care senderul a
// primit deja DEADLINE_EXCEEDED.
func TestServerDropsPublishWhoseDeadlineExpired(t *testing.T) {
	slow := grpc.ChainUnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		if info.FullMethod == brokerpb.Broker_Publish_FullMethodName {
			time.Sleep(150 * time.Millisecond)
		}
		return h(ctx, req)
	})
	e := start(t, ServerConfig{ExtraOptions: []grpc.ServerOption{slow}})
	r := subscribe(t, e.conn, "r", "news")

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := brokerpb.NewBrokerClient(e.conn).Publish(ctx, &brokerpb.PublishRequest{Message: common.NewMessage("", "s", "news", "prea târziu")})
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("client: cod %v, așteptat DeadlineExceeded", status.Code(err))
	}
	eventually(t, "brokerul a logat mesajul expirat", func() bool { return strings.Contains(e.logs.String(), `"event":"message.expired"`) })
	r.expectNothing(t, 300*time.Millisecond)
	if n := e.b.Pending("r"); n != 0 {
		t.Fatalf("coada are %d mesaje, așteptat 0", n)
	}
}

// Procesare concurentă: mai mulți senderi publică în paralel, iar receiverul primește
// fiecare mesaj exact o dată și îl confirmă.
func TestConcurrentPublishers(t *testing.T) {
	e := start(t, ServerConfig{})
	r := subscribe(t, e.conn, "receiver-1", "news")

	const senders, each = 10, 20
	var wg sync.WaitGroup
	for s := 0; s < senders; s++ {
		wg.Add(1)
		go func(s int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				publish(t, e.conn, "news", fmt.Sprintf("s%d-%d", s, i))
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
		ack(t, e.conn, "receiver-1", m)
	}
	if n := e.b.Pending("receiver-1"); n != 0 {
		t.Fatalf("după confirmări coada are %d mesaje", n)
	}
}

func TestSameReceiverIdTakesOver(t *testing.T) {
	e := start(t, ServerConfig{})
	old := subscribe(t, e.conn, "receiver-1", "news")
	newer := subscribe(t, e.conn, "receiver-1", "news")

	_, err := old.stream.Recv()
	if status.Code(err) != codes.Canceled || !strings.Contains(status.Convert(err).Message(), "preluat") {
		t.Fatalf("sesiunea veche: %v, așteptat Canceled cu motivul preluării", err)
	}
	sent := publish(t, e.conn, "news", "pentru sesiunea nouă")
	if got := newer.recv(t); got.MessageId != sent.MessageId {
		t.Fatal("sesiunea nouă nu a primit mesajul")
	}
}

// #7: o abonare nouă înlocuiește topicurile celei vechi, iar mesajele din coadă de pe
// topicurile renunțate sunt șterse.
func TestResubscribeReplacesTopics(t *testing.T) {
	e := start(t, ServerConfig{})
	s := subscribe(t, e.conn, "r", "news", "alerts")
	s.cancel()
	waitOnline(t, e.b, 0)
	publish(t, e.conn, "alerts", "pus în coadă cât timp r era abonat la alerts")
	keep := publish(t, e.conn, "news", "rămâne")

	again := subscribe(t, e.conn, "r", "news")
	publish(t, e.conn, "alerts", "nu trebuie să ajungă")
	marker := publish(t, e.conn, "news", "marker")

	for _, want := range []*brokerpb.Message{keep, marker} {
		if got := again.recv(t); got.MessageId != want.MessageId {
			t.Fatalf("primit %q pe %s, așteptat %q", got.Payload, got.Topic, want.Payload)
		}
	}
	again.expectNothing(t, 200*time.Millisecond)
	if !strings.Contains(e.logs.String(), `"purged":1`) {
		t.Fatal("brokerul nu a raportat mesajul șters de pe alerts")
	}
	reply := publishMsg(t, e.conn, common.NewMessage("", "s", "alerts", "fără abonați"))
	if reply.Subscribers != 0 {
		t.Fatalf("alerts are încă %d abonați", reply.Subscribers)
	}
}

// #8: motivul închiderii sesiunii e corect: deconectare simplă, preluare sau dezabonare.
func TestSessionEndReason(t *testing.T) {
	e := start(t, ServerConfig{})
	s := subscribe(t, e.conn, "r", "news")
	s.cancel()
	eventually(t, "sesiune închisă cu motivul disconnected", func() bool {
		return strings.Contains(e.logs.String(), `"event":"session.end","receiver":"r","session":1,"reason":"disconnected"`)
	})

	subscribe(t, e.conn, "r", "news")
	subscribe(t, e.conn, "r", "news")
	eventually(t, "sesiune închisă cu motivul replaced", func() bool {
		return strings.Contains(e.logs.String(), `"session":2,"reason":"replaced"`)
	})
	if strings.Contains(e.logs.String(), `"session":1,"reason":"replaced"`) {
		t.Fatal("deconectarea simplă a fost raportată ca preluare")
	}
}

// #9: sesiunea veche primește un mesaj, nu-l confirmă și e preluată. Sesiunea nouă
// trebuie să-l primească (nu se pierde), urmat de mesajele noi, în ordine.
func TestTakeoverRedeliversUnacked(t *testing.T) {
	e := start(t, ServerConfig{})
	old := subscribe(t, e.conn, "r", "news")
	first := publish(t, e.conn, "news", "primul")
	if got := old.recv(t); got.MessageId != first.MessageId {
		t.Fatal("sesiunea veche nu a primit mesajul")
	}

	newer := subscribe(t, e.conn, "r", "news")
	second := publish(t, e.conn, "news", "al doilea")
	for _, want := range []*brokerpb.Message{first, second} {
		got := newer.recv(t)
		if got.MessageId != want.MessageId {
			t.Fatalf("sesiunea nouă: primit %q, așteptat %q", got.Payload, want.Payload)
		}
		ack(t, e.conn, "r", got)
	}
	newer.expectNothing(t, 200*time.Millisecond)
}

// #10: un mesaj trimis pe stream, dar neconfirmat (receiverul a căzut înainte să-l
// proceseze), rămâne în coadă și e retrimis. Un mesaj confirmat nu mai e retrimis.
func TestUnackedMessageIsRedelivered(t *testing.T) {
	e := start(t, ServerConfig{})
	s := subscribe(t, e.conn, "r", "news")
	acked := publish(t, e.conn, "news", "confirmat")
	if got := s.recv(t); !ack(t, e.conn, "r", got) {
		t.Fatal("Ack nu a scos mesajul din coadă")
	}
	lost := publish(t, e.conn, "news", "în zbor")
	if got := s.recv(t); got.MessageId != lost.MessageId { // primit, dar receiverul cade înainte de Ack
		t.Fatal("mesaj greșit")
	}
	s.cancel()
	waitOnline(t, e.b, 0)
	if n := e.b.Pending("r"); n != 1 {
		t.Fatalf("coada are %d mesaje, așteptat 1 (cel neconfirmat)", n)
	}

	again := subscribe(t, e.conn, "r", "news")
	got := again.recv(t)
	if got.MessageId != lost.MessageId {
		t.Fatalf("retrimis %q, așteptat %q", got.Payload, lost.Payload)
	}
	if got.MessageId == acked.MessageId {
		t.Fatal("mesajul confirmat a fost retrimis")
	}
	ack(t, e.conn, "r", got)
	if ack(t, e.conn, "r", got) {
		t.Fatal("al doilea Ack pentru același mesaj trebuia să întoarcă removed=false")
	}
	again.expectNothing(t, 200*time.Millisecond)
}

// Retrimiterea unui mesaj cu același messageId (senderul a primit o eroare și reîncearcă)
// nu îl pune din nou în cozi.
func TestDuplicatePublishIsNotRequeued(t *testing.T) {
	e := start(t, ServerConfig{})
	r := subscribe(t, e.conn, "r", "news")
	m := common.NewMessage("", "s", "news", "o singură dată")
	if first := publishMsg(t, e.conn, m); first.Duplicate || first.Subscribers != 1 {
		t.Fatalf("prima publicare: %+v", first)
	}
	if again := publishMsg(t, e.conn, m); !again.Duplicate || again.Subscribers != 1 {
		t.Fatalf("a doua publicare trebuia marcată duplicate: %+v", again)
	}
	r.recv(t)
	r.expectNothing(t, 200*time.Millisecond)
}

func TestUnsubscribe(t *testing.T) {
	e := start(t, ServerConfig{})
	s := subscribe(t, e.conn, "r", "news")
	publish(t, e.conn, "news", "neconfirmat")
	s.recv(t)

	reply, err := brokerpb.NewBrokerClient(e.conn).Unsubscribe(context.Background(), &brokerpb.UnsubscribeRequest{ReceiverId: "r"})
	if err != nil || reply.Dropped != 1 {
		t.Fatalf("Unsubscribe: %v %v", reply, err)
	}
	if _, err := s.stream.Recv(); status.Code(err) != codes.Canceled || !strings.Contains(err.Error(), "dezabonat") {
		t.Fatalf("sesiunea activă: %v, așteptat Canceled (dezabonat)", err)
	}
	if e.b.Pending("r") != -1 {
		t.Fatal("receiverul încă există")
	}
	if n := publishMsg(t, e.conn, common.NewMessage("", "s", "news", "x")).Subscribers; n != 0 {
		t.Fatalf("news are încă %d abonați", n)
	}
}

// Un receiver offline de mai mult de ReceiverTTL e uitat: nu mai strânge mesaje.
func TestOfflineReceiverExpires(t *testing.T) {
	var mu sync.Mutex
	now := time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
	advance := func(d time.Duration) { mu.Lock(); now = now.Add(d); mu.Unlock() }

	e := start(t, ServerConfig{Broker: Config{ReceiverTTL: time.Hour, Now: clock}})
	online := subscribe(t, e.conn, "online", "news")
	gone := subscribe(t, e.conn, "gone", "news")
	gone.cancel()
	waitOnline(t, e.b, 1)
	publish(t, e.conn, "news", "x")

	advance(59 * time.Minute)
	if n := e.b.Expire(); n != 0 {
		t.Fatalf("expirat prea devreme: %d", n)
	}
	advance(2 * time.Minute)
	if n := e.b.Expire(); n != 1 {
		t.Fatalf("expirate %d, așteptat 1 (doar cel offline)", n)
	}
	if e.b.Pending("gone") != -1 || e.b.Pending("online") != 1 {
		t.Fatalf("pending: gone=%d online=%d", e.b.Pending("gone"), e.b.Pending("online"))
	}
	online.recv(t)
	if n := publishMsg(t, e.conn, common.NewMessage("", "s", "news", "y")).Subscribers; n != 1 {
		t.Fatalf("abonați după expirare: %d, așteptat 1", n)
	}
}

// Același lucru, dar cu ceasul real și goroutine-ul care verifică periodic.
func TestJanitorExpiresWithRealClock(t *testing.T) {
	e := start(t, ServerConfig{Broker: Config{ReceiverTTL: 100 * time.Millisecond}})
	s := subscribe(t, e.conn, "r", "news")
	s.cancel()
	eventually(t, "receiverul expirat", func() bool { return e.b.Pending("r") == -1 })
	if !strings.Contains(e.logs.String(), `"event":"receiver.expired"`) {
		t.Fatal("lipsește evenimentul receiver.expired")
	}
}
