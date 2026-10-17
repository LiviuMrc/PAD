package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"lab1/common"
)

// Teste de integrare: broker real pe localhost, clienți TCP reali.

func startBroker(t *testing.T, mcast common.Transport) (*Broker, string) {
	t.Helper()
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))
	if testing.Verbose() {
		log = common.NewLogger("broker")
	}
	b := NewBroker(log, mcast)
	addr, err := b.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	return b, addr
}

func dial(t *testing.T, addr string) *common.TCPTransport {
	t.Helper()
	c, err := common.DialTCP(addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

// expect citește următorul mesaj și verifică tipul lui (cu deadline, ca testul să nu se blocheze).
func expect(t *testing.T, c *common.TCPTransport, msgType string) *common.Message {
	t.Helper()
	c.SetReadTimeout(2 * time.Second)
	m, err := c.Receive()
	if err != nil {
		t.Fatalf("aștept %s, am primit eroarea: %v", msgType, err)
	}
	if m.MessageType != msgType {
		t.Fatalf("aștept %s, am primit %s (%s)", msgType, m.MessageType, m.Payload)
	}
	return m
}

func subscribe(t *testing.T, addr, id, topic string) *common.TCPTransport {
	t.Helper()
	c := dial(t, addr)
	_ = c.Send(common.NewMessage(common.TypeSubscribe, "", id, topic, ""))
	expect(t, c, common.TypeAck)
	return c
}

func publish(t *testing.T, c *common.TCPTransport, topic, payload string) *common.Message {
	t.Helper()
	m := common.NewMessage(common.TypePublish, "", "sender-1", topic, payload)
	_ = c.Send(m)
	ack := expect(t, c, common.TypeAck)
	if ack.CorrelationID != m.CorrelationID {
		t.Fatalf("ACK cu correlationId %s, așteptat %s", ack.CorrelationID, m.CorrelationID)
	}
	return m
}

func waitOnline(t *testing.T, b *Broker, want int) {
	t.Helper()
	for i := 0; i < 100; i++ {
		b.mu.Lock()
		n := len(b.online)
		b.mu.Unlock()
		if n == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("brokerul nu a ajuns la %d receivere online", want)
}

func TestPublishReachesBothReceivers(t *testing.T) {
	_, addr := startBroker(t, nil)
	r1 := subscribe(t, addr, "receiver-1", "news")
	r2 := subscribe(t, addr, "receiver-2", "news")
	sender := dial(t, addr)

	sent := publish(t, sender, "news", "salut")

	for _, r := range []*common.TCPTransport{r1, r2} {
		got := expect(t, r, common.TypePublish)
		if got.MessageID != sent.MessageID || got.CorrelationID != sent.CorrelationID || got.Payload != "salut" {
			t.Fatalf("mesaj diferit: %+v", got)
		}
	}
}

func TestRoutingByTopic(t *testing.T) {
	_, addr := startBroker(t, nil)
	news := subscribe(t, addr, "receiver-1", "news")
	alerts := subscribe(t, addr, "receiver-2", "alerts")
	sender := dial(t, addr)

	publish(t, sender, "alerts", "doar pentru alerts")

	if got := expect(t, alerts, common.TypePublish); got.Topic != "alerts" {
		t.Fatalf("topic greșit: %s", got.Topic)
	}
	news.SetReadTimeout(200 * time.Millisecond)
	if m, err := news.Receive(); err == nil {
		t.Fatalf("receiverul de pe news nu trebuia să primească: %+v", m)
	}
}

// Scenariu de eșec: un receiver cade, între timp se publică mesaje, iar la
// reconectare le primește pe toate, în ordine.
func TestOfflineReceiverGetsQueuedMessagesOnReconnect(t *testing.T) {
	b, addr := startBroker(t, nil)
	r1 := subscribe(t, addr, "receiver-1", "news")
	waitOnline(t, b, 1)

	r1.Close() // injectarea eșecului: receiverul se deconectează
	waitOnline(t, b, 0)

	sender := dial(t, addr)
	var sent []*common.Message
	for i := 1; i <= 3; i++ {
		sent = append(sent, publish(t, sender, "news", fmt.Sprintf("mesaj %d", i)))
	}

	r1again := subscribe(t, addr, "receiver-1", "news") // recuperarea: reconectare cu același id
	for i, want := range sent {
		got := expect(t, r1again, common.TypePublish)
		if got.MessageID != want.MessageID {
			t.Fatalf("mesajul %d: primit %s, așteptat %s", i+1, got.Payload, want.Payload)
		}
	}
	b.mu.Lock()
	left := len(b.queues["receiver-1"])
	b.mu.Unlock()
	if left != 0 {
		t.Fatalf("coada trebuia golită, mai are %d mesaje", left)
	}
}

func TestInvalidMessagesAreRejected(t *testing.T) {
	_, addr := startBroker(t, nil)
	c := dial(t, addr)

	files := []string{"invalid-type.xml", "invalid-missing-topic.xml", "invalid-not-utc.xml", "invalid-version.xml"}
	for _, f := range files {
		data, err := os.ReadFile("../contracts/examples/" + f)
		if err != nil {
			t.Fatal(err)
		}
		_ = c.SendRaw([]byte(strings.ReplaceAll(string(data), "\n", "")))
		errMsg := expect(t, c, common.TypeError)
		t.Logf("%s -> ERROR: %s", f, errMsg.Payload)
	}
	_ = c.SendRaw([]byte("asta nu e xml"))
	expect(t, c, common.TypeError)

	// după mesajele respinse, conexiunea funcționează în continuare
	publish(t, c, "news", "valid")
}

func TestBrokerOnlyAcceptsSubscribeAndPublish(t *testing.T) {
	_, addr := startBroker(t, nil)
	c := dial(t, addr)
	_ = c.Send(common.NewMessage(common.TypeAck, "", "sender-1", "news", ""))
	expect(t, c, common.TypeError)
}

func TestHealth(t *testing.T) {
	b, _ := startBroker(t, nil)
	h := b.HealthHandler()

	for path, wantStatus := range map[string]string{"/health/live": "up", "/health/ready": "degraded"} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest("GET", path, nil))
		var body map[string]any
		_ = json.Unmarshal(rec.Body.Bytes(), &body)
		if rec.Code != 200 || body["status"] != wantStatus {
			t.Errorf("%s: cod %d, status %v (așteptat %s)", path, rec.Code, body["status"], wantStatus)
		}
	}
}

func TestMulticastBroadcast(t *testing.T) {
	group := common.Env("MULTICAST_ADDR", "239.0.0.1:9999")
	recv, err := common.NewMulticastReceiver(group)
	if err != nil {
		t.Skipf("multicast indisponibil pe această mașină: %v", err)
	}
	defer recv.Close()
	mcast, err := common.NewMulticastSender(group)
	if err != nil {
		t.Skipf("multicast indisponibil: %v", err)
	}
	_, addr := startBroker(t, mcast)

	got := make(chan *common.Message, 1)
	go func() {
		if m, err := recv.Receive(); err == nil {
			got <- m
		}
	}()

	sent := publish(t, dial(t, addr), MulticastTopic, "pentru toți")
	select {
	case m := <-got:
		if m.MessageID != sent.MessageID {
			t.Fatalf("alt mesaj pe multicast: %+v", m)
		}
	case <-time.After(2 * time.Second):
		// cel mai des, firewall-ul (de ex. ufw) blochează datagrama: sudo ufw allow 9999/udp
		msg := "niciun mesaj pe grupul " + group + " (firewall? încearcă: sudo ufw allow 9999/udp)"
		if os.Getenv("MULTICAST_REQUIRED") == "1" {
			t.Fatal(msg)
		}
		t.Skip(msg)
	}
}
