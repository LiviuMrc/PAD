package broker

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"

	"lab2/common"
	"lab2/gen/brokerpb"
)

// Persistență: cozile, confirmările, abonările și deduplicarea supraviețuiesc
// repornirii brokerului (proces nou, același director de date).
func TestStateSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	cfg := ServerConfig{DataDir: dir, Fsync: true}

	first := start(t, cfg)
	r1 := subscribe(t, first.conn, "receiver-1", "news")
	subscribe(t, first.conn, "receiver-2", "news", "alerts").cancel()
	waitOnline(t, first.b, 1)
	var news []*brokerpb.Message
	for i := 1; i <= 3; i++ {
		news = append(news, publish(t, first.conn, "news", fmt.Sprintf("news %d", i)))
	}
	alert := publish(t, first.conn, "alerts", "alertă")
	ack(t, first.conn, "receiver-1", r1.recv(t)) // receiver-1 confirmă doar primul mesaj
	r1.recv(t)                                   // al doilea e primit, dar neconfirmat
	first.stop()

	second := start(t, cfg)
	if !strings.Contains(second.logs.String(), `"event":"journal.replay","records":`) {
		t.Fatal("brokerul nu a refăcut starea din jurnal")
	}
	if p1, p2 := second.b.Pending("receiver-1"), second.b.Pending("receiver-2"); p1 != 2 || p2 != 4 {
		t.Fatalf("după repornire: receiver-1=%d (așteptat 2), receiver-2=%d (așteptat 4)", p1, p2)
	}
	again1 := subscribe(t, second.conn, "receiver-1", "news")
	for _, want := range news[1:] {
		if got := again1.recv(t); got.MessageId != want.MessageId || got.Payload != want.Payload {
			t.Fatalf("receiver-1: primit %q, așteptat %q", got.Payload, want.Payload)
		}
	}
	again2 := subscribe(t, second.conn, "receiver-2", "news", "alerts")
	for _, want := range append(news, alert) {
		if got := again2.recv(t); got.MessageId != want.MessageId {
			t.Fatalf("receiver-2: primit %q, așteptat %q", got.Payload, want.Payload)
		}
	}
	if reply := publishMsg(t, second.conn, news[0]); !reply.Duplicate {
		t.Fatal("deduplicarea nu a supraviețuit repornirii")
	}
}

// Un broker oprit brusc (kill -9) poate lăsa ultima linie scrisă pe jumătate.
// Ea e tăiată la pornire, iar restul jurnalului rămâne valid.
func TestTornTailIsTrimmed(t *testing.T) {
	dir := t.TempDir()
	cfg := ServerConfig{DataDir: dir, Fsync: true}
	first := start(t, cfg)
	subscribe(t, first.conn, "r", "news").cancel()
	waitOnline(t, first.b, 0)
	kept := publish(t, first.conn, "news", "complet")
	first.stop()

	path := dir + "/broker.journal"
	good, _ := os.ReadFile(path)
	f, _ := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	f.WriteString(`{"op":"pub","msg":{"messageId":"jumătate`) // fără '\n': scriere întreruptă
	f.Close()

	second := start(t, cfg)
	if !strings.Contains(second.logs.String(), "a fost tăiată") {
		t.Fatal("lipsește avertismentul despre linia tăiată")
	}
	now, _ := os.ReadFile(path)
	if !bytes.HasPrefix(now, good) || bytes.Contains(now, []byte("jumătate")) {
		t.Fatal("jurnalul nu a fost tăiat corect")
	}
	if got := subscribe(t, second.conn, "r", "news").recv(t); got.MessageId != kept.MessageId {
		t.Fatalf("primit %q", got.Payload)
	}
}

func TestCorruptJournalIsAnError(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(dir+"/broker.journal", []byte("{\"op\":\"sub\",\"receiver\":\"r\",\"topics\":[\"news\"]}\nnu e json\n{\"op\":\"ack\"}\n"), 0o644)
	_, err := NewServer(ServerConfig{DataDir: dir}, slog.New(slog.NewJSONHandler(io.Discard, nil)))
	if err == nil || !strings.Contains(err.Error(), ":2: înregistrare coruptă") {
		t.Fatalf("eroare: %v, așteptat corupere la linia 2", err)
	}
}

// Compactarea: după multe operații jurnalul e rescris ca snapshot mic, iar starea
// refăcută din snapshot e aceeași.
func TestCompactionKeepsState(t *testing.T) {
	dir := t.TempDir()
	cfg := ServerConfig{DataDir: dir, Fsync: false}
	first := start(t, cfg)
	first.srv.journal.compactAt = 50

	a := subscribe(t, first.conn, "a", "news")
	subscribe(t, first.conn, "b", "alerts").cancel()
	eventually(t, "b offline", func() bool { return first.b.Online() == 1 })
	var pending []*brokerpb.Message // ce rămâne neconfirmat la a
	for i := 0; i < 100; i++ {
		m := publish(t, first.conn, "news", fmt.Sprintf("m%d", i))
		got := a.recv(t)
		if i%10 == 0 {
			pending = append(pending, m)
			continue
		}
		ack(t, first.conn, "a", got)
	}
	alert := publish(t, first.conn, "alerts", "pentru b")
	if !strings.Contains(first.logs.String(), `"event":"journal.compact","result":"ok"`) {
		t.Fatal("jurnalul nu a fost compactat")
	}
	first.stop()

	lines := bytes.Count(must(os.ReadFile(dir+"/broker.journal")), []byte("\n"))
	if lines >= 50 {
		t.Fatalf("jurnalul are %d linii după compactare", lines)
	}
	t.Logf("jurnal: %d linii pentru 1 + 100*2 + 1 operații", lines)

	second := start(t, cfg)
	if p := second.b.Pending("a"); p != len(pending) {
		t.Fatalf("a: %d neconfirmate, așteptat %d", p, len(pending))
	}
	ra := subscribe(t, second.conn, "a", "news")
	for _, want := range pending {
		if got := ra.recv(t); got.MessageId != want.MessageId {
			t.Fatalf("a: primit %q, așteptat %q", got.Payload, want.Payload)
		}
	}
	if p := second.b.Pending("b"); p != 1 {
		t.Fatalf("b: %d neconfirmate, așteptat 1", p)
	}
	if got := subscribe(t, second.conn, "b", "alerts").recv(t); got.MessageId != alert.MessageId {
		t.Fatalf("b: primit %q, așteptat alerta", got.Payload)
	}
	if !publishMsg(t, second.conn, pending[0]).Duplicate {
		t.Fatal("deduplicarea s-a pierdut la compactare")
	}
	if publishMsg(t, second.conn, common.NewMessage("", "s", "news", "nou")).Duplicate {
		t.Fatal("mesaj nou marcat duplicat")
	}
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}
