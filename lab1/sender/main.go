// Sender: publică mesaje pe un topic și așteaptă ACK/ERROR de la broker.
//
//	go run ./sender -topic news -payload "salut"         un singur mesaj
//	go run ./sender -topic news                          citește mesajele din stdin, câte unul pe linie
//	go run ./sender -raw contracts/examples/invalid.xml  trimite un fișier XML așa cum e (demo validare)
//	go run ./sender -topic news -payload x -json         afișează și varianta JSON a mesajului
package main

import (
	"bufio"
	"bytes"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"lab1/common"
)

const replyTimeout = 3 * time.Second

func main() {
	addr := flag.String("addr", common.Env("BROKER_ADDR", "127.0.0.1:7000"), "adresa brokerului")
	id := flag.String("id", "sender-1", "identificatorul senderului")
	topic := flag.String("topic", "news", "topicul")
	payload := flag.String("payload", "", "conținutul mesajului (gol = citește din stdin)")
	raw := flag.String("raw", "", "fișier XML trimis neschimbat")
	showJSON := flag.Bool("json", false, "afișează mesajul și serializat în JSON")
	flag.Parse()

	log := common.NewLogger("sender")
	t, err := common.DialTCP(*addr)
	if err != nil {
		log.Error("nu mă pot conecta la broker", "event", "connect", "result", "failed", "addr", *addr, "error", err.Error())
		os.Exit(1)
	}
	defer t.Close()

	if *raw != "" {
		data, err := os.ReadFile(*raw)
		if err != nil {
			log.Error("nu pot citi fișierul", "event", "raw.read", "error", err.Error())
			os.Exit(1)
		}
		// pe TCP un mesaj ocupă o linie, așa că se elimină newline-urile din fișier
		data = bytes.ReplaceAll(bytes.ReplaceAll(data, []byte("\r"), nil), []byte("\n"), nil)
		if err := t.SendRaw(data); err != nil {
			log.Error("trimitere eșuată", "event", "raw.send", "error", err.Error())
			os.Exit(1)
		}
		log.Info("XML trimis neschimbat", "event", "raw.sent", "file", *raw)
		waitReply(t, log, "")
		return
	}

	publish := func(text string) bool {
		m := common.NewMessage(common.TypePublish, "", *id, *topic, text)
		if *showJSON {
			x, _ := m.ToXML()
			j, _ := m.ToJSON()
			fmt.Printf("XML : %s\nJSON: %s\n", x, j)
		}
		start := time.Now()
		if err := t.Send(m); err != nil {
			log.Error("trimitere eșuată", "event", "publish", "result", "failed",
				"correlationId", m.CorrelationID, "messageId", m.MessageID, "error", err.Error())
			return false
		}
		ok := waitReply(t, log, m.CorrelationID)
		log.Info("publicare terminată", "event", "publish", "result", map[bool]string{true: "ack", false: "error"}[ok],
			"correlationId", m.CorrelationID, "messageId", m.MessageID, "topic", *topic,
			"duration_ms", time.Since(start).Milliseconds())
		return ok
	}

	if *payload != "" {
		if !publish(*payload) {
			os.Exit(1)
		}
		return
	}

	fmt.Fprintf(os.Stderr, "Scrie mesaje pentru topicul %q (Ctrl+D pentru ieșire):\n", *topic)
	in := bufio.NewScanner(os.Stdin)
	for in.Scan() {
		if line := in.Text(); line != "" {
			publish(line)
		}
	}
}

// waitReply așteaptă ACK sau ERROR, cel mult replyTimeout (deadline explicit).
func waitReply(t *common.TCPTransport, log *slog.Logger, correlationID string) bool {
	t.SetReadTimeout(replyTimeout)
	defer t.SetReadTimeout(0)
	reply, err := t.Receive()
	var invalid *common.InvalidMessageError
	switch {
	case errors.As(err, &invalid):
		log.Warn("răspuns invalid de la broker", "event", "reply", "error", invalid.Reason)
		return false
	case err != nil:
		log.Warn("niciun răspuns de la broker", "event", "reply", "result", "timeout", "error", err.Error())
		return false
	case correlationID != "" && reply.CorrelationID != correlationID:
		log.Warn("răspuns cu alt correlationId", "event", "reply", "expected", correlationID, "got", reply.CorrelationID)
		return false
	case reply.MessageType == common.TypeError:
		log.Warn("brokerul a respins mesajul", "event", "reply", "result", "error",
			"correlationId", reply.CorrelationID, "error", reply.Payload)
		return false
	}
	log.Info("ACK primit", "event", "reply", "result", "ack", "correlationId", reply.CorrelationID, "ackFor", reply.Payload)
	return true
}
