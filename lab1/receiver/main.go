// Receiver: se abonează la unul sau mai multe topicuri și afișează mesajele primite.
// Dacă pierde conexiunea, se reconectează de cel mult RECONNECT_MAX ori, la interval fix.
// La reconectare, brokerul îi trimite mesajele strânse în coadă cât a fost offline.
//
//	go run ./receiver -id receiver-1 -topics news
//	go run ./receiver -id receiver-2 -topics news,alerts -multicast
package main

import (
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"lab1/common"
)

func main() {
	addr := flag.String("addr", common.Env("BROKER_ADDR", "127.0.0.1:7000"), "adresa brokerului")
	id := flag.String("id", "receiver-1", "identificator stabil (cheia cozii din broker)")
	topics := flag.String("topics", "news", "topicuri separate prin virgulă")
	multicast := flag.Bool("multicast", false, "ascultă și grupul UDP multicast")
	mcastAddr := flag.String("multicast-addr", common.Env("MULTICAST_ADDR", "239.0.0.1:9999"), "grupul multicast")
	maxRetries := flag.Int("retries", common.EnvInt("RECONNECT_MAX", 5), "număr maxim de reconectări")
	interval := flag.Duration("interval", time.Duration(common.EnvInt("RECONNECT_INTERVAL_SEC", 2))*time.Second, "interval între reconectări")
	flag.Parse()

	log := common.NewLogger("receiver").With("receiver", *id)

	if *multicast {
		go listenMulticast(*mcastAddr, log)
	}

	attempts := 0
	for {
		err := session(*addr, *id, strings.Split(*topics, ","), log)
		if err == nil {
			attempts = 0 // sesiunea a mers, deci contorul de reconectări pornește iar de la zero
		}
		attempts++
		if attempts > *maxRetries {
			log.Error("renunț la reconectare", "event", "reconnect", "result", "gave_up", "attempts", *maxRetries)
			os.Exit(1)
		}
		log.Warn("conexiune pierdută, reîncerc", "event", "reconnect", "attempt", attempts,
			"max", *maxRetries, "in", interval.String())
		time.Sleep(*interval)
	}
}

// session se conectează, se abonează și citește mesaje până se pierde conexiunea.
// Întoarce eroare doar dacă nu s-a putut conecta.
func session(addr, id string, topics []string, log *slog.Logger) error {
	t, err := common.DialTCP(addr)
	if err != nil {
		log.Warn("brokerul nu răspunde", "event", "connect", "result", "failed", "error", err.Error())
		return err
	}
	defer t.Close()
	log.Info("conectat la broker", "event", "connect", "result", "ok", "addr", addr)

	for _, topic := range topics {
		if err := t.Send(common.NewMessage(common.TypeSubscribe, "", id, strings.TrimSpace(topic), "")); err != nil {
			return nil
		}
	}

	for {
		m, err := t.Receive()
		var invalid *common.InvalidMessageError
		if errors.As(err, &invalid) {
			log.Warn("mesaj invalid ignorat", "event", "receive", "result", "invalid", "error", invalid.Reason)
			continue
		}
		if err != nil {
			return nil
		}
		show(m, "tcp", log)
	}
}

func listenMulticast(addr string, log *slog.Logger) {
	u, err := common.NewMulticastReceiver(addr)
	if err != nil {
		log.Warn("nu pot intra în grupul multicast", "event", "multicast.join", "result", "failed", "error", err.Error())
		return
	}
	log.Info("în grupul multicast", "event", "multicast.join", "result", "ok", "group", addr)
	for {
		m, err := u.Receive()
		if err != nil {
			log.Warn("datagramă invalidă", "event", "multicast.receive", "error", err.Error())
			continue
		}
		show(m, "udp-multicast", log)
	}
}

func show(m *common.Message, via string, log *slog.Logger) {
	switch m.MessageType {
	case common.TypePublish:
		fmt.Printf(">>> [%s] %s de la %s: %s\n", via, m.Topic, m.Sender, m.Payload)
		log.Info("mesaj primit", "event", "receive", "result", "ok", "via", via,
			"correlationId", m.CorrelationID, "messageId", m.MessageID, "topic", m.Topic)
	case common.TypeAck:
		log.Info("abonare confirmată", "event", "subscribe", "result", "ack", "topic", m.Topic, "correlationId", m.CorrelationID)
	case common.TypeError:
		log.Warn("eroare de la broker", "event", "broker.error", "correlationId", m.CorrelationID, "error", m.Payload)
	}
}
