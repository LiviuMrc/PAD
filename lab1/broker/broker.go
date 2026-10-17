package main

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"lab1/common"
)

// MulticastTopic: mesajele publicate pe acest topic sunt trimise prin UDP multicast,
// nu prin cozile TCP.
const MulticastTopic = "broadcast"

// MaxQueue limitează coada unui receiver offline, ca memoria să nu crească la nesfârșit.
const MaxQueue = 1000

type Broker struct {
	mu        sync.Mutex
	subs      map[string]map[string]bool      // topic -> receiverId-urile abonate
	queues    map[string][]*common.Message    // receiverId -> mesaje încă nelivrate
	online    map[string]*common.TCPTransport // receiverId -> conexiunea activă
	multicast common.Transport                // nil dacă multicastul nu e disponibil
	log       *slog.Logger
	listener  net.Listener
}

func NewBroker(log *slog.Logger, multicast common.Transport) *Broker {
	return &Broker{
		subs:      map[string]map[string]bool{},
		queues:    map[string][]*common.Message{},
		online:    map[string]*common.TCPTransport{},
		multicast: multicast,
		log:       log,
	}
}

// Listen pornește serverul TCP și întoarce adresa pe care ascultă.
func (b *Broker) Listen(addr string) (string, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return "", err
	}
	b.listener = ln
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return // listener închis
			}
			go b.handleConn(common.NewTCPTransport(conn))
		}
	}()
	b.log.Info("broker pornit", "event", "broker.started", "addr", ln.Addr().String())
	return ln.Addr().String(), nil
}

func (b *Broker) Close() error {
	if b.listener == nil {
		return nil
	}
	return b.listener.Close()
}

func (b *Broker) handleConn(t *common.TCPTransport) {
	defer b.disconnect(t)
	for {
		m, err := t.Receive()
		var invalid *common.InvalidMessageError
		if errors.As(err, &invalid) {
			b.reject(t, m, invalid.Reason)
			continue
		}
		if err != nil {
			return
		}

		switch m.MessageType {
		case common.TypeSubscribe:
			b.reply(t, m, common.TypeAck, m.MessageID)
			b.subscribe(m, t)
		case common.TypePublish:
			b.publish(m)
			b.reply(t, m, common.TypeAck, m.MessageID)
		default:
			b.reject(t, m, "brokerul acceptă doar SUBSCRIBE și PUBLISH")
		}
	}
}

func (b *Broker) subscribe(m *common.Message, t *common.TCPTransport) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.subs[m.Topic] == nil {
		b.subs[m.Topic] = map[string]bool{}
	}
	b.subs[m.Topic][m.Sender] = true
	b.online[m.Sender] = t
	b.log.Info("receiver abonat", "event", "subscribe", "result", "ok",
		"correlationId", m.CorrelationID, "messageId", m.MessageID,
		"receiver", m.Sender, "topic", m.Topic, "queued", len(b.queues[m.Sender]))
	b.flush(m.Sender)
}

// publish pune mesajul în coada fiecărui abonat la topic și încearcă livrarea imediată.
func (b *Broker) publish(m *common.Message) {
	start := time.Now()
	if m.Topic == MulticastTopic && b.multicast != nil {
		err := b.multicast.Send(m)
		b.log.Info("mesaj trimis prin multicast", "event", "publish.multicast", "result", result(err),
			"correlationId", m.CorrelationID, "messageId", m.MessageID, "error", errText(err))
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	for receiver := range b.subs[m.Topic] {
		q := append(b.queues[receiver], m)
		if len(q) > MaxQueue {
			b.log.Warn("coada plină, se elimină cel mai vechi mesaj", "event", "queue.overflow",
				"receiver", receiver, "droppedMessageId", q[0].MessageID)
			q = q[1:]
		}
		b.queues[receiver] = q
		b.flush(receiver)
	}
	b.log.Info("mesaj publicat", "event", "publish", "result", "ok",
		"correlationId", m.CorrelationID, "messageId", m.MessageID, "topic", m.Topic,
		"subscribers", len(b.subs[m.Topic]), "duration_ms", time.Since(start).Milliseconds())
}

// flush trimite coada unui receiver dacă e online. Se apelează cu b.mu blocat.
func (b *Broker) flush(receiver string) {
	t := b.online[receiver]
	if t == nil {
		return
	}
	q := b.queues[receiver]
	for len(q) > 0 {
		if err := t.Send(q[0]); err != nil {
			b.log.Warn("livrare eșuată, receiverul trece offline", "event", "deliver", "result", "failed",
				"receiver", receiver, "messageId", q[0].MessageID, "error", err.Error())
			delete(b.online, receiver)
			t.Close()
			break
		}
		b.log.Info("mesaj livrat", "event", "deliver", "result", "ok", "receiver", receiver,
			"correlationId", q[0].CorrelationID, "messageId", q[0].MessageID)
		q = q[1:]
	}
	b.queues[receiver] = q
}

func (b *Broker) disconnect(t *common.TCPTransport) {
	t.Close()
	b.mu.Lock()
	defer b.mu.Unlock()
	for receiver, conn := range b.online {
		if conn == t {
			delete(b.online, receiver)
			b.log.Info("receiver deconectat, mesajele noi rămân în coadă", "event", "receiver.offline",
				"receiver", receiver)
		}
	}
}

func (b *Broker) reject(t *common.TCPTransport, m *common.Message, reason string) {
	correlationID, messageID := "", ""
	if m != nil {
		correlationID, messageID = m.CorrelationID, m.MessageID
	}
	b.log.Warn("mesaj respins", "event", "message.rejected", "result", "rejected",
		"correlationId", correlationID, "messageId", messageID, "error", reason)
	b.reply(t, m, common.TypeError, reason)
}

// reply trimite ACK/ERROR cu același correlationId ca solicitarea.
func (b *Broker) reply(t *common.TCPTransport, req *common.Message, msgType, payload string) {
	correlationID, topic := "", "broker"
	if req != nil {
		correlationID = req.CorrelationID
		if req.Topic != "" && req.Validate() == nil {
			topic = req.Topic
		}
	}
	resp := common.NewMessage(msgType, correlationID, "broker", topic, payload)
	if resp.Validate() != nil { // correlationId primit era invalid
		resp.CorrelationID = common.NewID()
	}
	_ = t.Send(resp)
}

// HealthHandler: /health/live = procesul rulează; /health/ready = acceptă trafic,
// cu status "degraded" dacă multicastul (dependența UDP) nu e disponibil.
// Doar citește starea, nu modifică nimic.
func (b *Broker) HealthHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health/live", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"status": "up"})
	})
	mux.HandleFunc("/health/ready", func(w http.ResponseWriter, _ *http.Request) {
		b.mu.Lock()
		online, topics := len(b.online), len(b.subs)
		b.mu.Unlock()
		status, code := "ready", http.StatusOK
		if b.listener == nil {
			status, code = "starting", http.StatusServiceUnavailable
		} else if b.multicast == nil {
			status = "degraded"
		}
		writeJSON(w, code, map[string]any{
			"status": status, "onlineReceivers": online, "topics": topics,
			"multicast": b.multicast != nil,
		})
	})
	return mux
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func result(err error) string {
	if err != nil {
		return "failed"
	}
	return "ok"
}

func errText(err error) string {
	if err != nil {
		return err.Error()
	}
	return ""
}
