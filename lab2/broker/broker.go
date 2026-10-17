package main

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"lab2/common"
	"lab2/gen/brokerpb"
)

// MaxQueue limitează coada unui receiver offline, ca memoria să nu crească la nesfârșit.
const MaxQueue = 1000

// receiver ține coada unui receiver. notify îl trezește pe Subscribe când apare un mesaj nou.
type receiver struct {
	queue   []*brokerpb.Message
	notify  chan struct{}
	session int                // crește la fiecare Subscribe; sesiunea veche se oprește
	cancel  context.CancelFunc // oprește sesiunea curentă (nil = offline)
}

// Broker implementează serviciul gRPC din contracts/broker.proto.
// gRPC rulează fiecare apel în propria goroutine (procesare concurentă),
// iar mu protejează starea comună.
type Broker struct {
	brokerpb.UnimplementedBrokerServer

	mu        sync.Mutex
	subs      map[string]map[string]bool // topic -> receiverId-urile abonate
	receivers map[string]*receiver       // receiverId -> coada lui
	log       *slog.Logger
}

func NewBroker(log *slog.Logger) *Broker {
	return &Broker{
		subs:      map[string]map[string]bool{},
		receivers: map[string]*receiver{},
		log:       log,
	}
}

// Publish validează mesajul și îl pune în coada fiecărui abonat la topic.
func (b *Broker) Publish(_ context.Context, req *brokerpb.PublishRequest) (*brokerpb.PublishReply, error) {
	start := time.Now()
	m := req.GetMessage()
	if err := common.Validate(m); err != nil {
		b.log.Warn("mesaj respins", "event", "message.rejected", "result", "rejected",
			"correlationId", m.GetCorrelationId(), "messageId", m.GetMessageId(), "error", err.Error())
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	b.mu.Lock()
	n := 0
	for id := range b.subs[m.Topic] {
		r := b.receivers[id]
		r.queue = append(r.queue, m)
		if len(r.queue) > MaxQueue {
			b.log.Warn("coada plină, se elimină cel mai vechi mesaj", "event", "queue.overflow",
				"receiver", id, "droppedMessageId", r.queue[0].MessageId)
			r.queue = r.queue[1:]
		}
		select { // trezește sesiunea receiverului, dacă e online
		case r.notify <- struct{}{}:
		default:
		}
		n++
	}
	b.mu.Unlock()

	b.log.Info("mesaj publicat", "event", "publish", "result", "ok",
		"correlationId", m.CorrelationId, "messageId", m.MessageId, "topic", m.Topic,
		"subscribers", n, "duration_ms", time.Since(start).Milliseconds())
	return &brokerpb.PublishReply{MessageId: m.MessageId, CorrelationId: m.CorrelationId, Subscribers: int32(n)}, nil
}

// Subscribe înregistrează receiverul și îi trimite prin stream mesajele din coadă,
// apoi pe cele noi, până când clientul închide conexiunea.
func (b *Broker) Subscribe(req *brokerpb.SubscribeRequest, stream grpc.ServerStreamingServer[brokerpb.Message]) error {
	if err := common.ValidateSubscribe(req); err != nil {
		b.log.Warn("abonare respinsă", "event", "subscribe", "result", "rejected",
			"correlationId", req.CorrelationId, "error", err.Error())
		return status.Error(codes.InvalidArgument, err.Error())
	}
	id := req.ReceiverId
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	b.mu.Lock()
	r := b.receivers[id]
	if r == nil {
		r = &receiver{notify: make(chan struct{}, 1)}
		b.receivers[id] = r
	}
	if r.cancel != nil {
		r.cancel() // același receiver s-a reconectat: sesiunea veche se închide
	}
	r.session++
	session := r.session
	r.cancel = cancel
	for _, t := range req.Topics {
		if b.subs[t] == nil {
			b.subs[t] = map[string]bool{}
		}
		b.subs[t][id] = true
	}
	queued := len(r.queue)
	b.mu.Unlock()

	defer b.offline(id, session)
	b.log.Info("receiver abonat", "event", "subscribe", "result", "ok", "correlationId", req.CorrelationId,
		"receiver", id, "topics", req.Topics, "queued", queued)

	// header-ul trimis imediat confirmă abonarea (echivalentul ACK din Lab 1)
	if err := stream.SendHeader(metadata.Pairs("subscribed", "true")); err != nil {
		return err
	}

	for {
		b.mu.Lock()
		if len(r.queue) == 0 {
			b.mu.Unlock()
			select {
			case <-r.notify:
				continue
			case <-ctx.Done():
				// fie clientul a închis conexiunea, fie s-a conectat altă sesiune cu același id
				return status.Error(codes.Canceled, "sesiune închisă: alt Subscribe cu același receiver_id")
			}
		}
		m := r.queue[0]
		r.queue = r.queue[1:]
		b.mu.Unlock()

		if err := stream.Send(m); err != nil {
			// livrarea a eșuat: mesajul revine în fața cozii pentru reconectare
			b.mu.Lock()
			r.queue = append([]*brokerpb.Message{m}, r.queue...)
			b.mu.Unlock()
			b.log.Warn("livrare eșuată, mesajul rămâne în coadă", "event", "deliver", "result", "failed",
				"receiver", id, "messageId", m.MessageId, "error", err.Error())
			return err
		}
		b.log.Info("mesaj livrat", "event", "deliver", "result", "ok", "receiver", id,
			"correlationId", m.CorrelationId, "messageId", m.MessageId)
	}
}

func (b *Broker) offline(id string, session int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	r := b.receivers[id]
	if r.session != session {
		return // între timp s-a conectat o sesiune nouă
	}
	r.cancel = nil
	b.log.Info("receiver deconectat, mesajele noi rămân în coadă", "event", "receiver.offline",
		"receiver", id, "queued", len(r.queue))
}

// Online întoarce câte receivere au o sesiune activă (folosit în teste).
func (b *Broker) Online() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := 0
	for _, r := range b.receivers {
		if r.cancel != nil {
			n++
		}
	}
	return n
}
