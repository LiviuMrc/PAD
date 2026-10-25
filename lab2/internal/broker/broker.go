// Package broker conține logica brokerului (rutare pe topic, cozi per receiver,
// confirmări, deduplicare, persistență) și serverul gRPC care o expune.
// Același cod e folosit de broker/main.go și de teste.
package broker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"lab2/common"
	"lab2/gen/brokerpb"
)

// Config are valorile implicite folosite când un câmp e 0.
type Config struct {
	MaxQueue    int           // mesaje neconfirmate per receiver (implicit 1000)
	DedupSize   int           // câte messageId-uri ține minte pentru deduplicare (implicit 10000)
	ReceiverTTL time.Duration // după cât timp offline e uitat un receiver (0 = niciodată)
	Journal     *Journal      // nil = stare doar în memorie
	Now         func() time.Time
}

// De ce s-a închis o sesiune Subscribe (cauza contextului ei).
var (
	errReplaced     = errors.New("alt Subscribe cu același receiver_id a preluat coada")
	errUnsubscribed = errors.New("receiverul s-a dezabonat")
	errShutdown     = errors.New("brokerul se oprește")
)

type entry struct {
	seq uint64 // ordinea globală a publicării (pentru snapshot)
	msg *brokerpb.Message
}

// receiver ține abonarea și coada unui receiver. Un mesaj rămâne în queue până la Ack.
// sent = câte mesaje de la începutul cozii a trimis sesiunea curentă; o sesiune nouă
// pornește de la 0, deci retrimite tot ce nu a fost confirmat.
type receiver struct {
	topics   []string
	queue    []entry
	sent     int
	notify   chan struct{}           // al sesiunii curente; trezește bucla de livrare
	session  int                     // crește la fiecare Subscribe
	cancel   context.CancelCauseFunc // închide sesiunea curentă (nil = offline)
	lastSeen time.Time               // când a fost văzut ultima dată online
}

// Broker implementează serviciul gRPC din contracts/broker.proto.
// gRPC rulează fiecare apel în propria goroutine, iar mu protejează toată starea.
// Fiecare schimbare de stare e scrisă întâi în jurnal (dacă există), apoi aplicată în memorie.
type Broker struct {
	brokerpb.UnimplementedBrokerServer

	cfg Config
	log *slog.Logger

	mu        sync.Mutex
	subs      map[string]map[string]bool // topic -> receiverId-urile abonate
	receivers map[string]*receiver
	seen      *common.Recent[int32] // messageId -> subscribers, pentru deduplicare
	seq       uint64
	closed    bool
	stop      chan struct{}
}

// New creează brokerul și, dacă are jurnal, reface starea din el.
func New(cfg Config, log *slog.Logger) (*Broker, error) {
	if cfg.MaxQueue <= 0 {
		cfg.MaxQueue = 1000
	}
	if cfg.DedupSize <= 0 {
		cfg.DedupSize = 10000
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	b := &Broker{
		cfg:       cfg,
		log:       log,
		subs:      map[string]map[string]bool{},
		receivers: map[string]*receiver{},
		seen:      common.NewRecent[int32](cfg.DedupSize),
		stop:      make(chan struct{}),
	}
	if cfg.Journal != nil {
		recs, err := cfg.Journal.Load()
		if err != nil {
			return nil, err
		}
		for i, r := range recs {
			if err := b.apply(r); err != nil {
				return nil, fmt.Errorf("jurnal, înregistrarea %d: %w", i+1, err)
			}
		}
		now := cfg.Now()
		queued := 0
		for _, r := range b.receivers {
			r.lastSeen = now // TTL-ul se numără de la pornire, nu de la căderea brokerului
			queued += len(r.queue)
		}
		log.Info("stare refăcută din jurnal", "event", "journal.replay", "records", len(recs),
			"receivers", len(b.receivers), "queued", queued, "path", cfg.Journal.Path())
	}
	if cfg.ReceiverTTL > 0 {
		go b.janitor()
	}
	return b, nil
}

// ---- operațiile aplicate în memorie (aceleași la rulare și la refacerea din jurnal) ----

func (b *Broker) apply(r record) error {
	switch r.Op {
	case opSubscribe:
		b.applySubscribe(r.Receiver, r.Topics)
	case opPublish:
		m, err := r.message()
		if err != nil {
			return err
		}
		b.applyPublish(m, r.To, r.Subscribers)
	case opAck:
		b.applyAck(r.Receiver, r.MessageID)
	case opUnsubscribe:
		b.applyUnsubscribe(r.Receiver)
	case opSeen:
		for i, id := range r.Seen {
			b.seen.Add(id, r.SeenSubscribers[i])
		}
	default:
		return fmt.Errorf("operație necunoscută %q", r.Op)
	}
	return nil
}

// applySubscribe înlocuiește topicurile receiverului. Mesajele din coadă de pe topicurile
// renunțate sunt șterse. Întoarce câte au fost șterse.
func (b *Broker) applySubscribe(id string, topics []string) int {
	r := b.receivers[id]
	if r == nil {
		r = &receiver{lastSeen: b.cfg.Now()}
		b.receivers[id] = r
	}
	for _, t := range r.topics {
		if !slices.Contains(topics, t) {
			b.removeSub(t, id)
		}
	}
	r.topics = dedupTopics(topics)
	for _, t := range r.topics {
		if b.subs[t] == nil {
			b.subs[t] = map[string]bool{}
		}
		b.subs[t][id] = true
	}
	purged := 0
	kept := r.queue[:0]
	for i, e := range r.queue {
		if slices.Contains(r.topics, e.msg.Topic) {
			kept = append(kept, e)
			continue
		}
		purged++
		if i < r.sent {
			r.sent--
		}
	}
	clear(r.queue[len(kept):])
	r.queue = kept
	return purged
}

// applyPublish pune mesajul în coada fiecărui receiver din to. Întoarce, pe receiver,
// messageId-ul eliminat când coada a depășit MaxQueue.
func (b *Broker) applyPublish(m *brokerpb.Message, to []string, subscribers int32) map[string]string {
	b.seq++
	b.seen.Add(m.MessageId, subscribers)
	var dropped map[string]string
	for _, id := range to {
		r := b.receivers[id]
		if r == nil {
			continue
		}
		r.queue = append(r.queue, entry{seq: b.seq, msg: m})
		if len(r.queue) > b.cfg.MaxQueue {
			if dropped == nil {
				dropped = map[string]string{}
			}
			dropped[id] = r.queue[0].msg.MessageId
			r.queue[0] = entry{}
			r.queue = r.queue[1:]
			if r.sent > 0 {
				r.sent--
			}
		}
	}
	return dropped
}

func (b *Broker) applyAck(id, messageID string) bool {
	r := b.receivers[id]
	if r == nil {
		return false
	}
	for i, e := range r.queue {
		if e.msg.MessageId == messageID {
			r.queue = slices.Delete(r.queue, i, i+1)
			if i < r.sent {
				r.sent--
			}
			return true
		}
	}
	return false
}

func (b *Broker) applyUnsubscribe(id string) int {
	r := b.receivers[id]
	if r == nil {
		return 0
	}
	for _, t := range r.topics {
		b.removeSub(t, id)
	}
	delete(b.receivers, id)
	return len(r.queue)
}

func (b *Broker) removeSub(topic, id string) {
	delete(b.subs[topic], id)
	if len(b.subs[topic]) == 0 {
		delete(b.subs, topic)
	}
}

func dedupTopics(topics []string) []string {
	out := slices.Clone(topics)
	sort.Strings(out)
	return slices.Compact(out)
}

// commit scrie înregistrarea în jurnal. Se apelează cu mu luat, înainte de apply,
// ca ordinea din jurnal să fie aceeași cu cea din memorie. După apply urmează compact.
func (b *Broker) commit(r record) error {
	if b.cfg.Journal == nil {
		return nil
	}
	if err := b.cfg.Journal.Append(r); err != nil {
		b.log.Error("scriere în jurnal eșuată", "event", "journal.append", "result", "failed", "error", err.Error())
		return status.Error(codes.Unavailable, "brokerul nu poate salva starea")
	}
	return nil
}

// compact rescrie jurnalul ca snapshot, dacă a crescut mult. Se apelează cu mu luat,
// după apply: snapshot-ul trebuie să includă și operația tocmai scrisă.
func (b *Broker) compact() {
	if b.cfg.Journal == nil {
		return
	}
	if b.cfg.Journal.NeedsCompaction(b.liveSize()) {
		if err := b.cfg.Journal.Rewrite(b.snapshot()); err != nil {
			b.log.Error("compactarea jurnalului eșuată", "event", "journal.compact", "result", "failed", "error", err.Error())
		} else {
			b.log.Info("jurnal compactat", "event", "journal.compact", "result", "ok", "records", b.cfg.Journal.Records())
		}
	}
}

// ---- RPC-urile ----

// Publish validează mesajul și îl pune în coada fiecărui abonat la topic.
func (b *Broker) Publish(ctx context.Context, req *brokerpb.PublishRequest) (*brokerpb.PublishReply, error) {
	start := time.Now()
	m := req.GetMessage()
	if err := common.Validate(m); err != nil {
		b.log.Warn("mesaj respins", "event", "message.rejected", "result", "rejected",
			"correlationId", m.GetCorrelationId(), "messageId", m.GetMessageId(), "error", err.Error())
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, status.Error(codes.Unavailable, errShutdown.Error())
	}
	// clientul a renunțat deja (deadline sau anulare): mesajul nu se mai pune în cozi,
	// altfel senderul ar vedea o eroare pentru un mesaj totuși livrat
	if err := ctx.Err(); err != nil {
		b.log.Warn("mesaj ignorat: termenul apelului a expirat", "event", "message.expired", "result", "rejected",
			"correlationId", m.CorrelationId, "messageId", m.MessageId, "error", err.Error())
		return nil, status.FromContextError(err).Err()
	}
	reply := &brokerpb.PublishReply{MessageId: m.MessageId, CorrelationId: m.CorrelationId}
	if n, dup := b.seen.Get(m.MessageId); dup {
		reply.Subscribers, reply.Duplicate = n, true
		b.log.Info("mesaj duplicat, nu se pune din nou în cozi", "event", "publish", "result", "duplicate",
			"correlationId", m.CorrelationId, "messageId", m.MessageId)
		return reply, nil
	}

	to := make([]string, 0, len(b.subs[m.Topic]))
	for id := range b.subs[m.Topic] {
		to = append(to, id)
	}
	sort.Strings(to)
	reply.Subscribers = int32(len(to))
	rec, err := publishRecord(m, to, reply.Subscribers)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if err := b.commit(rec); err != nil {
		return nil, err
	}
	dropped := b.applyPublish(m, to, reply.Subscribers)
	b.compact()
	for id, droppedID := range dropped {
		b.log.Warn("coada plină, se elimină cel mai vechi mesaj", "event", "queue.overflow",
			"receiver", id, "droppedMessageId", droppedID)
	}
	for _, id := range to {
		select { // trezește sesiunea receiverului, dacă e online
		case b.receivers[id].notify <- struct{}{}:
		default:
		}
	}

	b.log.Info("mesaj publicat", "event", "publish", "result", "ok",
		"correlationId", m.CorrelationId, "messageId", m.MessageId, "topic", m.Topic,
		"subscribers", len(to), "duration_ms", time.Since(start).Milliseconds())
	return reply, nil
}

// Subscribe înregistrează receiverul și îi trimite prin stream mesajele neconfirmate,
// apoi pe cele noi, până se închide sesiunea.
func (b *Broker) Subscribe(req *brokerpb.SubscribeRequest, stream grpc.ServerStreamingServer[brokerpb.Message]) error {
	if err := common.ValidateSubscribe(req); err != nil {
		b.log.Warn("abonare respinsă", "event", "subscribe", "result", "rejected",
			"correlationId", req.CorrelationId, "error", err.Error())
		return status.Error(codes.InvalidArgument, err.Error())
	}
	id := req.ReceiverId
	ctx, cancel := context.WithCancelCause(stream.Context())
	defer cancel(nil)

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return status.Error(codes.Unavailable, errShutdown.Error())
	}
	if err := b.commit(record{Op: opSubscribe, Receiver: id, Topics: req.Topics}); err != nil {
		b.mu.Unlock()
		return err
	}
	purged := b.applySubscribe(id, req.Topics)
	b.compact()
	r := b.receivers[id]
	if r.cancel != nil {
		r.cancel(errReplaced) // același receiver s-a reconectat: sesiunea veche se închide
	}
	r.session++
	session := r.session
	r.cancel = cancel
	r.sent = 0 // tot ce nu e confirmat se retrimite
	r.notify = make(chan struct{}, 1)
	notify := r.notify
	queued, topics := len(r.queue), r.topics
	b.mu.Unlock()

	defer b.offline(id, session)
	b.log.Info("receiver abonat", "event", "subscribe", "result", "ok", "correlationId", req.CorrelationId,
		"receiver", id, "topics", topics, "queued", queued, "purged", purged, "session", session)

	// header-ul trimis imediat confirmă abonarea (echivalentul ACK din Lab 1)
	if err := stream.SendHeader(metadata.Pairs("subscribed", "true")); err != nil {
		return err
	}

	for {
		b.mu.Lock()
		// doar sesiunea curentă avansează cursorul; una înlocuită nu mai ia mesaje
		if ctx.Err() == nil && r.session == session && r.sent < len(r.queue) {
			m := r.queue[r.sent].msg
			r.sent++
			b.mu.Unlock()
			if err := stream.Send(m); err != nil {
				// mesajul rămâne în coadă (neconfirmat) și se retrimite la următoarea sesiune
				b.log.Warn("livrare eșuată, mesajul rămâne în coadă", "event", "deliver", "result", "failed",
					"receiver", id, "messageId", m.MessageId, "error", err.Error())
				return err
			}
			b.log.Info("mesaj livrat", "event", "deliver", "result", "ok", "receiver", id,
				"correlationId", m.CorrelationId, "messageId", m.MessageId)
			continue
		}
		b.mu.Unlock()
		select {
		case <-notify:
		case <-ctx.Done():
			return b.sessionEnded(ctx, id, session)
		}
	}
}

// sessionEnded transformă cauza închiderii sesiunii în codul gRPC din contract.
func (b *Broker) sessionEnded(ctx context.Context, id string, session int) error {
	cause := context.Cause(ctx)
	reason := "disconnected"
	var err error
	switch {
	case errors.Is(cause, errReplaced):
		reason, err = "replaced", status.Error(codes.Canceled, "sesiune închisă: "+cause.Error())
	case errors.Is(cause, errUnsubscribed):
		reason, err = "unsubscribed", status.Error(codes.Canceled, "sesiune închisă: "+cause.Error())
	case errors.Is(cause, errShutdown):
		reason, err = "shutdown", status.Error(codes.Unavailable, cause.Error())
	default:
		// clientul a închis conexiunea; nimeni nu mai citește răspunsul
		err = status.FromContextError(ctx.Err()).Err()
	}
	b.log.Info("sesiune închisă", "event", "session.end", "receiver", id, "session", session, "reason", reason)
	return err
}

func (b *Broker) offline(id string, session int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	r := b.receivers[id]
	if r == nil || r.session != session {
		return // receiverul s-a dezabonat sau s-a conectat între timp o sesiune nouă
	}
	r.cancel = nil
	r.lastSeen = b.cfg.Now()
	b.log.Info("receiver offline, mesajele neconfirmate rămân în coadă", "event", "receiver.offline",
		"receiver", id, "queued", len(r.queue))
}

// Ack scoate mesajul confirmat din coada receiverului.
func (b *Broker) Ack(_ context.Context, req *brokerpb.AckRequest) (*brokerpb.AckReply, error) {
	if err := errors.Join(common.ValidateID("receiverId", req.ReceiverId), common.ValidateID("messageId", req.MessageId)); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	r := b.receivers[req.ReceiverId]
	if r == nil {
		return nil, status.Errorf(codes.NotFound, "receiver necunoscut: %q", req.ReceiverId)
	}
	if !slices.ContainsFunc(r.queue, func(e entry) bool { return e.msg.MessageId == req.MessageId }) {
		return &brokerpb.AckReply{Removed: false}, nil // ACK repetat: nimic de făcut
	}
	if err := b.commit(record{Op: opAck, Receiver: req.ReceiverId, MessageID: req.MessageId}); err != nil {
		return nil, err
	}
	b.applyAck(req.ReceiverId, req.MessageId)
	b.compact()
	b.log.Info("mesaj confirmat", "event", "ack", "result", "ok", "receiver", req.ReceiverId, "messageId", req.MessageId)
	return &brokerpb.AckReply{Removed: true}, nil
}

// Unsubscribe șterge abonarea și coada receiverului.
func (b *Broker) Unsubscribe(_ context.Context, req *brokerpb.UnsubscribeRequest) (*brokerpb.UnsubscribeReply, error) {
	if err := common.ValidateID("receiverId", req.ReceiverId); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.receivers[req.ReceiverId] == nil {
		return nil, status.Errorf(codes.NotFound, "receiver necunoscut: %q", req.ReceiverId)
	}
	dropped, err := b.remove(req.ReceiverId, errUnsubscribed)
	if err != nil {
		return nil, err
	}
	b.log.Info("receiver dezabonat", "event", "unsubscribe", "result", "ok", "correlationId", req.CorrelationId,
		"receiver", req.ReceiverId, "dropped", dropped)
	return &brokerpb.UnsubscribeReply{Dropped: int32(dropped)}, nil
}

// remove șterge receiverul (cu mu luat) și îi închide sesiunea activă.
func (b *Broker) remove(id string, cause error) (int, error) {
	if err := b.commit(record{Op: opUnsubscribe, Receiver: id}); err != nil {
		return 0, err
	}
	if r := b.receivers[id]; r.cancel != nil {
		r.cancel(cause)
	}
	dropped := b.applyUnsubscribe(id)
	b.compact()
	return dropped, nil
}

// ---- expirarea receiverelor și oprirea ----

// Expire uită receiverele offline de mai mult de ReceiverTTL. Întoarce câte au fost șterse.
func (b *Broker) Expire() int {
	if b.cfg.ReceiverTTL <= 0 {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.cfg.Now()
	ids := make([]string, 0)
	for id, r := range b.receivers {
		if r.cancel == nil && now.Sub(r.lastSeen) > b.cfg.ReceiverTTL {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	n := 0
	for _, id := range ids {
		dropped, err := b.remove(id, errUnsubscribed)
		if err != nil {
			break
		}
		n++
		b.log.Info("receiver expirat: offline peste TTL, coada lui e ștearsă", "event", "receiver.expired",
			"receiver", id, "dropped", dropped, "ttl", b.cfg.ReceiverTTL.String())
	}
	return n
}

func (b *Broker) janitor() {
	every := min(max(b.cfg.ReceiverTTL/4, 10*time.Millisecond), time.Minute)
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			b.Expire()
		case <-b.stop:
			return
		}
	}
}

// Close refuză apelurile noi și închide toate sesiunile Subscribe cu UNAVAILABLE,
// ca receiverele să se reconecteze după repornire.
func (b *Broker) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return
	}
	b.closed = true
	close(b.stop)
	for _, r := range b.receivers {
		if r.cancel != nil {
			r.cancel(errShutdown)
		}
	}
}

// ---- inspecție (teste, log-uri) ----

// Online întoarce câte receivere au o sesiune activă.
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

// Pending întoarce câte mesaje neconfirmate are receiverul (-1 = receiver necunoscut).
func (b *Broker) Pending(id string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	if r := b.receivers[id]; r != nil {
		return len(r.queue)
	}
	return -1
}
