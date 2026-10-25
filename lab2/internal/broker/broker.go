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
	DLQMax      int           // mesaje în coada dead-letter (implicit 10000); peste, se elimină cel mai vechi
	Journal     *Journal      // nil = stare doar în memorie
	Now         func() time.Time
}

// De ce s-a închis o sesiune Subscribe (cauza contextului ei).
var (
	errReplaced     = errors.New("alt Subscribe cu același receiver_id a preluat coada")
	errUnsubscribed = errors.New("receiverul s-a dezabonat")
	errShutdown     = errors.New("brokerul se oprește")
)

// Motivele pentru care un mesaj ajunge în DLQ.
const (
	reasonOverflow = "queue_overflow"   // coada receiverului a depășit MaxQueue
	reasonExpired  = "receiver_expired" // receiverul a stat offline mai mult de ReceiverTTL
)

// deadLetter e un mesaj care nu mai putea fi livrat, păstrat în DLQ în loc să fie șters.
type deadLetter struct {
	msg      *brokerpb.Message
	receiver string
	reason   string
	at       time.Time
}

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
	dlq       []deadLetter          // în ordinea intrării, cel mult DLQMax
	seq       uint64
	replaying bool // în timpul refacerii din jurnal nu se loghează evenimentele vechi
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
	if cfg.DLQMax <= 0 {
		cfg.DLQMax = 10000
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
		b.replaying = true
		for i, r := range recs {
			if err := b.apply(r); err != nil {
				return nil, fmt.Errorf("jurnal, înregistrarea %d: %w", i+1, err)
			}
		}
		b.replaying = false
		now := cfg.Now()
		queued := 0
		for _, r := range b.receivers {
			r.lastSeen = now // TTL-ul se numără de la pornire, nu de la căderea brokerului
			queued += len(r.queue)
		}
		log.Info("stare refăcută din jurnal", "event", "journal.replay", "records", len(recs),
			"receivers", len(b.receivers), "queued", queued, "deadLetters", len(b.dlq), "path", cfg.Journal.Path())
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
		b.applyPublish(m, r.To, r.Subscribers, r.at(b.cfg.Now))
	case opAck:
		b.applyAck(r.Receiver, r.MessageID)
	case opUnsubscribe:
		b.applyUnsubscribe(r.Receiver)
	case opExpire:
		b.applyExpire(r.Receiver, r.at(b.cfg.Now))
	case opDead:
		m, err := r.message()
		if err != nil {
			return err
		}
		b.toDLQ(deadLetter{msg: m, receiver: r.Receiver, reason: r.Reason, at: r.at(b.cfg.Now)})
	case opRedrive:
		b.applyRedrive(r.Receiver, r.MessageIDs, r.at(b.cfg.Now))
	case opPurge:
		b.applyPurge(r.Receiver, r.MessageIDs)
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

// applyPublish pune mesajul în coada fiecărui receiver din to. Când o coadă depășește
// MaxQueue, cel mai vechi mesaj din ea e mutat în DLQ.
func (b *Broker) applyPublish(m *brokerpb.Message, to []string, subscribers int32, at time.Time) {
	b.seq++
	b.seen.Add(m.MessageId, subscribers)
	for _, id := range to {
		r := b.receivers[id]
		if r == nil {
			continue
		}
		r.queue = append(r.queue, entry{seq: b.seq, msg: m})
		b.trimQueue(id, r, at)
	}
}

// trimQueue mută în DLQ cele mai vechi mesaje cât timp coada depășește MaxQueue.
func (b *Broker) trimQueue(id string, r *receiver, at time.Time) {
	for len(r.queue) > b.cfg.MaxQueue {
		old := r.queue[0].msg
		r.queue[0] = entry{}
		r.queue = r.queue[1:]
		if r.sent > 0 {
			r.sent--
		}
		b.toDLQ(deadLetter{msg: old, receiver: id, reason: reasonOverflow, at: at})
	}
}

// toDLQ adaugă un mesaj în DLQ. Peste DLQMax, cel mai vechi mesaj din DLQ e eliminat
// definitiv: e singurul loc unde brokerul mai pierde un mesaj și e logat ca eroare.
func (b *Broker) toDLQ(d deadLetter) {
	b.dlq = append(b.dlq, d)
	if !b.replaying {
		b.log.Warn("mesaj mutat în DLQ", "event", "dlq.add", "reason", d.reason, "receiver", d.receiver,
			"correlationId", d.msg.CorrelationId, "messageId", d.msg.MessageId, "deadLetters", len(b.dlq))
	}
	if len(b.dlq) > b.cfg.DLQMax {
		ev := b.dlq[0]
		b.dlq[0] = deadLetter{}
		b.dlq = b.dlq[1:]
		if !b.replaying {
			b.log.Error("DLQ plină, cel mai vechi mesaj din DLQ e eliminat definitiv", "event", "dlq.evict",
				"receiver", ev.receiver, "messageId", ev.msg.MessageId, "reason", ev.reason, "dlqMax", b.cfg.DLQMax)
		}
	}
}

// applyAck scoate mesajul din coada receiverului. Dacă mesajul fusese deja trimis și apoi
// mutat în DLQ, receiverul l-a procesat totuși, deci e scos și din DLQ.
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
	if i := b.findDead(id, messageID); i >= 0 {
		b.dlq = slices.Delete(b.dlq, i, i+1)
		return true
	}
	return false
}

func (b *Broker) findDead(receiverID, messageID string) int {
	return slices.IndexFunc(b.dlq, func(d deadLetter) bool {
		return d.receiver == receiverID && d.msg.MessageId == messageID
	})
}

// applyExpire mută coada receiverului în DLQ și îl șterge.
func (b *Broker) applyExpire(id string, at time.Time) int {
	r := b.receivers[id]
	if r == nil {
		return 0
	}
	for _, e := range r.queue {
		b.toDLQ(deadLetter{msg: e.msg, receiver: id, reason: reasonExpired, at: at})
	}
	return b.applyUnsubscribe(id)
}

// redrivable întoarce messageId-urile din DLQ ale receiverului care pot fi retrimise
// (din ids, sau toate dacă ids e gol) și câte rămân pentru că topicul lor nu mai e abonat.
func (b *Broker) redrivable(id string, ids []string) (ok []string, skipped int) {
	r := b.receivers[id]
	want := idSet(ids)
	for _, d := range b.dlq {
		if d.receiver != id || (want != nil && !want[d.msg.MessageId]) {
			continue
		}
		if slices.Contains(r.topics, d.msg.Topic) {
			ok = append(ok, d.msg.MessageId)
		} else {
			skipped++
		}
	}
	return ok, skipped
}

// applyRedrive mută mesajele din DLQ la finalul cozii receiverului, în ordinea din DLQ.
func (b *Broker) applyRedrive(id string, ids []string, at time.Time) int {
	r := b.receivers[id]
	if r == nil {
		return 0
	}
	n := 0
	want := idSet(ids)
	kept := b.dlq[:0]
	var moved []*brokerpb.Message
	for _, d := range b.dlq {
		if d.receiver == id && want[d.msg.MessageId] {
			moved = append(moved, d.msg)
			continue
		}
		kept = append(kept, d)
	}
	clear(b.dlq[len(kept):])
	b.dlq = kept
	for _, m := range moved {
		b.seq++
		r.queue = append(r.queue, entry{seq: b.seq, msg: m})
		n++
	}
	b.trimQueue(id, r, at) // dacă acum coada depășește limita, cele mai vechi trec iar în DLQ
	return n
}

// idSet face din ids un set, ca filtrarea DLQ să fie O(n), nu O(n×m); nil = fără filtru.
func idSet(ids []string) map[string]bool {
	if len(ids) == 0 {
		return nil
	}
	set := make(map[string]bool, len(ids))
	for _, id := range ids {
		set[id] = true
	}
	return set
}

// applyPurge șterge din DLQ mesajele receiverului (gol = toate receiverele) din ids (gol = toate).
func (b *Broker) applyPurge(id string, ids []string) int {
	want := idSet(ids)
	kept := b.dlq[:0]
	for _, d := range b.dlq {
		if (id == "" || d.receiver == id) && (want == nil || want[d.msg.MessageId]) {
			continue
		}
		kept = append(kept, d)
	}
	n := len(b.dlq) - len(kept)
	clear(b.dlq[len(kept):])
	b.dlq = kept
	return n
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
	now := b.cfg.Now()
	rec, err := publishRecord(m, to, reply.Subscribers, now)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if err := b.commit(rec); err != nil {
		return nil, err
	}
	b.applyPublish(m, to, reply.Subscribers, now) // la coadă plină, cel mai vechi mesaj trece în DLQ
	b.compact()
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
	if !slices.ContainsFunc(r.queue, func(e entry) bool { return e.msg.MessageId == req.MessageId }) &&
		b.findDead(req.ReceiverId, req.MessageId) < 0 {
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

// ---- DLQ ----

func validateIDs(ids []string) error {
	for _, id := range ids {
		if err := common.ValidateID("messageId", id); err != nil {
			return err
		}
	}
	return nil
}

func (d deadLetter) proto() *brokerpb.DeadLetter {
	return &brokerpb.DeadLetter{Message: d.msg, ReceiverId: d.receiver, Reason: d.reason,
		DeadAt: d.at.UTC().Format(time.RFC3339)}
}

// ListDeadLetters întoarce mesajele din DLQ, cele mai vechi primele.
func (b *Broker) ListDeadLetters(_ context.Context, req *brokerpb.ListDeadLettersRequest) (*brokerpb.ListDeadLettersReply, error) {
	if req.ReceiverId != "" {
		if err := common.ValidateID("receiverId", req.ReceiverId); err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	reply := &brokerpb.ListDeadLettersReply{}
	for _, d := range b.dlq {
		if req.ReceiverId != "" && d.receiver != req.ReceiverId {
			continue
		}
		reply.Total++
		if req.Limit <= 0 || len(reply.DeadLetters) < int(req.Limit) {
			reply.DeadLetters = append(reply.DeadLetters, d.proto())
		}
	}
	return reply, nil
}

// RedriveDeadLetters mută mesaje din DLQ înapoi în coada receiverului.
func (b *Broker) RedriveDeadLetters(_ context.Context, req *brokerpb.RedriveRequest) (*brokerpb.RedriveReply, error) {
	if err := errors.Join(common.ValidateID("receiverId", req.ReceiverId), validateIDs(req.MessageIds)); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	r := b.receivers[req.ReceiverId]
	if r == nil {
		return nil, status.Errorf(codes.FailedPrecondition,
			"receiverul %q nu e abonat; abonează-l întâi (Subscribe), apoi reîncearcă", req.ReceiverId)
	}
	ids, skipped := b.redrivable(req.ReceiverId, req.MessageIds)
	reply := &brokerpb.RedriveReply{Skipped: int32(skipped)}
	if len(ids) > 0 {
		now := b.cfg.Now()
		if err := b.commit(record{Op: opRedrive, Receiver: req.ReceiverId, MessageIDs: ids, At: now.UTC().Format(time.RFC3339Nano)}); err != nil {
			return nil, err
		}
		reply.Redriven = int32(b.applyRedrive(req.ReceiverId, ids, now))
		b.compact()
		select { // trezește sesiunea receiverului, dacă e online
		case r.notify <- struct{}{}:
		default:
		}
	}
	b.log.Info("mesaje retrimise din DLQ", "event", "dlq.redrive", "receiver", req.ReceiverId,
		"redriven", reply.Redriven, "skipped", reply.Skipped, "deadLetters", len(b.dlq))
	return reply, nil
}

// PurgeDeadLetters șterge definitiv mesaje din DLQ.
func (b *Broker) PurgeDeadLetters(_ context.Context, req *brokerpb.PurgeDeadLettersRequest) (*brokerpb.PurgeDeadLettersReply, error) {
	if req.ReceiverId != "" {
		if err := common.ValidateID("receiverId", req.ReceiverId); err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
	}
	if err := validateIDs(req.MessageIds); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.commit(record{Op: opPurge, Receiver: req.ReceiverId, MessageIDs: req.MessageIds}); err != nil {
		return nil, err
	}
	n := b.applyPurge(req.ReceiverId, req.MessageIds)
	b.compact()
	b.log.Info("mesaje șterse din DLQ", "event", "dlq.purge", "receiver", req.ReceiverId, "purged", n,
		"deadLetters", len(b.dlq))
	return &brokerpb.PurgeDeadLettersReply{Purged: int32(n)}, nil
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
		if err := b.commit(record{Op: opExpire, Receiver: id, At: now.UTC().Format(time.RFC3339Nano)}); err != nil {
			break
		}
		queued := b.applyExpire(id, now) // mesajele neconfirmate trec în DLQ
		b.compact()
		n++
		b.log.Info("receiver expirat: offline peste TTL, mesajele lui sunt mutate în DLQ", "event", "receiver.expired",
			"receiver", id, "deadLettered", queued, "ttl", b.cfg.ReceiverTTL.String())
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

// DeadLetters întoarce câte mesaje sunt în DLQ.
func (b *Broker) DeadLetters() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.dlq)
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
