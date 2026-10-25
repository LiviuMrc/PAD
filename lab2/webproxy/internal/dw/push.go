package dw

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"lab2/webproxy/internal/model"
	"lab2/webproxy/internal/store"
)

// Pusher trimite modificările (POST/DELETE) spre nodurile sursă abonate (metoda PUSH):
// DW inițiază cererea HTTP spre sursă. Trimiterea e asincronă, ca răspunsul către client
// să nu aștepte; fiecare notificare se reîncearcă de cel mult 3 ori. Dacă o notificare se
// pierde, sursa o primește oricum prin pull (GET /update/employees/).
type Pusher struct {
	store  *store.Store
	dw     string
	log    *slog.Logger
	client *http.Client
	queue  chan model.Change
	wg     sync.WaitGroup

	mu      sync.Mutex // protejează lista de abonați ținută în memorie
	subs    []model.Subscription
	fetched time.Time

	sent, failed atomic.Int64
}

const pushWorkers = 4

func NewPusher(st *store.Store, dw string, log *slog.Logger) *Pusher {
	p := &Pusher{store: st, dw: dw, log: log, queue: make(chan model.Change, 1000),
		client: &http.Client{Timeout: 3 * time.Second}}
	for i := 0; i < pushWorkers; i++ {
		p.wg.Add(1)
		go p.worker()
	}
	return p
}

// Notify pune modificarea în coada de trimitere (fără să blocheze cererea HTTP).
func (p *Pusher) Notify(ch model.Change) {
	select {
	case p.queue <- ch:
	default:
		p.failed.Add(1)
		p.log.Warn("coada push plină, notificarea rămâne doar în feed-ul pull", "event", "push.dropped", "seq", ch.Seq)
	}
}

// Invalidate forțează recitirea abonaților din Cassandra (după o abonare nouă pe această instanță).
func (p *Pusher) Invalidate() {
	p.mu.Lock()
	p.fetched = time.Time{}
	p.mu.Unlock()
}

// Close termină notificările din coadă și oprește workerii.
func (p *Pusher) Close() {
	close(p.queue)
	p.wg.Wait()
}

// subscribers citește abonații din Cassandra cel mult o dată la 2 secunde: abonările făcute
// pe altă instanță DW ajung aici în cel mult 2 secunde.
func (p *Pusher) subscribers() []model.Subscription {
	p.mu.Lock()
	defer p.mu.Unlock()
	if time.Since(p.fetched) > 2*time.Second {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		subs, err := p.store.Subscribers(ctx)
		cancel()
		if err == nil {
			p.subs, p.fetched = subs, time.Now()
		}
	}
	return p.subs
}

func (p *Pusher) worker() {
	defer p.wg.Done()
	for ch := range p.queue {
		for _, sub := range p.subscribers() {
			p.deliver(sub, ch)
		}
	}
}

func (p *Pusher) deliver(sub model.Subscription, ch model.Change) {
	f := sub.Format
	if f == "" {
		f = model.JSON
	}
	var body bytes.Buffer
	model.Encode(&body, f, &ch)
	backoff := 200 * time.Millisecond
	for attempt := 1; attempt <= 3; attempt++ {
		req, _ := http.NewRequest(http.MethodPost, sub.URL, bytes.NewReader(body.Bytes()))
		req.Header.Set("Content-Type", f.ContentType())
		req.Header.Set("X-DW", p.dw)
		resp, err := p.client.Do(req)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode < 300 {
				p.sent.Add(1)
				p.log.Info("notificare push trimisă", "event", "push.sent", "url", sub.URL, "seq", ch.Seq,
					"id", ch.ID, "op", ch.Op, "attempt", attempt)
				return
			}
			err = &statusError{resp.StatusCode}
		}
		p.log.Warn("notificare push eșuată", "event", "push.retry", "url", sub.URL, "seq", ch.Seq,
			"attempt", attempt, "error", err.Error())
		time.Sleep(backoff)
		backoff *= 2
	}
	p.failed.Add(1)
}

type statusError struct{ code int }

func (e *statusError) Error() string { return "răspuns HTTP " + http.StatusText(e.code) }
