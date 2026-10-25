package proxy

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"
)

// backend e o instanță DW. Starea e citită și scrisă din goroutine-uri diferite (cereri,
// health checker), deci folosește doar operații atomice.
type backend struct {
	url      *url.URL
	healthy  atomic.Bool
	active   atomic.Int64 // cereri în curs (pentru least-connections)
	served   atomic.Int64
	failures atomic.Int64
}

func newBackends(raw []string) ([]*backend, error) {
	var out []*backend
	for _, r := range raw {
		u, err := url.Parse(r)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return nil, fmt.Errorf("backend invalid: %q", r)
		}
		b := &backend{url: u}
		b.healthy.Store(true)
		out = append(out, b)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("niciun backend DW configurat")
	}
	return out, nil
}

// balancer alege backend-ul pentru o cerere; skip conține backend-urile deja încercate.
type balancer interface {
	pick(bs []*backend, skip map[*backend]bool) *backend
	name() string
}

func newBalancer(algorithm string) (balancer, error) {
	switch algorithm {
	case "", "round-robin":
		return &roundRobin{}, nil
	case "least-connections":
		return &leastConnections{}, nil
	}
	return nil, fmt.Errorf("algoritm necunoscut %q (round-robin sau least-connections)", algorithm)
}

// roundRobin trece pe rând prin backend-urile sănătoase.
type roundRobin struct{ next atomic.Uint64 }

func (*roundRobin) name() string { return "round-robin" }

func (rr *roundRobin) pick(bs []*backend, skip map[*backend]bool) *backend {
	start := rr.next.Add(1) - 1
	for _, wantHealthy := range []bool{true, false} { // dacă niciunul nu e sănătos, încearcă oricum
		for i := 0; i < len(bs); i++ {
			b := bs[(start+uint64(i))%uint64(len(bs))]
			if !skip[b] && b.healthy.Load() == wantHealthy {
				return b
			}
		}
	}
	return nil
}

// leastConnections alege backend-ul sănătos cu cele mai puține cereri în curs.
type leastConnections struct{ rr roundRobin }

func (*leastConnections) name() string { return "least-connections" }

func (lc *leastConnections) pick(bs []*backend, skip map[*backend]bool) *backend {
	var best *backend
	start := lc.rr.next.Add(1) - 1 // la egalitate, pe rând
	for i := 0; i < len(bs); i++ {
		b := bs[(start+uint64(i))%uint64(len(bs))]
		if skip[b] || !b.healthy.Load() {
			continue
		}
		if best == nil || b.active.Load() < best.active.Load() {
			best = b
		}
	}
	if best == nil {
		return lc.rr.pick(bs, skip)
	}
	return best
}

// healthLoop verifică periodic GET /health pe fiecare backend și îl scoate din rotație
// cât timp nu răspunde 200.
func (p *Proxy) healthLoop(ctx context.Context) {
	t := time.NewTicker(p.cfg.HealthInterval)
	defer t.Stop()
	for {
		p.checkAll(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (p *Proxy) checkAll(ctx context.Context) {
	for _, b := range p.backends {
		ok := p.check(ctx, b)
		if was := b.healthy.Swap(ok); was != ok {
			if ok {
				p.log.Info("backend revenit în rotație", "event", "backend.up", "backend", b.url.Host)
			} else {
				p.log.Warn("backend scos din rotație", "event", "backend.down", "backend", b.url.Host, "reason", "health check")
			}
		}
	}
}

func (p *Proxy) check(ctx context.Context, b *backend) bool {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, b.url.JoinPath("/health").String(), nil)
	resp, err := p.transport.RoundTrip(req)
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}
