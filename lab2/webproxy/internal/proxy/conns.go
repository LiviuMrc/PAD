package proxy

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// connRegistry e partea de smart proxy: ține evidența conexiunilor cu clienții în Redis
// (cine e conectat, de când, câte cereri a trimis prin aceeași conexiune keep-alive, la ce
// DW a mers ultima cerere). Evidența e comună tuturor instanțelor proxy.
//
//	proxy:conns          setul id-urilor conexiunilor deschise
//	proxy:conn:<id>      hash: proxy, remote, openedAt, state, requests, lastRequestAt
//	proxy:stats          connsOpened / connsClosed
type connRegistry struct {
	rdb   *redis.Client
	proxy string
	log   *slog.Logger
	seq   atomic.Uint64
	ids   sync.Map // net.Conn -> id
}

type ctxKey struct{}

const (
	keyConns = "proxy:conns"
	connTTL  = 10 * time.Minute // dacă proxy-ul cade, intrările expiră singure
)

func connKey(id string) string { return "proxy:conn:" + id }

// context e apelat de http.Server o dată pe conexiune, înainte de StateNew: conexiunea
// primește un id, disponibil și în cererile ei.
func (cr *connRegistry) context(ctx context.Context, c net.Conn) context.Context {
	id := fmt.Sprintf("%s-%d-%d", cr.proxy, time.Now().UnixMilli(), cr.seq.Add(1))
	cr.ids.Store(c, id)
	return context.WithValue(ctx, ctxKey{}, id)
}

// state primește tranzițiile conexiunilor de la http.Server.
func (cr *connRegistry) state(c net.Conn, s http.ConnState) {
	v, ok := cr.ids.Load(c)
	if !ok {
		return
	}
	id := v.(string)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	pipe := cr.rdb.TxPipeline()
	switch s {
	case http.StateNew:
		pipe.HSet(ctx, connKey(id), "proxy", cr.proxy, "remote", c.RemoteAddr().String(),
			"openedAt", time.Now().UTC().Format(time.RFC3339Nano), "state", "new", "requests", 0)
		pipe.Expire(ctx, connKey(id), connTTL)
		pipe.SAdd(ctx, keyConns, id)
		pipe.HIncrBy(ctx, keyStats, "connsOpened", 1)
	case http.StateActive, http.StateIdle:
		pipe.HSet(ctx, connKey(id), "state", s.String())
	case http.StateClosed, http.StateHijacked:
		cr.ids.Delete(c)
		pipe.Del(ctx, connKey(id))
		pipe.SRem(ctx, keyConns, id)
		pipe.HIncrBy(ctx, keyStats, "connsClosed", 1)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		cr.log.Warn("evidența conexiunii în Redis a eșuat", "event", "conn.registry", "state", s.String(), "error", err.Error())
	}
}

// request înregistrează o cerere pe conexiunea ei și întoarce id-ul conexiunii.
func (cr *connRegistry) request(r *http.Request) string {
	id, _ := r.Context().Value(ctxKey{}).(string)
	if id == "" {
		return ""
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	pipe := cr.rdb.TxPipeline()
	pipe.HIncrBy(ctx, connKey(id), "requests", 1)
	pipe.HSet(ctx, connKey(id), "lastRequestAt", time.Now().UTC().Format(time.RFC3339Nano), "lastRequest", r.Method+" "+r.URL.RequestURI())
	pipe.Expire(ctx, connKey(id), connTTL)
	pipe.Exec(ctx)
	return id
}

// Conn e o conexiune din evidență (pentru /_proxy/connections).
type Conn struct {
	ID            string `json:"id" xml:"id"`
	Proxy         string `json:"proxy" xml:"proxy"`
	Remote        string `json:"remote" xml:"remote"`
	OpenedAt      string `json:"openedAt" xml:"openedAt"`
	State         string `json:"state" xml:"state"`
	Requests      int    `json:"requests" xml:"requests"`
	LastRequest   string `json:"lastRequest,omitempty" xml:"lastRequest,omitempty"`
	LastRequestAt string `json:"lastRequestAt,omitempty" xml:"lastRequestAt,omitempty"`
}

func (cr *connRegistry) list(ctx context.Context) ([]Conn, error) {
	ids, err := cr.rdb.SMembers(ctx, keyConns).Result()
	if err != nil {
		return nil, err
	}
	var out []Conn
	for _, id := range ids {
		h, err := cr.rdb.HGetAll(ctx, connKey(id)).Result()
		if err != nil {
			return nil, err
		}
		if len(h) == 0 { // expirată: proxy-ul care o deținea a căzut
			cr.rdb.SRem(ctx, keyConns, id)
			continue
		}
		n, _ := strconv.Atoi(h["requests"])
		out = append(out, Conn{ID: id, Proxy: h["proxy"], Remote: h["remote"], OpenedAt: h["openedAt"],
			State: h["state"], Requests: n, LastRequest: h["lastRequest"], LastRequestAt: h["lastRequestAt"]})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].OpenedAt < out[j].OpenedAt })
	return out, nil
}
