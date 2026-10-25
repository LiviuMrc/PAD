// Package proxy e reverse proxy-ul din fața nodurilor DW. Clienții (și nodurile de stocare)
// vorbesc doar cu proxy-ul, care:
//   - păstrează conexiunile cu clienții (keep-alive) și le ține evidența în Redis (smart proxy);
//   - folosește un pool de conexiuni keep-alive spre DW-uri;
//   - distribuie cererile între DW-uri (round-robin sau least-connections), ocolind
//     instanțele căzute (health checks + reîncercare pe alt backend);
//   - păstrează răspunsurile GET în cache (Redis), conform Cache-Control de la DW, și le
//     invalidează la scrieri (Cache-Tag).
package proxy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"lab2/webproxy/internal/model"
)

type Config struct {
	ID             string
	Backends       []string
	Algorithm      string        // round-robin (implicit) sau least-connections
	CacheTTL       time.Duration // limita superioară a TTL-ului din cache
	HealthInterval time.Duration
	Redis          *redis.Client
}

type Proxy struct {
	cfg       Config
	log       *slog.Logger
	backends  []*backend
	balancer  balancer
	transport *http.Transport
	cache     *cache
	conns     *connRegistry
	requests  atomic.Int64
}

func New(cfg Config, log *slog.Logger) (*Proxy, error) {
	if cfg.CacheTTL == 0 {
		cfg.CacheTTL = time.Minute
	}
	if cfg.HealthInterval == 0 {
		cfg.HealthInterval = 2 * time.Second
	}
	if cfg.Redis == nil {
		return nil, errors.New("proxy: lipsește clientul Redis")
	}
	bs, err := newBackends(cfg.Backends)
	if err != nil {
		return nil, err
	}
	bal, err := newBalancer(cfg.Algorithm)
	if err != nil {
		return nil, err
	}
	p := &Proxy{
		cfg: cfg, log: log, backends: bs, balancer: bal,
		// pool de conexiuni keep-alive spre DW-uri
		transport: &http.Transport{
			DialContext:           (&net.Dialer{Timeout: 2 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
			MaxIdleConns:          256,
			MaxIdleConnsPerHost:   64,
			IdleConnTimeout:       90 * time.Second,
			ResponseHeaderTimeout: 15 * time.Second,
		},
		cache: &cache{rdb: cfg.Redis, maxTTL: cfg.CacheTTL},
		conns: &connRegistry{rdb: cfg.Redis, proxy: cfg.ID, log: log},
	}
	return p, nil
}

// Run pornește health check-urile până la ctx.Done.
func (p *Proxy) Run(ctx context.Context) { p.healthLoop(ctx) }

// Server creează serverul HTTP al proxy-ului, cu evidența conexiunilor clienților.
func (p *Proxy) Server(addr string) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           p,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second, // conexiunile keep-alive cu clienții rămân deschise
		ConnState:         p.conns.state,
		ConnContext:       p.conns.context,
	}
}

var idempotent = map[string]bool{"GET": true, "HEAD": true, "PUT": true, "DELETE": true, "OPTIONS": true}

// hop-by-hop: header-e care privesc doar o conexiune și nu se transmit mai departe (RFC 7230).
var hopByHop = []string{"Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization",
	"Te", "Trailer", "Transfer-Encoding", "Upgrade", "Proxy-Connection"}

func removeHopByHop(h http.Header) {
	for _, f := range strings.Split(h.Get("Connection"), ",") {
		if f = strings.TrimSpace(f); f != "" {
			h.Del(f)
		}
	}
	for _, k := range hopByHop {
		h.Del(k)
	}
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p.requests.Add(1)
	start := time.Now()
	connID := p.conns.request(r)
	if strings.HasPrefix(r.URL.Path, "/_proxy/") {
		p.admin(w, r)
		return
	}
	w.Header().Set("X-Proxy", p.cfg.ID)
	status, cacheState, backendHost := p.handle(w, r)
	p.log.Info("cerere", "event", "proxy", "method", r.Method, "uri", r.URL.RequestURI(), "status", status,
		"cache", cacheState, "backend", backendHost, "conn", connID, "duration_ms", time.Since(start).Milliseconds())
}

func (p *Proxy) handle(w http.ResponseWriter, r *http.Request) (status int, cacheState, backendHost string) {
	ctx := r.Context()
	cacheable := r.Method == http.MethodGet
	var key string
	var gen int64
	skipLookup, skipStore := requestBypass(r)
	if cacheable {
		f, err := model.Negotiate(r)
		if err != nil {
			cacheable = false // DW va răspunde 406
		} else {
			key = cacheKey(r, f)
		}
	}
	if cacheable && !skipLookup {
		cr, err := p.cache.get(ctx, key)
		if err != nil {
			p.log.Warn("cache indisponibil, cererea merge la DW", "event", "cache.error", "error", err.Error())
		} else if cr != nil {
			p.cache.count(ctx, "hits")
			return p.serveCached(w, r, cr), "HIT", cr.Header.Get("X-Served-By")
		}
	}
	if cacheable {
		var err error
		if gen, err = p.cache.generation(ctx); err != nil {
			skipStore = true
		}
	}

	resp, b, err := p.forward(r)
	if err != nil {
		code := http.StatusBadGateway
		if errors.Is(err, errNoBackend) {
			code = http.StatusServiceUnavailable
		}
		p.fail(w, r, code, err.Error())
		return code, "", ""
	}
	defer resp.Body.Close()
	backendHost = b.url.Host
	removeHopByHop(resp.Header)
	resp.Header.Set("X-Backend", backendHost)

	// scrierile reușite invalidează intrările din cache marcate cu aceleași tag-uri
	if !cacheable && resp.StatusCode < 300 && r.Method != http.MethodHead {
		if tags := parseTags(resp.Header.Get("Cache-Tag")); len(tags) > 0 {
			n, err := p.cache.invalidate(ctx, tags)
			if err != nil {
				p.log.Error("invalidarea cache-ului a eșuat", "event", "cache.invalidate", "error", err.Error())
			} else {
				p.cache.count(ctx, "invalidations")
				p.log.Info("cache invalidat", "event", "cache.invalidate", "tags", tags, "deleted", n)
			}
		}
	}

	ttl := time.Duration(0)
	if cacheable && resp.StatusCode == http.StatusOK && !skipStore {
		ttl = p.cache.ttlFor(resp.Header)
	}
	if ttl == 0 {
		cacheState = "BYPASS"
		if cacheable {
			cacheState = "MISS"
			p.cache.count(ctx, "misses")
		}
		copyHeader(w.Header(), resp.Header)
		w.Header().Set("X-Cache", cacheState)
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
		return resp.StatusCode, cacheState, backendHost
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBody+1))
	if err != nil {
		p.fail(w, r, http.StatusBadGateway, "răspuns DW întrerupt: "+err.Error())
		return http.StatusBadGateway, "", backendHost
	}
	p.cache.count(ctx, "misses")
	if len(body) <= maxBody {
		h := resp.Header.Clone()
		h.Del("Date")
		cr := &cachedResponse{Status: resp.StatusCode, Header: h, Body: body, StoredAt: time.Now().UTC()}
		if stored, err := p.cache.store(ctx, key, gen, cr, ttl, parseTags(resp.Header.Get("Cache-Tag"))); err != nil {
			p.log.Warn("salvarea în cache a eșuat", "event", "cache.store", "error", err.Error())
		} else if stored {
			p.cache.count(ctx, "stores")
		} else {
			p.log.Info("răspuns nesalvat: cache invalidat în timpul cererii", "event", "cache.stale")
		}
	}
	copyHeader(w.Header(), resp.Header)
	w.Header().Set("X-Cache", "MISS")
	w.WriteHeader(resp.StatusCode)
	w.Write(body)
	return resp.StatusCode, "MISS", backendHost
}

func (p *Proxy) serveCached(w http.ResponseWriter, r *http.Request, cr *cachedResponse) int {
	copyHeader(w.Header(), cr.Header)
	w.Header().Set("X-Cache", "HIT")
	w.Header().Set("Age", strconv.Itoa(int(time.Since(cr.StoredAt).Seconds())))
	if etag := cr.Header.Get("ETag"); etag != "" && matchETag(r.Header.Get("If-None-Match"), etag) {
		w.Header().Del("Content-Length")
		w.Header().Del("Content-Type")
		w.WriteHeader(http.StatusNotModified)
		return http.StatusNotModified
	}
	w.WriteHeader(cr.Status)
	w.Write(cr.Body)
	return cr.Status
}

func matchETag(header, etag string) bool {
	for _, t := range strings.Split(header, ",") {
		if t = strings.TrimPrefix(strings.TrimSpace(t), "W/"); t == etag || t == "*" {
			return true
		}
	}
	return false
}

func copyHeader(dst, src http.Header) {
	for k, vs := range src {
		dst[k] = append([]string(nil), vs...)
	}
}

var errNoBackend = errors.New("niciun nod DW disponibil")

// forward trimite cererea la un DW ales de balancer. La o eroare de conexiune, backend-ul
// e scos din rotație, iar cererea se reia pe următorul: orice metodă dacă cererea nu a
// plecat (eroare la conectare), doar metodele idempotente dacă e posibil să fi ajuns la DW.
func (p *Proxy) forward(r *http.Request) (*http.Response, *backend, error) {
	var body []byte
	if r.Body != nil {
		var err error
		body, err = io.ReadAll(io.LimitReader(r.Body, maxBody+1))
		if err != nil {
			return nil, nil, fmt.Errorf("corpul cererii: %w", err)
		}
		if len(body) > maxBody {
			return nil, nil, errors.New("corpul cererii depășește 1 MiB")
		}
	}
	tried := map[*backend]bool{}
	var lastErr error
	for len(tried) < len(p.backends) {
		b := p.balancer.pick(p.backends, tried)
		if b == nil {
			break
		}
		tried[b] = true
		out := p.outbound(r, b, body)
		b.active.Add(1)
		resp, err := p.transport.RoundTrip(out)
		b.active.Add(-1)
		if err == nil {
			b.served.Add(1)
			return resp, b, nil
		}
		lastErr = err
		b.failures.Add(1)
		if b.healthy.Swap(false) {
			p.log.Warn("backend scos din rotație", "event", "backend.down", "backend", b.url.Host, "reason", err.Error())
		}
		if r.Context().Err() != nil {
			return nil, nil, r.Context().Err()
		}
		var op *net.OpError
		notSent := errors.As(err, &op) && op.Op == "dial"
		if !notSent && !idempotent[r.Method] {
			return nil, nil, fmt.Errorf("DW %s nu a răspuns la %s (nu se reia: metoda nu e idempotentă): %w", b.url.Host, r.Method, err)
		}
		p.log.Warn("reîncerc pe alt DW", "event", "proxy.retry", "backend", b.url.Host, "method", r.Method, "error", err.Error())
	}
	if lastErr == nil {
		return nil, nil, errNoBackend
	}
	return nil, nil, fmt.Errorf("%w: %v", errNoBackend, lastErr)
}

func (p *Proxy) outbound(r *http.Request, b *backend, body []byte) *http.Request {
	u := *b.url
	u.Path = strings.TrimSuffix(b.url.Path, "/") + r.URL.Path
	u.RawQuery = r.URL.RawQuery
	out, _ := http.NewRequestWithContext(r.Context(), r.Method, u.String(), bytes.NewReader(body))
	out.Header = r.Header.Clone()
	removeHopByHop(out.Header)
	out.ContentLength = int64(len(body))
	if len(body) == 0 && r.Method != http.MethodPost && r.Method != http.MethodPut {
		out.Body = nil
	}
	if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		if prior := out.Header.Get("X-Forwarded-For"); prior != "" {
			host = prior + ", " + host
		}
		out.Header.Set("X-Forwarded-For", host)
	}
	out.Header.Set("X-Forwarded-Host", r.Host)
	out.Header.Set("X-Forwarded-Proto", "http")
	out.Header.Add("Via", "1.1 "+p.cfg.ID)
	return out
}

func (p *Proxy) fail(w http.ResponseWriter, r *http.Request, status int, msg string) {
	f, err := model.Negotiate(r)
	if err != nil {
		f = model.JSON
	}
	w.Header().Set("Content-Type", f.ContentType())
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	model.Encode(w, f, &model.Error{Status: status, Message: msg})
}
