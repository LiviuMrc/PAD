package proxy

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"lab2/webproxy/internal/dw"
	"lab2/webproxy/internal/model"
	"lab2/webproxy/internal/testenv"
)

// Teste pe sistemul real: 3 noduri DW (Cassandra, keyspace dw_test_proxy) în spatele
// proxy-ului, cu Redis real (baza 2).

const keyspace = "dw_test_proxy"

var quiet = slog.New(slog.NewJSONHandler(io.Discard, nil))

type dwNode struct {
	id   string
	srv  *dw.Server
	ts   *httptest.Server
	addr string
	slow time.Duration // doar pentru testul least-connections: întârzie răspunsurile
}

func startDW(t *testing.T, id string, maxAge time.Duration, addr string) *dwNode {
	t.Helper()
	st := testenv.Store(t, keyspace, id)
	n := &dwNode{id: id, srv: dw.New(dw.Config{ID: id, MaxAge: maxAge}, st, quiet)}
	n.ts = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if n.slow > 0 && r.URL.Path != "/health" {
			time.Sleep(n.slow)
		}
		n.srv.ServeHTTP(w, r)
	}))
	if addr != "" { // repornire pe aceeași adresă
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		n.ts.Listener.Close()
		n.ts.Listener = ln
	}
	n.ts.Start()
	n.addr = n.ts.Listener.Addr().String()
	t.Cleanup(n.stop)
	return n
}

func (n *dwNode) stop() { n.ts.Close() }

type env struct {
	t    *testing.T
	dws  []*dwNode
	p    *Proxy
	url  string
	http *http.Client
}

func setup(t *testing.T, algorithm string, maxAge time.Duration) *env {
	t.Helper()
	rdb := testenv.Redis(t, 2)
	e := &env{t: t, http: &http.Client{Transport: &http.Transport{}}}
	var urls []string
	for _, id := range []string{"dw1", "dw2", "dw3"} {
		n := startDW(t, id, maxAge, "")
		e.dws = append(e.dws, n)
		urls = append(urls, n.ts.URL)
	}
	testenv.Reset(t, testenv.Store(t, keyspace, "dw1"))
	e.p, e.url = startProxy(t, "proxy-test", urls, algorithm, rdb)
	return e
}

func startProxy(t *testing.T, id string, backends []string, algorithm string, rdb *redis.Client) (*Proxy, string) {
	t.Helper()
	p, err := New(Config{ID: id, Backends: backends, Algorithm: algorithm, CacheTTL: time.Minute,
		HealthInterval: 100 * time.Millisecond, Redis: rdb}, quiet)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go p.Run(ctx)
	srv := p.Server("")
	ts := httptest.NewUnstartedServer(p)
	ts.Config.ConnState, ts.Config.ConnContext, ts.Config.IdleTimeout = srv.ConnState, srv.ConnContext, srv.IdleTimeout
	ts.Start()
	t.Cleanup(func() { ts.Close(); cancel() })
	return p, ts.URL
}

func (e *env) do(method, path, ct, body string, headers ...string) (*http.Response, string) {
	e.t.Helper()
	req, _ := http.NewRequest(method, e.url+path, strings.NewReader(body))
	if ct != "" {
		req.Header.Set("Content-Type", ct)
	}
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	resp, err := e.http.Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	return resp, string(b)
}

func (e *env) put(id int, name string) {
	e.t.Helper()
	r, body := e.do("PUT", "/employee/", "application/json",
		`{"id":`+itoa(id)+`,"firstName":"`+name+`","lastName":"Test","salary":100,"source":"src1"}`)
	if r.StatusCode >= 300 {
		e.t.Fatalf("PUT: %d %s", r.StatusCode, body)
	}
}

func itoa(i int) string { b, _ := json.Marshal(i); return string(b) }

func TestRoundRobin(t *testing.T) {
	e := setup(t, "round-robin", 30*time.Second)
	var order []string
	for i := 0; i < 9; i++ {
		r, _ := e.do("GET", "/employees/", "", "", "Cache-Control", "no-cache") // fără cache: fiecare cerere ajunge la un DW
		order = append(order, r.Header.Get("X-Served-By"))
	}
	t.Logf("ordinea: %v", order)
	for i := 3; i < 9; i++ {
		if order[i] != order[i-3] {
			t.Fatalf("nu e round-robin: %v", order)
		}
	}
	seen := map[string]int{}
	for _, s := range order {
		seen[s]++
	}
	if len(seen) != 3 || seen["dw1"] != 3 || seen["dw2"] != 3 || seen["dw3"] != 3 {
		t.Fatalf("distribuție: %v", seen)
	}
}

// least-connections: dw1 răspunde încet, deci are mereu cereri în curs; proxy-ul trimite
// acolo mai puține cereri decât cele 1/3 pe care le-ar primi cu round-robin.
func TestLeastConnections(t *testing.T) {
	e := setup(t, "least-connections", 30*time.Second)
	e.dws[0].slow = 300 * time.Millisecond
	var mu sync.Mutex
	count := map[string]int{}
	var wg sync.WaitGroup
	for wave := 0; wave < 5; wave++ {
		for i := 0; i < 6; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				r, _ := e.do("GET", "/employees/", "", "", "Cache-Control", "no-cache")
				mu.Lock()
				count[r.Header.Get("X-Served-By")]++
				mu.Unlock()
			}()
		}
		time.Sleep(50 * time.Millisecond)
	}
	wg.Wait()
	t.Logf("distribuție cu dw1 lent: %v", count)
	if count["dw1"] >= 10 {
		t.Fatalf("dw1 (lent) a primit %d din 30, așteptat sub 10", count["dw1"])
	}
}

// Failover: un DW cade. Cererile continuă să primească 200 de la celelalte (inclusiv POST,
// care nu ajunsese la DW-ul căzut), iar după repornire DW-ul revine în rotație.
func TestFailoverAndRecovery(t *testing.T) {
	e := setup(t, "round-robin", 30*time.Second)
	e.put(1, "Ana")
	dw2 := e.dws[1]
	addr := dw2.addr
	dw2.stop()

	for i := 0; i < 9; i++ {
		r, body := e.do("GET", "/employee/?id=1", "", "", "Cache-Control", "no-cache")
		if r.StatusCode != 200 || r.Header.Get("X-Served-By") == "dw2" {
			t.Fatalf("cererea %d cu dw2 oprit: %d %s %s", i, r.StatusCode, r.Header.Get("X-Served-By"), body)
		}
	}
	if r, body := e.do("POST", "/employee/", "application/json", `{"id":1,"salary":200}`); r.StatusCode != 200 {
		t.Fatalf("POST cu dw2 oprit: %d %s", r.StatusCode, body)
	}
	_, body := e.do("GET", "/_proxy/backends", "", "")
	if !strings.Contains(body, `"url": "http://`+addr+`",
      "healthy": false`) {
		t.Fatalf("dw2 trebuia marcat nesănătos:\n%s", body)
	}

	startDW(t, "dw2", 30*time.Second, addr) // repornire pe aceeași adresă
	deadline := time.Now().Add(3 * time.Second)
	for {
		r, _ := e.do("GET", "/employee/?id=1", "", "", "Cache-Control", "no-cache")
		if r.Header.Get("X-Served-By") == "dw2" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("dw2 nu a revenit în rotație în 3s")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestCacheHitMissAndInvalidation(t *testing.T) {
	e := setup(t, "round-robin", 30*time.Second)
	e.put(1, "Ana")

	r1, b1 := e.do("GET", "/employee/?id=1", "", "")
	r2, b2 := e.do("GET", "/employee/?id=1", "", "")
	if r1.Header.Get("X-Cache") != "MISS" || r2.Header.Get("X-Cache") != "HIT" || b1 != b2 {
		t.Fatalf("MISS apoi HIT: %s %s", r1.Header.Get("X-Cache"), r2.Header.Get("X-Cache"))
	}
	if r2.Header.Get("X-Served-By") != r1.Header.Get("X-Served-By") || r2.Header.Get("Age") == "" {
		t.Fatalf("răspunsul din cache trebuie să fie cel salvat: %v", r2.Header)
	}
	// aceeași resursă în XML e altă intrare
	rx, bx := e.do("GET", "/employee/?id=1", "", "", "Accept", "application/xml")
	if rx.Header.Get("X-Cache") != "MISS" || !strings.Contains(bx, "<firstName>Ana</firstName>") {
		t.Fatalf("XML: %s %s", rx.Header.Get("X-Cache"), bx)
	}
	if r, _ := e.do("GET", "/employee/?format=xml&id=1", "", ""); r.Header.Get("X-Cache") != "HIT" {
		t.Fatalf("?format=xml cu parametrii în altă ordine trebuia să fie HIT: %s", r.Header.Get("X-Cache"))
	}
	e.do("GET", "/employees/", "", "")
	if r, _ := e.do("GET", "/employees/", "", ""); r.Header.Get("X-Cache") != "HIT" {
		t.Fatal("lista trebuia să fie în cache")
	}

	// scrierea prin proxy invalidează angajatul (ambele formate) și listele
	if r, _ := e.do("POST", "/employee/", "application/json", `{"id":1,"firstName":"Maria"}`); r.StatusCode != 200 {
		t.Fatal("POST")
	}
	for _, accept := range []string{"application/json", "application/xml"} {
		r, body := e.do("GET", "/employee/?id=1", "", "", "Accept", accept)
		if r.Header.Get("X-Cache") != "MISS" || !strings.Contains(body, "Maria") {
			t.Fatalf("după POST (%s): %s %s", accept, r.Header.Get("X-Cache"), body)
		}
	}
	if r, body := e.do("GET", "/employees/", "", ""); r.Header.Get("X-Cache") != "MISS" || !strings.Contains(body, "Maria") {
		t.Fatalf("lista după POST: %s", r.Header.Get("X-Cache"))
	}
	_, stats := e.do("GET", "/_proxy/stats", "", "")
	var s struct{ Hits, Misses, Stores, Invalidations int64 }
	json.Unmarshal([]byte(stats), &s)
	if s.Hits < 3 || s.Invalidations < 2 || s.Stores < 4 {
		t.Fatalf("statistici: %s", stats)
	}
}

func TestCacheTTLExpires(t *testing.T) {
	e := setup(t, "round-robin", time.Second) // DW: Cache-Control: max-age=1
	e.put(1, "Ana")
	e.do("GET", "/employee/?id=1", "", "")
	if r, _ := e.do("GET", "/employee/?id=1", "", ""); r.Header.Get("X-Cache") != "HIT" {
		t.Fatal("trebuia HIT")
	}
	time.Sleep(1200 * time.Millisecond)
	if r, _ := e.do("GET", "/employee/?id=1", "", ""); r.Header.Get("X-Cache") != "MISS" {
		t.Fatal("după TTL trebuia MISS")
	}
}

func TestCacheRespectsDirectives(t *testing.T) {
	e := setup(t, "round-robin", 30*time.Second)
	e.put(1, "Ana")
	e.do("POST", "/employee/", "application/json", `{"id":1,"salary":1}`)
	for i := 0; i < 3; i++ { // DW trimite no-store pe feed: nu se salvează niciodată
		if r, _ := e.do("GET", "/update/employees/", "", ""); r.Header.Get("X-Cache") == "HIT" {
			t.Fatal("feed-ul de modificări nu trebuie cache-uit")
		}
	}
	e.do("GET", "/employee/?id=1", "", "")
	if r, _ := e.do("GET", "/employee/?id=1", "", "", "Cache-Control", "no-cache"); r.Header.Get("X-Cache") != "MISS" {
		t.Fatal("Cache-Control: no-cache de la client trebuie să ocolească cache-ul")
	}
	// GET condiționat servit din cache
	r, _ := e.do("GET", "/employee/?id=1", "", "")
	if r2, body := e.do("GET", "/employee/?id=1", "", "", "If-None-Match", r.Header.Get("ETag")); r2.StatusCode != 304 || body != "" || r2.Header.Get("X-Cache") != "HIT" {
		t.Fatalf("If-None-Match: %d %s %q", r2.StatusCode, r2.Header.Get("X-Cache"), body)
	}
	if r, _ := e.do("GET", "/employee/?id=1", "", "", "Accept", "text/html"); r.StatusCode != 406 {
		t.Fatalf("406 trebuie să ajungă de la DW: %d", r.StatusCode)
	}
}

// Cache-ul e în Redis, comun pentru mai multe instanțe proxy.
func TestCacheSharedBetweenProxies(t *testing.T) {
	e := setup(t, "round-robin", 30*time.Second)
	e.put(1, "Ana")
	var urls []string
	for _, n := range e.dws {
		urls = append(urls, n.ts.URL)
	}
	_, url2 := startProxy(t, "proxy-2", urls, "round-robin", testenv.Redis(t, 2))
	testenv.Redis(t, 2) // golește baza: ambele proxy-uri pornesc cu cache gol
	e.do("GET", "/employee/?id=1", "", "")
	resp, err := http.Get(url2 + "/employee/?id=1")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.Header.Get("X-Cache") != "HIT" || resp.Header.Get("X-Proxy") != "proxy-2" {
		t.Fatalf("al doilea proxy: %s %s", resp.Header.Get("X-Cache"), resp.Header.Get("X-Proxy"))
	}
}

// Un GET care a citit date vechi înainte de o invalidare nu le mai salvează după ea.
func TestStaleResponseIsNotStoredAfterInvalidation(t *testing.T) {
	rdb := testenv.Redis(t, 2)
	c := &cache{rdb: rdb, maxTTL: time.Minute}
	ctx := context.Background()
	gen, _ := c.generation(ctx)
	if _, err := c.invalidate(ctx, []string{"employee:1"}); err != nil { // scriere concurentă
		t.Fatal(err)
	}
	ok, err := c.store(ctx, "proxy:cache:test", gen, &cachedResponse{Status: 200, Body: []byte("vechi")}, time.Minute, []string{"employee:1"})
	if err != nil || ok {
		t.Fatalf("răspunsul vechi a fost salvat: %v %v", ok, err)
	}
	gen, _ = c.generation(ctx)
	if ok, _ := c.store(ctx, "proxy:cache:test", gen, &cachedResponse{Status: 200}, time.Minute, []string{"employee:1"}); !ok {
		t.Fatal("cu generația curentă trebuia salvat")
	}
}

// Smart proxy: conexiunea keep-alive a clientului e în evidența din Redis, cu numărul de
// cereri trimise pe ea; la închidere dispare din evidență.
func TestConnectionRegistryInRedis(t *testing.T) {
	e := setup(t, "round-robin", 30*time.Second)
	for i := 0; i < 5; i++ {
		e.do("GET", "/employees/", "", "")
	}
	_, body := e.do("GET", "/_proxy/connections", "", "")
	var list struct {
		Count       int
		Connections []Conn
	}
	json.Unmarshal([]byte(body), &list)
	if list.Count != 1 || list.Connections[0].Requests != 6 || list.Connections[0].Proxy != "proxy-test" ||
		list.Connections[0].LastRequest != "GET /_proxy/connections" {
		t.Fatalf("o singură conexiune keep-alive cu 6 cereri:\n%s", body)
	}
	keys, _ := e.p.cfg.Redis.Keys(context.Background(), "proxy:conn:*").Result()
	if len(keys) != 1 {
		t.Fatalf("chei în Redis: %v", keys)
	}
	e.http.CloseIdleConnections()
	deadline := time.Now().Add(2 * time.Second)
	for {
		n, _ := e.p.cfg.Redis.SCard(context.Background(), keyConns).Result()
		if n == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("conexiunea închisă a rămas în evidență")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestBadConfig(t *testing.T) {
	rdb := testenv.Redis(t, 2)
	if _, err := New(Config{Backends: nil, Redis: rdb}, quiet); err == nil {
		t.Fatal("fără backend-uri trebuia eroare")
	}
	if _, err := New(Config{Backends: []string{"http://x"}, Algorithm: "random", Redis: rdb}, quiet); err == nil {
		t.Fatal("algoritm necunoscut acceptat")
	}
	_ = model.JSON
}
