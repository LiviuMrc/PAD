// Package dw e nodul informativ (data warehouse): un server HTTP cu procesare concurentă
// a cererilor. Fiecare rută e tratată de un Controller care mapează cererea HTTP pe o
// operație a serviciului (store.Store, în Cassandra).
//
// Concurență: net/http rulează fiecare conexiune în propria goroutine, iar cererile unei
// conexiuni HTTP/1.1 sunt tratate pe rând, deci fiecare cerere în curs are propriul fir de
// execuție (thread-per-request; goroutine-urile sunt planificate pe firele sistemului).
// Accesul simultan la date e serializat de Cassandra (LWT), iar starea locală (metrici,
// coada de notificări) folosește operații atomice și canale.
package dw

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"lab2/webproxy/internal/model"
	"lab2/webproxy/internal/store"
)

type Config struct {
	ID     string        // numele instanței, trimis în X-Served-By
	MaxAge time.Duration // Cache-Control: max-age pentru GET-urile cacheable
}

type Server struct {
	cfg   Config
	store *store.Store
	log   *slog.Logger
	push  *Pusher
	mux   *http.ServeMux

	requests    atomic.Int64
	inflight    atomic.Int64
	maxInflight atomic.Int64
}

func New(cfg Config, st *store.Store, log *slog.Logger) *Server {
	if cfg.MaxAge == 0 {
		cfg.MaxAge = 30 * time.Second
	}
	s := &Server{cfg: cfg, store: st, log: log, push: NewPusher(st, cfg.ID, log), mux: http.NewServeMux()}

	emp := employeeController{s}
	sub := subscriptionController{s}
	routes := []struct {
		method, path string
		h            http.HandlerFunc
	}{
		{"GET", "/employee/", emp.get},
		{"PUT", "/employee/", emp.put},
		{"POST", "/employee/", emp.post},
		{"DELETE", "/employee/", emp.delete},
		{"GET", "/employees/", employeesController{s}.list},
		{"GET", "/update/employees/", updatesController{s}.changes},
		{"GET", "/subscriptions/", sub.list},
		{"POST", "/subscriptions/", sub.create},
		{"DELETE", "/subscriptions/", sub.delete},
		{"GET", "/health", s.health},
		{"GET", "/metrics", s.metrics},
	}
	allow := map[string][]string{} // path -> metodele permise, pentru 405
	for _, rt := range routes {
		pattern := rt.method + " " + rt.path
		if strings.HasSuffix(rt.path, "/") {
			pattern += "{$}"
		}
		s.mux.HandleFunc(pattern, rt.h)
		allow[rt.path] = append(allow[rt.path], rt.method)
	}
	s.mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if methods, ok := allow[r.URL.Path]; ok {
			w.Header().Set("Allow", strings.Join(methods, ", "))
			s.fail(w, r, http.StatusMethodNotAllowed, "metoda "+r.Method+" nu e permisă pe "+r.URL.Path)
			return
		}
		s.fail(w, r, http.StatusNotFound, "rută necunoscută: "+r.URL.Path)
	})
	return s
}

// ServeHTTP numără cererile în curs (dovada procesării concurente) și marchează răspunsul
// cu instanța care l-a servit (vizibil prin proxy la load balancing).
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	n := s.inflight.Add(1)
	defer s.inflight.Add(-1)
	for {
		m := s.maxInflight.Load()
		if n <= m || s.maxInflight.CompareAndSwap(m, n) {
			break
		}
	}
	s.requests.Add(1)
	start := time.Now()
	w.Header().Set("X-Served-By", s.cfg.ID)
	rec := &statusRecorder{ResponseWriter: w, status: 200}
	s.mux.ServeHTTP(rec, r)
	s.log.Info("cerere", "event", "http", "method", r.Method, "uri", r.URL.RequestURI(), "status", rec.status,
		"inflight", n, "duration_ms", time.Since(start).Milliseconds(), "remote", r.RemoteAddr)
}

// Close oprește trimiterea notificărilor push (după ce serverul HTTP s-a oprit).
func (s *Server) Close() { s.push.Close() }

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

// ---- răspunsuri ----

type reply struct {
	status    int
	body      any
	cacheable bool     // GET servit din cache-ul proxy-ului (Cache-Control: max-age)
	tags      []string // Cache-Tag: ce intrări din cache depind de răspuns / sunt invalidate de el
	etag      string   // dacă e gol, se calculează din corp
	location  string
}

// write codifică răspunsul în formatul cerut de client (XML/JSON) și tratează If-None-Match.
func (s *Server) write(w http.ResponseWriter, r *http.Request, rep reply) {
	f, err := model.Negotiate(r)
	if err != nil {
		s.fail(w, r, http.StatusNotAcceptable, err.Error())
		return
	}
	h := w.Header()
	h.Set("Vary", "Accept")
	if len(rep.tags) > 0 {
		h.Set("Cache-Tag", strings.Join(rep.tags, ","))
	}
	if rep.location != "" {
		h.Set("Location", rep.location)
	}
	if rep.status == http.StatusNoContent {
		h.Set("Cache-Control", "no-store")
		w.WriteHeader(rep.status)
		return
	}
	var buf bytes.Buffer
	if err := model.Encode(&buf, f, rep.body); err != nil {
		s.fail(w, r, http.StatusInternalServerError, err.Error())
		return
	}
	etag := rep.etag
	if etag == "" {
		sum := sha256.Sum256(buf.Bytes())
		etag = `"` + hex.EncodeToString(sum[:8]) + `"`
	} else {
		etag = `"` + etag + "-" + string(f) + `"`
	}
	h.Set("ETag", etag)
	if rep.cacheable {
		h.Set("Cache-Control", fmt.Sprintf("public, max-age=%d", int(s.cfg.MaxAge.Seconds())))
	} else {
		h.Set("Cache-Control", "no-store")
	}
	if rep.cacheable && matchETag(r.Header.Get("If-None-Match"), etag) {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	h.Set("Content-Type", f.ContentType())
	h.Set("Content-Length", strconv.Itoa(buf.Len()))
	w.WriteHeader(rep.status)
	w.Write(buf.Bytes())
}

func matchETag(header, etag string) bool {
	for _, t := range strings.Split(header, ",") {
		t = strings.TrimPrefix(strings.TrimSpace(t), "W/")
		if t == etag || t == "*" {
			return true
		}
	}
	return false
}

// fail trimite o eroare în formatul cerut (JSON dacă formatul cerut nu e acceptat).
func (s *Server) fail(w http.ResponseWriter, r *http.Request, status int, msg string) {
	f, err := model.Negotiate(r)
	if err != nil {
		f = model.JSON
	}
	w.Header().Set("Content-Type", f.ContentType())
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	model.Encode(w, f, &model.Error{Status: status, Message: msg})
}

// storeError traduce erorile din store în coduri HTTP.
func (s *Server) storeError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, store.ErrNotFound):
		s.fail(w, r, http.StatusNotFound, err.Error())
	case errors.Is(err, store.ErrVersionConflict):
		s.fail(w, r, http.StatusPreconditionFailed, err.Error())
	case errors.Is(err, store.ErrContention):
		w.Header().Set("Retry-After", "1")
		s.fail(w, r, http.StatusServiceUnavailable, err.Error())
	case errors.Is(err, store.ErrBadCursor), errors.Is(err, store.ErrInvalid):
		s.fail(w, r, http.StatusBadRequest, err.Error())
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		s.fail(w, r, http.StatusGatewayTimeout, err.Error())
	default:
		s.log.Error("eroare Cassandra", "event", "store", "error", err.Error())
		s.fail(w, r, http.StatusServiceUnavailable, "baza de date nu răspunde")
	}
}

// ---- parametri ----

func intParam(q url.Values, name string, def, min, max int) (int, error) {
	v := q.Get(name)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < min || n > max {
		return 0, fmt.Errorf("%s trebuie să fie un număr între %d și %d", name, min, max)
	}
	return n, nil
}

// ifMatchVersion citește versiunea din If-Match (ETag-ul unui angajat: "<id>-v<versiune>-<format>").
func ifMatchVersion(r *http.Request, id int) (int64, error) {
	h := strings.TrimSpace(r.Header.Get("If-Match"))
	if h == "" || h == "*" {
		return 0, nil
	}
	t := strings.Trim(strings.TrimPrefix(h, "W/"), `"`)
	prefix := strconv.Itoa(id) + "-v"
	if !strings.HasPrefix(t, prefix) {
		return 0, fmt.Errorf("If-Match nu e ETag-ul angajatului %d: %s", id, h)
	}
	rest := strings.TrimPrefix(t, prefix)
	if i := strings.IndexByte(rest, '-'); i >= 0 {
		rest = rest[:i]
	}
	v, err := strconv.ParseInt(rest, 10, 64)
	if err != nil || v <= 0 {
		return 0, fmt.Errorf("If-Match invalid: %s", h)
	}
	return v, nil
}

func employeeETag(e model.Employee) string { return fmt.Sprintf("%d-v%d", e.ID, e.Version) }

func tagsFor(id int) []string { return []string{"employee:" + strconv.Itoa(id), "employees"} }

// ---- health și metrici ----

type health struct {
	XMLName   struct{} `xml:"health" json:"-"`
	Status    string   `xml:"status" json:"status"`
	DW        string   `xml:"dw" json:"dw"`
	Cassandra string   `xml:"cassandra" json:"cassandra"`
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	h := health{Status: "ok", DW: s.cfg.ID, Cassandra: "ok"}
	status := http.StatusOK
	if err := s.store.Ping(ctx); err != nil {
		h.Status, h.Cassandra, status = "unavailable", err.Error(), http.StatusServiceUnavailable
	}
	s.write(w, r, reply{status: status, body: &h})
}

type metrics struct {
	XMLName     struct{} `xml:"metrics" json:"-"`
	DW          string   `xml:"dw" json:"dw"`
	Requests    int64    `xml:"requests" json:"requests"`
	Inflight    int64    `xml:"inflight" json:"inflight"`
	MaxInflight int64    `xml:"maxInflight" json:"maxInflight"` // > 1 = cereri procesate simultan
	PushSent    int64    `xml:"pushSent" json:"pushSent"`
	PushFailed  int64    `xml:"pushFailed" json:"pushFailed"`
}

func (s *Server) metrics(w http.ResponseWriter, r *http.Request) {
	s.write(w, r, reply{status: http.StatusOK, body: &metrics{
		DW: s.cfg.ID, Requests: s.requests.Load(), Inflight: s.inflight.Load(), MaxInflight: s.maxInflight.Load(),
		PushSent: s.push.sent.Load(), PushFailed: s.push.failed.Load(),
	}})
}
