package proxy

import (
	"net/http"
	"strconv"

	"lab2/webproxy/internal/model"
)

// admin servește /_proxy/*: starea proxy-ului, citită din memorie și din Redis.
//
//	/_proxy/backends     instanțele DW: sănătoase, cereri în curs, servite, erori
//	/_proxy/connections  conexiunile clienților (evidența din Redis, toate instanțele proxy)
//	/_proxy/stats        contoarele cache-ului și ale conexiunilor (Redis)
func (p *Proxy) admin(w http.ResponseWriter, r *http.Request) {
	f, err := model.Negotiate(r)
	if err != nil {
		p.fail(w, r, http.StatusNotAcceptable, err.Error())
		return
	}
	var body any
	switch r.URL.Path {
	case "/_proxy/backends":
		type be struct {
			URL      string `json:"url" xml:"url"`
			Healthy  bool   `json:"healthy" xml:"healthy"`
			Active   int64  `json:"active" xml:"active"`
			Served   int64  `json:"served" xml:"served"`
			Failures int64  `json:"failures" xml:"failures"`
		}
		out := struct {
			XMLName   struct{} `json:"-" xml:"backends"`
			Algorithm string   `json:"algorithm" xml:"algorithm,attr"`
			Backends  []be     `json:"backends" xml:"backend"`
		}{Algorithm: p.balancer.name()}
		for _, b := range p.backends {
			out.Backends = append(out.Backends, be{b.url.String(), b.healthy.Load(), b.active.Load(), b.served.Load(), b.failures.Load()})
		}
		body = &out
	case "/_proxy/connections":
		list, err := p.conns.list(r.Context())
		if err != nil {
			p.fail(w, r, http.StatusServiceUnavailable, "Redis: "+err.Error())
			return
		}
		body = &struct {
			XMLName     struct{} `json:"-" xml:"connections"`
			Count       int      `json:"count" xml:"count,attr"`
			Connections []Conn   `json:"connections" xml:"connection"`
		}{Count: len(list), Connections: list}
	case "/_proxy/stats":
		h, err := p.cfg.Redis.HGetAll(r.Context(), keyStats).Result()
		if err != nil {
			p.fail(w, r, http.StatusServiceUnavailable, "Redis: "+err.Error())
			return
		}
		n := func(k string) int64 { v, _ := strconv.ParseInt(h[k], 10, 64); return v }
		body = &struct {
			XMLName       struct{} `json:"-" xml:"stats"`
			Proxy         string   `json:"proxy" xml:"proxy"`
			Requests      int64    `json:"requests" xml:"requests"` // ale acestei instanțe
			Hits          int64    `json:"hits" xml:"hits"`
			Misses        int64    `json:"misses" xml:"misses"`
			Stores        int64    `json:"stores" xml:"stores"`
			Invalidations int64    `json:"invalidations" xml:"invalidations"`
			ConnsOpened   int64    `json:"connsOpened" xml:"connsOpened"`
			ConnsClosed   int64    `json:"connsClosed" xml:"connsClosed"`
		}{struct{}{}, p.cfg.ID, p.requests.Load(), n("hits"), n("misses"), n("stores"), n("invalidations"), n("connsOpened"), n("connsClosed")}
	default:
		p.fail(w, r, http.StatusNotFound, "endpoint necunoscut: "+r.URL.Path)
		return
	}
	w.Header().Set("Content-Type", f.ContentType())
	w.Header().Set("Cache-Control", "no-store")
	model.Encode(w, f, body)
}
