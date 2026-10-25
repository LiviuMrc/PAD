// Package source e un nod de stocare: deține o parte din date (în fișierul lui local),
// le trimite la DW prin PUT și preia înapoi modificările făcute de clienți prin POST/DELETE:
//   - pull: interoghează periodic GET /update/employees/?since=<cursor>;
//   - push: se abonează (POST /subscriptions/), iar DW îi trimite fiecare modificare.
//
// Toată comunicarea cu DW trece prin proxy. O modificare primită de două ori (prin pull și
// prin push, sau prin suprapunerea ferestrei de pull) e aplicată o singură dată, după seq.
package source

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"lab2/webproxy/internal/model"
)

type Config struct {
	ID           string
	ProxyURL     string        // toate cererile spre DW trec prin proxy
	DataFile     string        // starea locală (angajați, cursor), persistentă
	SeedFile     string        // datele inițiale, dacă DataFile nu există
	Format       model.Format  // formatul în care trimite datele (xml sau json)
	PollInterval time.Duration // 0 = fără pull
	PushURL      string        // adresa publică a callback-ului /push; gol = fără push
}

// state e ce se salvează pe disc.
type state struct {
	Employees map[int]model.Employee `json:"employees"`
	Cursor    string                 `json:"cursor"`  // ultimul seq citit prin pull
	Applied   []string               `json:"applied"` // ultimele seq aplicate (deduplicare)
}

type Node struct {
	cfg    Config
	log    *slog.Logger
	client *http.Client

	mu sync.Mutex // protejează st (pull, push și cererile HTTP rulează în paralel)
	st state
}

const maxApplied = 2000

func New(cfg Config, log *slog.Logger) (*Node, error) {
	if cfg.Format == "" {
		cfg.Format = model.JSON
	}
	n := &Node{cfg: cfg, log: log, client: &http.Client{Timeout: 10 * time.Second}}
	if err := n.load(); err != nil {
		return nil, err
	}
	return n, nil
}

func (n *Node) load() error {
	n.st = state{Employees: map[int]model.Employee{}}
	raw, err := os.ReadFile(n.cfg.DataFile)
	if err == nil {
		if err := json.Unmarshal(raw, &n.st); err != nil {
			return fmt.Errorf("%s: %w", n.cfg.DataFile, err)
		}
		if n.st.Employees == nil {
			n.st.Employees = map[int]model.Employee{}
		}
		n.log.Info("stare locală încărcată", "event", "source.load", "employees", len(n.st.Employees), "cursor", n.st.Cursor)
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if n.cfg.SeedFile == "" {
		return n.save()
	}
	raw, err = os.ReadFile(n.cfg.SeedFile)
	if err != nil {
		return err
	}
	var seed []model.Employee
	if err := json.Unmarshal(raw, &seed); err != nil {
		return fmt.Errorf("%s: %w", n.cfg.SeedFile, err)
	}
	for _, e := range seed {
		e.Source = n.cfg.ID
		if err := e.Validate(); err != nil {
			return fmt.Errorf("seed, angajatul %d: %w", e.ID, err)
		}
		n.st.Employees[e.ID] = e
	}
	n.log.Info("date inițiale încărcate", "event", "source.seed", "employees", len(seed), "file", n.cfg.SeedFile)
	return n.save()
}

// save scrie starea atomic (fișier temporar + rename). Se apelează cu mu luat (sau la pornire).
func (n *Node) save() error {
	raw, err := json.MarshalIndent(n.st, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(n.cfg.DataFile), 0o755); err != nil {
		return err
	}
	tmp := n.cfg.DataFile + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, n.cfg.DataFile)
}

// Employees întoarce o copie a datelor locale, ordonate după id.
func (n *Node) Employees() []model.Employee {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([]model.Employee, 0, len(n.st.Employees))
	for _, e := range n.st.Employees {
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// ---- PUT: datele locale spre DW ----

// SyncAll trimite toate datele locale la DW (PUT), reîncercând cât timp proxy-ul sau DW-ul
// nu răspund.
func (n *Node) SyncAll(ctx context.Context) error {
	for _, e := range n.Employees() {
		for attempt := 1; ; attempt++ {
			err := n.put(ctx, e)
			if err == nil {
				break
			}
			var perm *permanentError
			if errors.As(err, &perm) {
				return err
			}
			n.log.Warn("PUT eșuat, reîncerc", "event", "source.put", "id", e.ID, "attempt", attempt, "error", err.Error())
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(min(time.Duration(attempt)*500*time.Millisecond, 5*time.Second)):
			}
		}
	}
	n.log.Info("date trimise la DW", "event", "source.synced", "employees", len(n.Employees()), "format", n.cfg.Format)
	return nil
}

type permanentError struct{ msg string }

func (e *permanentError) Error() string { return e.msg }

func (n *Node) put(ctx context.Context, e model.Employee) error {
	var body bytes.Buffer
	if err := model.Encode(&body, n.cfg.Format, &e); err != nil {
		return err
	}
	req, _ := http.NewRequestWithContext(ctx, http.MethodPut, n.cfg.ProxyURL+"/employee/", &body)
	req.Header.Set("Content-Type", n.cfg.Format.ContentType())
	req.Header.Set("Accept", n.cfg.Format.ContentType())
	resp, err := n.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	switch {
	case resp.StatusCode < 300:
		return nil
	case resp.StatusCode >= 500:
		return fmt.Errorf("PUT %d: %s", resp.StatusCode, b)
	default:
		return &permanentError{fmt.Sprintf("PUT angajatul %d respins: %d %s", e.ID, resp.StatusCode, b)}
	}
}

// ---- aplicarea modificărilor (pull și push) ----

// apply aplică o modificare a unei entități deținute de acest nod. Întoarce false dacă era
// deja aplicată (același seq) sau nu privește acest nod.
func (n *Node) apply(ch model.Change, via string) (bool, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, s := range n.st.Applied {
		if s == ch.Seq {
			return false, nil
		}
	}
	n.st.Applied = append(n.st.Applied, ch.Seq)
	if len(n.st.Applied) > maxApplied {
		n.st.Applied = n.st.Applied[len(n.st.Applied)-maxApplied:]
	}
	if ch.Employee == nil || ch.Employee.Source != n.cfg.ID {
		return false, n.save() // modificarea privește alt nod de stocare
	}
	cur, exists := n.st.Employees[ch.ID]
	if exists && ch.Op == "update" && cur.Version >= ch.Employee.Version {
		return false, n.save() // o versiune mai nouă e deja aplicată (ordinea pull/push diferă)
	}
	switch ch.Op {
	case "update":
		n.st.Employees[ch.ID] = *ch.Employee
	case "delete":
		delete(n.st.Employees, ch.ID)
	default:
		return false, fmt.Errorf("operație necunoscută %q", ch.Op)
	}
	if err := n.save(); err != nil {
		return false, err
	}
	n.log.Info("modificare aplicată local", "event", "source.apply", "via", via, "op", ch.Op, "id", ch.ID,
		"seq", ch.Seq, "dw", ch.DW, "version", ch.Employee.Version)
	return true, nil
}

// Pull cere o dată modificările noi (GET /update/employees/) și le aplică. Fereastra de
// suprapunere (overlap) prinde modificările făcute concurent pe alte instanțe DW.
func (n *Node) Pull(ctx context.Context) (applied int, err error) {
	for {
		n.mu.Lock()
		cursor := n.st.Cursor
		n.mu.Unlock()
		q := url.Values{"limit": {"500"}, "overlap": {"5s"}}
		if cursor != "" {
			q.Set("since", cursor)
		}
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, n.cfg.ProxyURL+"/update/employees/?"+q.Encode(), nil)
		req.Header.Set("Accept", n.cfg.Format.ContentType())
		resp, err := n.client.Do(req)
		if err != nil {
			return applied, err
		}
		var cl model.ChangeList
		if resp.StatusCode != http.StatusOK {
			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			return applied, fmt.Errorf("pull: %d %s", resp.StatusCode, b)
		}
		err = model.Decode(resp.Body, n.cfg.Format, &cl)
		resp.Body.Close()
		if err != nil {
			return applied, fmt.Errorf("pull: %w", err)
		}
		for _, ch := range cl.Changes {
			ok, err := n.apply(ch, "pull")
			if err != nil {
				return applied, err
			}
			if ok {
				applied++
			}
		}
		n.mu.Lock()
		advanced := cl.Next != "" && cl.Next != n.st.Cursor
		if advanced {
			n.st.Cursor = cl.Next
			err = n.save()
		}
		n.mu.Unlock()
		if err != nil || !advanced || len(cl.Changes) < 500 {
			return applied, err
		}
	}
}

// RunPull interoghează periodic DW-ul până la ctx.Done.
func (n *Node) RunPull(ctx context.Context) {
	if n.cfg.PollInterval <= 0 {
		return
	}
	t := time.NewTicker(n.cfg.PollInterval)
	defer t.Stop()
	for {
		if _, err := n.Pull(ctx); err != nil && ctx.Err() == nil {
			n.log.Warn("pull eșuat", "event", "source.pull", "error", err.Error())
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// Subscribe înregistrează callback-ul push la DW (prin proxy), reîncercând până reușește.
func (n *Node) Subscribe(ctx context.Context) error {
	if n.cfg.PushURL == "" {
		return nil
	}
	body, _ := json.Marshal(model.Subscription{URL: n.cfg.PushURL, Source: n.cfg.ID, Format: n.cfg.Format})
	for attempt := 1; ; attempt++ {
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, n.cfg.ProxyURL+"/subscriptions/", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := n.client.Do(req)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusCreated {
				n.log.Info("abonat la notificări push", "event", "source.subscribe", "url", n.cfg.PushURL)
				return nil
			}
			err = fmt.Errorf("răspuns %d", resp.StatusCode)
		}
		n.log.Warn("abonarea push a eșuat, reîncerc", "event", "source.subscribe", "attempt", attempt, "error", err.Error())
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

// Handler servește callback-ul push (POST /push) și datele locale (GET /employees/).
func (n *Node) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /push", func(w http.ResponseWriter, r *http.Request) {
		f, err := model.BodyFormat(r.Header.Get("Content-Type"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusUnsupportedMediaType)
			return
		}
		var ch model.Change
		if err := model.Decode(r.Body, f, &ch); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if _, err := n.apply(ch, "push"); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /employees/", func(w http.ResponseWriter, r *http.Request) {
		f, err := model.Negotiate(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotAcceptable)
			return
		}
		es := n.Employees()
		w.Header().Set("Content-Type", f.ContentType())
		model.Encode(w, f, &model.EmployeeList{Limit: len(es), Total: len(es), Employees: es})
	})
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"status":"ok","source":"` + n.cfg.ID + `","employees":` + strconv.Itoa(len(n.Employees())) + "}\n"))
	})
	return mux
}
