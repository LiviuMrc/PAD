package source

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"lab2/webproxy/internal/dw"
	"lab2/webproxy/internal/model"
	"lab2/webproxy/internal/proxy"
	"lab2/webproxy/internal/testenv"
)

// Teste pe lanțul real: sursă -> proxy -> 2 DW -> Cassandra (keyspace dw_test_source), Redis baza 3.

var quiet = slog.New(slog.NewJSONHandler(io.Discard, nil))

func system(t *testing.T) string {
	t.Helper()
	var urls []string
	for _, id := range []string{"dw1", "dw2"} {
		st := testenv.Store(t, "dw_test_source", id)
		srv := dw.New(dw.Config{ID: id, MaxAge: 30 * time.Second}, st, quiet)
		ts := httptest.NewServer(srv)
		t.Cleanup(func() { ts.Close(); srv.Close() })
		urls = append(urls, ts.URL)
	}
	testenv.Reset(t, testenv.Store(t, "dw_test_source", "dw1"))
	p, err := proxy.New(proxy.Config{ID: "proxy-src-test", Backends: urls, Redis: testenv.Redis(t, 3),
		HealthInterval: 200 * time.Millisecond}, quiet)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go p.Run(ctx)
	pts := httptest.NewServer(p)
	t.Cleanup(func() { pts.Close(); cancel() })
	return pts.URL
}

func seedFile(t *testing.T, employees string) string {
	p := filepath.Join(t.TempDir(), "seed.json")
	os.WriteFile(p, []byte(employees), 0o644)
	return p
}

func httpDo(t *testing.T, method, url, ct, body string) (int, string) {
	t.Helper()
	req, _ := http.NewRequest(method, url, strings.NewReader(body))
	if ct != "" {
		req.Header.Set("Content-Type", ct)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	return resp.StatusCode, string(b)
}

const seed = `[{"id":1,"firstName":"Ana","lastName":"Popescu","salary":5000},{"id":2,"firstName":"Dan","lastName":"Rusu","salary":4000}]`

// PUT: nodul de stocare trimite datele (în XML) la DW prin proxy.
func TestSyncAllPutsDataThroughProxy(t *testing.T) {
	proxyURL := system(t)
	n, err := New(Config{ID: "src1", ProxyURL: proxyURL, DataFile: filepath.Join(t.TempDir(), "src1.json"),
		SeedFile: seedFile(t, seed), Format: model.XML}, quiet)
	if err != nil {
		t.Fatal(err)
	}
	if err := n.SyncAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	code, body := httpDo(t, "GET", proxyURL+"/employees/", "", "")
	var l model.EmployeeList
	json.Unmarshal([]byte(body), &l)
	if code != 200 || l.Total != 2 || l.Employees[0].Source != "src1" || l.Employees[1].LastName != "Rusu" {
		t.Fatalf("DW după sync: %d %s", code, body)
	}
}

// Pull: modificările făcute de un client prin POST/DELETE ajung în fișierul local al sursei.
// Starea (inclusiv cursorul) supraviețuiește repornirii nodului.
func TestPullAppliesModificationsAndPersists(t *testing.T) {
	proxyURL := system(t)
	dataFile := filepath.Join(t.TempDir(), "src1.json")
	cfg := Config{ID: "src1", ProxyURL: proxyURL, DataFile: dataFile, SeedFile: seedFile(t, seed), Format: model.JSON}
	n, _ := New(cfg, quiet)
	n.SyncAll(context.Background())

	httpDo(t, "POST", proxyURL+"/employee/", "application/json", `{"id":1,"salary":7777}`)
	httpDo(t, "POST", proxyURL+"/employee/", "application/xml", `<employee><id>2</id><department>HR</department></employee>`)
	applied, err := n.Pull(context.Background())
	if err != nil || applied != 2 {
		t.Fatalf("pull: %d %v", applied, err)
	}
	es := n.Employees()
	if es[0].Salary != 7777 || es[1].Department != "HR" {
		t.Fatalf("local: %+v", es)
	}
	if again, _ := n.Pull(context.Background()); again != 0 {
		t.Fatalf("al doilea pull (cu overlap) a reaplicat %d modificări", again)
	}

	restarted, err := New(cfg, quiet) // aceeași stare, din fișier
	if err != nil {
		t.Fatal(err)
	}
	if es := restarted.Employees(); es[0].Salary != 7777 {
		t.Fatalf("după repornire: %+v", es)
	}
	httpDo(t, "DELETE", proxyURL+"/employee/?id=2", "", "")
	if applied, err := restarted.Pull(context.Background()); err != nil || applied != 1 {
		t.Fatalf("pull după repornire: %d %v (cursorul trebuia păstrat)", applied, err)
	}
	if es := restarted.Employees(); len(es) != 1 || es[0].ID != 1 {
		t.Fatalf("după DELETE: %+v", es)
	}
	raw, _ := os.ReadFile(dataFile)
	if !strings.Contains(string(raw), `"salary": 7777`) || strings.Contains(string(raw), `"Rusu"`) {
		t.Fatalf("fișierul local:\n%s", raw)
	}
}

// Push: DW trimite modificarea la callback-ul sursei imediat, fără pull.
func TestPushDeliversModifications(t *testing.T) {
	proxyURL := system(t)
	n, _ := New(Config{ID: "src1", ProxyURL: proxyURL, DataFile: filepath.Join(t.TempDir(), "s.json"),
		SeedFile: seedFile(t, seed), Format: model.XML}, quiet)
	cb := httptest.NewServer(n.Handler())
	defer cb.Close()
	n.cfg.PushURL = cb.URL + "/push"
	n.SyncAll(context.Background())
	if err := n.Subscribe(context.Background()); err != nil {
		t.Fatal(err)
	}
	httpDo(t, "POST", proxyURL+"/employee/", "application/json", `{"id":1,"position":"Lead"}`)
	deadline := time.Now().Add(5 * time.Second)
	for n.Employees()[0].Position != "Lead" {
		if time.Now().After(deadline) {
			t.Fatal("modificarea nu a ajuns prin push în 5s")
		}
		time.Sleep(20 * time.Millisecond)
	}
	// pull-ul ulterior vede aceeași modificare, dar nu o mai aplică
	if applied, _ := n.Pull(context.Background()); applied != 0 {
		t.Fatalf("pull după push a reaplicat %d", applied)
	}
	code, body := httpDo(t, "GET", cb.URL+"/employees/", "", "")
	if code != 200 || !strings.Contains(body, `"position": "Lead"`) {
		t.Fatalf("datele locale: %d %s", code, body)
	}
}

// O sursă aplică doar modificările entităților ei.
func TestIgnoresOtherSourcesEntities(t *testing.T) {
	proxyURL := system(t)
	a, _ := New(Config{ID: "src1", ProxyURL: proxyURL, DataFile: filepath.Join(t.TempDir(), "a.json"), SeedFile: seedFile(t, seed)}, quiet)
	b, _ := New(Config{ID: "src2", ProxyURL: proxyURL, DataFile: filepath.Join(t.TempDir(), "b.json"),
		SeedFile: seedFile(t, `[{"id":50,"firstName":"Ion","lastName":"Lungu"}]`)}, quiet)
	a.SyncAll(context.Background())
	b.SyncAll(context.Background())
	httpDo(t, "POST", proxyURL+"/employee/", "application/json", `{"id":50,"salary":999}`)
	if n, _ := a.Pull(context.Background()); n != 0 {
		t.Fatalf("src1 a aplicat %d modificări ale lui src2", n)
	}
	if n, _ := b.Pull(context.Background()); n != 1 || b.Employees()[0].Salary != 999 {
		t.Fatalf("src2: %d %+v", n, b.Employees())
	}
}

// Proxy-ul nu răspunde încă: SyncAll reîncearcă până pornește.
func TestSyncRetriesUntilProxyIsUp(t *testing.T) {
	ln := httptest.NewUnstartedServer(nil)
	addr := ln.Listener.Addr().String()
	ln.Listener.Close()
	n, _ := New(Config{ID: "src1", ProxyURL: "http://" + addr, DataFile: filepath.Join(t.TempDir(), "s.json"),
		SeedFile: seedFile(t, seed)}, quiet)
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	if err := n.SyncAll(ctx); err != context.DeadlineExceeded {
		t.Fatalf("fără proxy: %v, așteptat să reîncerce până la deadline", err)
	}
}
