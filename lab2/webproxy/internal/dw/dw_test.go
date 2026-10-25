package dw_test

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"lab2/webproxy/internal/dw"
	"lab2/webproxy/internal/model"
	"lab2/webproxy/internal/testenv"
)

// Teste HTTP pe un server DW real, cu Cassandra reală (keyspace dw_test_http).

func startDW(t *testing.T, id string) (*dw.Server, *httptest.Server) {
	t.Helper()
	st := testenv.Store(t, "dw_test_http", id)
	srv := dw.New(dw.Config{ID: id, MaxAge: 30 * time.Second}, st, slog.New(slog.NewJSONHandler(io.Discard, nil)))
	ts := httptest.NewServer(srv)
	t.Cleanup(func() { ts.Close(); srv.Close() })
	return srv, ts
}

func do(t *testing.T, method, url, contentType, body string, headers ...string) (*http.Response, string) {
	t.Helper()
	req, _ := http.NewRequest(method, url, strings.NewReader(body))
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	return resp, string(b)
}

const annaXML = `<employee><id>1</id><firstName>Ana</firstName><lastName>Popescu</lastName>
<email>ana@example.com</email><department>IT</department><position>dev</position><salary>5000</salary><source>src1</source></employee>`

func TestFormatsXMLAndJSON(t *testing.T) {
	_, ts := startDW(t, "dw-t1")
	testenv.Reset(t, testenv.Store(t, "dw_test_http", "dw-t1"))

	resp, body := do(t, "PUT", ts.URL+"/employee/", "application/xml", annaXML, "Accept", "application/xml")
	if resp.StatusCode != 201 || !strings.HasPrefix(resp.Header.Get("Content-Type"), "application/xml") ||
		!strings.Contains(body, "<firstName>Ana</firstName>") || resp.Header.Get("Location") != "/employee/?id=1" {
		t.Fatalf("PUT XML: %d %s\n%s", resp.StatusCode, resp.Header, body)
	}

	resp, body = do(t, "GET", ts.URL+"/employee/?id=1", "", "", "Accept", "application/xml")
	var x model.Employee
	if resp.StatusCode != 200 || xml.Unmarshal([]byte(body), &x) != nil || x.LastName != "Popescu" || x.Version != 1 {
		t.Fatalf("GET XML: %d %s", resp.StatusCode, body)
	}
	resp, body = do(t, "GET", ts.URL+"/employee/?id=1", "", "", "Accept", "application/json")
	var j model.Employee
	if resp.StatusCode != 200 || json.Unmarshal([]byte(body), &j) != nil || j.Email != "ana@example.com" ||
		!strings.HasPrefix(resp.Header.Get("Content-Type"), "application/json") {
		t.Fatalf("GET JSON: %d %s", resp.StatusCode, body)
	}
	if resp, body := do(t, "GET", ts.URL+"/employee/?id=1&format=xml", "", "", "Accept", "application/json"); !strings.Contains(body, "<employee>") {
		t.Fatalf("?format=xml: %d %s", resp.StatusCode, body)
	}
	if resp, body := do(t, "GET", ts.URL+"/employee/?id=1", "", "", "Accept", "text/html"); resp.StatusCode != 406 {
		t.Fatalf("Accept text/html: %d %s", resp.StatusCode, body)
	}
	if resp.Header.Get("X-Served-By") != "dw-t1" || resp.Header.Get("Vary") != "Accept" {
		t.Fatalf("header-e: %v", resp.Header)
	}
}

func TestCRUDAndStatusCodes(t *testing.T) {
	_, ts := startDW(t, "dw-t1")
	testenv.Reset(t, testenv.Store(t, "dw_test_http", "dw-t1"))
	u := ts.URL + "/employee/"

	if r, _ := do(t, "PUT", u, "application/json", `{"id":2,"firstName":"Dan","lastName":"Ionescu","salary":3000,"source":"src2"}`); r.StatusCode != 201 {
		t.Fatalf("PUT nou: %d", r.StatusCode)
	}
	if r, _ := do(t, "PUT", u, "application/json", `{"id":2,"firstName":"Dan","lastName":"Ionescu","salary":3100,"source":"src2"}`); r.StatusCode != 200 {
		t.Fatalf("PUT existent: %d", r.StatusCode)
	}
	if r, _ := do(t, "PATCH", u, "application/json", `{}`); r.Header.Get("Allow") != "GET, PUT, POST, DELETE" {
		t.Errorf("Allow: %q", r.Header.Get("Allow"))
	}
	for name, c := range map[string]struct {
		method, url, ct, body string
		want                  int
	}{
		"GET inexistent":     {"GET", u + "?id=99", "", "", 404},
		"GET fără id":        {"GET", u, "", "", 400},
		"GET id invalid":     {"GET", u + "?id=abc", "", "", 400},
		"PUT invalid":        {"PUT", u, "application/json", `{"id":3,"firstName":"","lastName":"x"}`, 400},
		"PUT corp stricat":   {"PUT", u, "application/json", `{"id":`, 400},
		"PUT text/plain":     {"PUT", u, "text/plain", "x", 415},
		"POST gol":           {"POST", u, "application/json", `{"id":2}`, 400},
		"POST inexistent":    {"POST", u, "application/json", `{"id":99,"salary":1}`, 404},
		"POST invalid":       {"POST", u, "application/json", `{"id":2,"salary":-5}`, 400},
		"PATCH nepermis":     {"PATCH", u, "application/json", `{}`, 405},
		"rută necunoscută":   {"GET", ts.URL + "/nimic", "", "", 404},
		"limit prea mare":    {"GET", ts.URL + "/employees/?limit=1000", "", "", 400},
		"cursor invalid":     {"GET", ts.URL + "/update/employees/?since=abc", "", "", 400},
		"abonare url greșit": {"POST", ts.URL + "/subscriptions/", "application/json", `{"url":"ftp://x"}`, 400},
	} {
		if r, body := do(t, c.method, c.url, c.ct, c.body); r.StatusCode != c.want {
			t.Errorf("%s: %d, așteptat %d (%s)", name, r.StatusCode, c.want, body)
		}
	}

	// POST: modificare parțială în XML, cu If-Match pe ETag-ul citit
	r, _ := do(t, "GET", u+"?id=2", "", "")
	etag := r.Header.Get("ETag")
	r, body := do(t, "POST", u, "application/xml", `<employee><id>2</id><department>HR</department></employee>`, "If-Match", etag)
	var e model.Employee
	json.Unmarshal([]byte(body), &e)
	if r.StatusCode != 200 || e.Department != "HR" || e.Salary != 3100 || e.Version != 3 {
		t.Fatalf("POST: %d %s", r.StatusCode, body)
	}
	if r, _ := do(t, "POST", u, "application/json", `{"id":2,"salary":1}`, "If-Match", etag); r.StatusCode != 412 {
		t.Fatalf("POST cu ETag vechi: %d, așteptat 412", r.StatusCode)
	}
	if r, _ := do(t, "DELETE", u+"?id=2", "", ""); r.StatusCode != 204 {
		t.Fatalf("DELETE: %d", r.StatusCode)
	}
	if r, _ := do(t, "DELETE", u+"?id=2", "", ""); r.StatusCode != 404 {
		t.Fatalf("DELETE repetat: %d", r.StatusCode)
	}
}

func TestListPagination(t *testing.T) {
	_, ts := startDW(t, "dw-t1")
	testenv.Reset(t, testenv.Store(t, "dw_test_http", "dw-t1"))
	for i := 1; i <= 7; i++ {
		do(t, "PUT", ts.URL+"/employee/", "application/json", fmt.Sprintf(`{"id":%d,"firstName":"F%d","lastName":"L%d"}`, i, i, i))
	}
	_, body := do(t, "GET", ts.URL+"/employees/?offset=1&limit=4", "", "")
	var l model.EmployeeList
	if json.Unmarshal([]byte(body), &l) != nil || l.Total != 7 || len(l.Employees) != 4 || l.Employees[0].ID != 2 || l.Employees[3].ID != 5 {
		t.Fatalf("pagina: %s", body)
	}
	_, body = do(t, "GET", ts.URL+"/employees/?offset=5&limit=4", "", "", "Accept", "application/xml")
	var lx model.EmployeeList
	if xml.Unmarshal([]byte(body), &lx) != nil || len(lx.Employees) != 2 || lx.Total != 7 || !strings.Contains(body, `offset="5"`) {
		t.Fatalf("pagina XML: %s", body)
	}
}

func TestCachingHeadersAndConditionalGET(t *testing.T) {
	_, ts := startDW(t, "dw-t1")
	testenv.Reset(t, testenv.Store(t, "dw_test_http", "dw-t1"))
	r, _ := do(t, "PUT", ts.URL+"/employee/", "application/xml", annaXML)
	if r.Header.Get("Cache-Tag") != "employee:1,employees" || r.Header.Get("Cache-Control") != "no-store" {
		t.Fatalf("PUT: %v", r.Header)
	}
	r, _ = do(t, "GET", ts.URL+"/employee/?id=1", "", "")
	etag := r.Header.Get("ETag")
	if r.Header.Get("Cache-Control") != "public, max-age=30" || etag != `"1-v1-json"` || r.Header.Get("Cache-Tag") != "employee:1,employees" {
		t.Fatalf("GET: %v", r.Header)
	}
	if r, body := do(t, "GET", ts.URL+"/employee/?id=1", "", "", "If-None-Match", etag); r.StatusCode != 304 || body != "" {
		t.Fatalf("If-None-Match: %d %q", r.StatusCode, body)
	}
	if r, _ := do(t, "GET", ts.URL+"/employee/?id=1", "", "", "If-None-Match", etag, "Accept", "application/xml"); r.StatusCode != 200 {
		t.Fatalf("ETag-ul JSON nu trebuie să valideze reprezentarea XML: %d", r.StatusCode)
	}
	if r, _ := do(t, "GET", ts.URL+"/update/employees/", "", ""); r.Header.Get("Cache-Control") != "no-store" {
		t.Fatalf("feed-ul nu trebuie cache-uit: %v", r.Header)
	}
}

func TestUpdatesFeedForPull(t *testing.T) {
	_, ts := startDW(t, "dw-t1")
	testenv.Reset(t, testenv.Store(t, "dw_test_http", "dw-t1"))
	do(t, "PUT", ts.URL+"/employee/", "application/xml", annaXML)
	_, body := do(t, "GET", ts.URL+"/update/employees/", "", "")
	var cl model.ChangeList
	if json.Unmarshal([]byte(body), &cl) != nil || len(cl.Changes) != 0 {
		t.Fatalf("PUT-ul nodului de stocare nu e o modificare pentru pull: %s", body)
	}
	do(t, "POST", ts.URL+"/employee/", "application/json", `{"id":1,"salary":7000}`)
	do(t, "POST", ts.URL+"/employee/", "application/json", `{"id":1,"position":"lead"}`)
	_, body = do(t, "GET", ts.URL+"/update/employees/", "", "")
	json.Unmarshal([]byte(body), &cl)
	if len(cl.Changes) != 2 || cl.Changes[0].Employee.Salary != 7000 || cl.Changes[1].Employee.Position != "lead" || cl.Next != cl.Changes[1].Seq {
		t.Fatalf("feed: %s", body)
	}
	do(t, "DELETE", ts.URL+"/employee/?id=1", "", "")
	_, body = do(t, "GET", ts.URL+"/update/employees/?since="+cl.Next, "", "", "Accept", "application/xml")
	var cx model.ChangeList
	if xml.Unmarshal([]byte(body), &cx) != nil || len(cx.Changes) != 1 || cx.Changes[0].Op != "delete" || cx.Changes[0].DW != "dw-t1" {
		t.Fatalf("feed după cursor (XML): %s", body)
	}
}

// PUSH: DW inițiază cererea HTTP spre nodul sursă abonat, la fiecare modificare.
func TestPushNotifiesSubscribers(t *testing.T) {
	_, ts := startDW(t, "dw-t1")
	testenv.Reset(t, testenv.Store(t, "dw_test_http", "dw-t1"))
	got := make(chan string, 10)
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got <- r.Header.Get("Content-Type") + "|" + r.Header.Get("X-DW") + "|" + string(b)
	}))
	defer receiver.Close()

	if r, body := do(t, "POST", ts.URL+"/subscriptions/", "application/json",
		`{"url":"`+receiver.URL+`/push","source":"src1","format":"xml"}`); r.StatusCode != 201 {
		t.Fatalf("abonare: %d %s", r.StatusCode, body)
	}
	do(t, "PUT", ts.URL+"/employee/", "application/xml", annaXML)
	do(t, "POST", ts.URL+"/employee/", "application/json", `{"id":1,"salary":9000}`)
	select {
	case msg := <-got:
		if !strings.HasPrefix(msg, "application/xml") || !strings.Contains(msg, "|dw-t1|") || !strings.Contains(msg, "<salary>9000</salary>") || !strings.Contains(msg, "<op>update</op>") {
			t.Fatalf("notificare: %s", msg)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("nicio notificare push în 5s")
	}
	_, body := do(t, "GET", ts.URL+"/metrics", "", "")
	if !strings.Contains(body, `"pushSent": 1`) {
		t.Fatalf("metrici: %s", body)
	}
}

// Procesare concurentă: 30 de clienți modifică simultan același angajat. Serverul tratează
// cererile în paralel (maxInflight > 1), iar LWT păstrează toate modificările.
func TestConcurrentRequests(t *testing.T) {
	_, ts := startDW(t, "dw-t1")
	testenv.Reset(t, testenv.Store(t, "dw_test_http", "dw-t1"))
	do(t, "PUT", ts.URL+"/employee/", "application/json", `{"id":1,"firstName":"A","lastName":"B","salary":0}`)

	const n = 30
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				var e model.Employee
				_, body := do(t, "GET", ts.URL+"/employee/?id=1", "", "")
				json.Unmarshal([]byte(body), &e)
				resp, _ := do(t, "POST", ts.URL+"/employee/", "application/json",
					fmt.Sprintf(`{"id":1,"salary":%d}`, e.Salary+1), "If-Match", fmt.Sprintf(`"1-v%d"`, e.Version))
				if resp.StatusCode == 200 {
					return
				}
				if resp.StatusCode != 412 && resp.StatusCode != 503 {
					t.Errorf("POST: %d", resp.StatusCode)
					return
				}
			}
		}()
	}
	wg.Wait()
	_, body := do(t, "GET", ts.URL+"/employee/?id=1", "", "")
	var e model.Employee
	json.Unmarshal([]byte(body), &e)
	if e.Salary != n {
		t.Fatalf("salary = %d, așteptat %d (modificări pierdute)", e.Salary, n)
	}
	_, body = do(t, "GET", ts.URL+"/metrics", "", "")
	var m struct{ MaxInflight int64 }
	json.Unmarshal([]byte(body), &m)
	if m.MaxInflight < 2 {
		t.Fatalf("maxInflight = %d: cererile nu au fost procesate simultan", m.MaxInflight)
	}
	t.Logf("cereri simultane (max): %d", m.MaxInflight)
}

func TestHealth(t *testing.T) {
	_, ts := startDW(t, "dw-t1")
	r, body := do(t, "GET", ts.URL+"/health", "", "", "Accept", "application/xml")
	if r.StatusCode != 200 || !strings.Contains(body, "<cassandra>ok</cassandra>") || !strings.Contains(body, "<dw>dw-t1</dw>") {
		t.Fatalf("health: %d %s", r.StatusCode, body)
	}
}
