package dw

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"lab2/webproxy/internal/model"
	"lab2/webproxy/internal/store"
)

// decode citește corpul cererii după Content-Type (XML sau JSON).
func (s *Server) decode(w http.ResponseWriter, r *http.Request, v any) bool {
	f, err := model.BodyFormat(r.Header.Get("Content-Type"))
	if err != nil {
		s.fail(w, r, http.StatusUnsupportedMediaType, err.Error())
		return false
	}
	if err := model.Decode(r.Body, f, v); err != nil {
		s.fail(w, r, http.StatusBadRequest, "corp invalid ("+string(f)+"): "+err.Error())
		return false
	}
	return true
}

// ---- /employee/ ----

// employeeController: o singură entitate. GET citește, PUT creează/înlocuiește (nodurile de
// stocare), POST aplică o modificare parțială (intră în feed-ul pentru pull și e trimisă push),
// DELETE șterge.
type employeeController struct{ s *Server }

func (c employeeController) id(w http.ResponseWriter, r *http.Request) (int, bool) {
	id, err := intParam(r.URL.Query(), "id", 0, 1, 1<<31-1)
	if err != nil || id == 0 {
		c.s.fail(w, r, http.StatusBadRequest, "parametrul id lipsește sau e invalid (ex.: /employee/?id=1)")
		return 0, false
	}
	return id, true
}

func (c employeeController) get(w http.ResponseWriter, r *http.Request) {
	id, ok := c.id(w, r)
	if !ok {
		return
	}
	e, err := c.s.store.Get(r.Context(), id)
	if err != nil {
		c.s.storeError(w, r, err)
		return
	}
	c.s.write(w, r, reply{status: http.StatusOK, body: &e, cacheable: true, tags: tagsFor(id), etag: employeeETag(e)})
}

func (c employeeController) put(w http.ResponseWriter, r *http.Request) {
	var e model.Employee
	if !c.s.decode(w, r, &e) {
		return
	}
	if q := r.URL.Query().Get("id"); q != "" && q != strconv.Itoa(e.ID) {
		c.s.fail(w, r, http.StatusBadRequest, "id din URL diferă de id din corp")
		return
	}
	if err := e.Validate(); err != nil {
		c.s.fail(w, r, http.StatusBadRequest, err.Error())
		return
	}
	saved, created, err := c.s.store.Put(r.Context(), e)
	if err != nil {
		c.s.storeError(w, r, err)
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	c.s.write(w, r, reply{status: status, body: &saved, tags: tagsFor(saved.ID), etag: employeeETag(saved),
		location: "/employee/?id=" + strconv.Itoa(saved.ID)})
}

func (c employeeController) post(w http.ResponseWriter, r *http.Request) {
	var p model.Patch
	if !c.s.decode(w, r, &p) {
		return
	}
	if q := r.URL.Query().Get("id"); q != "" {
		id, err := strconv.Atoi(q)
		if err != nil || (p.ID != 0 && p.ID != id) {
			c.s.fail(w, r, http.StatusBadRequest, "id din URL diferă de id din corp")
			return
		}
		p.ID = id
	}
	if p.ID <= 0 || p.Empty() {
		c.s.fail(w, r, http.StatusBadRequest, "modificarea trebuie să aibă id și cel puțin un câmp")
		return
	}
	ifVersion, err := ifMatchVersion(r, p.ID)
	if err != nil {
		c.s.fail(w, r, http.StatusBadRequest, err.Error())
		return
	}
	e, ch, err := c.s.store.Modify(r.Context(), p, ifVersion)
	if err != nil {
		c.s.storeError(w, r, err)
		return
	}
	c.s.push.Notify(ch)
	c.s.write(w, r, reply{status: http.StatusOK, body: &e, tags: tagsFor(e.ID), etag: employeeETag(e)})
}

func (c employeeController) delete(w http.ResponseWriter, r *http.Request) {
	id, ok := c.id(w, r)
	if !ok {
		return
	}
	ifVersion, err := ifMatchVersion(r, id)
	if err != nil {
		c.s.fail(w, r, http.StatusBadRequest, err.Error())
		return
	}
	ch, err := c.s.store.Delete(r.Context(), id, ifVersion)
	if err != nil {
		c.s.storeError(w, r, err)
		return
	}
	c.s.push.Notify(ch)
	c.s.write(w, r, reply{status: http.StatusNoContent, tags: tagsFor(id)})
}

// ---- /employees/ ----

type employeesController struct{ s *Server }

func (c employeesController) list(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	offset, err := intParam(q, "offset", 0, 0, store.MaxOffset)
	if err != nil {
		c.s.fail(w, r, http.StatusBadRequest, err.Error())
		return
	}
	limit, err := intParam(q, "limit", 10, 1, 100)
	if err != nil {
		c.s.fail(w, r, http.StatusBadRequest, err.Error())
		return
	}
	es, total, err := c.s.store.List(r.Context(), offset, limit)
	if err != nil {
		c.s.storeError(w, r, err)
		return
	}
	c.s.write(w, r, reply{status: http.StatusOK, cacheable: true, tags: []string{"employees"},
		body: &model.EmployeeList{Offset: offset, Limit: limit, Total: total, Employees: es}})
}

// ---- /update/employees/ (pull) ----

// updatesController: nodurile sursă cer periodic modificările făcute prin POST/DELETE.
// ?since=<cursor> (gol = de la început), ?limit=, ?overlap=<durată> (recitește modificările
// recente de dinaintea cursorului; clientul le recunoaște după seq).
type updatesController struct{ s *Server }

func (c updatesController) changes(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, err := intParam(q, "limit", 100, 1, 1000)
	if err != nil {
		c.s.fail(w, r, http.StatusBadRequest, err.Error())
		return
	}
	var overlap time.Duration
	if v := q.Get("overlap"); v != "" {
		if overlap, err = time.ParseDuration(v); err != nil || overlap < 0 || overlap > time.Hour {
			c.s.fail(w, r, http.StatusBadRequest, "overlap invalid (ex.: 5s, cel mult 1h)")
			return
		}
	}
	since := q.Get("since")
	changes, next, err := c.s.store.Changes(r.Context(), since, overlap, limit)
	if err != nil {
		c.s.storeError(w, r, err)
		return
	}
	c.s.write(w, r, reply{status: http.StatusOK, body: &model.ChangeList{Since: since, Next: next, Changes: changes}})
}

// ---- /subscriptions/ (push) ----

type subscriptionController struct{ s *Server }

func (c subscriptionController) create(w http.ResponseWriter, r *http.Request) {
	var sub model.Subscription
	if !c.s.decode(w, r, &sub) {
		return
	}
	u, err := url.Parse(sub.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		c.s.fail(w, r, http.StatusBadRequest, fmt.Sprintf("url invalid: %q", sub.URL))
		return
	}
	if sub.Format == "" {
		sub.Format = model.JSON
	}
	if sub.Format != model.JSON && sub.Format != model.XML {
		c.s.fail(w, r, http.StatusBadRequest, "format: json sau xml")
		return
	}
	if err := c.s.store.Subscribe(r.Context(), sub); err != nil {
		c.s.storeError(w, r, err)
		return
	}
	c.s.push.Invalidate()
	c.s.log.Info("abonat push înregistrat", "event", "push.subscribe", "url", sub.URL, "source", sub.Source)
	c.s.write(w, r, reply{status: http.StatusCreated, body: &sub})
}

func (c subscriptionController) delete(w http.ResponseWriter, r *http.Request) {
	u := r.URL.Query().Get("url")
	if u == "" {
		c.s.fail(w, r, http.StatusBadRequest, "parametrul url lipsește")
		return
	}
	if err := c.s.store.Unsubscribe(r.Context(), u); err != nil {
		c.s.storeError(w, r, err)
		return
	}
	c.s.push.Invalidate()
	c.s.write(w, r, reply{status: http.StatusNoContent})
}

type subscriptionList struct {
	XMLName struct{}             `xml:"subscriptions" json:"-"`
	Items   []model.Subscription `xml:"subscription" json:"subscriptions"`
}

func (c subscriptionController) list(w http.ResponseWriter, r *http.Request) {
	subs, err := c.s.store.Subscribers(r.Context())
	if err != nil {
		c.s.storeError(w, r, err)
		return
	}
	c.s.write(w, r, reply{status: http.StatusOK, body: &subscriptionList{Items: subs}})
}
