// Package store păstrează datele DW în Cassandra. Toate instanțele DW folosesc aceeași
// instanță Cassandra, deci nu au nevoie de sincronizare între ele.
//
// Excluderea reciprocă la accesul simultan (inclusiv între instanțe DW diferite) e făcută
// de Cassandra prin lightweight transactions (Paxos): fiecare scriere e condiționată de
// versiunea citită (UPDATE ... IF version = ?), iar la conflict operația se reia.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/gocql/gocql"

	"lab2/webproxy/internal/model"
)

var (
	ErrNotFound        = errors.New("angajatul nu există")
	ErrVersionConflict = errors.New("versiunea nu corespunde (entitatea a fost modificată între timp)")
	ErrContention      = errors.New("prea multe modificări simultane; reîncearcă")
	ErrBadCursor       = errors.New("cursor invalid")
	ErrInvalid         = errors.New("date invalide")
)

const (
	bucket     = 0     // toți angajații în aceeași partiție, ordonați după id (paginare simplă)
	maxRetries = 20    // reîncercări LWT la conflict
	MaxOffset  = 10000 // Cassandra nu are OFFSET: se citesc offset+limit rânduri
)

type Config struct {
	Hosts    []string // host:port; pentru Docker, adresa publicată pe host
	Keyspace string
	DW       string // numele instanței DW, salvat în feed-ul de modificări
	Timeout  time.Duration
}

type Store struct {
	s   *gocql.Session
	dw  string
	now func() time.Time
}

// Connect se conectează la Cassandra (reîncearcă până la Timeout, cât timp pornește) și
// creează keyspace-ul și tabelele, dacă lipsesc.
func Connect(ctx context.Context, cfg Config) (*Store, error) {
	if cfg.Timeout == 0 {
		cfg.Timeout = 2 * time.Minute
	}
	if strings.ContainsFunc(cfg.Keyspace, func(r rune) bool { return !(r == '_' || r >= 'a' && r <= 'z' || r >= '0' && r <= '9') }) {
		return nil, fmt.Errorf("keyspace invalid: %q", cfg.Keyspace)
	}
	deadline := time.Now().Add(cfg.Timeout)
	for {
		s, err := open(cfg)
		if err == nil {
			return &Store{s: s, dw: cfg.DW, now: time.Now}, nil
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("cassandra indisponibilă după %v: %w", cfg.Timeout, err)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

// open creează keyspace-ul (sesiune fără keyspace), apoi deschide sesiunea legată de
// keyspace și creează tabelele.
func open(cfg Config) (*gocql.Session, error) {
	boot, err := cluster(cfg, "").CreateSession()
	if err != nil {
		return nil, err
	}
	err = boot.Query(`CREATE KEYSPACE IF NOT EXISTS ` + cfg.Keyspace +
		` WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}`).Exec()
	boot.Close()
	if err != nil {
		return nil, fmt.Errorf("keyspace: %w", err)
	}
	s, err := cluster(cfg, cfg.Keyspace).CreateSession()
	if err != nil {
		return nil, err
	}
	for _, q := range []string{
		`CREATE TABLE IF NOT EXISTS employees (
			bucket int, id int, first_name text, last_name text, email text, department text,
			position text, salary int, source text, version bigint, updated_at timestamp,
			PRIMARY KEY ((bucket), id))`,
		// feed-ul de modificări pentru pull (GET /update/employees/); păstrat 7 zile
		`CREATE TABLE IF NOT EXISTS employee_changes (
			bucket int, seq timeuuid, op text, id int, dw text, employee text,
			PRIMARY KEY ((bucket), seq)) WITH default_time_to_live = 604800`,
		`CREATE TABLE IF NOT EXISTS push_subscribers (url text PRIMARY KEY, source text, format text, created_at timestamp)`,
	} {
		if err := s.Query(q).Exec(); err != nil {
			s.Close()
			return nil, fmt.Errorf("schema: %w", err)
		}
	}
	return s, nil
}

func cluster(cfg Config, keyspace string) *gocql.ClusterConfig {
	c := gocql.NewCluster(cfg.Hosts...)
	c.Keyspace = keyspace
	c.Consistency = gocql.Quorum
	c.SerialConsistency = gocql.LocalSerial
	c.Timeout = 10 * time.Second
	c.ConnectTimeout = 5 * time.Second
	c.ProtoVersion = 4
	// Cassandra din Docker își anunță adresa internă a containerului; conexiunile merg
	// mereu la adresa configurată (portul publicat pe host).
	c.DisableInitialHostLookup = true
	if host, portStr, err := net.SplitHostPort(cfg.Hosts[0]); err == nil {
		port, _ := strconv.Atoi(portStr)
		ip := net.ParseIP(host)
		if ip == nil {
			if ips, err := net.LookupIP(host); err == nil && len(ips) > 0 {
				ip = ips[0]
			}
		}
		c.AddressTranslator = gocql.AddressTranslatorFunc(func(net.IP, int) (net.IP, int) { return ip, port })
	}
	return c
}

func (st *Store) Close() { st.s.Close() }

// Ping verifică legătura cu Cassandra.
func (st *Store) Ping(ctx context.Context) error {
	return st.s.Query(`SELECT release_version FROM system.local`).WithContext(ctx).Exec()
}

const cols = `id, first_name, last_name, email, department, position, salary, source, version, updated_at`

func scanEmployee(sc interface{ Scan(...any) bool }) (model.Employee, bool) {
	var e model.Employee
	ok := sc.Scan(&e.ID, &e.FirstName, &e.LastName, &e.Email, &e.Department, &e.Position,
		&e.Salary, &e.Source, &e.Version, &e.UpdatedAt)
	e.UpdatedAt = e.UpdatedAt.UTC()
	return e, ok
}

// Get citește un angajat.
func (st *Store) Get(ctx context.Context, id int) (model.Employee, error) {
	it := st.s.Query(`SELECT `+cols+` FROM employees WHERE bucket = ? AND id = ?`, bucket, id).WithContext(ctx).Iter()
	e, ok := scanEmployee(it)
	if err := it.Close(); err != nil {
		return model.Employee{}, err
	}
	if !ok {
		return model.Employee{}, ErrNotFound
	}
	return e, nil
}

// List întoarce angajații ordonați după id, de la offset, cel mult limit, plus totalul.
func (st *Store) List(ctx context.Context, offset, limit int) ([]model.Employee, int, error) {
	if offset < 0 || offset > MaxOffset || limit <= 0 {
		return nil, 0, fmt.Errorf("offset 0-%d și limit > 0", MaxOffset)
	}
	var total int
	if err := st.s.Query(`SELECT COUNT(*) FROM employees WHERE bucket = ?`, bucket).WithContext(ctx).Scan(&total); err != nil {
		return nil, 0, err
	}
	it := st.s.Query(`SELECT `+cols+` FROM employees WHERE bucket = ? LIMIT ?`, bucket, offset+limit).
		WithContext(ctx).PageSize(500).Iter()
	out := []model.Employee{}
	for i := 0; ; i++ {
		e, ok := scanEmployee(it)
		if !ok {
			break
		}
		if i >= offset {
			out = append(out, e)
		}
	}
	return out, total, it.Close()
}

// Put creează sau înlocuiește un angajat (date trimise de nodurile de stocare).
// created = true dacă nu exista.
func (st *Store) Put(ctx context.Context, e model.Employee) (model.Employee, bool, error) {
	e.UpdatedAt = st.now().UTC().Truncate(time.Millisecond)
	e.Version = 1
	applied, err := st.s.Query(`INSERT INTO employees (bucket, `+cols+`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS`,
		bucket, e.ID, e.FirstName, e.LastName, e.Email, e.Department, e.Position, e.Salary, e.Source, e.Version, e.UpdatedAt).
		WithContext(ctx).MapScanCAS(map[string]any{})
	if err != nil {
		return model.Employee{}, false, err
	}
	if applied {
		return e, true, nil
	}
	for i := 0; i < maxRetries; i++ {
		cur, err := st.Get(ctx, e.ID)
		if errors.Is(err, ErrNotFound) {
			return st.Put(ctx, e) // a fost șters între timp
		}
		if err != nil {
			return model.Employee{}, false, err
		}
		e.Version = cur.Version + 1
		ok, err := st.cas(ctx, e, cur.Version)
		if err != nil {
			return model.Employee{}, false, err
		}
		if ok {
			return e, false, nil
		}
	}
	return model.Employee{}, false, ErrContention
}

// cas scrie e doar dacă versiunea din Cassandra e încă expect.
func (st *Store) cas(ctx context.Context, e model.Employee, expect int64) (bool, error) {
	return st.s.Query(`UPDATE employees SET first_name = ?, last_name = ?, email = ?, department = ?,
		position = ?, salary = ?, source = ?, version = ?, updated_at = ?
		WHERE bucket = ? AND id = ? IF version = ?`,
		e.FirstName, e.LastName, e.Email, e.Department, e.Position, e.Salary, e.Source, e.Version, e.UpdatedAt,
		bucket, e.ID, expect).WithContext(ctx).MapScanCAS(map[string]any{})
}

// Modify aplică o modificare parțială (POST). ifVersion > 0 cere ca entitatea să aibă exact
// acea versiune (If-Match). Modificarea e adăugată în feed-ul pentru nodurile sursă.
func (st *Store) Modify(ctx context.Context, p model.Patch, ifVersion int64) (model.Employee, model.Change, error) {
	for i := 0; i < maxRetries; i++ {
		cur, err := st.Get(ctx, p.ID)
		if err != nil {
			return model.Employee{}, model.Change{}, err
		}
		if ifVersion > 0 && cur.Version != ifVersion {
			return cur, model.Change{}, ErrVersionConflict
		}
		next, err := p.Apply(cur)
		if err != nil {
			return model.Employee{}, model.Change{}, fmt.Errorf("%w: %v", ErrInvalid, err)
		}
		next.Version = cur.Version + 1
		next.UpdatedAt = st.now().UTC().Truncate(time.Millisecond)
		ok, err := st.cas(ctx, next, cur.Version)
		if err != nil {
			return model.Employee{}, model.Change{}, err
		}
		if ok {
			ch, err := st.recordChange(ctx, "update", next.ID, &next)
			return next, ch, err
		}
	}
	return model.Employee{}, model.Change{}, ErrContention
}

// Delete șterge un angajat (ifVersion > 0: doar dacă are acea versiune).
func (st *Store) Delete(ctx context.Context, id int, ifVersion int64) (model.Change, error) {
	cur, err := st.Get(ctx, id)
	if err != nil {
		return model.Change{}, err
	}
	if ifVersion > 0 && cur.Version != ifVersion {
		return model.Change{}, ErrVersionConflict
	}
	applied, err := st.s.Query(`DELETE FROM employees WHERE bucket = ? AND id = ? IF EXISTS`, bucket, id).
		WithContext(ctx).MapScanCAS(map[string]any{})
	if err != nil {
		return model.Change{}, err
	}
	if !applied {
		return model.Change{}, ErrNotFound
	}
	cur.Version++
	return st.recordChange(ctx, "delete", id, &cur)
}

func (st *Store) recordChange(ctx context.Context, op string, id int, e *model.Employee) (model.Change, error) {
	seq := gocql.UUIDFromTime(st.now())
	js, err := json.Marshal(e)
	if err != nil {
		return model.Change{}, err
	}
	ch := model.Change{Seq: seq.String(), Op: op, ID: id, At: seq.Time().UTC(), DW: st.dw, Employee: e}
	err = st.s.Query(`INSERT INTO employee_changes (bucket, seq, op, id, dw, employee) VALUES (?, ?, ?, ?, ?, ?)`,
		bucket, seq, op, id, st.dw, string(js)).WithContext(ctx).Exec()
	return ch, err
}

// Changes întoarce modificările de după cursorul since (gol = de la început), cel mult limit.
// overlap > 0 citește și modificările din ultimele overlap dinaintea cursorului: o modificare
// făcută de altă instanță DW poate primi un seq puțin mai vechi decât cursorul (ceasuri,
// scrieri lente), iar clientul o recunoaște după seq.
func (st *Store) Changes(ctx context.Context, since string, overlap time.Duration, limit int) ([]model.Change, string, error) {
	q := st.s.Query(`SELECT seq, op, id, dw, employee FROM employee_changes WHERE bucket = ? LIMIT ?`, bucket, limit)
	if since != "" {
		u, err := gocql.ParseUUID(since)
		if err != nil || u.Version() != 1 {
			return nil, "", ErrBadCursor
		}
		if overlap > 0 {
			q = st.s.Query(`SELECT seq, op, id, dw, employee FROM employee_changes WHERE bucket = ? AND seq > minTimeuuid(?) LIMIT ?`,
				bucket, u.Time().Add(-overlap), limit)
		} else {
			q = st.s.Query(`SELECT seq, op, id, dw, employee FROM employee_changes WHERE bucket = ? AND seq > ? LIMIT ?`,
				bucket, u, limit)
		}
	}
	it := q.WithContext(ctx).Iter()
	out := []model.Change{}
	next := since
	var seq gocql.UUID
	var op, dw, js string
	var id int
	for it.Scan(&seq, &op, &id, &dw, &js) {
		ch := model.Change{Seq: seq.String(), Op: op, ID: id, DW: dw, At: seq.Time().UTC()}
		var e model.Employee
		if json.Unmarshal([]byte(js), &e) == nil {
			ch.Employee = &e
		}
		out = append(out, ch)
		if next == "" || laterUUID(seq, next) {
			next = seq.String()
		}
	}
	return out, next, it.Close()
}

func laterUUID(u gocql.UUID, than string) bool {
	t, err := gocql.ParseUUID(than)
	return err != nil || u.Time().After(t.Time()) || (u.Time().Equal(t.Time()) && u.String() > than)
}

// ---- abonații push ----

func (st *Store) Subscribe(ctx context.Context, s model.Subscription) error {
	return st.s.Query(`INSERT INTO push_subscribers (url, source, format, created_at) VALUES (?, ?, ?, ?)`,
		s.URL, s.Source, string(s.Format), st.now().UTC()).WithContext(ctx).Exec()
}

func (st *Store) Unsubscribe(ctx context.Context, url string) error {
	return st.s.Query(`DELETE FROM push_subscribers WHERE url = ?`, url).WithContext(ctx).Exec()
}

func (st *Store) Subscribers(ctx context.Context) ([]model.Subscription, error) {
	it := st.s.Query(`SELECT url, source, format FROM push_subscribers`).WithContext(ctx).Iter()
	var out []model.Subscription
	var s model.Subscription
	var f string
	for it.Scan(&s.URL, &s.Source, &f) {
		s.Format = model.Format(f)
		out = append(out, s)
	}
	return out, it.Close()
}

// Truncate golește tabelele (folosit de teste, pe un keyspace de test).
func (st *Store) Truncate(ctx context.Context) error {
	for _, t := range []string{"employees", "employee_changes", "push_subscribers"} {
		if err := st.s.Query(`TRUNCATE ` + t).WithContext(ctx).Exec(); err != nil {
			return err
		}
	}
	return nil
}
