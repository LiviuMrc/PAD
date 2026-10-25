package broker

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"google.golang.org/protobuf/encoding/protojson"

	"lab2/gen/brokerpb"
)

// Jurnalul e un fișier append-only cu o înregistrare JSON pe linie. Fiecare schimbare de
// stare (abonare, publicare, confirmare, dezabonare) e scrisă și, implicit, sincronizată pe
// disc (fsync) înainte ca brokerul să răspundă. La pornire, înregistrările sunt aplicate din
// nou, în ordine, și refac cozile. Când jurnalul crește mult față de starea curentă, e rescris
// ca snapshot (fișier temporar + rename, deci atomic).

const (
	opSubscribe   = "sub"
	opPublish     = "pub"
	opAck         = "ack"
	opUnsubscribe = "unsub"
	opSeen        = "seen"    // messageId-urile ținute minte pentru deduplicare (doar în snapshot)
	opExpire      = "expire"  // receiver offline peste TTL: coada lui trece în DLQ
	opDead        = "dead"    // un mesaj din DLQ (doar în snapshot)
	opRedrive     = "redrive" // mesaje mutate din DLQ înapoi în coada receiverului
	opPurge       = "purge"   // mesaje șterse din DLQ
)

type record struct {
	Op              string          `json:"op"`
	Receiver        string          `json:"receiver,omitempty"`
	Topics          []string        `json:"topics,omitempty"`
	Msg             json.RawMessage `json:"msg,omitempty"` // brokerpb.Message în protojson
	To              []string        `json:"to,omitempty"`
	Subscribers     int32           `json:"subscribers,omitempty"`
	MessageID       string          `json:"messageId,omitempty"`
	Seen            []string        `json:"seen,omitempty"`
	SeenSubscribers []int32         `json:"seenSubscribers,omitempty"`
	Reason          string          `json:"reason,omitempty"`
	MessageIDs      []string        `json:"messageIds,omitempty"`
	At              string          `json:"at,omitempty"` // momentul operației, UTC (pentru dead_at)
}

// at întoarce momentul operației; jurnalele mai vechi nu îl au, deci se folosește acum.
func (r record) at(now func() time.Time) time.Time {
	if t, err := time.Parse(time.RFC3339Nano, r.At); err == nil {
		return t
	}
	return now()
}

func publishRecord(m *brokerpb.Message, to []string, subscribers int32, at time.Time) (record, error) {
	js, err := protojson.Marshal(m)
	if err != nil {
		return record{}, err
	}
	return record{Op: opPublish, Msg: js, To: to, Subscribers: subscribers, At: at.UTC().Format(time.RFC3339Nano)}, nil
}

func (r record) message() (*brokerpb.Message, error) {
	m := &brokerpb.Message{}
	if err := protojson.Unmarshal(r.Msg, m); err != nil {
		return nil, fmt.Errorf("mesaj invalid în jurnal: %w", err)
	}
	return m, nil
}

type Journal struct {
	path      string
	fsync     bool
	compactAt int // compactare abia de la acest număr de înregistrări

	f       *os.File
	records int
	trimmed int // octeți tăiați de la final la Load (scriere întreruptă)
}

// OpenJournal folosește fișierul broker.journal din dir. fsync = false e mai rapid,
// dar o cădere a sistemului (nu doar a procesului) poate pierde ultimele înregistrări.
func OpenJournal(dir string, fsync bool) (*Journal, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &Journal{path: filepath.Join(dir, "broker.journal"), fsync: fsync, compactAt: 10000}, nil
}

func (j *Journal) Path() string { return j.path }
func (j *Journal) Records() int { return j.records }

// Trimmed întoarce câți octeți incompleți de la finalul jurnalului au fost tăiați la Load.
func (j *Journal) Trimmed() int { return j.trimmed }

// Load citește înregistrările și deschide jurnalul pentru scriere. O ultimă linie incompletă
// (procesul a căzut în timpul scrierii) e tăiată: brokerul nu a confirmat niciodată acea operație.
// O linie invalidă în mijlocul fișierului înseamnă corupere și e o eroare.
func (j *Journal) Load() ([]record, error) {
	data, err := os.ReadFile(j.path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	var recs []record
	good := 0
	for line := 1; good < len(data); line++ {
		end := bytes.IndexByte(data[good:], '\n')
		if end < 0 {
			break // ultima linie, fără '\n': scriere întreruptă
		}
		var r record
		if err := json.Unmarshal(data[good:good+end], &r); err != nil {
			if good+end+1 == len(data) {
				break // ultima linie e coruptă: tot o scriere întreruptă
			}
			return nil, fmt.Errorf("%s:%d: înregistrare coruptă: %w", j.path, line, err)
		}
		recs = append(recs, r)
		good += end + 1
	}
	if good < len(data) {
		j.trimmed = len(data) - good
		if err := os.Truncate(j.path, int64(good)); err != nil {
			return nil, err
		}
	}
	j.f, err = os.OpenFile(j.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	j.records = len(recs)
	return recs, nil
}

// Append scrie o înregistrare (o singură scriere, cu '\n' la final) și face fsync.
func (j *Journal) Append(r record) error {
	if j.f == nil {
		return errors.New("jurnal închis")
	}
	line, err := json.Marshal(r)
	if err != nil {
		return err
	}
	if _, err := j.f.Write(append(line, '\n')); err != nil {
		return err
	}
	if j.fsync {
		if err := j.f.Sync(); err != nil {
			return err
		}
	}
	j.records++
	return nil
}

// NeedsCompaction: jurnalul are cel puțin compactAt înregistrări și de peste 2 ori
// mai multe decât ar avea un snapshot al stării curente.
func (j *Journal) NeedsCompaction(liveSize int) bool {
	return j.records >= j.compactAt && j.records > 2*liveSize
}

// Rewrite înlocuiește atomic jurnalul cu recs (snapshot-ul stării curente).
func (j *Journal) Rewrite(recs []record) error {
	tmp := j.path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	var buf bytes.Buffer
	for _, r := range recs {
		line, err := json.Marshal(r)
		if err != nil {
			f.Close()
			return err
		}
		buf.Write(line)
		buf.WriteByte('\n')
	}
	if _, err := f.Write(buf.Bytes()); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, j.path); err != nil {
		return err
	}
	if d, err := os.Open(filepath.Dir(j.path)); err == nil { // rename-ul e durabil abia după fsync pe director
		d.Sync()
		d.Close()
	}
	if j.f != nil {
		j.f.Close()
	}
	j.f, err = os.OpenFile(j.path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	j.records = len(recs)
	return nil
}

func (j *Journal) Close() error {
	if j == nil || j.f == nil {
		return nil
	}
	err := j.f.Close()
	j.f = nil
	return err
}

// snapshot descrie starea curentă ca înregistrări minime (cu mu luat): messageId-urile
// pentru deduplicare, abonările, mesajele neconfirmate în ordinea publicării (fiecare cu
// receiverele care încă îl au în coadă), apoi DLQ-ul, în ordine.
func (b *Broker) snapshot() []record {
	var recs []record
	if keys := b.seen.Keys(); len(keys) > 0 {
		subs := make([]int32, len(keys))
		for i, k := range keys {
			subs[i], _ = b.seen.Get(k)
		}
		recs = append(recs, record{Op: opSeen, Seen: keys, SeenSubscribers: subs})
	}
	ids := make([]string, 0, len(b.receivers))
	for id := range b.receivers {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	type pending struct {
		msg *brokerpb.Message
		to  []string
	}
	bySeq := map[uint64]*pending{}
	for _, id := range ids {
		r := b.receivers[id]
		recs = append(recs, record{Op: opSubscribe, Receiver: id, Topics: r.topics})
		for _, e := range r.queue {
			p := bySeq[e.seq]
			if p == nil {
				p = &pending{msg: e.msg}
				bySeq[e.seq] = p
			}
			p.to = append(p.to, id)
		}
	}
	seqs := make([]uint64, 0, len(bySeq))
	for s := range bySeq {
		seqs = append(seqs, s)
	}
	sort.Slice(seqs, func(a, c int) bool { return seqs[a] < seqs[c] })
	for _, s := range seqs {
		p := bySeq[s]
		n, ok := b.seen.Get(p.msg.MessageId)
		if !ok {
			n = int32(len(p.to))
		}
		rec, err := publishRecord(p.msg, p.to, n, time.Time{})
		if err == nil {
			rec.At = "" // în snapshot cozile nu depășesc MaxQueue, deci momentul nu contează
			recs = append(recs, rec)
		}
	}
	for _, d := range b.dlq {
		js, err := protojson.Marshal(d.msg)
		if err != nil {
			continue
		}
		recs = append(recs, record{Op: opDead, Receiver: d.receiver, Reason: d.reason, Msg: js,
			At: d.at.UTC().Format(time.RFC3339Nano)})
	}
	return recs
}

func (b *Broker) liveSize() int {
	n := 1 + len(b.receivers) + len(b.dlq)
	for _, r := range b.receivers {
		n += len(r.queue)
	}
	return n
}
