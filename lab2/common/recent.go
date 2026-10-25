package common

// Recent ține minte ultimele n chei adăugate (FIFO), fiecare cu o valoare.
// Brokerul o folosește pentru deduplicarea Publish după messageId, iar receiverul
// pentru a ignora mesajele retrimise. Nu e sigură pentru apeluri concurente.
type Recent[V any] struct {
	n     int
	m     map[string]V
	order []string
}

func NewRecent[V any](n int) *Recent[V] {
	return &Recent[V]{n: n, m: make(map[string]V, n)}
}

func (r *Recent[V]) Get(key string) (V, bool) {
	v, ok := r.m[key]
	return v, ok
}

// Add adaugă cheia, dacă nu există deja. Când se depășesc n chei, se uită cea mai veche.
func (r *Recent[V]) Add(key string, v V) {
	if _, ok := r.m[key]; ok || r.n <= 0 {
		return
	}
	r.m[key] = v
	r.order = append(r.order, key)
	if len(r.order) > r.n {
		delete(r.m, r.order[0])
		r.order = r.order[1:]
	}
}

// Keys întoarce cheile în ordinea adăugării (pentru snapshot-ul jurnalului).
func (r *Recent[V]) Keys() []string {
	return append([]string(nil), r.order...)
}

func (r *Recent[V]) Len() int { return len(r.order) }
