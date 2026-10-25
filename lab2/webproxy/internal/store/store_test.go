package store_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"lab2/webproxy/internal/model"
	"lab2/webproxy/internal/store"
	"lab2/webproxy/internal/testenv"
)

// Teste pe Cassandra reală (keyspace dw_test_store).

func emp(id int) model.Employee {
	return model.Employee{ID: id, FirstName: "Ion", LastName: fmt.Sprintf("Test%d", id), Email: "ion@example.com",
		Department: "IT", Position: "dev", Salary: 1000, Source: "src1"}
}

func TestPutGetListDelete(t *testing.T) {
	st := testenv.Store(t, "dw_test_store", "dw-test")
	testenv.Reset(t, st)
	ctx := context.Background()

	for _, id := range []int{3, 1, 2, 5, 4} {
		e, created, err := st.Put(ctx, emp(id))
		if err != nil || !created || e.Version != 1 {
			t.Fatalf("put %d: %+v %v %v", id, e, created, err)
		}
	}
	upd := emp(2)
	upd.Salary = 2000
	e, created, err := st.Put(ctx, upd)
	if err != nil || created || e.Version != 2 {
		t.Fatalf("put peste existent: %+v %v %v", e, created, err)
	}
	got, err := st.Get(ctx, 2)
	if err != nil || got.Salary != 2000 || got.Version != 2 || got.UpdatedAt.IsZero() {
		t.Fatalf("get: %+v %v", got, err)
	}
	if _, err := st.Get(ctx, 99); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("get inexistent: %v", err)
	}

	page, total, err := st.List(ctx, 1, 3)
	if err != nil || total != 5 || len(page) != 3 || page[0].ID != 2 || page[2].ID != 4 {
		t.Fatalf("list offset=1 limit=3: %v total=%d %v", ids(page), total, err)
	}
	if page, _, _ := st.List(ctx, 4, 10); len(page) != 1 || page[0].ID != 5 {
		t.Fatalf("ultima pagină: %v", ids(page))
	}

	ch, err := st.Delete(ctx, 3, 0)
	if err != nil || ch.Op != "delete" || ch.ID != 3 {
		t.Fatalf("delete: %+v %v", ch, err)
	}
	if _, err := st.Delete(ctx, 3, 0); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("delete repetat: %v", err)
	}
}

func ids(es []model.Employee) []int {
	var out []int
	for _, e := range es {
		out = append(out, e.ID)
	}
	return out
}

// Excludere reciprocă: 40 de modificări concurente pe același angajat, din 4 "instanțe DW"
// (sesiuni Cassandra separate). LWT garantează că nicio modificare nu se pierde.
func TestConcurrentModifyLosesNothing(t *testing.T) {
	stores := []*store.Store{
		testenv.Store(t, "dw_test_store", "dw-a"), testenv.Store(t, "dw_test_store", "dw-b"),
		testenv.Store(t, "dw_test_store", "dw-c"), testenv.Store(t, "dw_test_store", "dw-d"),
	}
	testenv.Reset(t, stores[0])
	ctx := context.Background()
	if _, _, err := stores[0].Put(ctx, emp(1)); err != nil {
		t.Fatal(err)
	}

	const n = 40
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			st := stores[i%len(stores)]
			for {
				cur, err := st.Get(ctx, 1)
				if err != nil {
					errs <- err
					return
				}
				salary := cur.Salary + 1
				// If-Match pe versiunea citită: o scriere concurentă face ca aceasta să fie respinsă
				_, _, err = st.Modify(ctx, model.Patch{ID: 1, Salary: &salary}, cur.Version)
				if errors.Is(err, store.ErrVersionConflict) || errors.Is(err, store.ErrContention) {
					continue
				}
				errs <- err
				return
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	got, _ := stores[0].Get(ctx, 1)
	if got.Salary != 1000+n || got.Version != 1+n {
		t.Fatalf("salary=%d version=%d, așteptat %d și %d (modificări pierdute)", got.Salary, got.Version, 1000+n, 1+n)
	}
	changes, _, err := stores[0].Changes(ctx, "", 0, 1000)
	if err != nil || len(changes) != n {
		t.Fatalf("feed: %d modificări, așteptat %d (%v)", len(changes), n, err)
	}
	dws := map[string]bool{}
	for _, c := range changes {
		dws[c.DW] = true
	}
	if len(dws) != 4 {
		t.Fatalf("modificările vin de la %d instanțe, așteptat 4", len(dws))
	}
}

func TestIfVersionConflict(t *testing.T) {
	st := testenv.Store(t, "dw_test_store", "dw-test")
	testenv.Reset(t, st)
	ctx := context.Background()
	st.Put(ctx, emp(1))
	dept := "HR"
	if _, _, err := st.Modify(ctx, model.Patch{ID: 1, Department: &dept}, 7); !errors.Is(err, store.ErrVersionConflict) {
		t.Fatalf("versiune greșită: %v", err)
	}
	e, ch, err := st.Modify(ctx, model.Patch{ID: 1, Department: &dept}, 1)
	if err != nil || e.Version != 2 || e.Department != "HR" || ch.Employee.Department != "HR" {
		t.Fatalf("modify: %+v %+v %v", e, ch, err)
	}
	if _, err := st.Delete(ctx, 1, 1); !errors.Is(err, store.ErrVersionConflict) {
		t.Fatalf("delete cu versiune veche: %v", err)
	}
	if _, _, err := st.Modify(ctx, model.Patch{ID: 42, Department: &dept}, 0); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("modify inexistent: %v", err)
	}
}

func TestChangesFeedCursorAndOverlap(t *testing.T) {
	st := testenv.Store(t, "dw_test_store", "dw-test")
	testenv.Reset(t, st)
	ctx := context.Background()
	st.Put(ctx, emp(1))
	for i := 1; i <= 5; i++ {
		s := 1000 + i
		if _, _, err := st.Modify(ctx, model.Patch{ID: 1, Salary: &s}, 0); err != nil {
			t.Fatal(err)
		}
	}
	first, next, err := st.Changes(ctx, "", 0, 3)
	if err != nil || len(first) != 3 || next != first[2].Seq {
		t.Fatalf("prima pagină: %d %s %v", len(first), next, err)
	}
	rest, next2, err := st.Changes(ctx, next, 0, 100)
	if err != nil || len(rest) != 2 || rest[1].Employee.Salary != 1005 || next2 != rest[1].Seq {
		t.Fatalf("a doua pagină: %d %v", len(rest), err)
	}
	if none, n3, _ := st.Changes(ctx, next2, 0, 100); len(none) != 0 || n3 != next2 {
		t.Fatalf("după ultima: %d %s", len(none), n3)
	}
	// cu overlap, modificările recente de dinaintea cursorului se citesc din nou
	if again, _, _ := st.Changes(ctx, next2, time.Minute, 100); len(again) != 5 {
		t.Fatalf("overlap: %d, așteptat 5", len(again))
	}
	if _, _, err := st.Changes(ctx, "nu-e-uuid", 0, 10); !errors.Is(err, store.ErrBadCursor) {
		t.Fatalf("cursor invalid: %v", err)
	}
}

func TestSubscribers(t *testing.T) {
	st := testenv.Store(t, "dw_test_store", "dw-test")
	testenv.Reset(t, st)
	ctx := context.Background()
	st.Subscribe(ctx, model.Subscription{URL: "http://127.0.0.1:1/push", Source: "src1"})
	st.Subscribe(ctx, model.Subscription{URL: "http://127.0.0.1:2/push", Source: "src2"})
	st.Subscribe(ctx, model.Subscription{URL: "http://127.0.0.1:1/push", Source: "src1"}) // idempotent
	subs, err := st.Subscribers(ctx)
	if err != nil || len(subs) != 2 {
		t.Fatalf("abonați: %v %v", subs, err)
	}
	st.Unsubscribe(ctx, "http://127.0.0.1:2/push")
	if subs, _ := st.Subscribers(ctx); len(subs) != 1 || subs[0].Source != "src1" {
		t.Fatalf("după dezabonare: %v", subs)
	}
}
