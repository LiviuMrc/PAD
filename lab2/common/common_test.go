package common

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"google.golang.org/grpc/encoding"
	"google.golang.org/protobuf/proto"

	"lab2/gen/brokerpb"
)

// #4: .env setează variabilele lipsă, iar mediul real are prioritate.
func TestLoadDotEnv(t *testing.T) {
	p := filepath.Join(t.TempDir(), ".env")
	os.WriteFile(p, []byte(`# comentariu
LAB2_TEST_ADDR=127.0.0.1:6000
export LAB2_TEST_QUOTED="cu spații"
LAB2_TEST_REAL=din-fișier

`), 0o644)
	t.Setenv("LAB2_TEST_REAL", "din-mediu")
	os.Unsetenv("LAB2_TEST_ADDR")
	os.Unsetenv("LAB2_TEST_QUOTED")
	t.Cleanup(func() { os.Unsetenv("LAB2_TEST_ADDR"); os.Unsetenv("LAB2_TEST_QUOTED") })

	keys, err := LoadDotEnv(p)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(keys, []string{"LAB2_TEST_ADDR", "LAB2_TEST_QUOTED"}) {
		t.Fatalf("chei setate: %v", keys)
	}
	if Env("LAB2_TEST_ADDR", "") != "127.0.0.1:6000" || Env("LAB2_TEST_QUOTED", "") != "cu spații" {
		t.Fatal("valori citite greșit")
	}
	if Env("LAB2_TEST_REAL", "") != "din-mediu" {
		t.Fatal(".env a suprascris mediul real")
	}
	if keys, err := LoadDotEnv(filepath.Join(t.TempDir(), "lipsă")); err != nil || keys != nil {
		t.Fatalf("fișier lipsă: %v %v", keys, err)
	}
	bad := filepath.Join(t.TempDir(), ".env")
	os.WriteFile(bad, []byte("FARA_EGAL\n"), 0o644)
	if _, err := LoadDotEnv(bad); err == nil || !strings.Contains(err.Error(), ":1:") {
		t.Fatalf("linie invalidă: %v", err)
	}
}

func TestJSONCodecIsRegisteredAndRoundTrips(t *testing.T) {
	c := encoding.GetCodec("json")
	if c == nil {
		t.Fatal("codec-ul json nu e înregistrat în gRPC")
	}
	m := NewMessage("", "sender-1", "news", "ă î ș ț")
	b, err := c.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `"payload":"ă î ș ț"`) {
		t.Fatalf("nu e JSON: %s", b)
	}
	var back brokerpb.Message
	if err := c.Unmarshal(append(b[:len(b)-1], []byte(`,"campNou":1}`)...), &back); err != nil || !proto.Equal(m, &back) {
		t.Fatalf("round-trip (cu un câmp necunoscut): %v", err)
	}
	if _, err := c.Marshal("nu e proto"); err == nil {
		t.Fatal("trebuia eroare pentru un tip non-proto")
	}
}

func TestRecentKeepsLastN(t *testing.T) {
	r := NewRecent[int](3)
	for i, k := range []string{"a", "b", "c", "a", "d"} {
		r.Add(k, i)
	}
	if _, ok := r.Get("a"); ok {
		t.Fatal("cea mai veche cheie trebuia uitată")
	}
	if v, _ := r.Get("b"); v != 1 || !slices.Equal(r.Keys(), []string{"b", "c", "d"}) {
		t.Fatalf("chei: %v", r.Keys())
	}
}
