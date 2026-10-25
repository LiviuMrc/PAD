package model

import (
	"bytes"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func sample() Employee {
	return Employee{ID: 7, FirstName: "Ana", LastName: "Popescu", Email: "ana@example.com",
		Department: "IT", Position: "dev", Salary: 5000, Source: "src1", Version: 3,
		UpdatedAt: time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)}
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	for _, f := range []Format{JSON, XML} {
		var buf bytes.Buffer
		e := sample()
		if err := Encode(&buf, f, &e); err != nil {
			t.Fatal(err)
		}
		var back Employee
		if err := Decode(&buf, f, &back); err != nil {
			t.Fatalf("%s: %v", f, err)
		}
		back.XMLName = e.XMLName
		if back != e {
			t.Fatalf("%s: %+v != %+v", f, back, e)
		}
	}
	var buf bytes.Buffer
	e := sample()
	Encode(&buf, XML, &e)
	if !strings.HasPrefix(buf.String(), "<?xml") || !strings.Contains(buf.String(), "<firstName>Ana</firstName>") {
		t.Fatalf("XML: %s", buf.String())
	}
}

func TestNegotiate(t *testing.T) {
	cases := []struct {
		url, accept string
		want        Format
		err         bool
	}{
		{"/employees/", "", JSON, false},
		{"/employees/", "application/xml", XML, false},
		{"/employees/", "text/xml", XML, false},
		{"/employees/", "application/json;q=0.5, application/xml;q=0.9", XML, false},
		{"/employees/", "application/xml;q=0.1, application/json", JSON, false},
		{"/employees/", "*/*", JSON, false},
		{"/employees/", "text/html, */*;q=0.8, application/xml", XML, false},
		{"/employees/?format=xml", "application/json", XML, false},
		{"/employees/?format=json", "application/xml", JSON, false},
		{"/employees/", "text/html", "", true},
		{"/employees/", "application/xml;q=0", "", true},
		{"/employees/?format=yaml", "", "", true},
	}
	for _, c := range cases {
		r := httptest.NewRequest("GET", c.url, nil)
		r.Header.Set("Accept", c.accept)
		got, err := Negotiate(r)
		if (err != nil) != c.err || got != c.want {
			t.Errorf("%s Accept=%q: %q %v, așteptat %q (eroare=%v)", c.url, c.accept, got, err, c.want, c.err)
		}
	}
}

func TestBodyFormat(t *testing.T) {
	for ct, want := range map[string]Format{"": JSON, "application/json": JSON, "application/xml; charset=utf-8": XML, "text/xml": XML} {
		if got, err := BodyFormat(ct); err != nil || got != want {
			t.Errorf("%q: %q %v", ct, got, err)
		}
	}
	if _, err := BodyFormat("text/plain"); err != ErrUnsupportedMedia {
		t.Errorf("text/plain: %v", err)
	}
}

func TestValidateAndPatch(t *testing.T) {
	e := sample()
	if err := e.Validate(); err != nil {
		t.Fatal(err)
	}
	for name, bad := range map[string]func(*Employee){
		"id":     func(e *Employee) { e.ID = 0 },
		"nume":   func(e *Employee) { e.FirstName = "  " },
		"salary": func(e *Employee) { e.Salary = -1 },
		"email":  func(e *Employee) { e.Email = "nu-e-email" },
	} {
		x := sample()
		bad(&x)
		if x.Validate() == nil {
			t.Errorf("%s: trebuia respins", name)
		}
	}
	salary, dept := 6000, "HR"
	p := Patch{ID: 7, Salary: &salary, Department: &dept}
	got, err := p.Apply(sample())
	if err != nil || got.Salary != 6000 || got.Department != "HR" || got.FirstName != "Ana" {
		t.Fatalf("patch: %+v %v", got, err)
	}
	empty := ""
	if _, err := (&Patch{ID: 7, LastName: &empty}).Apply(sample()); err == nil {
		t.Fatal("patch care golește lastName trebuia respins")
	}
	if !(&Patch{ID: 7}).Empty() || p.Empty() {
		t.Fatal("Empty greșit")
	}
}

func TestDecodeRejectsUnknownJSONFields(t *testing.T) {
	var e Employee
	if err := Decode(strings.NewReader(`{"id":1,"firstName":"a","lastName":"b","nuExista":1}`), JSON, &e); err == nil {
		t.Fatal("câmp necunoscut acceptat")
	}
}
