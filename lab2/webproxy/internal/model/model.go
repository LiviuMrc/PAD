// Package model conține entitățile schimbate prin HTTP (angajați, modificări) și
// codificarea lor în XML sau JSON, la alegerea clientului.
package model

import (
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/mail"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// Employee e entitatea păstrată de DW. Version crește la fiecare modificare și e folosită
// pentru concurența optimistă (LWT în Cassandra, If-Match în HTTP).
type Employee struct {
	XMLName    xml.Name  `xml:"employee" json:"-"`
	ID         int       `xml:"id" json:"id"`
	FirstName  string    `xml:"firstName" json:"firstName"`
	LastName   string    `xml:"lastName" json:"lastName"`
	Email      string    `xml:"email" json:"email"`
	Department string    `xml:"department" json:"department"`
	Position   string    `xml:"position" json:"position"`
	Salary     int       `xml:"salary" json:"salary"`
	Source     string    `xml:"source" json:"source"` // nodul de stocare care deține entitatea
	Version    int64     `xml:"version" json:"version"`
	UpdatedAt  time.Time `xml:"updatedAt" json:"updatedAt"`
}

// Validate verifică o entitate primită prin PUT.
func (e *Employee) Validate() error {
	switch {
	case e.ID <= 0:
		return errors.New("id trebuie să fie > 0")
	case blank(e.FirstName) || utf8.RuneCountInString(e.FirstName) > 100:
		return errors.New("firstName: 1-100 caractere")
	case blank(e.LastName) || utf8.RuneCountInString(e.LastName) > 100:
		return errors.New("lastName: 1-100 caractere")
	case e.Salary < 0:
		return errors.New("salary nu poate fi negativ")
	case utf8.RuneCountInString(e.Department) > 100 || utf8.RuneCountInString(e.Position) > 100:
		return errors.New("department/position: cel mult 100 de caractere")
	case utf8.RuneCountInString(e.Source) > 64:
		return errors.New("source: cel mult 64 de caractere")
	}
	if e.Email != "" {
		if a, err := mail.ParseAddress(e.Email); err != nil || a.Address != e.Email {
			return fmt.Errorf("email invalid: %q", e.Email)
		}
	}
	return nil
}

func blank(s string) bool { return strings.TrimSpace(s) == "" }

// Patch e o modificare parțială trimisă prin POST: doar câmpurile prezente se schimbă.
type Patch struct {
	XMLName    xml.Name `xml:"employee" json:"-"`
	ID         int      `xml:"id" json:"id"`
	FirstName  *string  `xml:"firstName" json:"firstName,omitempty"`
	LastName   *string  `xml:"lastName" json:"lastName,omitempty"`
	Email      *string  `xml:"email" json:"email,omitempty"`
	Department *string  `xml:"department" json:"department,omitempty"`
	Position   *string  `xml:"position" json:"position,omitempty"`
	Salary     *int     `xml:"salary" json:"salary,omitempty"`
}

// Apply întoarce entitatea modificată și verifică rezultatul.
func (p *Patch) Apply(e Employee) (Employee, error) {
	set := func(dst *string, v *string) {
		if v != nil {
			*dst = *v
		}
	}
	set(&e.FirstName, p.FirstName)
	set(&e.LastName, p.LastName)
	set(&e.Email, p.Email)
	set(&e.Department, p.Department)
	set(&e.Position, p.Position)
	if p.Salary != nil {
		e.Salary = *p.Salary
	}
	return e, e.Validate()
}

// Empty: patch-ul nu schimbă nimic.
func (p *Patch) Empty() bool {
	return p.FirstName == nil && p.LastName == nil && p.Email == nil && p.Department == nil &&
		p.Position == nil && p.Salary == nil
}

// EmployeeList e o pagină din GET /employees/.
type EmployeeList struct {
	XMLName   xml.Name   `xml:"employees" json:"-"`
	Offset    int        `xml:"offset,attr" json:"offset"`
	Limit     int        `xml:"limit,attr" json:"limit"`
	Total     int        `xml:"total,attr" json:"total"`
	Employees []Employee `xml:"employee" json:"employees"`
}

// Change e o modificare din feed-ul GET /update/employees/ (și corpul notificării push).
type Change struct {
	XMLName  xml.Name  `xml:"change" json:"-"`
	Seq      string    `xml:"seq" json:"seq"` // timeuuid, ordonat în timp
	Op       string    `xml:"op" json:"op"`   // "update" sau "delete"
	ID       int       `xml:"id" json:"id"`
	At       time.Time `xml:"at" json:"at"`
	DW       string    `xml:"dw" json:"dw"` // instanța DW care a făcut modificarea
	Employee *Employee `xml:"employee,omitempty" json:"employee,omitempty"`
}

// ChangeList e răspunsul la GET /update/employees/.
type ChangeList struct {
	XMLName xml.Name `xml:"changes" json:"-"`
	Since   string   `xml:"since,attr" json:"since"`
	Next    string   `xml:"next,attr" json:"next"` // cursorul pentru următoarea interogare
	Changes []Change `xml:"change" json:"changes"`
}

// Subscription înregistrează un nod sursă pentru notificări push.
type Subscription struct {
	XMLName xml.Name `xml:"subscription" json:"-"`
	URL     string   `xml:"url" json:"url"`
	Source  string   `xml:"source" json:"source"`
	Format  Format   `xml:"format" json:"format"` // formatul notificărilor: json (implicit) sau xml
}

// Error e corpul răspunsurilor de eroare.
type Error struct {
	XMLName xml.Name `xml:"error" json:"-"`
	Status  int      `xml:"status" json:"status"`
	Message string   `xml:"message" json:"message"`
}

// ---- formatul datelor (XML / JSON) ----

type Format string

const (
	JSON Format = "json"
	XML  Format = "xml"
)

func (f Format) ContentType() string {
	if f == XML {
		return "application/xml; charset=utf-8"
	}
	return "application/json; charset=utf-8"
}

// ErrNotAcceptable: clientul nu acceptă niciun format pe care îl putem produce.
var ErrNotAcceptable = errors.New("format neacceptat: folosește application/json sau application/xml")

// Negotiate alege formatul răspunsului: întâi ?format=xml|json, apoi header-ul Accept
// (cu q-values), implicit JSON.
func Negotiate(r *http.Request) (Format, error) {
	switch strings.ToLower(r.URL.Query().Get("format")) {
	case "xml":
		return XML, nil
	case "json":
		return JSON, nil
	case "":
	default:
		return "", ErrNotAcceptable
	}
	return FromAccept(r.Header.Get("Accept"))
}

// FromAccept alege formatul după header-ul Accept.
func FromAccept(accept string) (Format, error) {
	if strings.TrimSpace(accept) == "" {
		return JSON, nil
	}
	type cand struct {
		f     Format
		q     float64
		order int
	}
	var cands []cand
	for i, part := range strings.Split(accept, ",") {
		mt, params, err := mime.ParseMediaType(strings.TrimSpace(part))
		if err != nil {
			continue
		}
		q := 1.0
		if v, ok := params["q"]; ok {
			if q, err = strconv.ParseFloat(v, 64); err != nil {
				continue
			}
		}
		if q <= 0 {
			continue
		}
		switch mt {
		case "application/json", "text/json":
			cands = append(cands, cand{JSON, q, i})
		case "application/xml", "text/xml":
			cands = append(cands, cand{XML, q, i})
		case "*/*", "application/*", "text/*":
			cands = append(cands, cand{JSON, q - 0.0001, i}) // wildcard: JSON, dar după tipurile explicite
		}
	}
	if len(cands) == 0 {
		return "", ErrNotAcceptable
	}
	sort.SliceStable(cands, func(a, b int) bool { return cands[a].q > cands[b].q })
	return cands[0].f, nil
}

// ErrUnsupportedMedia: corpul cererii nu e nici JSON, nici XML.
var ErrUnsupportedMedia = errors.New("Content-Type neacceptat: folosește application/json sau application/xml")

// BodyFormat alege formatul corpului cererii după Content-Type (implicit JSON).
func BodyFormat(contentType string) (Format, error) {
	if contentType == "" {
		return JSON, nil
	}
	mt, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		return "", ErrUnsupportedMedia
	}
	switch mt {
	case "application/json", "text/json":
		return JSON, nil
	case "application/xml", "text/xml":
		return XML, nil
	}
	return "", ErrUnsupportedMedia
}

// Encode scrie v în formatul cerut.
func Encode(w io.Writer, f Format, v any) error {
	if f == XML {
		if _, err := io.WriteString(w, xml.Header); err != nil {
			return err
		}
		enc := xml.NewEncoder(w)
		enc.Indent("", "  ")
		if err := enc.Encode(v); err != nil {
			return err
		}
		_, err := io.WriteString(w, "\n")
		return err
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

// Decode citește v din corp (cel mult 1 MiB), în formatul dat.
func Decode(r io.Reader, f Format, v any) error {
	r = io.LimitReader(r, 1<<20)
	if f == XML {
		return xml.NewDecoder(r).Decode(v)
	}
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}
