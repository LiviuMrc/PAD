package common

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const examplesDir = "../contracts/examples"
const xsdPath = "../contracts/message.xsd"

func TestXMLRoundTrip(t *testing.T) {
	m := NewMessage(TypePublish, "", "sender-1", "news", "salut <lume> & \"tot\"")
	data, err := m.ToXML()
	if err != nil {
		t.Fatal(err)
	}
	got, err := ParseXML(data)
	if err != nil {
		t.Fatal(err)
	}
	got.XMLName = m.XMLName
	if *got != *m {
		t.Fatalf("după round-trip:\n got  %+v\n want %+v", got, m)
	}
	if err := got.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestJSONIsSameMessage(t *testing.T) {
	m := NewMessage(TypePublish, "corr-1", "sender-1", "news", "salut")
	data, err := m.ToJSON()
	if err != nil {
		t.Fatal(err)
	}
	got, err := FromJSON(data)
	if err != nil {
		t.Fatal(err)
	}
	if *got != *m {
		t.Fatalf("JSON diferă: %+v vs %+v", got, m)
	}
	// exemplul JSON din contracts/ e același mesaj ca exemplul XML
	x, _ := os.ReadFile(filepath.Join(examplesDir, "valid-publish.xml"))
	j, _ := os.ReadFile(filepath.Join(examplesDir, "valid-publish.json"))
	fromXML, err := ParseXML(x)
	if err != nil {
		t.Fatal(err)
	}
	fromJSON, err := FromJSON(j)
	if err != nil {
		t.Fatal(err)
	}
	if *fromXML != *fromJSON {
		t.Fatalf("exemplele XML și JSON nu descriu același mesaj")
	}
}

func TestPayloadWithNewlineStaysOnOneLine(t *testing.T) {
	m := NewMessage(TypePublish, "", "s", "news", "linia 1\nlinia 2")
	data, _ := m.ToXML()
	if bytes.ContainsRune(data, '\n') {
		t.Fatalf("XML-ul conține newline, iar framing-ul TCP s-ar strica: %s", data)
	}
	got, err := ParseXML(data)
	if err != nil || got.Payload != m.Payload {
		t.Fatalf("payload pierdut: %v %q", err, got.Payload)
	}
}

func TestParseRejectsBadStructure(t *testing.T) {
	cases := map[string]string{
		"rădăcină greșită": `<msg><messageId>a</messageId></msg>`,
		"ordine greșită":   `<message><correlationId>a</correlationId><messageId>a</messageId></message>`,
		"element în plus":  `<message><messageId>a</messageId><extra>x</extra></message>`,
		"xml malformat":    `<message><messageId>a</message>`,
		"imbricat":         `<message><messageId><x/></messageId></message>`,
		"gol":              ``,
	}
	// pornind de la un mesaj valid, modificări pe care XSD-ul le respinge
	valid, _ := NewMessage(TypePublish, "", "s", "news", "x").ToXML()
	if _, err := ParseXML(valid); err != nil {
		t.Fatal(err)
	}
	cases["text liber în <message>"] = strings.Replace(string(valid), "<message>", "<message>text", 1)
	cases["două rădăcini"] = string(valid) + string(valid)

	for name, doc := range cases {
		if _, err := ParseXML([]byte(doc)); err == nil {
			t.Errorf("%s: trebuia respins", name)
		}
	}
}

// Test de contract: fiecare exemplu din contracts/examples e acceptat sau respins
// de cod, iar xmllint (validator XSD real) ajunge la același verdict.
// Așa se verifică dacă regulile din Validate() corespund cu XSD-ul.
func TestContractExamples(t *testing.T) {
	files, _ := filepath.Glob(filepath.Join(examplesDir, "*.xml"))
	if len(files) == 0 {
		t.Fatal("nu există exemple în contracts/examples")
	}
	for _, f := range files {
		wantValid := strings.HasPrefix(filepath.Base(f), "valid")
		data, _ := os.ReadFile(f)
		_, err := decode(data)
		if (err == nil) != wantValid {
			t.Errorf("%s: cod valid=%v, așteptat %v (err=%v)", f, err == nil, wantValid, err)
		}
		if ok, ran := xmllint(t, f); ran && ok != wantValid {
			t.Errorf("%s: xmllint valid=%v, așteptat %v", f, ok, wantValid)
		}
	}
}

// Mesajele generate de cod trebuie să treacă de XSD.
func TestGeneratedMessagesMatchXSD(t *testing.T) {
	for _, typ := range []string{TypeSubscribe, TypePublish, TypeAck, TypeError} {
		m := NewMessage(typ, "", "sender-1", "news", "ă î ș ț <>&")
		data, _ := m.ToXML()
		f := filepath.Join(t.TempDir(), "m.xml")
		_ = os.WriteFile(f, data, 0o644)
		if ok, ran := xmllint(t, f); ran && !ok {
			t.Errorf("%s generat nu respectă XSD: %s", typ, data)
		}
	}
}

func xmllint(t *testing.T, file string) (valid, ran bool) {
	t.Helper()
	if _, err := exec.LookPath("xmllint"); err != nil {
		t.Log("xmllint nu e instalat, se sare peste verificarea XSD")
		return false, false
	}
	err := exec.Command("xmllint", "--noout", "--schema", xsdPath, file).Run()
	return err == nil, true
}
