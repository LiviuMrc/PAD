// Package common conține contractul mesajului și stratul de transport,
// folosite de broker, sender și receiver.
package common

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

// Tipurile de mesaj din protocol.
const (
	TypeSubscribe = "SUBSCRIBE"
	TypePublish   = "PUBLISH"
	TypeAck       = "ACK"
	TypeError     = "ERROR"

	SchemaVersion = "1.0"
	MaxPayload    = 4096
)

// Message este mesajul din contracts/message.xsd. Ordinea câmpurilor
// e aceeași ca în xs:sequence, pentru că xml.Marshal le scrie în ordinea asta.
type Message struct {
	XMLName       xml.Name `xml:"message" json:"-"`
	MessageID     string   `xml:"messageId" json:"messageId"`
	CorrelationID string   `xml:"correlationId" json:"correlationId"`
	MessageType   string   `xml:"messageType" json:"messageType"`
	SchemaVersion string   `xml:"schemaVersion" json:"schemaVersion"`
	OccurredAt    string   `xml:"occurredAt" json:"occurredAt"`
	Sender        string   `xml:"sender" json:"sender"`
	Topic         string   `xml:"topic" json:"topic"`
	Payload       string   `xml:"payload" json:"payload"`
}

// NewMessage creează un mesaj cu messageId nou, occurredAt în UTC și versiunea schemei.
// Dacă correlationID e gol, se generează unul nou.
func NewMessage(msgType, correlationID, sender, topic, payload string) *Message {
	if correlationID == "" {
		correlationID = NewID()
	}
	return &Message{
		MessageID:     NewID(),
		CorrelationID: correlationID,
		MessageType:   msgType,
		SchemaVersion: SchemaVersion,
		OccurredAt:    time.Now().UTC().Format(time.RFC3339),
		Sender:        sender,
		Topic:         topic,
		Payload:       payload,
	}
}

// NewID generează un UUID v4 folosind doar biblioteca standard.
func NewID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// ToXML serializează mesajul pe o singură linie. Newline-urile din payload
// sunt escapate (&#xA;), deci "\n" poate fi folosit ca delimitator pe TCP.
func (m *Message) ToXML() ([]byte, error) {
	return xml.Marshal(m)
}

// ToJSON serializează același mesaj în JSON (suportul JSON minim).
func (m *Message) ToJSON() ([]byte, error) {
	return json.Marshal(m)
}

// FromJSON citește un mesaj serializat în JSON.
func FromJSON(data []byte) (*Message, error) {
	var m Message
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

// fieldOrder este ordinea elementelor din xs:sequence (contracts/message.xsd).
var fieldOrder = []string{
	"messageId", "correlationId", "messageType", "schemaVersion",
	"occurredAt", "sender", "topic", "payload",
}

// ParseXML parsează mesajul în stil SAX: citește evenimente (StartElement,
// CharData, EndElement) din xml.Decoder, fără să construiască tot arborele în memorie.
// Tot aici verifică structura cerută de XSD: rădăcina <message>, elementele
// cunoscute, ordinea lor și faptul că nu există elemente imbricate.
func ParseXML(data []byte) (*Message, error) {
	dec := xml.NewDecoder(bytes.NewReader(data))
	values := map[string]string{}
	depth, next := 0, 0
	current := ""
	var text strings.Builder

	for {
		tok, err := dec.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("xml malformat: %w", err)
		}
		switch t := tok.(type) {
		case xml.StartElement:
			depth++
			switch depth {
			case 1:
				if t.Name.Local != "message" {
					return nil, fmt.Errorf("rădăcina trebuie să fie <message>, nu <%s>", t.Name.Local)
				}
			case 2:
				// caută elementul în ordinea din xs:sequence (payload e opțional)
				idx := indexOf(fieldOrder[next:], t.Name.Local)
				if idx < 0 {
					return nil, fmt.Errorf("element neașteptat sau în ordine greșită: <%s>", t.Name.Local)
				}
				for _, skipped := range fieldOrder[next : next+idx] {
					if skipped != "payload" {
						return nil, fmt.Errorf("lipsește elementul <%s>", skipped)
					}
				}
				next += idx + 1
				current = t.Name.Local
				text.Reset()
			default:
				return nil, fmt.Errorf("element imbricat nepermis: <%s>", t.Name.Local)
			}
		case xml.CharData:
			if depth == 2 {
				text.Write(t)
			}
		case xml.EndElement:
			if depth == 2 {
				values[current] = text.String()
			}
			depth--
		}
	}
	if depth != 0 {
		return nil, fmt.Errorf("document xml incomplet")
	}
	for _, f := range fieldOrder {
		if _, ok := values[f]; !ok && f != "payload" {
			return nil, fmt.Errorf("lipsește elementul <%s>", f)
		}
	}
	return &Message{
		MessageID:     values["messageId"],
		CorrelationID: values["correlationId"],
		MessageType:   values["messageType"],
		SchemaVersion: values["schemaVersion"],
		OccurredAt:    values["occurredAt"],
		Sender:        values["sender"],
		Topic:         values["topic"],
		Payload:       values["payload"],
	}, nil
}

func indexOf(list []string, s string) int {
	for i, v := range list {
		if v == s {
			return i
		}
	}
	return -1
}

var (
	idPattern    = regexp.MustCompile(`^[A-Za-z0-9-]{1,64}$`)
	topicPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)
)

// Validate aplică restricțiile de valori din contracts/message.xsd.
// Go nu are validator XSD în biblioteca standard, așa că regulile sunt
// rescrise aici. Testul de contract verifică cu xmllint că XSD-ul și codul sunt de acord.
func (m *Message) Validate() error {
	switch {
	case !idPattern.MatchString(m.MessageID):
		return fmt.Errorf("messageId invalid")
	case !idPattern.MatchString(m.CorrelationID):
		return fmt.Errorf("correlationId invalid")
	case m.SchemaVersion != SchemaVersion:
		return fmt.Errorf("schemaVersion nesuportat: %q (suportat: %s)", m.SchemaVersion, SchemaVersion)
	case m.Sender == "":
		return fmt.Errorf("sender trebuie să aibă 1-64 caractere")
	case !topicPattern.MatchString(m.Topic):
		return fmt.Errorf("topic invalid: %q", m.Topic)
	case utf8.RuneCountInString(m.Sender) > 64:
		return fmt.Errorf("sender trebuie să aibă 1-64 caractere")
	case utf8.RuneCountInString(m.Payload) > MaxPayload:
		return fmt.Errorf("payload depășește %d caractere", MaxPayload)
	}
	switch m.MessageType {
	case TypeSubscribe, TypePublish, TypeAck, TypeError:
	default:
		return fmt.Errorf("messageType necunoscut: %q", m.MessageType)
	}
	if _, err := time.Parse(time.RFC3339, m.OccurredAt); err != nil || !strings.HasSuffix(m.OccurredAt, "Z") {
		return fmt.Errorf("occurredAt trebuie să fie o dată UTC (RFC3339, cu sufixul Z): %q", m.OccurredAt)
	}
	return nil
}
