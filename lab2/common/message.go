// Package common conține validarea mesajelor și utilitarele comune
// pentru broker, sender și receiver.
package common

import (
	"crypto/rand"
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"lab2/gen/brokerpb"
)

const (
	SchemaVersion = "1.0"
	MaxPayload    = 4096
)

// NewMessage creează un mesaj cu messageId nou, occurredAt în UTC și versiunea schemei.
// Dacă correlationID e gol, se generează unul nou.
func NewMessage(correlationID, sender, topic, payload string) *brokerpb.Message {
	if correlationID == "" {
		correlationID = NewID()
	}
	return &brokerpb.Message{
		MessageId:     NewID(),
		CorrelationId: correlationID,
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

var (
	idPattern    = regexp.MustCompile(`^[A-Za-z0-9-]{1,64}$`)
	topicPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)
)

// Validate aplică aceleași reguli ca XSD-ul din Lab 1. Protobuf verifică doar
// tipurile câmpurilor, nu și valorile, așa că restul regulilor sunt verificate aici.
func Validate(m *brokerpb.Message) error {
	if m == nil {
		return fmt.Errorf("lipsește mesajul")
	}
	switch {
	case !idPattern.MatchString(m.MessageId):
		return fmt.Errorf("messageId invalid")
	case !idPattern.MatchString(m.CorrelationId):
		return fmt.Errorf("correlationId invalid")
	case m.SchemaVersion != SchemaVersion:
		return fmt.Errorf("schemaVersion nesuportat: %q (suportat: %s)", m.SchemaVersion, SchemaVersion)
	case m.Sender == "" || utf8.RuneCountInString(m.Sender) > 64:
		return fmt.Errorf("sender trebuie să aibă 1-64 caractere")
	case !topicPattern.MatchString(m.Topic):
		return fmt.Errorf("topic invalid: %q", m.Topic)
	case utf8.RuneCountInString(m.Payload) > MaxPayload:
		return fmt.Errorf("payload depășește %d caractere", MaxPayload)
	}
	if _, err := time.Parse(time.RFC3339, m.OccurredAt); err != nil || !strings.HasSuffix(m.OccurredAt, "Z") {
		return fmt.Errorf("occurredAt trebuie să fie o dată UTC (RFC3339, cu sufixul Z): %q", m.OccurredAt)
	}
	return nil
}

// ValidateID verifică un identificator (receiverId, messageId): 1-64 caractere [A-Za-z0-9-].
func ValidateID(field, v string) error {
	if !idPattern.MatchString(v) {
		return fmt.Errorf("%s invalid: %q", field, v)
	}
	return nil
}

// ValidateSubscribe verifică cererea de abonare.
func ValidateSubscribe(r *brokerpb.SubscribeRequest) error {
	if err := ValidateID("receiverId", r.ReceiverId); err != nil {
		return err
	}
	if len(r.Topics) == 0 {
		return fmt.Errorf("cel puțin un topic")
	}
	for _, t := range r.Topics {
		if !topicPattern.MatchString(t) {
			return fmt.Errorf("topic invalid: %q", t)
		}
	}
	return nil
}
