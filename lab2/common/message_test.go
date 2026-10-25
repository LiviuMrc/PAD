package common

import (
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"lab2/gen/brokerpb"
)

func TestNewMessageIsValid(t *testing.T) {
	m := NewMessage("", "sender-1", "news", "ă î ș ț")
	if err := Validate(m); err != nil {
		t.Fatal(err)
	}
	if m.MessageId == m.CorrelationId {
		t.Fatal("messageId și correlationId trebuie generate separat")
	}
}

func TestValidateRejects(t *testing.T) {
	cases := map[string]func(m *brokerpb.Message){
		"schemaVersion": func(m *brokerpb.Message) { m.SchemaVersion = "2.0" },
		"topic":         func(m *brokerpb.Message) { m.Topic = "a b" },
		"sender gol":    func(m *brokerpb.Message) { m.Sender = "" },
		"non-UTC":       func(m *brokerpb.Message) { m.OccurredAt = "2026-10-17T13:00:00+03:00" },
		"dată invalidă": func(m *brokerpb.Message) { m.OccurredAt = "ieri" },
		"correlationId": func(m *brokerpb.Message) { m.CorrelationId = "" },
	}
	for name, change := range cases {
		m := NewMessage("", "sender-1", "news", "x")
		change(m)
		if Validate(m) == nil {
			t.Errorf("%s: trebuia respins", name)
		}
	}
	if Validate(nil) == nil {
		t.Error("mesaj nil: trebuia respins")
	}
}

func TestProtoAndJSONRoundTrip(t *testing.T) {
	m := NewMessage("", "sender-1", "news", "salut")

	bin, err := proto.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	var fromBin brokerpb.Message
	if err := proto.Unmarshal(bin, &fromBin); err != nil || !proto.Equal(m, &fromBin) {
		t.Fatalf("round-trip binar: %v", err)
	}

	js, err := protojson.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	var fromJSON brokerpb.Message
	if err := protojson.Unmarshal(js, &fromJSON); err != nil || !proto.Equal(m, &fromJSON) {
		t.Fatalf("round-trip JSON: %v", err)
	}
}

// Test de contract: numele și numerele câmpurilor din broker.proto nu se schimbă
// în v1. Schimbarea lor ar strica clienții deja compilați.
func TestContractFieldNumbers(t *testing.T) {
	want := map[string]map[protoreflect.FieldNumber]string{
		"Message": {1: "message_id", 2: "correlation_id", 3: "schema_version", 4: "occurred_at",
			5: "sender", 6: "topic", 7: "payload"},
		"PublishRequest":     {1: "message"},
		"PublishReply":       {1: "message_id", 2: "correlation_id", 3: "subscribers", 4: "duplicate"},
		"SubscribeRequest":   {1: "receiver_id", 2: "topics", 3: "correlation_id"},
		"AckRequest":         {1: "receiver_id", 2: "message_id"},
		"AckReply":           {1: "removed"},
		"UnsubscribeRequest": {1: "receiver_id", 2: "correlation_id"},
		"UnsubscribeReply":   {1: "dropped"},
		"DeadLetter":         {1: "message", 2: "receiver_id", 3: "reason", 4: "dead_at"},
		"RedriveRequest":     {1: "receiver_id", 2: "message_ids"},
		"RedriveReply":       {1: "redriven", 2: "skipped"},
	}
	msgs := map[string]protoreflect.MessageDescriptor{
		"Message":            (&brokerpb.Message{}).ProtoReflect().Descriptor(),
		"PublishRequest":     (&brokerpb.PublishRequest{}).ProtoReflect().Descriptor(),
		"PublishReply":       (&brokerpb.PublishReply{}).ProtoReflect().Descriptor(),
		"SubscribeRequest":   (&brokerpb.SubscribeRequest{}).ProtoReflect().Descriptor(),
		"AckRequest":         (&brokerpb.AckRequest{}).ProtoReflect().Descriptor(),
		"AckReply":           (&brokerpb.AckReply{}).ProtoReflect().Descriptor(),
		"UnsubscribeRequest": (&brokerpb.UnsubscribeRequest{}).ProtoReflect().Descriptor(),
		"UnsubscribeReply":   (&brokerpb.UnsubscribeReply{}).ProtoReflect().Descriptor(),
		"DeadLetter":         (&brokerpb.DeadLetter{}).ProtoReflect().Descriptor(),
		"RedriveRequest":     (&brokerpb.RedriveRequest{}).ProtoReflect().Descriptor(),
		"RedriveReply":       (&brokerpb.RedriveReply{}).ProtoReflect().Descriptor(),
	}
	for name, fields := range want {
		d := msgs[name]
		if string(d.FullName()) != "broker.v1."+name {
			t.Errorf("%s: pachet schimbat: %s", name, d.FullName())
		}
		for num, field := range fields {
			f := d.Fields().ByNumber(num)
			if f == nil || string(f.Name()) != field {
				t.Errorf("%s: câmpul %d trebuie să fie %q", name, num, field)
			}
		}
	}
	svc := brokerpb.File_broker_proto.Services().ByName("Broker")
	for _, method := range []string{"Publish", "Subscribe", "Ack", "Unsubscribe", "ListDeadLetters", "RedriveDeadLetters", "PurgeDeadLetters"} {
		if svc.Methods().ByName(protoreflect.Name(method)) == nil {
			t.Errorf("lipsește metoda %s", method)
		}
	}
}

// Un PublishReply scris de brokerul vechi (fără câmpul duplicate) e citit de clientul
// nou cu duplicate=false, iar clientul vechi ignoră câmpul nou.
func TestPublishReplyIsBackwardCompatible(t *testing.T) {
	old := protowire.AppendTag(nil, 1, protowire.BytesType)
	old = protowire.AppendString(old, "m1")
	old = protowire.AppendTag(old, 3, protowire.VarintType)
	old = protowire.AppendVarint(old, 2)
	var r brokerpb.PublishReply
	if err := proto.Unmarshal(old, &r); err != nil || r.MessageId != "m1" || r.Subscribers != 2 || r.Duplicate {
		t.Fatalf("răspuns vechi citit greșit: %+v %v", &r, err)
	}
}

// Compatibilitate: un mesaj de la o versiune mai nouă, cu un câmp în plus,
// e citit fără eroare de codul actual. Câmpul necunoscut e ignorat.
func TestUnknownFieldsAreTolerated(t *testing.T) {
	m := NewMessage("", "sender-1", "news", "x")
	bin, _ := proto.Marshal(m)
	bin = protowire.AppendTag(bin, 99, protowire.BytesType)
	bin = protowire.AppendString(bin, "câmp nou din v1.1")

	var got brokerpb.Message
	if err := proto.Unmarshal(bin, &got); err != nil {
		t.Fatal(err)
	}
	if got.Payload != "x" || Validate(&got) != nil {
		t.Fatal("mesajul cu câmp nou nu mai e acceptat")
	}
}
