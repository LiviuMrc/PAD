package common

import (
	"fmt"

	"google.golang.org/grpc/encoding"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// JSONCodec serializează mesajele gRPC în JSON (protojson) în loc de protobuf binar.
// Clientul îl alege cu content-subtype "json" (Content-Type: application/grpc+json),
// iar serverul îl folosește automat pentru răspuns. Contractul rămâne broker.proto:
// se schimbă doar reprezentarea pe fir.
type JSONCodec struct{}

const JSONCodecName = "json"

func (JSONCodec) Name() string { return JSONCodecName }

func (JSONCodec) Marshal(v any) ([]byte, error) {
	m, ok := v.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("json codec: %T nu e un mesaj proto", v)
	}
	return protojson.Marshal(m)
}

func (JSONCodec) Unmarshal(data []byte, v any) error {
	m, ok := v.(proto.Message)
	if !ok {
		return fmt.Errorf("json codec: %T nu e un mesaj proto", v)
	}
	// câmpurile necunoscute (de la o versiune mai nouă) sunt ignorate, ca la protobuf binar
	return protojson.UnmarshalOptions{DiscardUnknown: true}.Unmarshal(data, m)
}

func init() {
	encoding.RegisterCodec(JSONCodec{})
}
