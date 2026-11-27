package grpcnet

import (
	"bytes"
	"encoding/gob"

	"google.golang.org/grpc/encoding"
)

const codecName = "gob"

type gobCodec struct{}

func (g gobCodec) Name() string { return codecName }

func (g gobCodec) Marshal(v any) ([]byte, error) {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(v)
	return buf.Bytes(), err
}

func (g gobCodec) Unmarshal(data []byte, v any) error {
	return gob.NewDecoder(bytes.NewReader(data)).Decode(v)
}

func init() {
	encoding.RegisterCodec(gobCodec{})
}
