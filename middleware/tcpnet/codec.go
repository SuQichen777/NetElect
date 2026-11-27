package tcpnet

import (
	"bytes"
	"encoding/gob"
)

func encodeBody(v interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeBody(data []byte, out interface{}) error {
	dec := gob.NewDecoder(bytes.NewReader(data))
	return dec.Decode(out)
}
