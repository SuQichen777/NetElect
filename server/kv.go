package server

import "encoding/gob"

func init() {
	gob.Register(KVCommand{})
}

type KVOp string

const (
	KVPut    KVOp = "Put"
	KVDelete KVOp = "Delete"
)

type KVCommand struct {
	Op    KVOp
	Key   string
	Value string // only used for Put
}
