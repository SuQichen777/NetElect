package tcpnet

import (
	"encoding/binary"
	"errors"
	"io"
)

const (
	protocolMagic   uint16 = 0x5246
	protocolVersion uint8  = 1
)

const (
	frameTypeRequest  uint8 = 1
	frameTypeResponse uint8 = 2
)

const (
	methodRequestVote   uint8 = 1
	methodAppendEntries uint8 = 2
	methodPing          uint8 = 3
	methodClientRead    uint8 = 4
	methodPenaltyScore  uint8 = 5
	methodPetition      uint8 = 6
	methodClientWrite   uint8 = 7
)

const (
	flagOK uint8 = 0
)

const headerSize = 18

type frameHeader struct {
	Type      uint8
	RequestID uint64
	Method    uint8
	Flags     uint8
	BodyLen   uint32
}

func writeHeader(w io.Writer, h frameHeader) error {
	var buf [headerSize]byte
	binary.BigEndian.PutUint16(buf[0:2], protocolMagic)
	buf[2] = protocolVersion
	buf[3] = h.Type
	binary.BigEndian.PutUint64(buf[4:12], h.RequestID)
	buf[12] = h.Method
	buf[13] = h.Flags
	binary.BigEndian.PutUint32(buf[14:18], h.BodyLen)
	_, err := w.Write(buf[:])
	return err
}

// writeFrame writes header and body in a single Write call so they are
// delivered in one TCP segment, avoiding the Nagle / two-packet penalty.
func writeFrame(w io.Writer, h frameHeader, body []byte) error {
	buf := make([]byte, headerSize+len(body))
	binary.BigEndian.PutUint16(buf[0:2], protocolMagic)
	buf[2] = protocolVersion
	buf[3] = h.Type
	binary.BigEndian.PutUint64(buf[4:12], h.RequestID)
	buf[12] = h.Method
	buf[13] = h.Flags
	binary.BigEndian.PutUint32(buf[14:18], h.BodyLen)
	copy(buf[headerSize:], body)
	_, err := w.Write(buf)
	return err
}

func readHeader(r io.Reader) (frameHeader, error) {
	var buf [headerSize]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return frameHeader{}, err
	}
	if binary.BigEndian.Uint16(buf[0:2]) != protocolMagic {
		return frameHeader{}, errors.New("tcpnet: bad magic")
	}
	if buf[2] != protocolVersion {
		return frameHeader{}, errors.New("tcpnet: unsupported version")
	}
	return frameHeader{
		Type:      buf[3],
		RequestID: binary.BigEndian.Uint64(buf[4:12]),
		Method:    buf[12],
		Flags:     buf[13],
		BodyLen:   binary.BigEndian.Uint32(buf[14:18]),
	}, nil
}

func methodID(method string) (uint8, bool) {
	switch method {
	case "Raft.RequestVote":
		return methodRequestVote, true
	case "Raft.AppendEntries":
		return methodAppendEntries, true
	case "Raft.Ping":
		return methodPing, true
	case "Raft.ClientRead":
		return methodClientRead, true
	case "Raft.PenaltyScore":
		return methodPenaltyScore, true
	case "Raft.Petition":
		return methodPetition, true
	case "Raft.ClientWrite":
		return methodClientWrite, true
	default:
		return 0, false
	}
}
