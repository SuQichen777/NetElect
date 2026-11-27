package tcpnet

import (
	"bufio"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"raft-leader-election/server"
)

type responseFrame struct {
	body []byte
	ok   bool
}

type DialTransport struct {
	addr    string
	timeout time.Duration

	writeMu sync.Mutex

	mu      sync.Mutex
	conn    net.Conn
	pending map[uint64]chan responseFrame
	nextID  uint64
	closed  bool
}

func NewDialTransport(addr string, timeout time.Duration) *DialTransport {
	if timeout <= 0 {
		timeout = 250 * time.Millisecond
	}
	return &DialTransport{
		addr:    addr,
		timeout: timeout,
		pending: make(map[uint64]chan responseFrame),
	}
}

func (t *DialTransport) Close() error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	conn := t.conn
	t.conn = nil
	pending := t.pending
	t.pending = make(map[uint64]chan responseFrame)
	t.mu.Unlock()

	if conn != nil {
		_ = conn.Close()
	}
	for _, ch := range pending {
		close(ch)
	}
	return nil
}

func (t *DialTransport) Call(method string, args interface{}, reply interface{}) bool {
	methodCode, ok := methodID(method)
	if !ok {
		return false
	}

	body, err := encodeBody(args)
	if err != nil {
		return false
	}

	reqID, waitCh, conn, ok := t.prepareCall()
	if !ok {
		return false
	}

	header := frameHeader{
		Type:      frameTypeRequest,
		RequestID: reqID,
		Method:    methodCode,
		Flags:     flagOK,
		BodyLen:   uint32(len(body)),
	}

	if err := t.sendFrame(conn, header, body); err != nil {
		t.cancelPending(reqID)
		t.failConn(conn)
		return false
	}

	timer := time.NewTimer(t.timeout)
	defer timer.Stop()

	select {
	case resp, ok := <-waitCh:
		if !ok || !resp.ok {
			return false
		}
		return decodeReply(methodCode, resp.body, reply) == nil
	case <-timer.C:
		t.cancelPending(reqID)
		return false
	}
}

func (t *DialTransport) prepareCall() (uint64, chan responseFrame, net.Conn, bool) {
	_, ok := t.ensureConn()
	if !ok {
		return 0, nil, nil, false
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed || t.conn == nil {
		return 0, nil, nil, false
	}
	t.nextID++
	reqID := t.nextID
	waitCh := make(chan responseFrame, 1)
	t.pending[reqID] = waitCh
	return reqID, waitCh, t.conn, true
}

func (t *DialTransport) ensureConn() (net.Conn, bool) {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil, false
	}
	if t.conn != nil {
		conn := t.conn
		t.mu.Unlock()
		return conn, true
	}
	t.mu.Unlock()

	conn, err := net.DialTimeout("tcp", t.addr, t.timeout)
	if err != nil {
		return nil, false
	}
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}

	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		_ = conn.Close()
		return nil, false
	}
	if t.conn != nil {
		existing := t.conn
		t.mu.Unlock()
		_ = conn.Close()
		return existing, true
	}
	t.conn = conn
	t.mu.Unlock()

	go t.readLoop(conn)
	return conn, true
}

func (t *DialTransport) sendFrame(conn net.Conn, h frameHeader, body []byte) error {
	t.writeMu.Lock()
	defer t.writeMu.Unlock()
	if err := conn.SetWriteDeadline(time.Now().Add(t.timeout)); err != nil {
		return err
	}
	defer conn.SetWriteDeadline(time.Time{})
	return writeFrame(conn, h, body)
}

func (t *DialTransport) readLoop(conn net.Conn) {
	reader := bufio.NewReader(conn)
	for {
		if err := conn.SetReadDeadline(time.Now().Add(24 * time.Hour)); err != nil {
			t.failConn(conn)
			return
		}
		h, err := readHeader(reader)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				t.failConn(conn)
			} else {
				t.failConn(conn)
			}
			return
		}
		if h.Type != frameTypeResponse {
			t.failConn(conn)
			return
		}
		body := make([]byte, h.BodyLen)
		if _, err := io.ReadFull(reader, body); err != nil {
			t.failConn(conn)
			return
		}

		t.mu.Lock()
		ch := t.pending[h.RequestID]
		if ch != nil {
			delete(t.pending, h.RequestID)
		}
		t.mu.Unlock()

		if ch != nil {
			ch <- responseFrame{body: body, ok: h.Flags == flagOK}
			close(ch)
		}
	}
}

func (t *DialTransport) cancelPending(reqID uint64) {
	t.mu.Lock()
	ch := t.pending[reqID]
	if ch != nil {
		delete(t.pending, reqID)
	}
	t.mu.Unlock()
	if ch != nil {
		close(ch)
	}
}

func (t *DialTransport) failConn(bad net.Conn) {
	t.mu.Lock()
	if t.conn != bad {
		t.mu.Unlock()
		return
	}
	t.conn = nil
	pending := t.pending
	t.pending = make(map[uint64]chan responseFrame)
	t.mu.Unlock()

	_ = bad.Close()
	for _, ch := range pending {
		close(ch)
	}
}

func decodeReply(method uint8, body []byte, reply interface{}) error {
	switch method {
	case methodRequestVote:
		out, ok := reply.(*server.RequestVoteReply)
		if !ok {
			return errors.New("tcpnet: bad RequestVote reply type")
		}
		return decodeBody(body, out)
	case methodAppendEntries:
		out, ok := reply.(*server.AppendEntriesReply)
		if !ok {
			return errors.New("tcpnet: bad AppendEntries reply type")
		}
		return decodeBody(body, out)
	case methodPing:
		out, ok := reply.(*server.PingReply)
		if !ok {
			return errors.New("tcpnet: bad Ping reply type")
		}
		return decodeBody(body, out)
	case methodClientRead:
		out, ok := reply.(*server.ClientReadReply)
		if !ok {
			return errors.New("tcpnet: bad ClientRead reply type")
		}
		return decodeBody(body, out)
	case methodPenaltyScore:
		out, ok := reply.(*server.PenaltyScoreReply)
		if !ok {
			return errors.New("tcpnet: bad PenaltyScore reply type")
		}
		return decodeBody(body, out)
	case methodPetition:
		out, ok := reply.(*server.PetitionReply)
		if !ok {
			return errors.New("tcpnet: bad Petition reply type")
		}
		return decodeBody(body, out)
	case methodClientWrite:
		out, ok := reply.(*server.ClientWriteReply)
		if !ok {
			return errors.New("tcpnet: bad ClientWrite reply type")
		}
		return decodeBody(body, out)
	default:
		return errors.New("tcpnet: unknown method")
	}
}

var _ server.Transport = (*DialTransport)(nil)
