package tcpnet

import (
	"bufio"
	"io"
	"net"
	"sync"

	"raft-leader-election/server"
)

type Node struct {
	Addr string
	Lis  net.Listener

	mu    sync.Mutex
	conns map[net.Conn]struct{}
}

func StartNode(rf *server.Raft, addr string) (*Node, error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return StartNodeWithListener(rf, lis)
}

func StartNodeWithListener(rf *server.Raft, lis net.Listener) (*Node, error) {
	n := &Node{
		Addr:  lis.Addr().String(),
		Lis:   lis,
		conns: make(map[net.Conn]struct{}),
	}
	go n.serve(&RaftAdapter{Raft: rf})
	return n, nil
}

func (n *Node) Stop() {
	if n.Lis != nil {
		_ = n.Lis.Close()
	}
	n.mu.Lock()
	conns := make([]net.Conn, 0, len(n.conns))
	for conn := range n.conns {
		conns = append(conns, conn)
	}
	n.mu.Unlock()
	for _, conn := range conns {
		_ = conn.Close()
	}
}

func (n *Node) serve(adapter *RaftAdapter) {
	for {
		conn, err := n.Lis.Accept()
		if err != nil {
			return
		}
		if tc, ok := conn.(*net.TCPConn); ok {
			_ = tc.SetNoDelay(true)
		}
		n.trackConn(conn, true)
		go n.serveConn(conn, adapter)
	}
}

func (n *Node) serveConn(conn net.Conn, adapter *RaftAdapter) {
	defer func() {
		n.trackConn(conn, false)
		_ = conn.Close()
	}()

	var writeMu sync.Mutex
	reader := bufio.NewReader(conn)
	for {
		h, err := readHeader(reader)
		if err != nil {
			return
		}
		if h.Type != frameTypeRequest {
			return
		}
		body := make([]byte, h.BodyLen)
		if _, err := io.ReadFull(reader, body); err != nil {
			return
		}

		// Dispatch each request in its own goroutine so that slow handlers
		// (e.g. ClientRead waiting for a quorum heartbeat) don't block the
		// read loop and starve concurrent requests on the same connection.
		go func(h frameHeader, body []byte) {
			respBody, ok := handleRequest(adapter, h.Method, body)
			if !ok {
				_ = conn.Close()
				return
			}
			respHeader := frameHeader{
				Type:      frameTypeResponse,
				RequestID: h.RequestID,
				Method:    h.Method,
				Flags:     flagOK,
				BodyLen:   uint32(len(respBody)),
			}
			writeMu.Lock()
			defer writeMu.Unlock()
			_ = writeFrame(conn, respHeader, respBody)
		}(h, body)
	}
}

func handleRequest(adapter *RaftAdapter, method uint8, body []byte) ([]byte, bool) {
	switch method {
	case methodRequestVote:
		var args server.RequestVoteArgs
		if err := decodeBody(body, &args); err != nil {
			return nil, false
		}
		resp, err := encodeBody(adapter.RequestVote(&args))
		return resp, err == nil
	case methodAppendEntries:
		var args server.AppendEntriesArgs
		if err := decodeBody(body, &args); err != nil {
			return nil, false
		}
		resp, err := encodeBody(adapter.AppendEntries(&args))
		return resp, err == nil
	case methodPing:
		var args server.PingArgs
		if err := decodeBody(body, &args); err != nil {
			return nil, false
		}
		resp, err := encodeBody(adapter.Ping(&args))
		return resp, err == nil
	case methodClientRead:
		var args server.ClientReadArgs
		if err := decodeBody(body, &args); err != nil {
			return nil, false
		}
		resp, err := encodeBody(adapter.ClientRead(&args))
		return resp, err == nil
	case methodPenaltyScore:
		var args server.PenaltyScoreArgs
		if err := decodeBody(body, &args); err != nil {
			return nil, false
		}
		resp, err := encodeBody(adapter.PenaltyScore(&args))
		return resp, err == nil
	case methodPetition:
		var args server.PetitionArgs
		if err := decodeBody(body, &args); err != nil {
			return nil, false
		}
		resp, err := encodeBody(adapter.Petition(&args))
		return resp, err == nil
	case methodClientWrite:
		var args server.ClientWriteArgs
		if err := decodeBody(body, &args); err != nil {
			return nil, false
		}
		resp, err := encodeBody(adapter.ClientWrite(&args))
		return resp, err == nil
	default:
		return nil, false
	}
}

func (n *Node) trackConn(conn net.Conn, add bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if add {
		n.conns[conn] = struct{}{}
		return
	}
	delete(n.conns, conn)
}
