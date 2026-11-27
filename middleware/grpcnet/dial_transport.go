package grpcnet

import (
	"context"
	"sync"
	"time"

	"raft-leader-election/server"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type DialTransport struct {
	addr    string
	timeout time.Duration

	mu     sync.Mutex
	conn   *grpc.ClientConn
	client RaftServiceClient
}

func NewDialTransport(addr string, timeout time.Duration) *DialTransport {
	if timeout <= 0 {
		timeout = 250 * time.Millisecond
	}
	return &DialTransport{addr: addr, timeout: timeout}
}

func (t *DialTransport) Close() error {
	t.mu.Lock()
	conn := t.conn
	t.conn = nil
	t.client = nil
	t.mu.Unlock()
	if conn != nil {
		return conn.Close()
	}
	return nil
}

func (t *DialTransport) Call(method string, args interface{}, reply interface{}) bool {
	cl, ok := t.ensureClient()
	if !ok {
		return false
	}
	tr := &Transport{client: cl, timeout: t.timeout}
	if tr.Call(method, args, reply) {
		return true
	}
	_ = t.Close()
	return false
}

func (t *DialTransport) ensureClient() (RaftServiceClient, bool) {
	t.mu.Lock()
	if t.client != nil {
		cl := t.client
		t.mu.Unlock()
		return cl, true
	}
	t.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), t.timeout)
	defer cancel()

	conn, err := grpc.DialContext(
		ctx,
		t.addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithDefaultCallOptions(grpc.CallContentSubtype(codecName)),
	)
	if err != nil {
		return nil, false
	}

	t.mu.Lock()
	if t.client != nil {
		cl := t.client
		t.mu.Unlock()
		_ = conn.Close()
		return cl, true
	}
	t.conn = conn
	t.client = NewRaftServiceClient(conn)
	cl := t.client
	t.mu.Unlock()
	return cl, true
}

var _ server.Transport = (*DialTransport)(nil)
