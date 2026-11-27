package client

import (
	"context"
	"math/rand"
	"raft-leader-election/server"
	"strconv"
	"sync"
	"time"
)

type ErrorKind string

const (
	ErrorNone      ErrorKind = ""
	ErrorNotLeader ErrorKind = "NotLeader"
	ErrorTimeout   ErrorKind = "Timeout"
	ErrorDropped   ErrorKind = "Dropped"
	ErrorNoQuorum  ErrorKind = "NoQuorum"
	ErrorUnknown   ErrorKind = "Unknown"
)

type Config struct {
	RequestTimeout time.Duration
	MinBackoff     time.Duration
	MaxBackoff     time.Duration
}

type Client struct {
	mu         sync.Mutex
	ends       []server.Transport
	leaderHint int
	rnd        *rand.Rand
	cfg        Config
}

func New(ends []server.Transport, seed int64, cfg Config) *Client {
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = 250 * time.Millisecond
	}
	if cfg.MinBackoff <= 0 {
		cfg.MinBackoff = 5 * time.Millisecond
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = 200 * time.Millisecond
	}
	return &Client{
		ends:       ends,
		leaderHint: -1,
		rnd:        rand.New(rand.NewSource(seed)),
		cfg:        cfg,
	}
}

type ReadResult struct {
	OK      bool
	Err     ErrorKind
	Latency time.Duration
	Node    int
	Value   string // value returned by the KV store
}

type WriteResult struct {
	OK      bool
	Err     ErrorKind
	Latency time.Duration
	Node    int
}

func (c *Client) Read(ctx context.Context, commandID string, payload string) ReadResult {
	start := time.Now()
	deadline := start.Add(c.cfg.RequestTimeout)
	backoff := c.cfg.MinBackoff

	for {
		if ctx.Err() != nil {
			return ReadResult{OK: false, Err: ErrorTimeout, Latency: time.Since(start), Node: -1}
		}
		if time.Now().After(deadline) {
			return ReadResult{OK: false, Err: ErrorTimeout, Latency: time.Since(start), Node: -1}
		}

		target := c.pickTarget()
		args := server.ClientReadArgs{CommandId: commandID, Payload: payload}
		var reply server.ClientReadReply

		if !c.ends[target].Call("Raft.ClientRead", &args, &reply) {
			c.clearLeaderHint(target)
			time.Sleep(c.jitter(backoff))
			backoff = minDuration(c.cfg.MaxBackoff, backoff*2)
			continue
		}

		if reply.Success {
			c.setLeaderHint(target)
			return ReadResult{OK: true, Err: ErrorNone, Latency: time.Since(start), Node: target, Value: reply.Result}
		}

		if reply.LeaderId != "" {
			if id, err := strconv.Atoi(reply.LeaderId); err == nil && id >= 0 && id < len(c.ends) {
				c.setLeaderHint(id)
				continue // retry immediately, no sleep
			}
		}

		c.clearLeaderHint(target)
		time.Sleep(c.jitter(backoff))
		backoff = minDuration(c.cfg.MaxBackoff, backoff*2)
	}
}

func (c *Client) Write(ctx context.Context, commandID string, op server.KVOp, key, value string) WriteResult {
	start := time.Now()
	deadline := start.Add(c.cfg.RequestTimeout)
	backoff := c.cfg.MinBackoff

	for {
		if ctx.Err() != nil {
			return WriteResult{OK: false, Err: ErrorTimeout, Latency: time.Since(start), Node: -1}
		}
		if time.Now().After(deadline) {
			return WriteResult{OK: false, Err: ErrorTimeout, Latency: time.Since(start), Node: -1}
		}

		target := c.pickTarget()
		args := server.ClientWriteArgs{CommandId: commandID, Op: op, Key: key, Value: value}
		var reply server.ClientWriteReply

		if !c.ends[target].Call("Raft.ClientWrite", &args, &reply) {
			c.clearLeaderHint(target)
			time.Sleep(c.jitter(backoff))
			backoff = minDuration(c.cfg.MaxBackoff, backoff*2)
			continue
		}

		if reply.Success {
			c.setLeaderHint(target)
			return WriteResult{OK: true, Err: ErrorNone, Latency: time.Since(start), Node: target}
		}

		if reply.LeaderId != "" {
			if id, err := strconv.Atoi(reply.LeaderId); err == nil && id >= 0 && id < len(c.ends) {
				c.setLeaderHint(id)
				continue
			}
		}

		c.clearLeaderHint(target)
		time.Sleep(c.jitter(backoff))
		backoff = minDuration(c.cfg.MaxBackoff, backoff*2)
	}
}

func (c *Client) pickTarget() int {
	c.mu.Lock()
	hint := c.leaderHint
	n := len(c.ends)
	r := c.rnd.Intn(n)
	c.mu.Unlock()

	if hint >= 0 && hint < n {
		return hint
	}
	return r
}

func (c *Client) setLeaderHint(id int) {
	c.mu.Lock()
	c.leaderHint = id
	c.mu.Unlock()
}

func (c *Client) clearLeaderHint(id int) {
	c.mu.Lock()
	if c.leaderHint == id {
		c.leaderHint = -1
	}
	c.mu.Unlock()
}

func (c *Client) jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	c.mu.Lock()
	f := 0.5 + c.rnd.Float64()
	c.mu.Unlock()
	return time.Duration(float64(d) * f)
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
