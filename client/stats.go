package client

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type Stats struct {
	success atomic.Int64
	failed  atomic.Int64

	notLeader atomic.Int64
	timeout   atomic.Int64
	dropped   atomic.Int64
	noQuorum  atomic.Int64
	unknown   atomic.Int64

	mu       sync.Mutex
	samples  []time.Duration
	capacity int
	next     int
}

func NewStats(sampleCapacity int) *Stats {
	if sampleCapacity <= 0 {
		sampleCapacity = 100000
	}
	return &Stats{
		samples:  make([]time.Duration, 0, sampleCapacity),
		capacity: sampleCapacity,
	}
}

func (s *Stats) Record(res ReadResult) {
	if res.OK {
		s.success.Add(1)
		s.addSample(res.Latency)
		return
	}
	s.failed.Add(1)
	switch res.Err {
	case ErrorNotLeader:
		s.notLeader.Add(1)
	case ErrorTimeout:
		s.timeout.Add(1)
	case ErrorDropped:
		s.dropped.Add(1)
	case ErrorNoQuorum:
		s.noQuorum.Add(1)
	default:
		s.unknown.Add(1)
	}
}

func (s *Stats) RecordWrite(res WriteResult) {
	if res.OK {
		s.success.Add(1)
		s.addSample(res.Latency)
		return
	}
	s.failed.Add(1)
	switch res.Err {
	case ErrorNotLeader:
		s.notLeader.Add(1)
	case ErrorTimeout:
		s.timeout.Add(1)
	case ErrorDropped:
		s.dropped.Add(1)
	case ErrorNoQuorum:
		s.noQuorum.Add(1)
	default:
		s.unknown.Add(1)
	}
}

func (s *Stats) Snapshot() Snapshot {
	s.mu.Lock()
	cp := make([]time.Duration, len(s.samples))
	copy(cp, s.samples)
	s.mu.Unlock()
	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })

	return Snapshot{
		Success:   s.success.Load(),
		Failed:    s.failed.Load(),
		NotLeader: s.notLeader.Load(),
		Timeout:   s.timeout.Load(),
		Dropped:   s.dropped.Load(),
		NoQuorum:  s.noQuorum.Load(),
		Unknown:   s.unknown.Load(),
		Latencies: cp,
	}
}

type Snapshot struct {
	Success   int64
	Failed    int64
	NotLeader int64
	Timeout   int64
	Dropped   int64
	NoQuorum  int64
	Unknown   int64
	Latencies []time.Duration
}

func (s *Snapshot) Percentile(p float64) time.Duration {
	if len(s.Latencies) == 0 {
		return 0
	}
	if p <= 0 {
		return s.Latencies[0]
	}
	if p >= 1 {
		return s.Latencies[len(s.Latencies)-1]
	}
	idx := int(float64(len(s.Latencies)-1) * p)
	return s.Latencies[idx]
}

func (s *Stats) addSample(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.samples) < s.capacity {
		s.samples = append(s.samples, d)
		return
	}
	s.samples[s.next] = d
	s.next++
	if s.next >= s.capacity {
		s.next = 0
	}
}
