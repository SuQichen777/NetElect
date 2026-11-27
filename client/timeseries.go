package client

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// IntervalStats tracks operation counts and latency samples for a single time window.
// Workers record operations concurrently; the time-series ticker calls Drain() each interval
// to atomically retrieve and reset all counters and samples.
type IntervalStats struct {
	success   atomic.Int64
	failed    atomic.Int64
	notLeader atomic.Int64
	timeout   atomic.Int64
	dropped   atomic.Int64
	noQuorum  atomic.Int64
	unknown   atomic.Int64

	mu      sync.Mutex
	samples []time.Duration
}

// IntervalSnapshot is the result of a single Drain call.
type IntervalSnapshot struct {
	Success   int64
	Failed    int64
	NotLeader int64
	Timeout   int64
	Dropped   int64
	NoQuorum  int64
	Unknown   int64
	Latencies []time.Duration // sorted ascending
}

func (s *IntervalSnapshot) Percentile(p float64) time.Duration {
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

func NewIntervalStats() *IntervalStats {
	return &IntervalStats{
		samples: make([]time.Duration, 0, 4096),
	}
}

func (s *IntervalStats) Record(res ReadResult) {
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

func (s *IntervalStats) RecordWrite(res WriteResult) {
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

func (s *IntervalStats) addSample(d time.Duration) {
	s.mu.Lock()
	s.samples = append(s.samples, d)
	s.mu.Unlock()
}

// Drain atomically retrieves all accumulated counts and samples, then resets to zero.
// Latencies in the returned snapshot are sorted ascending.
func (s *IntervalStats) Drain() IntervalSnapshot {
	s.mu.Lock()
	samples := s.samples
	s.samples = make([]time.Duration, 0, cap(samples))
	s.mu.Unlock()

	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })

	return IntervalSnapshot{
		Success:   s.success.Swap(0),
		Failed:    s.failed.Swap(0),
		NotLeader: s.notLeader.Swap(0),
		Timeout:   s.timeout.Swap(0),
		Dropped:   s.dropped.Swap(0),
		NoQuorum:  s.noQuorum.Swap(0),
		Unknown:   s.unknown.Swap(0),
		Latencies: samples,
	}
}
