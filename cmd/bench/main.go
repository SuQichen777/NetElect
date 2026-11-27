package main

import (
	"context"
	"flag"
	"fmt"
	"raft-leader-election/client"
	"raft-leader-election/middleware"
	"raft-leader-election/server"
	"strconv"
	"sync"
	"time"
)

func main() {
	var (
		n        = flag.Int("n", 3, "cluster size")
		workers  = flag.Int("workers", 32, "client workers")
		duration = flag.Duration("duration", 3*time.Second, "run duration")
		timeout  = flag.Duration("timeout", 250*time.Millisecond, "per-request timeout")
	)
	flag.Parse()

	net := middleware.MakeMockNetwork()
	rafts := make([]*server.Raft, *n)

	for i := 0; i < *n; i++ {
		peers := make([]*server.Peer, *n)
		for j := 0; j < *n; j++ {
			peers[j] = &server.Peer{ID: j, Transport: net.Connect(strconv.Itoa(j))}
		}
		rafts[i] = server.Make(peers, i)
		net.Register(strconv.Itoa(i), rafts[i])
	}

	ends := make([]server.Transport, *n)
	for i := 0; i < *n; i++ {
		ends[i] = net.Connect(strconv.Itoa(i))
	}

	cl := client.New(ends, time.Now().UnixNano(), client.Config{
		RequestTimeout: *timeout,
		MinBackoff:     5 * time.Millisecond,
		MaxBackoff:     200 * time.Millisecond,
	})

	stats := client.NewStats(200000)
	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(*workers)
	for w := 0; w < *workers; w++ {
		go func(workerID int) {
			defer wg.Done()
			i := 0
			for ctx.Err() == nil {
				res := cl.Read(ctx, fmt.Sprintf("%d-%d", workerID, i), "")
				stats.Record(res)
				i++
			}
		}(w)
	}
	wg.Wait()

	snap := stats.Snapshot()
	total := snap.Success + snap.Failed
	qps := float64(total) / duration.Seconds()

	fmt.Printf("total=%d success=%d failed=%d qps=%.2f\n", total, snap.Success, snap.Failed, qps)
	fmt.Printf("p50=%s p95=%s p99=%s samples=%d\n", snap.Percentile(0.50), snap.Percentile(0.95), snap.Percentile(0.99), len(snap.Latencies))

	for _, rf := range rafts {
		rf.Kill()
	}
}
