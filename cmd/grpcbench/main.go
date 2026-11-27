package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"raft-leader-election/client"
	"raft-leader-election/config"
	"raft-leader-election/middleware/grpcnet"
	"raft-leader-election/middleware/tcpnet"
	"raft-leader-election/server"
	"strings"
	"sync"
	"time"
)

func main() {
	var (
		configPath = flag.String("config", "", "cluster config JSON path")
		transport  = flag.String("transport", config.TransportGRPC, "transport: grpc|tcp")
		addrsStr   = flag.String("addrs", "", "comma-separated raft addrs, e.g. host0:7000,host1:7000,...")
		workers    = flag.Int("workers", 64, "client workers")
		duration   = flag.Duration("duration", 5*time.Second, "run duration")
		timeout    = flag.Duration("timeout", 250*time.Millisecond, "per-request timeout")
		sampleCap  = flag.Int("sample-cap", 200000, "latency sample capacity")
	)
	flag.Parse()

	addrs := splitAddrs(*addrsStr)
	usedTransport := *transport
	usedWorkers := *workers
	usedDuration := *duration
	usedTimeout := *timeout
	usedSampleCap := *sampleCap

	if *configPath != "" {
		cfg, err := config.Load(*configPath)
		if err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(2)
		}
		usedTransport = cfg.Transport
		addrs = cfg.BenchmarkAddrs()
		if cfg.Benchmark.Workers > 0 {
			usedWorkers = cfg.Benchmark.Workers
		}
		if cfg.Benchmark.Duration != "" {
			parsed, err := time.ParseDuration(cfg.Benchmark.Duration)
			if err != nil {
				fmt.Fprintln(os.Stderr, err.Error())
				os.Exit(2)
			}
			usedDuration = parsed
		}
		if cfg.Benchmark.Timeout != "" {
			parsed, err := time.ParseDuration(cfg.Benchmark.Timeout)
			if err != nil {
				fmt.Fprintln(os.Stderr, err.Error())
				os.Exit(2)
			}
			usedTimeout = parsed
		}
		if cfg.Benchmark.SampleCap > 0 {
			usedSampleCap = cfg.Benchmark.SampleCap
		}
	}

	if len(addrs) == 0 {
		fmt.Fprintln(os.Stderr, "missing benchmark addresses: use -addrs or -config")
		os.Exit(2)
	}

	ends := make([]server.Transport, len(addrs))
	closers := make([]interface{ Close() error }, 0, len(addrs))
	for i := range addrs {
		switch usedTransport {
		case config.TransportGRPC:
			dt := grpcnet.NewDialTransport(addrs[i], usedTimeout)
			ends[i] = dt
			closers = append(closers, dt)
		case config.TransportTCP:
			dt := tcpnet.NewDialTransport(addrs[i], usedTimeout)
			ends[i] = dt
			closers = append(closers, dt)
		default:
			fmt.Fprintln(os.Stderr, "invalid transport: expected grpc or tcp")
			os.Exit(2)
		}
	}
	defer func() {
		for _, d := range closers {
			_ = d.Close()
		}
	}()

	cl := client.New(ends, time.Now().UnixNano(), client.Config{
		RequestTimeout: usedTimeout,
		MinBackoff:     5 * time.Millisecond,
		MaxBackoff:     200 * time.Millisecond,
	})
	stats := client.NewStats(usedSampleCap)

	ctx, cancel := context.WithTimeout(context.Background(), usedDuration)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(usedWorkers)
	for w := 0; w < usedWorkers; w++ {
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
	qps := float64(total) / usedDuration.Seconds()

	fmt.Printf("transport=%s addrs=%s\n", usedTransport, strings.Join(addrs, ","))
	fmt.Printf("total=%d success=%d failed=%d qps=%.2f\n", total, snap.Success, snap.Failed, qps)
	fmt.Printf("p50=%s p95=%s p99=%s samples=%d\n", snap.Percentile(0.50), snap.Percentile(0.95), snap.Percentile(0.99), len(snap.Latencies))
}

func splitAddrs(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		out = append(out, p)
	}
	return out
}
