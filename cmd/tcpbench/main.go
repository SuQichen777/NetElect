package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"raft-leader-election/client"
	"raft-leader-election/config"
	"raft-leader-election/middleware/tcpnet"
	"raft-leader-election/server"
	"strings"
	"sync"
	"time"
)

func main() {
	run(config.TransportTCP)
}

func run(defaultTransport string) {
	var (
		configPath = flag.String("config", "", "cluster config JSON path")
		transport  = flag.String("transport", defaultTransport, "transport: grpc|tcp")
		addrsStr   = flag.String("addrs", "", "comma-separated raft addrs, e.g. host0:7000,host1:7000,...")
		workers    = flag.Int("workers", 64, "client workers")
		duration   = flag.Duration("duration", 5*time.Second, "run duration")
		timeout    = flag.Duration("timeout", 250*time.Millisecond, "per-request timeout")
		sampleCap  = flag.Int("sample-cap", 200000, "latency sample capacity")
	)
	flag.Parse()

	addrs, transportName, usedWorkers, usedDuration, usedTimeout, usedSampleCap := loadBenchConfig(*configPath, *transport, *addrsStr, *workers, *duration, *timeout, *sampleCap)

	ends := make([]server.Transport, len(addrs))
	closers := make([]interface{ Close() error }, 0, len(addrs))
	for i := range addrs {
		var tr server.Transport
		switch transportName {
		case config.TransportTCP:
			dt := tcpnet.NewDialTransport(addrs[i], usedTimeout)
			tr = dt
			closers = append(closers, dt)
		default:
			fmt.Fprintln(os.Stderr, "tcpbench supports only tcp transport")
			os.Exit(2)
		}
		ends[i] = tr
	}
	defer func() {
		for _, c := range closers {
			_ = c.Close()
		}
	}()

	runBenchmark(addrs, transportName, ends, usedWorkers, usedDuration, usedTimeout, usedSampleCap)
}

func loadBenchConfig(configPath string, transportName string, addrsStr string, workers int, duration time.Duration, timeout time.Duration, sampleCap int) ([]string, string, int, time.Duration, time.Duration, int) {
	addrs := splitAddrs(addrsStr)
	if configPath == "" {
		if len(addrs) == 0 {
			fmt.Fprintln(os.Stderr, "missing required flag: -addrs")
			os.Exit(2)
		}
		return addrs, transportName, workers, duration, timeout, sampleCap
	}

	cfg, err := config.Load(configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(2)
	}

	benchDuration := duration
	if cfg.Benchmark.Duration != "" {
		benchDuration, err = time.ParseDuration(cfg.Benchmark.Duration)
		if err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(2)
		}
	}
	benchTimeout := timeout
	if cfg.Benchmark.Timeout != "" {
		benchTimeout, err = time.ParseDuration(cfg.Benchmark.Timeout)
		if err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(2)
		}
	}
	if cfg.Benchmark.Workers > 0 {
		workers = cfg.Benchmark.Workers
	}
	if cfg.Benchmark.SampleCap > 0 {
		sampleCap = cfg.Benchmark.SampleCap
	}
	return cfg.BenchmarkAddrs(), cfg.Transport, workers, benchDuration, benchTimeout, sampleCap
}

func runBenchmark(addrs []string, transportName string, ends []server.Transport, workers int, duration time.Duration, timeout time.Duration, sampleCap int) {
	cl := client.New(ends, time.Now().UnixNano(), client.Config{
		RequestTimeout: timeout,
		MinBackoff:     5 * time.Millisecond,
		MaxBackoff:     200 * time.Millisecond,
	})
	stats := client.NewStats(sampleCap)

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
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

	fmt.Printf("transport=%s addrs=%s\n", transportName, strings.Join(addrs, ","))
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
