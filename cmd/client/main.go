package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"raft-leader-election/client"
	"raft-leader-election/config"
	"raft-leader-election/middleware/grpcnet"
	"raft-leader-election/middleware/tcpnet"
	"raft-leader-election/server"
	"strconv"
	"sync"
	"time"
)

func main() {
	var (
		configPath         = flag.String("config", "", "cluster config JSON path (required)")
		workers            = flag.Int("workers", 0, "override benchmark.workers from config")
		duration           = flag.Duration("duration", 0, "override benchmark.duration from config")
		timeout            = flag.Duration("timeout", 0, "override benchmark.timeout from config")
		sampleCap          = flag.Int("sample-cap", 0, "override benchmark.sample_cap from config")
		mode               = flag.String("mode", "", "override benchmark.mode from config: read | write | mixed")
		readRatio          = flag.Float64("read-ratio", 0, "override benchmark.read_ratio from config (0.0–1.0)")
		csvPath            = flag.String("csv", "", "write benchmark results to this CSV file (appends if exists)")
		timeseriesCSV      = flag.String("timeseries-csv", "", "write per-interval stats to this CSV file (appends if exists)")
		timeseriesInterval = flag.Duration("timeseries-interval", time.Second, "interval for timeseries CSV rows")
	)
	flag.Parse()

	if *configPath == "" {
		fmt.Fprintln(os.Stderr, "missing required flag: -config")
		os.Exit(2)
	}
	if *mode != "" && *mode != "read" && *mode != "write" && *mode != "mixed" {
		fmt.Fprintln(os.Stderr, "invalid -mode: expected read, write, or mixed")
		os.Exit(2)
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(2)
	}

	w := cfg.Benchmark.Workers
	if *workers > 0 {
		w = *workers
	}
	if w <= 0 {
		w = 64
	}

	dur, err := time.ParseDuration(cfg.Benchmark.Duration)
	if err != nil || dur <= 0 {
		dur = 10 * time.Second
	}
	if *duration > 0 {
		dur = *duration
	}

	to, err := time.ParseDuration(cfg.Benchmark.Timeout)
	if err != nil || to <= 0 {
		to, _ = cfg.Timeout()
	}
	if *timeout > 0 {
		to = *timeout
	}

	cap := cfg.Benchmark.SampleCap
	if *sampleCap > 0 {
		cap = *sampleCap
	}
	if cap <= 0 {
		cap = 200000
	}

	benchMode := cfg.Benchmark.Mode
	if *mode != "" {
		benchMode = *mode
	}
	if benchMode == "" {
		benchMode = "read"
	}

	ratio := cfg.Benchmark.ReadRatio
	if *readRatio > 0 {
		ratio = *readRatio
	}
	if ratio <= 0 {
		ratio = 0.8
	}

	addrs := cfg.BenchmarkAddrs()
	ends := make([]server.Transport, len(addrs))
	for i, addr := range addrs {
		switch cfg.Transport {
		case config.TransportGRPC:
			ends[i] = grpcnet.NewDialTransport(addr, to)
		case config.TransportTCP:
			ends[i] = tcpnet.NewDialTransport(addr, to)
		}
	}
	defer func() {
		for _, e := range ends {
			if c, ok := e.(interface{ Close() error }); ok {
				_ = c.Close()
			}
		}
	}()

	cl := client.New(ends, time.Now().UnixNano(), client.Config{
		RequestTimeout: to,
		MinBackoff:     5 * time.Millisecond,
		MaxBackoff:     200 * time.Millisecond,
	})
	readStats := client.NewStats(cap)
	writeStats := client.NewStats(cap)
	iReadStats := client.NewIntervalStats()
	iWriteStats := client.NewIntervalStats()

	fmt.Printf("transport=%s addrs=%v workers=%d duration=%s timeout=%s mode=%s\n",
		cfg.Transport, addrs, w, dur, to, benchMode)

	benchStart := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), dur)
	defer cancel()

	// Progress ticker: print live QPS + p99 every 5s.
	go runProgressTicker(ctx, benchMode, benchStart, readStats, writeStats)

	// Start time-series ticker if requested.
	tsCSV := *timeseriesCSV
	tsInterval := *timeseriesInterval
	if tsInterval <= 0 {
		tsInterval = time.Second
	}
	if tsCSV != "" {
		start := time.Now()
		go runTimeseriesTicker(ctx, tsCSV, benchMode, tsInterval, start, iReadStats, iWriteStats)
	}

	var wg sync.WaitGroup
	wg.Add(w)
	for i := 0; i < w; i++ {
		go func(workerID int) {
			defer wg.Done()
			rnd := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))
			j := 0
			for ctx.Err() == nil {
				id := fmt.Sprintf("%d-%d", workerID, j)
				key := fmt.Sprintf("k%d", rnd.Intn(1000))

				doRead := true
				switch benchMode {
				case "write":
					doRead = false
				case "mixed":
					doRead = rnd.Float64() < ratio
				}

				if doRead {
					res := cl.Read(ctx, id, key)
					readStats.Record(res)
					if tsCSV != "" {
						iReadStats.Record(res)
					}
				} else {
					val := fmt.Sprintf("v%d", rnd.Intn(100000))
					res := cl.Write(ctx, id, server.KVPut, key, val)
					writeStats.RecordWrite(res)
					if tsCSV != "" {
						iWriteStats.RecordWrite(res)
					}
				}
				j++
			}
		}(i)
	}
	wg.Wait()

	printStats("read", readStats, dur)
	if benchMode != "read" {
		printStats("write", writeStats, dur)
	}

	csvOut := cfg.Benchmark.CSVOutput
	if *csvPath != "" {
		csvOut = *csvPath
	}
	if csvOut != "" {
		if err := appendCSV(csvOut, benchMode, dur, readStats, writeStats); err != nil {
			fmt.Fprintf(os.Stderr, "csv write error: %v\n", err)
		}
	}
}

func printStats(label string, stats *client.Stats, dur time.Duration) {
	snap := stats.Snapshot()
	total := snap.Success + snap.Failed
	if total == 0 {
		return
	}
	qps := float64(total) / dur.Seconds()
	fmt.Printf("[%s] total=%d success=%d failed=%d qps=%.2f\n",
		label, total, snap.Success, snap.Failed, qps)
	fmt.Printf("[%s] p50=%s p95=%s p99=%s samples=%d\n",
		label, snap.Percentile(0.50), snap.Percentile(0.95), snap.Percentile(0.99), len(snap.Latencies))
	if snap.Failed > 0 {
		fmt.Printf("[%s] errors: timeout=%d notLeader=%d dropped=%d noQuorum=%d unknown=%d\n",
			label, snap.Timeout, snap.NotLeader, snap.Dropped, snap.NoQuorum, snap.Unknown)
	}
}

func runProgressTicker(ctx context.Context, benchMode string, start time.Time, readStats, writeStats *client.Stats) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	lastTime := start
	var lastRead, lastWrite int64
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			elapsed := now.Sub(lastTime).Seconds()
			lastTime = now
			rSnap := readStats.Snapshot()
			wSnap := writeStats.Snapshot()
			rTotal := rSnap.Success + rSnap.Failed
			wTotal := wSnap.Success + wSnap.Failed
			rQPS := float64(rTotal-lastRead) / elapsed
			wQPS := float64(wTotal-lastWrite) / elapsed
			lastRead, lastWrite = rTotal, wTotal
			since := time.Since(start).Truncate(time.Second)
			switch benchMode {
			case "read":
				fmt.Printf("[progress] +%s  read  qps=%.0f p99=%s errors=%d\n",
					since, rQPS, rSnap.Percentile(0.99), rSnap.Failed)
			case "write":
				fmt.Printf("[progress] +%s  write qps=%.0f p99=%s errors=%d\n",
					since, wQPS, wSnap.Percentile(0.99), wSnap.Failed)
			default:
				fmt.Printf("[progress] +%s  read qps=%.0f p99=%s | write qps=%.0f p99=%s | errors r=%d w=%d\n",
					since, rQPS, rSnap.Percentile(0.99), wQPS, wSnap.Percentile(0.99), rSnap.Failed, wSnap.Failed)
			}
		}
	}
}

var csvHeader = []string{
	"label", "total", "success", "failed", "qps",
	"p50_ms", "p95_ms", "p99_ms", "p999_ms",
	"timeout", "not_leader", "dropped", "no_quorum", "unknown",
}

func appendCSV(path, benchMode string, dur time.Duration, readStats, writeStats *client.Stats) error {
	needHeader := false
	if _, err := os.Stat(path); os.IsNotExist(err) {
		needHeader = true
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	if needHeader {
		if err := w.Write(csvHeader); err != nil {
			return err
		}
	}

	writeRow := func(label string, stats *client.Stats) error {
		snap := stats.Snapshot()
		total := snap.Success + snap.Failed
		if total == 0 {
			return nil
		}
		ms := func(d time.Duration) string {
			return strconv.FormatFloat(float64(d)/float64(time.Millisecond), 'f', 3, 64)
		}
		row := []string{
			label,
			strconv.FormatInt(total, 10),
			strconv.FormatInt(snap.Success, 10),
			strconv.FormatInt(snap.Failed, 10),
			strconv.FormatFloat(float64(total)/dur.Seconds(), 'f', 2, 64),
			ms(snap.Percentile(0.50)),
			ms(snap.Percentile(0.95)),
			ms(snap.Percentile(0.99)),
			ms(snap.Percentile(0.999)),
			strconv.FormatInt(snap.Timeout, 10),
			strconv.FormatInt(snap.NotLeader, 10),
			strconv.FormatInt(snap.Dropped, 10),
			strconv.FormatInt(snap.NoQuorum, 10),
			strconv.FormatInt(snap.Unknown, 10),
		}
		return w.Write(row)
	}

	if err := writeRow("read", readStats); err != nil {
		return err
	}
	if benchMode != "read" {
		if err := writeRow("write", writeStats); err != nil {
			return err
		}
	}
	w.Flush()
	return w.Error()
}

var tsHeader = []string{
	"timestamp_s", "label",
	"total", "success", "failed", "qps",
	"p50_ms", "p95_ms", "p99_ms",
	"not_leader", "timeout", "dropped", "no_quorum", "unknown",
}

// runTimeseriesTicker drains interval stats every `interval` and appends a row to `path`.
func runTimeseriesTicker(ctx context.Context, path, benchMode string, interval time.Duration,
	start time.Time, iRead, iWrite *client.IntervalStats) {

	needHeader := false
	if _, err := os.Stat(path); os.IsNotExist(err) {
		needHeader = true
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "timeseries csv open error: %v\n", err)
		return
	}
	defer f.Close()

	w := csv.NewWriter(f)
	if needHeader {
		_ = w.Write(tsHeader)
		w.Flush()
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	ms := func(d time.Duration) string {
		return strconv.FormatFloat(float64(d)/float64(time.Millisecond), 'f', 3, 64)
	}

	writeRow := func(label string, snap client.IntervalSnapshot, elapsed float64) {
		total := snap.Success + snap.Failed
		qps := float64(0)
		if elapsed > 0 {
			qps = float64(total) / elapsed
		}
		row := []string{
			strconv.FormatFloat(time.Since(start).Seconds(), 'f', 3, 64),
			label,
			strconv.FormatInt(total, 10),
			strconv.FormatInt(snap.Success, 10),
			strconv.FormatInt(snap.Failed, 10),
			strconv.FormatFloat(qps, 'f', 2, 64),
			ms(snap.Percentile(0.50)),
			ms(snap.Percentile(0.95)),
			ms(snap.Percentile(0.99)),
			strconv.FormatInt(snap.NotLeader, 10),
			strconv.FormatInt(snap.Timeout, 10),
			strconv.FormatInt(snap.Dropped, 10),
			strconv.FormatInt(snap.NoQuorum, 10),
			strconv.FormatInt(snap.Unknown, 10),
		}
		_ = w.Write(row)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			elapsed := interval.Seconds()
			readSnap := iRead.Drain()
			writeSnap := iWrite.Drain()
			writeRow("read", readSnap, elapsed)
			if benchMode != "read" {
				writeRow("write", writeSnap, elapsed)
			}
			w.Flush()
		}
	}
}
