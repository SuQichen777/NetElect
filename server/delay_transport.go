package server

import (
	"sync/atomic"
	"time"
)

// DelayTransport wraps a Transport and injects a configurable fixed delay before every outbound call.
// The delay is applied before the actual RPC so RTT measurements (e.g. Raft.Ping) observe the injected latency.
// Safe for concurrent use; delay can be changed at runtime via SetDelay.
type DelayTransport struct {
	Inner   Transport
	delayMs int64 // atomic, milliseconds; 0 = no delay
}

func (d *DelayTransport) Call(method string, args interface{}, reply interface{}) bool {
	if ms := atomic.LoadInt64(&d.delayMs); ms > 0 {
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
	return d.Inner.Call(method, args, reply)
}

// SetDelay sets the outbound delay in milliseconds. Pass 0 to disable.
func (d *DelayTransport) SetDelay(ms int64) {
	atomic.StoreInt64(&d.delayMs, ms)
}

// GetDelay returns the current outbound delay in milliseconds.
func (d *DelayTransport) GetDelay() int64 {
	return atomic.LoadInt64(&d.delayMs)
}

// Close delegates to Inner if it implements io.Closer.
func (d *DelayTransport) Close() error {
	if c, ok := d.Inner.(interface{ Close() error }); ok {
		return c.Close()
	}
	return nil
}

var _ Transport = (*DelayTransport)(nil)
