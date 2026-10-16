package byterate

import (
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"
)

// Counter adds up the inbound and outbound bytes of a transfer, for
// accounting and for measuring throughput, without limiting it. Swap takes
// the totals since the last Swap, so that each byte is reported exactly once
// however Adds and Swaps interleave, and Throughput reports the bytes per
// second.
//
// The zero value is ready to use and adds to one pair of counters, which
// suits a counter that few goroutines add to at once, such as one per
// connection. NewSharedCounter spreads the Adds of many goroutines, such as
// all the connections of a user, over counters per processor. Add costs an
// atomic add per direction and does not read the clock.
//
// All methods are safe for concurrent use and on a nil *Counter. A Counter
// must not be copied after first use.
type Counter struct {
	in, out atomic.Int64   // totals since the Counter began, unless sharded
	shards  []counterShard // spread Adds, made by NewSharedCounter
	started atomic.Bool    // the first throughput window has begun

	mu                    sync.Mutex // serializes Load, Swap and Throughput
	swappedIn, swappedOut int64      // totals at the last Swap
	windowAt              int64      // nowNs at which the current window began
	windowIn, windowOut   int64      // totals then
	inBps, outBps         int64      // throughput of the last window closed
}

// counterShard is one of the counter pairs of a shared Counter, on a cache
// line of its own.
type counterShard struct {
	in, out atomic.Int64
	_       [cacheLineSize - 16]byte
}

// NewSharedCounter returns a Counter that spreads Adds over counters, one per
// processor or more, so that goroutines adding at once do not contend for
// one. It takes a cache line per counter, so keep it for counters that many
// goroutines share.
func NewSharedCounter() *Counter {
	n := 1
	for n < runtime.GOMAXPROCS(0) {
		n <<= 1
	}
	c := &Counter{shards: make([]counterShard, n)}
	c.start()
	return c
}

// Add adds in inbound and out outbound bytes; negative counts subtract.
func (c *Counter) Add(in, out int64) {
	if c == nil {
		return
	}
	ci, co := &c.in, &c.out
	if s := c.shards; s != nil {
		shard := &s[shardKey()&uint32(len(s)-1)]
		ci, co = &shard.in, &shard.out
	}
	if in != 0 {
		ci.Add(in)
	}
	if out != 0 {
		co.Add(out)
	}
	if !c.started.Load() {
		c.start()
	}
}

// start begins the first throughput window, at the first Add of a zero
// value.
func (c *Counter) start() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.started.Load() {
		c.windowAt = nowNs()
		c.started.Store(true)
	}
}

// ChargeIn adds n inbound bytes and returns nil, so that it can stand for a
// Limiter's Charge, such as a relay's hook after each write.
func (c *Counter) ChargeIn(n int64) error {
	c.Add(n, 0)
	return nil
}

// ChargeOut adds n outbound bytes and returns nil, as ChargeIn does.
func (c *Counter) ChargeOut(n int64) error {
	c.Add(0, n)
	return nil
}

// totals returns the inbound and outbound bytes since c began.
func (c *Counter) totals() (in, out int64) {
	in, out = c.in.Load(), c.out.Load()
	for i := range c.shards {
		in += c.shards[i].in.Load()
		out += c.shards[i].out.Load()
	}
	return in, out
}

// Load returns the inbound and outbound bytes since the last Swap.
func (c *Counter) Load() (in, out int64) {
	if c == nil {
		return 0, 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	in, out = c.totals()
	return in - c.swappedIn, out - c.swappedOut
}

// Swap returns the inbound and outbound bytes since the last Swap and starts
// counting them over. A count that an Add adds while Swap runs lands in this
// Swap's result or in the next one's, never in both or neither; the inbound
// and outbound counts of one Add may land in different Swaps. Swap does not
// change Throughput.
func (c *Counter) Swap() (in, out int64) {
	if c == nil {
		return 0, 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	totalIn, totalOut := c.totals()
	in, out = totalIn-c.swappedIn, totalOut-c.swappedOut
	c.swappedIn, c.swappedOut = totalIn, totalOut
	return in, out
}

// Throughput returns the inbound and outbound bytes per second over the last
// sampling window of at least one second, closing the current window if it
// is due: the window runs from the first Add, or the previous window's end,
// to a call of Throughput a second or more later. It reports 0 for a window
// with no traffic, and before the first window closes.
func (c *Counter) Throughput() (inBps, outBps int64) {
	if c == nil || !c.started.Load() {
		return 0, 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	now := nowNs()
	if dt := subSat(now, c.windowAt); dt >= sampleIntervalNs {
		totalIn, totalOut := c.totals()
		c.inBps = bytesPerSec(totalIn-c.windowIn, dt)
		c.outBps = bytesPerSec(totalOut-c.windowOut, dt)
		c.windowAt, c.windowIn, c.windowOut = now, totalIn, totalOut
	}
	return c.inBps, c.outBps
}

// shardKey spreads goroutines over the shards of a shared Counter by where
// their stacks are, which mostly keeps a goroutine on one shard and costs no
// more than a multiplication. A deeper call or a stack that moves only takes
// its goroutine to another shard.
func shardKey() uint32 {
	var probe byte
	return uint32(uint64(uintptr(unsafe.Pointer(&probe))>>12) * 0x9E3779B97F4A7C15 >> 32)
}
