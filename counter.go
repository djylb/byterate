package byterate

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"
)

// Counter adds up the bytes read and written by a transfer, for accounting
// and for measuring throughput, without limiting it. Swap takes the totals
// since the last Swap, so that each byte is reported exactly once however
// Adds and Swaps interleave, and Throughput reports the bytes per second of
// the last whole second.
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
	read, written atomic.Int64   // totals since the Counter began, unless sharded
	shards        []counterShard // spread Adds, made by NewSharedCounter
	meter         window         // over the totals

	mu      sync.Mutex // serializes Load, Swap, Restore and Set
	swapped [2]int64   // totals at the last Swap
}

// counterShard is one of the counter pairs of a shared Counter, on a cache
// line of its own.
type counterShard struct {
	read, written atomic.Int64
	_             [cacheLineSize - 16]byte
}

// NewSharedCounter returns a Counter that spreads Adds over counters, one per
// processor or more, so that goroutines adding at once do not contend for
// one. It takes a cache line per counter, so keep it for counters that many
// goroutines share.
func NewSharedCounter() *Counter {
	return &Counter{shards: make([]counterShard, pow2AtLeast(runtime.GOMAXPROCS(0)))}
}

// Add adds read bytes read and written bytes written; negative counts
// subtract. It does not read the clock, so for Throughput the bytes count in
// the sampling window that is open when the next count through one of c's
// views, or Throughput, closes it; the views count with the time of each
// charge.
func (c *Counter) Add(read, written int64) {
	if c == nil {
		return
	}
	cr, cw := &c.read, &c.written
	if s := c.shards; s != nil {
		shard := &s[shardKey()&uint32(len(s)-1)]
		cr, cw = &shard.read, &shard.written
	}
	if read != 0 {
		cr.Add(read)
	}
	if written != 0 {
		cw.Add(written)
	}
}

// addAt is Add at now, as nowNs returns it: it closes the sampling window
// before counting once now is past it, so that the bytes count in now's.
func (c *Counter) addAt(read, written, now int64) {
	if c.meter.due(now) && c.meter.mu.TryLock() {
		c.meter.roll(now, c.totals())
		c.meter.mu.Unlock()
	}
	c.Add(read, written)
}

// Total returns the bytes read and written since c began, which Swap does
// not change: the running totals to persist, or to report as snapshots that
// the receiver takes differences of. Refunds through c's views take bytes
// off, so a total can also go down. Restore and Set take persisted totals
// back.
func (c *Counter) Total() (read, written int64) {
	if c == nil {
		return 0, 0
	}
	t := c.totals()
	return t[0], t[1]
}

// Restore adds read and written to c's totals as bytes counted before, such
// as the persisted totals of a counter that resumes after a restart: Total
// includes them, but neither Swap nor Throughput reports them as new
// traffic.
func (c *Counter) Restore(read, written int64) {
	if c == nil {
		return
	}
	c.shift([2]int64{read, written}, false)
}

// Set makes read and written c's totals, as Restore does for a difference:
// Total returns them, but neither Swap nor Throughput reports the change as
// traffic. Each byte that an Add adds while Set runs counts as added before
// Set, and is replaced, or after it, and is added to the new totals.
func (c *Counter) Set(read, written int64) {
	if c == nil {
		return
	}
	c.shift([2]int64{read, written}, true)
}

// shift adds v to the totals, or with to makes them v, and to the baselines
// of Swap and Throughput, so that neither reports the change.
func (c *Counter) shift(v [2]int64, to bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.meter.mu.Lock()
	defer c.meter.mu.Unlock()
	if to {
		t := c.totals()
		v[0], v[1] = v[0]-t[0], v[1]-t[1]
	}
	c.read.Add(v[0])
	c.written.Add(v[1])
	for i := range v {
		c.swapped[i] += v[i]
		c.meter.start[i] += v[i]
	}
}

// Reads returns a Limiter that never limits and counts the bytes charged to
// it as c's bytes read, refunds taken off. Put it in a NewHierarchy with the
// limiters of the reads it counts, such as the read Limiter of
// NewDuplexConn, so that c counts what the reads take, also through a
// Switch.
func (c *Counter) Reads() Limiter { return (*counterReads)(c) }

// Writes returns a Limiter that counts the bytes charged to it as c's bytes
// written, as Reads does for bytes read.
func (c *Counter) Writes() Limiter { return (*counterWrites)(c) }

// counterReads and counterWrites are the Limiter views of a Counter. They
// count with the time of each charge, for Throughput.
type (
	counterReads  Counter
	counterWrites Counter
)

func (c *counterReads) Wait(_ context.Context, n int64) error { return c.Charge(n) }
func (c *counterReads) TryWait(n int64) error                 { return c.Charge(n) }
func (c *counterReads) Ready(_ context.Context, n int64) (int64, error) {
	return n, nil
}

func (c *counterReads) Refund(n int64) {
	if c != nil && n > 0 {
		(*Counter)(c).addAt(-n, 0, nowNs())
	}
}

func (c *counterReads) Charge(n int64) error {
	if c != nil && n > 0 {
		(*Counter)(c).addAt(n, 0, nowNs())
	}
	return nil
}

func (c *counterWrites) Wait(_ context.Context, n int64) error { return c.Charge(n) }
func (c *counterWrites) TryWait(n int64) error                 { return c.Charge(n) }
func (c *counterWrites) Ready(_ context.Context, n int64) (int64, error) {
	return n, nil
}

func (c *counterWrites) Refund(n int64) {
	if c != nil && n > 0 {
		(*Counter)(c).addAt(0, -n, nowNs())
	}
}

func (c *counterWrites) Charge(n int64) error {
	if c != nil && n > 0 {
		(*Counter)(c).addAt(0, n, nowNs())
	}
	return nil
}

// totals returns the bytes read and written since c began.
func (c *Counter) totals() [2]int64 {
	t := [2]int64{c.read.Load(), c.written.Load()}
	for i := range c.shards {
		t[0] += c.shards[i].read.Load()
		t[1] += c.shards[i].written.Load()
	}
	return t
}

// Load returns the bytes read and written since the last Swap.
func (c *Counter) Load() (read, written int64) {
	if c == nil {
		return 0, 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	t := c.totals()
	return t[0] - c.swapped[0], t[1] - c.swapped[1]
}

// Swap returns the bytes read and written since the last Swap and starts
// counting them over. A count that an Add adds while Swap runs lands in this
// Swap's result or in the next one's, never in both or neither; the two
// counts of one Add may land in different Swaps. Swap does not change
// Throughput.
func (c *Counter) Swap() (read, written int64) {
	if c == nil {
		return 0, 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	t := c.totals()
	read, written = t[0]-c.swapped[0], t[1]-c.swapped[1]
	c.swapped = t
	return read, written
}

// Throughput returns the bytes per second read and written in the last whole
// second, in the sampling windows that Rate.Throughput has: one-second
// slices of a clock that every Rate and Counter shares, of which it reports
// the one before the current one, so that what it reports depends on when
// the bytes passed and not on how often or by how many callers it is
// called. Bytes counted through c's views count in the window of their
// charge; those of Add, which does not read the clock, in the window open
// when the next count through a view, or a call of Throughput, closes it,
// so a Counter that only Add feeds and Throughput reads every N seconds
// reports N seconds of bytes as one. Bytes added before c's first window
// count in no window. A window in which more was refunded than counted
// reports 0 and takes the rest off the following ones.
func (c *Counter) Throughput() (readBps, writtenBps int64) {
	if c == nil {
		return 0, 0
	}
	t := c.throughput(nowNs())
	return t[0], t[1]
}

// throughput is Throughput at now.
func (c *Counter) throughput(now int64) [2]int64 {
	c.meter.mu.Lock()
	defer c.meter.mu.Unlock()
	c.meter.roll(now, c.totals())
	return c.meter.last()
}

// shardKey spreads goroutines over the shards of a shared Counter by where
// their stacks are, which mostly keeps a goroutine on one shard and costs no
// more than a multiplication. A deeper call or a stack that moves only takes
// its goroutine to another shard.
func shardKey() uint32 {
	var probe byte
	return mix32(uintptr(unsafe.Pointer(&probe)) >> 12)
}
