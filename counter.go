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
// atomic add per direction and does not read the clock. Attach adds up the
// Meters of connections instead, which costs their reads and writes nothing
// at all.
//
// All methods are safe for concurrent use and on a nil *Counter. A Counter
// must not be copied after first use.
type Counter struct {
	read, written atomic.Int64   // totals since the Counter began, unless sharded
	shards        []counterShard // spread Adds, made by NewSharedCounter
	meter         window         // over the totals

	mu      sync.Mutex // serializes Load, Swap, Restore and Set
	swapped [2]int64   // totals at the last Swap

	clock atomic.Pointer[clockBox] // SetClock's, nil for the monotonic clock

	// The Meters added up in the totals, with their totals when attached.
	srcMu sync.Mutex
	srcs  map[*Meter]*meterSrc
	nsrc  atomic.Int32 // len(srcs), so that totals skips srcMu without any
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
// subtract. It does not read the clock: Throughput samples the totals.
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

// reads returns a Limiter that never limits and counts the bytes charged to
// it as c's bytes read, refunds taken off, for the Reads of a Group.
func (c *Counter) reads() Limiter { return (*counterReads)(c) }

// writes returns a Limiter that counts the bytes charged to it as c's bytes
// written, as reads does for bytes read.
func (c *Counter) writes() Limiter { return (*counterWrites)(c) }

// counterReads and counterWrites are the Limiter views of a Counter. They
// add as Add does, without reading the clock.
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
		(*Counter)(c).Add(-n, 0)
	}
}

func (c *counterReads) Charge(n int64) error {
	if c != nil && n > 0 {
		(*Counter)(c).Add(n, 0)
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
		(*Counter)(c).Add(0, -n)
	}
}

func (c *counterWrites) Charge(n int64) error {
	if c != nil && n > 0 {
		(*Counter)(c).Add(0, n)
	}
	return nil
}

// totals returns the bytes read and written since c began.
func (c *Counter) totals() [2]int64 {
	if c.nsrc.Load() > 0 {
		// Under srcMu, so that a Detach that moves a Meter's bytes into
		// the totals is seen whole or not at all.
		c.srcMu.Lock()
		defer c.srcMu.Unlock()
	}
	t := [2]int64{c.read.Load(), c.written.Load()}
	for i := range c.shards {
		t[0] += c.shards[i].read.Load()
		t[1] += c.shards[i].written.Load()
	}
	for m, src := range c.srcs {
		r, w := m.Total()
		t[0] += r - src.base[0]
		t[1] += w - src.base[1]
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
// second. Counting never reads the clock: the totals are sampled when a
// whole second of c's clock has passed since the last sample, by Throughput
// itself or by Sample, and the bytes between two samples count evenly over
// the seconds between them. Called at least once a second, as a ticker of
// the caller's does with Sample, it reports each second on its own; called
// every N seconds, the average of those N seconds. The windows are whole
// seconds of c's clock, the same for every reader, so what it reports does
// not depend on how many callers read it. Bytes counted before c's first
// sample count in no window. A window in which more was refunded than
// counted reports 0 and takes the rest off the following ones.
func (c *Counter) Throughput() (readBps, writtenBps int64) {
	if c == nil {
		return 0, 0
	}
	t := c.throughput(c.now())
	return t[0], t[1]
}

// Sample samples c's totals for Throughput if a second has passed since the
// last sample. A caller that wants the throughput of every second, however
// rarely it reads it, calls Sample once a second, such as from a ticker
// shared by all its Counters; it costs a lock and a read of the clock.
func (c *Counter) Sample() {
	if c != nil {
		c.throughput(c.now())
	}
}

// SetClock makes c read the time of its throughput samples from clk, such
// as a fake clock in a test; nil goes back to the monotonic clock. Set it
// before the first sample.
func (c *Counter) SetClock(clk Clock) {
	if c == nil {
		return
	}
	if clk == nil {
		c.clock.Store(nil)
		return
	}
	c.clock.Store(&clockBox{c: clk})
}

// now returns the time of c's clock in nanoseconds.
func (c *Counter) now() int64 {
	if b := c.clock.Load(); b != nil {
		return b.c.Now().UnixNano()
	}
	return nowNs()
}

// throughput is Throughput at now.
func (c *Counter) throughput(now int64) [2]int64 {
	c.meter.mu.Lock()
	defer c.meter.mu.Unlock()
	c.meter.roll(now, c.totals())
	return c.meter.last()
}

// Attach adds m's bytes from now on to c's totals, without counting them
// on every read and write: c adds up its Meters when it is read, so that a
// Counter shared by many connections, such as a user's, costs their reads
// and writes nothing. The bytes m counted before stay out of c. Detach
// ends it, keeping the bytes in c. A Meter attached more than once, such as
// that of a wrapper given c by WithCounter and as a Group's Traffic, counts
// once, until as many Detaches.
func (c *Counter) Attach(m *Meter) {
	if c == nil || m == nil {
		return
	}
	c.srcMu.Lock()
	defer c.srcMu.Unlock()
	if src, ok := c.srcs[m]; ok {
		src.refs++
		return
	}
	if c.srcs == nil {
		c.srcs = make(map[*Meter]*meterSrc)
	}
	r, w := m.Total()
	c.srcs[m] = &meterSrc{base: [2]int64{r, w}, refs: 1}
	c.nsrc.Store(int32(len(c.srcs)))
}

// meterSrc is a Meter attached to a Counter: its totals when attached and
// the Attaches not yet detached.
type meterSrc struct {
	base [2]int64
	refs int
}

// Detach ends an Attach of m, and with the last one stops adding m's bytes
// to c, keeping those it added so far as if they had been counted by Add.
func (c *Counter) Detach(m *Meter) {
	if c == nil || m == nil {
		return
	}
	c.srcMu.Lock()
	defer c.srcMu.Unlock()
	src, ok := c.srcs[m]
	if !ok {
		return
	}
	if src.refs--; src.refs > 0 {
		return
	}
	r, w := m.Total()
	// Added before the Meter leaves, so that totals never misses it.
	c.read.Add(r - src.base[0])
	c.written.Add(w - src.base[1])
	delete(c.srcs, m)
	c.nsrc.Store(int32(len(c.srcs)))
}

// shardKey spreads goroutines over the shards of a shared Counter by where
// their stacks are, which mostly keeps a goroutine on one shard and costs no
// more than a multiplication. A deeper call or a stack that moves only takes
// its goroutine to another shard.
func shardKey() uint32 {
	var probe byte
	return mix32(uintptr(unsafe.Pointer(&probe)) >> 12)
}
