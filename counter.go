package byterate

import (
	"context"
	"sync"
	"sync/atomic"
)

// Counter adds up the bytes read and written by a transfer, for accounting
// and for measuring throughput, without limiting it. Swap takes the totals
// since the last Swap, so that each byte is reported exactly once however
// Adds and Swaps interleave, and Throughput reports the bytes per second of
// the last whole second.
//
// The zero value is ready to use. Add costs an atomic add per direction and
// does not read the clock; a Counter that many goroutines add to at once
// spreads their Adds over counters of their own as it finds them
// contending, and one that few add to stays one pair of counters. Attach
// adds up the Meters of connections instead, which costs their reads and
// writes nothing at all.
//
// All methods are safe for concurrent use and on a nil *Counter. A Counter
// must not be copied after first use.
type Counter struct {
	clock atomic.Pointer[clockBox] // SetClock's, nil for the monotonic clock
	tally                          // Adds, and the Meters and Groups attached
	meter window                   // over the totals

	mu      sync.Mutex // serializes Load, Swap, Restore and Set
	swapped [2]int64   // totals at the last Swap
}

// Add adds read bytes read and written bytes written; negative counts
// subtract. It does not read the clock: Throughput samples the totals.
func (c *Counter) Add(read, written int64) {
	if c != nil {
		c.add(read, written)
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
	t := c.total()
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
		t := c.total()
		v[0], v[1] = v[0]-t[0], v[1]-t[1]
	}
	c.added.base[0].Add(v[0])
	c.added.base[1].Add(v[1])
	for i := range v {
		c.swapped[i] += v[i]
		c.meter.start[i] += v[i]
	}
}

// reads returns a Limiter that never limits and counts the bytes charged to
// it as t's bytes read, refunds taken off, for the Reads of a Group.
func (t *tally) reads() Limiter { return (*counterReads)(t) }

// writes returns a Limiter that counts the bytes charged to it as t's bytes
// written, as reads does for bytes read.
func (t *tally) writes() Limiter { return (*counterWrites)(t) }

// counterReads and counterWrites are the Limiter views of a tally. They
// add as Add does, without reading the clock.
type (
	counterReads  tally
	counterWrites tally
)

func (c *counterReads) Wait(_ context.Context, n int64) error { return c.Charge(n) }
func (c *counterReads) TryWait(n int64) error                 { return c.Charge(n) }
func (c *counterReads) Ready(_ context.Context, n int64) (int64, error) {
	return n, nil
}

func (c *counterReads) Refund(n int64) {
	if c != nil && n > 0 {
		(*tally)(c).add(-n, 0)
	}
}

func (c *counterReads) Charge(n int64) error {
	if c != nil && n > 0 {
		(*tally)(c).add(n, 0)
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
		(*tally)(c).add(0, -n)
	}
}

func (c *counterWrites) Charge(n int64) error {
	if c != nil && n > 0 {
		(*tally)(c).add(0, n)
	}
	return nil
}

// Load returns the bytes read and written since the last Swap.
func (c *Counter) Load() (read, written int64) {
	if c == nil {
		return 0, 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	t := c.total()
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
	t := c.total()
	read, written = t[0]-c.swapped[0], t[1]-c.swapped[1]
	c.swapped = t
	return read, written
}

// Throughput returns the bytes per second read and written between the
// last two samples of c's totals, or 0s until a second sample. Counting
// never reads the clock: the totals are sampled by Throughput itself or by
// Sample, at most once in each whole second of c's clock, and the bytes
// counted between two samples are averaged over the whole seconds between
// them. So the first call without Sample reports 0, a reader that samples
// once a second sees about each second on its own, and one that reads every
// N seconds sees the average of those N seconds. What it reports depends on
// when the samples were taken, by any caller: call Sample once a second, as
// from a ticker of the caller's, for readings that do not depend on how
// often or by how many Throughput is called. Bytes counted before c's first
// sample count in none. A span in which more was refunded than counted
// reports 0 and takes the rest off the following ones.
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
	c.meter.roll(now, c.total())
	return c.meter.last()
}

// Attach adds m's bytes from now on to c's totals, without counting them
// on every read and write: c adds up its Meters when it is read, so that a
// Counter shared by many connections costs their reads and writes nothing.
// The bytes m counted before stay out of c. Detach ends it, keeping the
// bytes in c. A Meter attached more than once counts once, until as many
// Detaches. It returns ErrCounterOverlap, and attaches nothing, when c is
// the Traffic of the Group m's wrapper was opened in or of an ancestor,
// which counts m already.
func (c *Counter) Attach(m *Meter) error {
	if c == nil || m == nil {
		return nil
	}
	if m.node != nil {
		cycleMu.Lock()
		defer cycleMu.Unlock()
		if c.countsAbove(m.node) {
			return ErrCounterOverlap
		}
	}
	c.attach(m, m.totals())
	return nil
}

// Detach ends an Attach of m, and with the last one stops adding m's bytes
// to c, keeping those it added so far as if they had been counted by Add.
func (c *Counter) Detach(m *Meter) {
	if c != nil && m != nil {
		c.detach(m, m.totals())
	}
}
