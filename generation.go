package byterate

import (
	"runtime"
	"sync/atomic"
	"unsafe"
)

// generation is the state of a Rate from one Enable that clears it to the
// next: its debt, its meter and the channels that Disable closes. Enable
// replaces the generation instead of clearing it in place, and a new one is
// complete before it is published, so a charge, a refund and a sampling
// window each act on the one generation they loaded, and one that an Enable
// retired keeps its effects to itself.
//
// It fills two cache lines of its size class exactly: the first is read by
// every charge and written once per sampling window, the second written by
// every charge.
type generation struct {
	generationRead
	_     [cacheLineSize - unsafe.Sizeof(generationRead{})]byte
	tat   atomic.Int64 // theoretical arrival time (ns since epoch), can be negative
	total atomic.Int64 // bytes charged, net of refunds, for meter
	_     [cacheLineSize - 16]byte
}

// generationRead are the fields of a generation that every charge reads.
type generationRead struct {
	stop  atomic.Pointer[stopSet] // made by the first waiter
	meter window                  // over total
}

// stopSet holds the channels of a generation that Disable closes. Waiters
// spread over them, as a wait locks the channel it selects on, and one
// channel for every waiter of a shared Rate would serialize them. A
// generation makes a small set for its first waiter and each channel for the
// first waiter keyed to it, so that a Rate that never makes callers wait pays
// for none, and one of a single connection, whose directions keep a key
// each, pays for a channel per direction. Once every channel of the small set
// is made, the Rate is shared, and a set sized for the processors replaces
// it, keeping the small one for Disable to close.
type stopSet struct {
	shards []atomic.Pointer[stopChan] // a power of two of them
	made   atomic.Int32               // channels made in shards
	prev   *stopSet                   // the set this one replaced, still to close
}

// initialStopShards is the size of a generation's first stop set.
const initialStopShards = 8

func newStopSet(n int, prev *stopSet) *stopSet {
	return &stopSet{shards: make([]atomic.Pointer[stopChan], n), prev: prev}
}

// sharedStopShards is the size of the set that replaces a full first one:
// eight channels per processor, at most 256.
func sharedStopShards() int {
	return min(pow2AtLeast(max(8*runtime.GOMAXPROCS(0), initialStopShards)), 256)
}

// stopChan is one of the channels of a generation that Disable closes.
type stopChan struct{ ch chan struct{} }

var (
	// stoppedSet is left by Disable in a generation that no waiter had given a
	// set, and closedStop in the shards of a set that no waiter had made.
	stoppedSet = &stopSet{}
	closedStop = &stopChan{ch: closedChan}
)

// newGeneration returns a generation with a full burst at now and an empty
// meter.
func newGeneration(now, burstNs int64) *generation {
	g := &generation{}
	g.tat.Store(subSat(now, burstNs))
	return g
}

// waitCh returns the channel of g that Disable closes for the waiter keyed by
// key, or nil for no generation.
func (g *generation) waitCh(key uint32) <-chan struct{} {
	if g == nil {
		return nil
	}
	set := g.stop.Load()
	if set == nil {
		set = newStopSet(initialStopShards, nil)
		if !g.stop.CompareAndSwap(nil, set) {
			set = g.stop.Load() // another waiter's, or stoppedSet
		}
	}
	if set == stoppedSet {
		return closedChan
	}
	s := &set.shards[key&uint32(len(set.shards)-1)]
	c := s.Load()
	if c == nil {
		c = &stopChan{ch: make(chan struct{})}
		if !s.CompareAndSwap(nil, c) {
			return s.Load().ch // another waiter's, or closedStop
		}
		if int(set.made.Add(1)) == len(set.shards) {
			g.grow(set)
		}
	}
	return c.ch
}

// grow replaces g's full stop set with a larger one, unless Disable or another
// waiter replaced it first. Waiters keep the channels they took from the
// full set, which Disable closes through the new set's prev.
func (g *generation) grow(full *stopSet) {
	if n := sharedStopShards(); n > len(full.shards) {
		g.stop.CompareAndSwap(full, newStopSet(n, full))
	}
}

// closeStop closes g's channels, waking its waiters, and leaves stoppedSet
// and closedStop for waiters to come. It runs once, for Disable.
func (g *generation) closeStop() {
	for set := g.stop.Swap(stoppedSet); set != nil; set = set.prev {
		for i := range set.shards {
			if c := set.shards[i].Swap(closedStop); c != nil {
				close(c.ch)
			}
		}
	}
}

// count meters size bytes charged at now, a negative size for a refund.
func (g *generation) count(size, now int64) {
	if g.meter.due(now) && g.meter.mu.TryLock() {
		g.meter.roll(now, [2]int64{g.total.Load()})
		g.meter.mu.Unlock()
	}
	g.total.Add(size)
}

// throughput returns the bytes per second metered in the window before
// now's.
func (g *generation) throughput(now int64) int64 {
	g.meter.mu.Lock()
	defer g.meter.mu.Unlock()
	g.meter.roll(now, [2]int64{g.total.Load()})
	return g.meter.last()[0]
}
