package byterate

import (
	"runtime"
	"sync/atomic"
	"unsafe"
)

// stripes is a pair of sums that goroutines add to at once. It starts as
// two plain atomics, which is all that a sum few goroutines add to ever
// takes, and spreads the adds over cells of their own cache lines only once
// they contend: an add whose compare-and-swap loses to another's makes the
// cells, and one that loses on a cell doubles them, up to one per
// processor. Its memory so grows with the contention it sees, never with
// the processors alone.
//
// The cells an add went to keep their counts when more are made, so a sum
// reads every set made so far.
type stripes struct {
	cells atomic.Pointer[stripeSet]
	base  [2]atomic.Int64
}

// stripeSet is one set of cells of a stripes, and the smaller one it
// replaced.
type stripeSet struct {
	prev  *stripeSet
	cells []stripeCell // a power of two of them
}

// stripeCell is a pair of sums on a cache line of its own.
type stripeCell struct {
	v [2]atomic.Int64
	_ [(cacheLineSize - unsafe.Sizeof([2]atomic.Int64{})%cacheLineSize) % cacheLineSize]byte
}

// add adds n to sum i and returns the counter it added to, for a caller
// that must take it off the same one again. It tells contention by
// whether another add came between its load of the counter and its own
// add, which costs no more than the load: both always add.
func (s *stripes) add(i int, n int64) *atomic.Int64 {
	if p := &s.base[i]; s.cells.Load() == nil {
		if v := p.Load(); p.Add(n) == v+n {
			return p
		}
		s.grow(nil)
		return p
	}
	return s.addSpread(i, n)
}

// addSpread is add once the base contended: to the goroutine's cell,
// doubling the cells when it contends there too.
func (s *stripes) addSpread(i int, n int64) *atomic.Int64 {
	set := s.cells.Load()
	p := &set.cells[shardIndex(shardKey(), len(set.cells))].v[i]
	if v := p.Load(); p.Add(n) != v+n {
		s.grow(set)
	}
	return p
}

// grow replaces set, the current cells or nil for none, by twice as many,
// and returns the current set: set itself once it has a cell per processor.
func (s *stripes) grow(set *stripeSet) *stripeSet {
	n := 2
	if set != nil {
		n = 2 * len(set.cells)
		if n > pow2AtLeast(runtime.GOMAXPROCS(0)) {
			return set
		}
	}
	next := &stripeSet{cells: make([]stripeCell, n), prev: set}
	if s.cells.CompareAndSwap(set, next) {
		return next
	}
	return s.cells.Load()
}

// sum returns sum i.
func (s *stripes) sum(i int) int64 {
	n := s.base[i].Load()
	for set := s.cells.Load(); set != nil; set = set.prev {
		for j := range set.cells {
			n += set.cells[j].v[i].Load()
		}
	}
	return n
}

// shardKey spreads goroutines over cells by where their stacks are, which
// mostly keeps a goroutine on one cell and costs no more than a
// multiplication. A deeper call or a stack that moves only takes its
// goroutine to another cell.
func shardKey() uint32 {
	var probe byte
	return mix32(addrOf(&probe) >> 12)
}
