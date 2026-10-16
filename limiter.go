package byterate

import (
	"context"
	"slices"
)

// Limiter charges and refunds byte budgets. *Rate and *HierarchicalLimiter
// implement it.
type Limiter interface {
	// Get charges size bytes, blocking until the limit allows them.
	Get(size int64)
	// ReturnBucket refunds size bytes previously charged by Get.
	ReturnBucket(size int64)
}

// ContextLimiter is a Limiter whose wait can be abandoned. NewRateConn and
// NewRateReadWriteCloser use it so that Close, and for NewRateConn a
// deadline, ends a Read or Write blocked in the limiter. Other errors from
// GetContext are returned by that Read or Write as they are.
type ContextLimiter interface {
	Limiter
	// GetContext is like Get but returns ctx.Err() if ctx is done before the
	// wait ends. The charge is kept; refund unused bytes with ReturnBucket.
	GetContext(ctx context.Context, size int64) error
}

var (
	_ ContextLimiter = (*Rate)(nil)
	_ ContextLimiter = (*HierarchicalLimiter)(nil)
)

// HierarchicalLimiter charges every one of its Rates and waits until every one
// allows the bytes, so the strictest Rate sets the pace. Stop on a Rate ends
// the wait that Rate imposes, and the others still apply. Every started Rate
// also meters the traffic, whether or not it has a limit, so each level
// reports its own throughput through Now, and a limit set on any of them later
// applies to the next charge. A stopped or never-started Rate is skipped until
// Start.
type HierarchicalLimiter struct {
	first  *Rate
	second *Rate
	third  *Rate
	extra  []*Rate
}

// NewHierarchicalLimiter combines rates, such as a connection's, its user's
// and a global one, into one Limiter. Nil rates are left out, and so are
// repeats, so that a Rate given twice is charged once; unlimited ones are
// kept so that they meter and can be limited later. It returns nil if no rate
// remains and that *Rate itself if only one remains.
//
// Pass nil for levels that should neither limit nor meter: a limiter of
// unlimited Rates still charges every call, and wrapping a connection with
// NewRateConn hides the zero-copy ReadFrom and WriteTo of *net.TCPConn.
func NewHierarchicalLimiter(rates ...*Rate) Limiter {
	b := hierarchicalLimiterBuilder{}
	for _, r := range rates {
		b.add(r)
	}
	return b.build()
}

// NewHierarchicalLimiter2 is NewHierarchicalLimiter for two rates, without
// the variadic slice.
func NewHierarchicalLimiter2(first, second *Rate) Limiter {
	b := hierarchicalLimiterBuilder{}
	b.add(first)
	b.add(second)
	return b.build()
}

// NewHierarchicalLimiter3 is NewHierarchicalLimiter for three rates, without
// the variadic slice.
func NewHierarchicalLimiter3(first, second, third *Rate) Limiter {
	b := hierarchicalLimiterBuilder{}
	b.add(first)
	b.add(second)
	b.add(third)
	return b.build()
}

// Get charges size bytes to every Rate and blocks until every one allows
// them. Stop on a Rate ends the wait that Rate imposes; the others still
// apply.
func (l *HierarchicalLimiter) Get(size int64) {
	var waits waitSet
	l.reserve(size, &waits)
	waits.sleep(nil, nil)
}

// GetContext is like Get but returns ctx.Err() if ctx is done before the wait
// ends. The charge is kept either way; refund unused bytes with ReturnBucket.
func (l *HierarchicalLimiter) GetContext(ctx context.Context, size int64) error {
	var waits waitSet
	l.reserve(size, &waits)
	if waits.n > 0 && !waits.sleep(nil, ctx.Done()) {
		return ctx.Err()
	}
	return nil
}

// reserve charges size to every Rate at one point in time, records in waits,
// unless it is nil, the levels that must wait, and returns the longest wait.
func (l *HierarchicalLimiter) reserve(size int64, waits *waitSet) int64 {
	if l == nil || size <= 0 {
		return 0
	}
	now := nowNs()
	var longest int64
	charge := func(r *Rate) {
		wait, g := r.reserveAt(size, now)
		waits.add(wait, now, g)
		longest = max(longest, wait)
	}
	charge(l.first)
	charge(l.second)
	if l.third != nil {
		charge(l.third)
	}
	for _, r := range l.extra {
		charge(r)
	}
	return longest
}

// levelWait is the wait one level of a hierarchical charge imposes: when it
// ends, and the generation charged, whose Stop ends it early.
type levelWait struct {
	end int64 // nowNs at which the level allows the bytes
	gen *generation
}

// waitSet holds the waits of the levels of a hierarchical charge that must
// wait at all, so that Stop on one level ends the wait that level imposes
// alone. Up to eight levels fit without allocating.
type waitSet struct {
	fixed [8]levelWait
	extra []levelWait
	n     int
}

// add records a level's wait of wait from now, which Stop on g ends early. A
// wait short enough to be coalesced into the debt is left out.
func (w *waitSet) add(wait, now int64, g *generation) {
	if w == nil || wait <= coalesceWaitNs {
		return
	}
	lw := levelWait{end: clampAdd(now, wait), gen: g}
	if w.n < len(w.fixed) {
		w.fixed[w.n] = lw
	} else {
		w.extra = append(w.extra, lw)
	}
	w.n++
}

func (w *waitSet) at(i int) *levelWait {
	if i < len(w.fixed) {
		return &w.fixed[i]
	}
	return &w.extra[i-len(w.fixed)]
}

// sleep waits on t until every level in w allows the bytes, and reports
// false if done ended the wait first. It sleeps until the level that ends
// last, which covers the others; if that level's Stop ends its wait, the
// others still apply.
func (w *waitSet) sleep(t *waitTimer, done <-chan struct{}) bool {
	key := t.key()
	for w.n > 0 {
		last := 0
		for i := 1; i < w.n; i++ {
			if w.at(i).end > w.at(last).end {
				last = i
			}
		}
		lw := *w.at(last)
		wait := subSat(lw.end, nowNs())
		if wait <= coalesceWaitNs {
			return true
		}
		stop := lw.gen.waitCh(key)
		if !t.sleep(wait, stop, done) {
			return false
		}
		if !signaled(stop) {
			return true // the wait elapsed, and the others with it
		}
		*w.at(last) = *w.at(w.n - 1)
		w.n--
	}
	return true
}

// ReturnBucket refunds size bytes to every Rate.
func (l *HierarchicalLimiter) ReturnBucket(size int64) {
	if l == nil || size <= 0 {
		return
	}
	l.first.ReturnBucket(size)
	l.second.ReturnBucket(size)
	l.third.ReturnBucket(size)
	for _, r := range l.extra {
		r.ReturnBucket(size)
	}
}

type hierarchicalLimiterBuilder struct {
	first  *Rate
	second *Rate
	third  *Rate
	extra  []*Rate
	count  int
}

func (b *hierarchicalLimiterBuilder) add(r *Rate) {
	if r == nil || r == b.first || r == b.second || r == b.third || slices.Contains(b.extra, r) {
		return
	}
	switch b.count {
	case 0:
		b.first = r
	case 1:
		b.second = r
	case 2:
		b.third = r
	default:
		b.extra = append(b.extra, r)
	}
	b.count++
}

func (b *hierarchicalLimiterBuilder) build() Limiter {
	switch b.count {
	case 0:
		return nil
	case 1:
		return b.first
	default:
		return &HierarchicalLimiter{
			first:  b.first,
			second: b.second,
			third:  b.third,
			extra:  b.extra,
		}
	}
}
