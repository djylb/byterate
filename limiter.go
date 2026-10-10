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

// HierarchicalLimiter charges every one of its Rates and waits for the longest
// resulting delay, so the strictest Rate sets the pace. Every started Rate also
// meters the traffic, whether or not it has a limit, so each level reports its
// own throughput through Now, and a limit set on any of them later applies to
// the next charge. A stopped or never-started Rate is skipped until Start.
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

// Get charges size bytes to every Rate and blocks for the longest wait. Stop
// on the Rate imposing that wait ends it early.
func (l *HierarchicalLimiter) Get(size int64) {
	if wait, stopCh := l.reserve(size); wait > coalesceWaitNs {
		sleepNs(wait, stopCh, nil)
	}
}

// GetContext is like Get but returns ctx.Err() if ctx is done before the wait
// ends. The charge is kept either way; refund unused bytes with ReturnBucket.
func (l *HierarchicalLimiter) GetContext(ctx context.Context, size int64) error {
	if wait, stopCh := l.reserve(size); wait > coalesceWaitNs {
		if !sleepNs(wait, stopCh, ctx.Done()) {
			return ctx.Err()
		}
	}
	return nil
}

// reserve charges size to every Rate at one point in time and returns the
// longest wait together with the stop channel of the Rate that imposed it.
func (l *HierarchicalLimiter) reserve(size int64) (int64, <-chan struct{}) {
	if l == nil || size <= 0 {
		return 0, nil
	}
	now := nowNs()
	var maxWait int64
	var maxStopCh <-chan struct{}
	charge := func(r *Rate) {
		stopCh := r.stopCh() // before reserving, as in Rate.Get
		if wait := r.reserveAt(size, now); wait > maxWait {
			maxWait = wait
			maxStopCh = stopCh
		}
	}
	charge(l.first)
	charge(l.second)
	if l.third != nil {
		charge(l.third)
	}
	for _, r := range l.extra {
		charge(r)
	}
	return maxWait, maxStopCh
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
