package byterate

import "context"

// Limiter charges and refunds byte budgets. *Rate and *HierarchicalLimiter
// implement it.
type Limiter interface {
	// Get charges size bytes, blocking until the limit allows them.
	Get(size int64)
	// ReturnBucket refunds size bytes previously charged by Get.
	ReturnBucket(size int64)
}

// ContextLimiter is a Limiter whose wait can be abandoned. NewRateConn and NewRateReadWriteCloser use it
// so that Close ends a Read or Write blocked in the limiter.
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
// resulting delay, so the strictest Rate sets the pace.
type HierarchicalLimiter struct {
	first  *Rate
	second *Rate
	third  *Rate
	extra  []*Rate
}

// NewHierarchicalLimiter combines limiters into one Limiter. Nil rates and
// rates whose Limit is <= 0 at call time are left out. It returns nil if no
// rate remains and that *Rate itself if only one remains.
func NewHierarchicalLimiter(limiters ...*Rate) Limiter {
	switch len(limiters) {
	case 0:
		return nil
	case 1:
		if current := enabledLimiter(limiters[0]); current != nil {
			return current
		}
		return nil
	case 2:
		return NewHierarchicalLimiter2(limiters[0], limiters[1])
	case 3:
		return NewHierarchicalLimiter3(limiters[0], limiters[1], limiters[2])
	}
	builder := hierarchicalLimiterBuilder{}
	for _, current := range limiters {
		builder.add(current)
	}
	return builder.build()
}

// NewHierarchicalLimiter2 is NewHierarchicalLimiter for exactly two rates.
func NewHierarchicalLimiter2(first, second *Rate) Limiter {
	builder := hierarchicalLimiterBuilder{}
	builder.add(first)
	builder.add(second)
	return builder.build()
}

// NewHierarchicalLimiter3 is NewHierarchicalLimiter for exactly three rates.
func NewHierarchicalLimiter3(first, second, third *Rate) Limiter {
	builder := hierarchicalLimiterBuilder{}
	builder.add(first)
	builder.add(second)
	builder.add(third)
	return builder.build()
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

// reserve charges size to every Rate and returns the longest wait together
// with the stop channel of the Rate that imposed it.
func (l *HierarchicalLimiter) reserve(size int64) (int64, <-chan struct{}) {
	if l == nil || size <= 0 {
		return 0, nil
	}
	var maxWait int64
	var maxStopCh <-chan struct{}
	if l.first != nil {
		if wait := l.first.reserve(size); wait > maxWait {
			maxWait = wait
			maxStopCh = l.first.stopCh()
		}
	}
	if l.second != nil {
		if wait := l.second.reserve(size); wait > maxWait {
			maxWait = wait
			maxStopCh = l.second.stopCh()
		}
	}
	if l.third != nil {
		if wait := l.third.reserve(size); wait > maxWait {
			maxWait = wait
			maxStopCh = l.third.stopCh()
		}
	}
	for _, current := range l.extra {
		if wait := current.reserve(size); wait > maxWait {
			maxWait = wait
			maxStopCh = current.stopCh()
		}
	}
	return maxWait, maxStopCh
}

// ReturnBucket refunds size bytes to every Rate.
func (l *HierarchicalLimiter) ReturnBucket(size int64) {
	if l == nil || size <= 0 {
		return
	}
	if l.first != nil {
		l.first.ReturnBucket(size)
	}
	if l.second != nil {
		l.second.ReturnBucket(size)
	}
	if l.third != nil {
		l.third.ReturnBucket(size)
	}
	for _, current := range l.extra {
		current.ReturnBucket(size)
	}
}

type hierarchicalLimiterBuilder struct {
	first  *Rate
	second *Rate
	third  *Rate
	extra  []*Rate
	count  int
}

func (b *hierarchicalLimiterBuilder) add(current *Rate) {
	current = enabledLimiter(current)
	if current == nil {
		return
	}
	switch b.count {
	case 0:
		b.first = current
	case 1:
		b.second = current
	case 2:
		b.third = current
	default:
		b.extra = append(b.extra, current)
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

func enabledLimiter(current *Rate) *Rate {
	if current == nil || current.Limit() <= 0 {
		return nil
	}
	return current
}
