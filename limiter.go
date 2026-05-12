package byterate

type Limiter interface {
	Get(int64)
	ReturnBucket(int64)
}

type HierarchicalLimiter struct {
	first  *Rate
	second *Rate
	third  *Rate
	extra  []*Rate
	count  int
}

func NewHierarchicalLimiter(limiters ...*Rate) Limiter {
	switch len(limiters) {
	case 0:
		return nil
	case 1:
		return enabledLimiter(limiters[0])
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

func NewHierarchicalLimiter2(first, second *Rate) Limiter {
	builder := hierarchicalLimiterBuilder{}
	builder.add(first)
	builder.add(second)
	return builder.build()
}

func NewHierarchicalLimiter3(first, second, third *Rate) Limiter {
	builder := hierarchicalLimiterBuilder{}
	builder.add(first)
	builder.add(second)
	builder.add(third)
	return builder.build()
}

func (l *HierarchicalLimiter) Get(size int64) {
	if l == nil || size <= 0 {
		return
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
	if maxWait > coalesceWaitNs {
		sleepNs(maxWait, maxStopCh)
	}
}

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
			count:  b.count,
		}
	}
}

func enabledLimiter(current *Rate) *Rate {
	if current == nil || current.Limit() <= 0 {
		return nil
	}
	return current
}
