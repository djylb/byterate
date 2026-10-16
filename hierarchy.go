package byterate

import (
	"context"
	"slices"
)

// Hierarchy is a Limiter of nested Rates, such as a connection's within its
// user's within a global one. It charges every Rate and waits until every one
// allows the bytes, so the strictest sets the pace. Every enabled Rate also
// meters the traffic, whether or not it has a limit, so each level reports
// its own Throughput, and a limit set on any of them later applies to the
// next charge. Disable on a Rate ends the wait it imposes, and the others
// still apply; a disabled Rate is skipped until Enable.
//
// Every Rate is charged when the bytes are, not when the strictest lets them
// pass, so a looser Rate counts bytes still waiting on a stricter one. Each
// keeps its long-run limit, but when many waits on stricter Rates end
// together, a looser Rate they share can pass more than its burst at once:
// up to the bytes of those waits.
type Hierarchy struct {
	first  *Rate
	second *Rate
	third  *Rate
	extra  []*Rate
}

// NewHierarchy combines rates into one Limiter. Nil rates are left out, and
// so are repeats, so that a Rate given twice is charged once; unlimited ones
// are kept, as they meter and can be limited later. It returns nil when no
// Rate remains and that *Rate itself when one does.
//
// Pass nil for levels that should neither limit nor meter: an unlimited Rate
// still costs a charge per call.
func NewHierarchy(rates ...*Rate) Limiter {
	b := hierarchyBuilder{}
	for _, r := range rates {
		b.add(r)
	}
	return b.build()
}

// Wait charges n bytes to every Rate and blocks until every one allows them,
// or until ctx is done, when it returns ctx.Err() and keeps the charges.
func (l *Hierarchy) Wait(ctx context.Context, n int64) error {
	var waits waitSet
	l.reserve(n, &waits)
	if waits.n > 0 && !waits.sleep(nil, ctx.Done()) {
		return ctx.Err()
	}
	return nil
}

// Refund gives back n bytes to every Rate.
func (l *Hierarchy) Refund(n int64) {
	if l == nil || n <= 0 {
		return
	}
	for i := range l.len() {
		l.rate(i).Refund(n)
	}
}

// reserve charges size to every Rate at one point in time, records in waits,
// unless it is nil, the levels that must wait, and returns the longest wait.
func (l *Hierarchy) reserve(size int64, waits *waitSet) int64 {
	if l == nil || size <= 0 {
		return 0
	}
	now := nowNs()
	var longest int64
	for i := range l.len() {
		wait, g := l.rate(i).reserveAt(size, now)
		waits.add(wait, now, g)
		longest = max(longest, wait)
	}
	return longest
}

// len returns the number of l's Rates.
func (l *Hierarchy) len() int {
	if l.third == nil {
		return 2
	}
	return 3 + len(l.extra)
}

// rate returns l's i-th Rate.
func (l *Hierarchy) rate(i int) *Rate {
	switch i {
	case 0:
		return l.first
	case 1:
		return l.second
	case 2:
		return l.third
	}
	return l.extra[i-3]
}

// allowance returns how much of size every Rate of l lets through at now in
// one charge: all of it when every Rate's credit covers it, and otherwise
// the least of what each Rate grants, its credit or its quantum.
func (l *Hierarchy) allowance(size, now int64) int64 {
	n := l.len()
	i := 0
	for ; i < n; i++ {
		creditNs, limit := l.rate(i).credit(now)
		if !covers(size, creditNs, limit) {
			break
		}
	}
	if i == n {
		return size
	}
	for i = 0; i < n; i++ {
		size = min(size, l.rate(i).allowance(size, now))
	}
	return size
}

// quantum returns the least quantum of l's Rates, or maxI64 if none limits.
func (l *Hierarchy) quantum() int64 {
	q := maxI64
	for i := range l.len() {
		q = min(q, l.rate(i).quantum())
	}
	return q
}

// debts records in waits the Rates of l whose debt at now is worth a wait.
func (l *Hierarchy) debts(now int64, waits *waitSet) {
	for i := range l.len() {
		if end, g := l.rate(i).debtEnd(now); end != 0 {
			waits.add(subSat(end, now), now, g)
		}
	}
}

// levelWait is the wait one level of a hierarchical charge imposes: when it
// ends, and the generation charged, whose Disable ends it early.
type levelWait struct {
	end int64 // nowNs at which the level allows the bytes
	gen *generation
}

// waitSet holds the waits of the levels of a hierarchical charge that must
// wait at all, so that Disable on one level ends the wait that level imposes
// alone. Up to eight levels fit without allocating.
type waitSet struct {
	fixed [8]levelWait
	extra []levelWait
	n     int
}

// add records a level's wait of wait from now, which Disable on g ends early. A
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
// last, which covers the others; if Disable on that level ends its wait, the
// others still apply.
func (w *waitSet) sleep(t *waitTimer, done <-chan struct{}) bool {
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
		switch t.sleep(wait, lw.gen, done) {
		case canceled:
			return false
		case slept:
			return true // the wait elapsed, and the others with it
		case stopped:
			// Disable ended this level's wait alone; the others still apply.
			*w.at(last) = *w.at(w.n - 1)
			w.n--
		}
	}
	return true
}

type hierarchyBuilder struct {
	first  *Rate
	second *Rate
	third  *Rate
	extra  []*Rate
	count  int
}

func (b *hierarchyBuilder) add(r *Rate) {
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

func (b *hierarchyBuilder) build() Limiter {
	switch b.count {
	case 0:
		return nil
	case 1:
		return b.first
	default:
		return &Hierarchy{
			first:  b.first,
			second: b.second,
			third:  b.third,
			extra:  b.extra,
		}
	}
}
