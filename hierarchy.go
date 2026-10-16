package byterate

import "context"

// Hierarchy is a Limiter of nested levels, such as a connection's Rate
// within its user's Rate and Budget within a global Rate. It charges every
// level and waits until every one allows the bytes, so the strictest sets the
// pace. Every enabled Rate also meters the traffic, whether or not it has a
// limit, so each level reports its own Throughput, and a limit set on any of
// them later applies to the next charge. Disable on a Rate ends the wait it
// imposes, and the others still apply; a disabled Rate is skipped until
// Enable.
//
// A level that refuses a charge, such as an exhausted Budget, refuses it for
// the Hierarchy: Wait then gives the bytes back to every level that took
// them. Charge, which records bytes already transferred, leaves them on every
// level and returns the refusal.
//
// Every level is charged when the bytes are, not when the strictest lets them
// pass, so a looser Rate counts bytes still waiting on a stricter one. Each
// keeps its long-run limit, but when many waits on stricter levels end
// together, a looser Rate they share can pass more than its burst at once:
// up to the bytes of those waits.
type Hierarchy struct {
	levels []level
}

// NewHierarchy combines limiters into one Limiter, the levels of a Hierarchy
// among them taking its place. Nil limiters, also nil pointers of this
// package's types, are left out, and so are repeats of a *Rate, *Budget or
// *Switch, so that a level given twice is charged once; unlimited Rates are
// kept, as they meter and can be limited later. It returns nil when no level
// remains and that level itself when one does.
//
// Pass nil for levels that should neither limit nor meter: an unlimited Rate
// still costs a charge per call.
func NewHierarchy(limiters ...Limiter) Limiter {
	var lv []level
	for _, l := range limiters {
		lv = addLevels(lv, l)
	}
	switch len(lv) {
	case 0:
		return nil
	case 1:
		if lv[0].rate != nil {
			return lv[0].rate
		}
		return lv[0].other
	}
	return &Hierarchy{levels: lv}
}

// addLevels appends the levels of l to lv, unless they are nil or already in
// it.
func addLevels(lv []level, l Limiter) []level {
	switch t := nilLimiter(l).(type) {
	case nil:
	case *Hierarchy:
		for _, inner := range t.levels {
			if inner.rate != nil {
				lv = addLevels(lv, inner.rate)
			} else {
				lv = addLevels(lv, inner.other)
			}
		}
	case *Rate:
		for _, x := range lv {
			if x.rate == t {
				return lv
			}
		}
		lv = append(lv, level{rate: t})
	default:
		// Only pointers known to compare are looked up: comparing other
		// Limiters could panic on an incomparable dynamic type.
		switch t.(type) {
		case *Budget, *Switch:
			for _, x := range lv {
				if x.other == t {
					return lv
				}
			}
		}
		lv = append(lv, level{other: t})
	}
	return lv
}

// Wait charges n bytes to every level and blocks until every one allows
// them, or until ctx is done, when it returns ctx.Err() and keeps the
// charges. A level that refuses them refuses them for l, and every level
// gets them back.
func (l *Hierarchy) Wait(ctx context.Context, n int64) error {
	if l == nil || n <= 0 {
		return nil
	}
	v := levels{h: l.levels}
	var receipt writeCharge
	_, err := v.wait(ctx, nil, nil, n, true, &receipt)
	return err
}

// Refund gives back n bytes to every level.
func (l *Hierarchy) Refund(n int64) {
	if l == nil || n <= 0 {
		return
	}
	for _, lv := range l.levels {
		if lv.rate != nil {
			lv.rate.Refund(n)
		} else {
			lv.other.Refund(n)
		}
	}
}

// Ready blocks until every level is ready for a transfer, or until ctx is
// done, and returns how many of n bytes it may take: the least that any level
// grants, at least 1. It charges nothing.
func (l *Hierarchy) Ready(ctx context.Context, n int64) (int64, error) {
	if l == nil || n <= 0 {
		return n, nil
	}
	v := levels{h: l.levels}
	return v.ready(ctx, nil, nil, n, false)
}

// Charge records n transferred bytes on every level without waiting, as debt
// that the next Ready waits for. Every level keeps them; it returns the first
// level's refusal, such as ErrBudgetExhausted.
func (l *Hierarchy) Charge(n int64) error {
	if l == nil || n <= 0 {
		return nil
	}
	v := levels{h: l.levels}
	return v.charge(n)
}

// levelWait is the wait one level of a hierarchical charge imposes: when it
// ends, and the generation charged, whose Disable ends it early.
type levelWait struct {
	end int64 // nowNs at which the level allows the bytes
	gen *generation
}

// waitSet holds the waits of the Rate levels of a charge that must wait at
// all, so that Disable on one level ends the wait that level imposes alone.
// Up to eight levels fit without allocating.
type waitSet struct {
	fixed [8]levelWait
	extra []levelWait
	n     int
}

// add records a level's wait of wait from now, which Disable on g ends early.
// A wait short enough to be coalesced into the debt is left out.
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

// sleep waits on t until every level in w allows the bytes. It sleeps until
// the level that ends last, which covers the others; if Disable on that
// level ends its wait, the others still apply. It returns ctx.Err() if ctx
// ends the wait first, and errSwitched if switchCh closes first.
func (w *waitSet) sleep(t *waitTimer, ctx context.Context, switchCh <-chan struct{}) error {
	var done <-chan struct{}
	if w.n > 0 {
		done = ctx.Done()
	}
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
			return nil
		}
		switch t.sleep(wait, lw.gen, done, switchCh) {
		case canceled:
			return ctx.Err()
		case switched:
			return errSwitched
		case slept:
			return nil // the wait elapsed, and the others with it
		case stopped:
			// Disable ended this level's wait alone; the others still apply.
			*w.at(last) = *w.at(w.n - 1)
			w.n--
		}
	}
	return nil
}
