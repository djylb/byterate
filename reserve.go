package byterate

import (
	"context"
	"errors"
	"time"
)

// Reserver is a Limiter that also reserves a range of bytes ahead, for a
// forwarder or a batched writer that learns how much it sends only after
// it has reserved room for it. Every Limiter of this package implements
// it: *Rate, *Budget, *Switch, the result of NewHierarchy and the Reads and
// Writes of a Group, which return it as a Reserver.
//
// Acquire and TryAcquire take as much of the range as the limit allows at
// once, at least minBytes, on the same charges as Wait and TryWait, and
// Refund gives back what was not sent. Grants never add up to more than the
// limit allows, however many callers share it, and a hierarchy takes them on
// every level or on none. A maxBytes of 0 or less takes nothing; a minBytes
// below 1 counts as 1 and one above maxBytes as maxBytes.
type Reserver interface {
	Limiter
	// Acquire blocks until the limit lets at least minBytes through, takes
	// between minBytes and maxBytes, as many as it lets through at once,
	// and returns how many it took. Acquire(ctx, n, n) waits for all n
	// bytes, as Wait does, and a minBytes above a Rate's burst is granted
	// once the debt beyond the burst is repaid. Unlike Wait, it keeps
	// nothing on error: ctx.Err() when ctx ends first, or the error of a
	// limit that refuses the bytes, such as ErrBudgetExhausted.
	Acquire(ctx context.Context, minBytes, maxBytes int64) (int64, error)
	// TryAcquire takes between minBytes and maxBytes, as many as the limit
	// lets through at once, if that is at least minBytes, and returns how
	// many it took. Otherwise it takes nothing and returns ErrWouldWait, or
	// first the error of a limit that refuses the bytes. TryAcquire(n, n)
	// takes all n or none, as TryWait does; TryAcquire(1, n) takes what it
	// can of n. A limited Rate never lets through more than its burst at
	// once.
	TryAcquire(minBytes, maxBytes int64) (int64, error)
}

var (
	_ Reserver = (*Rate)(nil)
	_ Reserver = (*hierarchy)(nil)
	_ Reserver = (*Budget)(nil)
	_ Reserver = (*Switch)(nil)
	_ Reserver = (*groupReads)(nil)
	_ Reserver = (*groupWrites)(nil)
	_ Reserver = (*counterReads)(nil)
	_ Reserver = (*counterWrites)(nil)
)

// reserveOn is what Acquire and TryAcquire charge: a Rate alone, a Budget
// alone, the level one[0], which a Switch's changes expand anew, or the
// fixed levels v. one must not be on the caller's stack, so that it costs
// no allocation.
type reserveOn struct {
	rate   *Rate
	budget *Budget
	one    []level
	v      levels
}

// wait charges size bytes, or unless whole the grant of them, records the
// charges in rc and waits, as levels.wait does. It returns the bytes
// charged; on error rc holds what is still charged.
func (o *reserveOn) wait(ctx context.Context, size int64, whole bool, rc *receipt) (int64, error) {
	switch {
	case o.rate != nil:
		return o.rate.grant(ctx, nil, size, whole, rc, nil)
	case o.budget != nil:
		c := cutNone
		if !whole {
			n, err := o.budget.ready(ctx, nil, size, nil)
			if err != nil {
				return 0, err
			}
			size, c = n, cutBudget
		}
		return o.budget.wait(ctx, nil, size, c, rc, nil)
	case o.one != nil:
		return waitOn(ctx, o.one, nil, size, cutOf(whole), rc)
	default:
		return o.v.wait(ctx, nil, size, cutOf(whole), rc)
	}
}

// credit returns how much of size the limit lets through at now without a
// wait, as far as it can tell without charging.
func (o *reserveOn) credit(size, now int64) int64 {
	switch {
	case o.rate != nil:
		return o.rate.creditBytes(size, now)
	case o.budget != nil:
		return o.budget.credit(size, now)
	case o.one != nil:
		v := expand(o.one)
		return v.credit(size, now)
	default:
		return o.v.credit(size, now)
	}
}

// tryWait charges size bytes at now if none of the limit's levels makes the
// caller wait or refuses them, as levels.tryWait does.
func (o *reserveOn) tryWait(size, now int64) error {
	var rc receipt
	switch {
	case o.rate != nil:
		if !o.rate.tryTake(size, now, nil) {
			return ErrWouldWait
		}
		return nil
	case o.budget != nil:
		return o.budget.tryWait(size, now, &rc)
	case o.one != nil:
		return tryOn(o.one, size, now, &rc)
	default:
		return o.v.tryWait(size, now, &rc)
	}
}

// bounds returns minBytes and maxBytes as Acquire and TryAcquire take them: maxBytes, if
// positive, and minBytes within [1, maxBytes]. ok is false for a maxBytes that takes
// nothing.
func bounds(minN, maxN int64) (lo, hi int64, ok bool) {
	if maxN <= 0 {
		return 0, 0, false
	}
	return min(max(minN, 1), maxN), maxN, true
}

// acquire is Acquire. A whole request charges its bytes at once. Otherwise
// it charges a grant of up to maxN as a cut charge does, then, when that
// grant is below minN, the rest as one whole charge. Every charge is
// recorded, so an error takes all of them back.
func (o *reserveOn) acquire(ctx context.Context, minN, maxN int64) (int64, error) {
	lo, hi, ok := bounds(minN, maxN)
	if !ok {
		return 0, nil
	}
	var rc receipt
	n, err := o.wait(ctx, hi, lo == hi, &rc)
	if err != nil {
		rc.refund(n)
		return 0, err
	}
	if n < lo {
		m := rc.mark()
		rest := lo - n
		if _, err := o.wait(ctx, rest, true, &rc); err != nil {
			rc.refundFrom(m, rest)
			rc.refund(n)
			return 0, err
		}
		n = lo
	}
	return n, nil
}

// tryAcquire is TryAcquire: it cuts maxN to what every level lets through
// at once, no less than minN, and charges that, all or nothing. When the
// cut was too optimistic, as for a level it cannot ask without charging, it
// tries minN once more.
func (o *reserveOn) tryAcquire(minN, maxN int64) (int64, error) {
	lo, hi, ok := bounds(minN, maxN)
	if !ok {
		return 0, nil
	}
	now := nowNs()
	size := max(o.credit(hi, now), lo)
	err := o.tryWait(size, now)
	if errors.Is(err, ErrWouldWait) && size > lo {
		size = lo
		err = o.tryWait(size, now)
	}
	if err != nil {
		return 0, err
	}
	return size, nil
}

// credit returns how much of size the levels let through at now without a
// wait, as far as they can tell without charging: each Rate its credit, each
// Budget up to its Stop and its step's Limit its credit, recursively. A
// charge that loses the credit to another charger meanwhile still waits, so
// tryWait has the last word.
func (v *levels) credit(size, now int64) int64 {
	for i := range v.lv {
		if size <= 0 {
			return 0
		}
		lv := &v.lv[i]
		switch lv.kind {
		case kindRate:
			size = lv.rate.creditBytes(size, now)
		case kindBudget:
			size = lv.l.(*Budget).credit(size, now)
		case kindSwitch, kindHierarchy, kindGroupReads, kindGroupWrites:
			inner := expand(v.lv[i : i+1])
			size = inner.credit(size, now)
		case kindNone, kindOther, kindCountReads, kindCountWrites:
			// Asked by tryWait alone, or never limiting.
		}
	}
	return max(size, 0)
}

// credit returns how much of size b lets through at now without a wait: up
// to its Stop, and as much as the Limit of its step lets through.
func (b *Budget) credit(size, now int64) int64 {
	if b == nil {
		return size
	}
	size = min(size, b.Remaining())
	if _, s := b.stepSet().at(b.Used()); s != nil && s.Limit != nil && size > 0 {
		if s.t.fixed.lv != nil {
			return s.t.fixed.credit(size, now)
		}
		v := expand(s.t.one[:])
		return v.credit(size, now)
	}
	return size
}

// creditBytes returns how much of size r lets through at now without a
// wait: all of it, or its credit in bytes.
func (r *Rate) creditBytes(size, now int64) int64 {
	creditNs, limit := r.credit(now)
	if limit > 0 && !covers(size, creditNs, limit) {
		return mulDiv(creditNs, limit, int64(time.Second))
	}
	return size
}

// Acquire blocks until r lets at least minBytes bytes through and takes between
// minBytes and maxBytes of them, as Reserver describes: r's credit, or a quantum of
// about 20 ms of traffic when it has too little, but never less than minBytes.
// When ctx ends first it returns ctx.Err() and keeps nothing; Disable and
// Reset end the wait early. A nil, disabled or unlimited r grants maxBytes at
// once.
func (r *Rate) Acquire(ctx context.Context, minBytes, maxBytes int64) (int64, error) {
	if r == nil {
		return (&reserveOn{}).acquire(ctx, minBytes, maxBytes)
	}
	return (&reserveOn{rate: r}).acquire(ctx, minBytes, maxBytes)
}

// TryAcquire takes between minBytes and maxBytes bytes of r's credit if it has at
// least minBytes, as Reserver describes, and otherwise returns ErrWouldWait. A
// limited r never lets through more than its burst at once, so set the
// burst to at least the largest datagram.
func (r *Rate) TryAcquire(minBytes, maxBytes int64) (int64, error) {
	if r == nil {
		return (&reserveOn{}).tryAcquire(minBytes, maxBytes)
	}
	return (&reserveOn{rate: r}).tryAcquire(minBytes, maxBytes)
}

// Acquire takes between minBytes and maxBytes bytes from b and from the Limit of the
// step that applies, up to the next step, and waits for that Limit. It
// refuses with ErrBudgetExhausted, taking nothing, when min bytes would
// take the usage past a step with Stop, or after the expiry.
func (b *Budget) Acquire(ctx context.Context, minBytes, maxBytes int64) (int64, error) {
	if b == nil {
		return (&reserveOn{}).acquire(ctx, minBytes, maxBytes)
	}
	return (&reserveOn{budget: b}).acquire(ctx, minBytes, maxBytes)
}

// TryAcquire takes between minBytes and maxBytes bytes from b and the Limit of the
// step that applies, as many as they let through at once, or refuses them
// as Acquire does, or returns ErrWouldWait.
func (b *Budget) TryAcquire(minBytes, maxBytes int64) (int64, error) {
	if b == nil {
		return (&reserveOn{}).tryAcquire(minBytes, maxBytes)
	}
	return (&reserveOn{budget: b}).tryAcquire(minBytes, maxBytes)
}

// Acquire takes between minBytes and maxBytes bytes from the current Limiter, as its
// Acquire does; if Set replaces it meanwhile, the charge moves to the new
// one.
func (s *Switch) Acquire(ctx context.Context, minBytes, maxBytes int64) (int64, error) {
	return (&reserveOn{one: s.state().self[:]}).acquire(ctx, minBytes, maxBytes)
}

// TryAcquire takes between minBytes and maxBytes bytes from the current Limiter, as
// its TryAcquire does.
func (s *Switch) TryAcquire(minBytes, maxBytes int64) (int64, error) {
	return (&reserveOn{one: s.state().self[:]}).tryAcquire(minBytes, maxBytes)
}

func (h *hierarchy) Acquire(ctx context.Context, minBytes, maxBytes int64) (int64, error) {
	return (&reserveOn{v: h.levels()}).acquire(ctx, minBytes, maxBytes)
}

func (h *hierarchy) TryAcquire(minBytes, maxBytes int64) (int64, error) {
	return (&reserveOn{v: h.levels()}).tryAcquire(minBytes, maxBytes)
}

func (l *groupReads) Acquire(ctx context.Context, minBytes, maxBytes int64) (int64, error) {
	return (&reserveOn{v: l.hier().levels()}).acquire(ctx, minBytes, maxBytes)
}

func (l *groupReads) TryAcquire(minBytes, maxBytes int64) (int64, error) {
	return (&reserveOn{v: l.hier().levels()}).tryAcquire(minBytes, maxBytes)
}

func (l *groupWrites) Acquire(ctx context.Context, minBytes, maxBytes int64) (int64, error) {
	return (&reserveOn{v: l.hier().levels()}).acquire(ctx, minBytes, maxBytes)
}

func (l *groupWrites) TryAcquire(minBytes, maxBytes int64) (int64, error) {
	return (&reserveOn{v: l.hier().levels()}).tryAcquire(minBytes, maxBytes)
}

func (c *counterReads) Acquire(_ context.Context, _, maxBytes int64) (int64, error) {
	return c.TryAcquire(1, maxBytes)
}

func (c *counterReads) TryAcquire(_, maxBytes int64) (int64, error) {
	maxBytes = max(maxBytes, 0)
	_ = c.Charge(maxBytes)
	return maxBytes, nil
}

func (c *counterWrites) Acquire(_ context.Context, _, maxBytes int64) (int64, error) {
	return c.TryAcquire(1, maxBytes)
}

func (c *counterWrites) TryAcquire(_, maxBytes int64) (int64, error) {
	maxBytes = max(maxBytes, 0)
	_ = c.Charge(maxBytes)
	return maxBytes, nil
}
