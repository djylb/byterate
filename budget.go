package byterate

import (
	"context"
	"errors"
	"sync/atomic"
	"time"
)

// ErrBudgetExhausted is returned by a Budget that has no bytes left or whose
// grant has lapsed, unless a fallback Limiter takes over.
var ErrBudgetExhausted = errors.New("byterate: budget exhausted")

// Budget is a Limiter of a total number of bytes rather than a rate: each
// charge takes bytes from it. Once they run out, or its grant lapses, it is
// spent, and by default refuses further transfers with ErrBudgetExhausted,
// which ends the connection reads and writes charged to it. With a fallback
// set by SetFallback, a spent Budget passes the bytes on to that Limiter
// instead, such as a slow Rate, so that traffic goes on at its pace.
//
// A spent Budget still takes the bytes it lets through, so Left goes below
// zero by the bytes used beyond it, and a later Grant pays them back first.
// The fallback is charged for those bytes as well, to set their pace; it
// does not pay for them instead of the Budget. The choice between refusing
// and slowing down does not change what the Budget, or a Counter beside it,
// counts.
//
// Grant adds bytes, optionally until a time after which the whole Budget
// lapses, and OnLow reports when it runs low, for example so that more can
// be requested in time. When and how to grant, and what to do with bytes
// left at a lapse, is up to the caller.
//
// A Budget without a fallback never blocks: Wait takes the bytes or refuses
// them at once. Put it in a Hierarchy with Rates to limit the speed as well.
//
// All methods are safe for concurrent use and on a nil *Budget, which never
// refuses. The zero value has no bytes.
type Budget struct {
	left     atomic.Int64
	expires  atomic.Int64 // nowNs after which the Budget lapses, 0 for never
	lowMark  atomic.Int64
	onLow    atomic.Pointer[func(left int64)]
	fallback atomic.Pointer[fallbackLimiter]
}

// fallbackLimiter holds the Limiter of a spent Budget and the bytes charged
// to it and not refunded, which no refund to it may exceed.
type fallbackLimiter struct {
	l    Limiter
	owed atomic.Int64
}

// NewBudget returns a Budget of n bytes that does not lapse.
func NewBudget(n int64) *Budget {
	b := &Budget{}
	b.left.Store(n)
	return b
}

// Grant adds n bytes to b and sets when b lapses: never for a zero expires.
// Bytes left at a lapse stay in b and count again after a Grant that sets a
// later time, less what a fallback let through meanwhile.
func (b *Budget) Grant(n int64, expires time.Time) {
	if b == nil {
		return
	}
	var at int64
	if !expires.IsZero() {
		// On the monotonic clock, measured from now; 0 means never.
		if at = clampAdd(nowNs(), int64(time.Until(expires))); at == 0 {
			at = -1
		}
	}
	b.expires.Store(at)
	b.left.Add(n)
}

// SetFallback makes a spent b pass the bytes charged to it on to l, which
// then sets their pace, instead of refusing them; a nil l restores the
// refusal. Meant for pacing, l is charged for those bytes in addition to b.
// Refunds of bytes charged to an earlier fallback do not reach it. l must not
// be, contain or fall back to b.
func (b *Budget) SetFallback(l Limiter) {
	if b == nil {
		return
	}
	if l = nilLimiter(l); l == nil {
		b.fallback.Store(nil)
		return
	}
	if l == Limiter(b) {
		panic("byterate: Budget falls back to itself")
	}
	b.fallback.Store(&fallbackLimiter{l: l})
}

// Left returns the bytes left in b, at most 0 once b has lapsed. It is
// negative by the bytes used beyond b, which a spent b let through to its
// fallback or Charge recorded.
func (b *Budget) Left() int64 {
	if b == nil {
		return 0
	}
	left := b.left.Load()
	if b.lapsed() {
		return min(left, 0)
	}
	return left
}

// OnLow makes b call f with the bytes left whenever a charge takes it from
// mark bytes or more to fewer: once as it runs low, and again after a Grant
// or Refund has lifted it back to mark. f runs on the goroutine of that
// charge, so it must return quickly. A nil f stops the reports.
func (b *Budget) OnLow(mark int64, f func(left int64)) {
	if b == nil {
		return
	}
	b.lowMark.Store(mark)
	if f == nil {
		b.onLow.Store(nil)
		return
	}
	b.onLow.Store(&f)
}

// Wait takes n bytes from b. When fewer are left, or b has lapsed, it refuses
// them with ErrBudgetExhausted, taking none, or with a fallback takes them
// anyway and waits for the fallback for the bytes beyond what was left.
func (b *Budget) Wait(ctx context.Context, n int64) error {
	return b.wait(ctx, nil, n)
}

// wait is Wait in timer t, nil for one per wait.
func (b *Budget) wait(ctx context.Context, t *waitTimer, n int64) error {
	if b == nil || n <= 0 {
		return nil
	}
	lapsed := b.lapsed()
	if !lapsed {
		for {
			left := b.left.Load()
			if left < n {
				break
			}
			if b.left.CompareAndSwap(left, left-n) {
				b.checkLow(left, left-n)
				return nil
			}
		}
	}
	fb := b.fallback.Load()
	if fb == nil {
		return ErrBudgetExhausted
	}
	// The fallback first, so that one refusing leaves b as it was.
	beyond := b.beyond(n, b.left.Load(), lapsed)
	err := fb.wait(ctx, t, beyond)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr == nil || !errors.Is(err, ctxErr) {
			return err // refused, so neither b nor the fallback kept a charge
		}
	}
	left := b.left.Add(-n)
	b.checkLow(left+n, left)
	// Charges since the first look moved the bytes beyond b: settle the
	// difference with the fallback, which a refusal here cannot undo.
	switch d := b.beyond(n, left+n, lapsed) - beyond; {
	case d > 0:
		fb.owed.Add(d)
		_ = fb.l.Charge(d)
	case d < 0:
		fb.refund(-d)
	}
	return err
}

// Ready returns how many of n bytes b has left. A spent b returns
// ErrBudgetExhausted, or with a fallback what the fallback's Ready returns.
// It charges nothing.
func (b *Budget) Ready(ctx context.Context, n int64) (int64, error) {
	return b.ready(ctx, nil, n)
}

// ready is Ready in timer t, nil for one per wait.
func (b *Budget) ready(ctx context.Context, t *waitTimer, n int64) (int64, error) {
	if b == nil || n <= 0 {
		return n, nil
	}
	if !b.lapsed() {
		if left := b.left.Load(); left > 0 {
			return min(n, left), nil
		}
	}
	if fb := b.fallback.Load(); fb != nil {
		v := levelsOf(fb.l)
		return v.ready(ctx, t, nil, n, false)
	}
	return 0, ErrBudgetExhausted
}

// Charge takes n transferred bytes from b, also beyond what is left. When
// that spent b, it returns ErrBudgetExhausted, or with a fallback charges the
// fallback for the bytes beyond what was left and returns its error.
func (b *Budget) Charge(n int64) error {
	if b == nil || n <= 0 {
		return nil
	}
	left := b.left.Add(-n)
	b.checkLow(left+n, left)
	lapsed := b.lapsed()
	if left >= 0 && !lapsed {
		return nil
	}
	fb := b.fallback.Load()
	if fb == nil {
		return ErrBudgetExhausted
	}
	beyond := b.beyond(n, left+n, lapsed)
	fb.owed.Add(beyond)
	return fb.l.Charge(beyond)
}

// Refund gives back n bytes that were taken but not transferred. The part
// that lifts b back from below zero goes back to the fallback as well, up to
// what the fallback took and has not had back, so a lapse, Grant or
// SetFallback between a charge and its refund can leave the fallback with
// some of it, never with less than it took.
func (b *Budget) Refund(n int64) {
	if b == nil || n <= 0 {
		return
	}
	was := b.left.Add(n) - n
	if fb := b.fallback.Load(); fb != nil {
		fb.refund(min(n, max(-was, 0)))
	}
}

// wait charges the fallback n bytes in timer t and waits for it, keeping
// count of what it took.
func (fb *fallbackLimiter) wait(ctx context.Context, t *waitTimer, n int64) error {
	if n <= 0 {
		return nil
	}
	v := levelsOf(fb.l)
	var receipt writeCharge
	_, err := v.wait(ctx, t, nil, n, true, &receipt)
	if err == nil || ctx.Err() != nil && errors.Is(err, ctx.Err()) {
		fb.owed.Add(n) // taken, as a wait that ctx ended keeps its charge
	}
	return err
}

// refund gives back up to n bytes to the fallback, no more than it is owed.
func (fb *fallbackLimiter) refund(n int64) {
	for n > 0 {
		owed := fb.owed.Load()
		back := min(n, owed)
		if back <= 0 {
			return
		}
		if fb.owed.CompareAndSwap(owed, owed-back) {
			fb.l.Refund(back)
			return
		}
	}
}

// beyond returns how many of n bytes charged when b had was left go beyond
// it, to the fallback: all of them while b has lapsed.
func (b *Budget) beyond(n, was int64, lapsed bool) int64 {
	if lapsed {
		return n
	}
	return n - min(max(was, 0), n)
}

// lapsed reports whether b's grant has run out of time.
func (b *Budget) lapsed() bool {
	at := b.expires.Load()
	return at != 0 && nowNs() >= at
}

// checkLow reports to the OnLow callback when a charge took b from was to
// left bytes across the mark. Each charge sees its own change of the count,
// so only the one that crosses the mark reports it.
func (b *Budget) checkLow(was, left int64) {
	f := b.onLow.Load()
	if f == nil {
		return
	}
	if mark := b.lowMark.Load(); was >= mark && left < mark {
		(*f)(left)
	}
}
