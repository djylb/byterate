package byterate

import (
	"context"
	"errors"
	"sync/atomic"
	"time"
)

// ErrBudgetExhausted is returned by a Budget that has no bytes left or whose
// grant has lapsed.
var ErrBudgetExhausted = errors.New("byterate: budget exhausted")

// Budget is a Limiter of a total number of bytes rather than a rate: each
// charge takes bytes from it, and once they run out it refuses further
// transfers with ErrBudgetExhausted, which ends the connection reads and
// writes charged to it. Grant adds bytes, optionally until a time after which
// the whole Budget lapses, and OnLow reports when it runs low, for example so
// that more can be requested in time. When and how to grant, and what to do
// with bytes left at a lapse, is up to the caller.
//
// A Budget never blocks: Wait takes the bytes or refuses them at once. Put it
// in a Hierarchy with Rates to limit the speed as well.
//
// All methods are safe for concurrent use and on a nil *Budget, which never
// refuses. The zero value has no bytes.
type Budget struct {
	left    atomic.Int64
	expires atomic.Int64 // nowNs after which the Budget lapses, 0 for never
	lowMark atomic.Int64
	onLow   atomic.Pointer[func(left int64)]
}

// NewBudget returns a Budget of n bytes that does not lapse.
func NewBudget(n int64) *Budget {
	b := &Budget{}
	b.left.Store(n)
	return b
}

// Grant adds n bytes to b and sets when b lapses: never for a zero expires.
// Bytes left at a lapse stay in b, and count again after a Grant that sets a
// later time.
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

// Left returns the bytes left in b, or 0 once b has lapsed. It is negative
// when Charge recorded more bytes than were left.
func (b *Budget) Left() int64 {
	if b == nil {
		return 0
	}
	if b.lapsed() {
		return 0
	}
	return b.left.Load()
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

// Wait takes n bytes from b, or refuses them with ErrBudgetExhausted, taking
// none, when fewer are left or b has lapsed. It never blocks.
func (b *Budget) Wait(_ context.Context, n int64) error {
	if b == nil || n <= 0 {
		return nil
	}
	if b.lapsed() {
		return ErrBudgetExhausted
	}
	for {
		left := b.left.Load()
		if left < n {
			return ErrBudgetExhausted
		}
		if b.left.CompareAndSwap(left, left-n) {
			b.checkLow(left, left-n)
			return nil
		}
	}
}

// Ready returns how many of n bytes b has left, or ErrBudgetExhausted when
// none are left or b has lapsed. It never blocks and charges nothing.
func (b *Budget) Ready(_ context.Context, n int64) (int64, error) {
	if b == nil || n <= 0 {
		return n, nil
	}
	if b.lapsed() {
		return 0, ErrBudgetExhausted
	}
	left := b.left.Load()
	if left <= 0 {
		return 0, ErrBudgetExhausted
	}
	return min(n, left), nil
}

// Charge takes n transferred bytes from b, also beyond what is left, and
// returns ErrBudgetExhausted when that overdrew b or b has lapsed.
func (b *Budget) Charge(n int64) error {
	if b == nil || n <= 0 {
		return nil
	}
	left := b.left.Add(-n)
	b.checkLow(left+n, left)
	if left < 0 || b.lapsed() {
		return ErrBudgetExhausted
	}
	return nil
}

// Refund gives back n bytes that were taken but not transferred.
func (b *Budget) Refund(n int64) {
	if b == nil || n <= 0 {
		return
	}
	b.left.Add(n)
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
