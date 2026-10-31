package byterate

import "time"

// Clock is the time source of the parts of this package that read time off
// the charge path: the throughput of a Counter and the expiry of a Budget.
// Any type with a Now method fits, such as a timeshim.Clock or its Fake, so
// that tests can step time by hand.
//
// A Rate keeps the monotonic clock, which a testing/synctest bubble fakes.
type Clock interface {
	Now() time.Time
}

// clockBox holds a Clock in an atomic.Pointer.
type clockBox struct{ c Clock }

// SetClock makes b read the time of its expiry from c; nil goes back to the
// monotonic clock. Set it before SetExpiry.
func (b *Budget) SetClock(c Clock) {
	if b == nil {
		return
	}
	if c == nil {
		b.clock.Store(nil)
		return
	}
	b.clock.Store(&clockBox{c: c})
}
