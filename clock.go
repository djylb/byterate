package byterate

import "time"

// Clock is the time source of the parts of this package that read time off
// the charge path: the throughput of a Counter and the expiry of a Budget.
// Any type with a Now method fits, such as a fake clock that tests step by
// hand.
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
	var box *clockBox
	if c != nil {
		box = &clockBox{c: c}
	}
	b.lessees.mu.Lock()
	defer b.lessees.mu.Unlock()
	b.clock.Store(box)
	b.scheduleLocked()
	b.revokeLocked()
}
