package byterate

import (
	"context"
	"errors"
	"sync/atomic"
)

// Switch is a Limiter that passes every charge on to another Limiter, which
// Set can replace at any time, such as when a connection's traffic moves to
// another account. Charges made after Set go to the new Limiter. A wait in
// progress on a Rate of the old one ends at once and starts over on the new
// one, its charge refunded to the old, so that a connection limited through
// a Switch follows the new Limiter for the bytes it has not yet sent or
// received.
//
// The connection wrappers and a Hierarchy that holds the Switch refund a
// canceled or failed charge to the Limiter that took it. Refund called
// directly goes to the current Limiter, so call it before Set for bytes
// charged to the old one.
//
// All methods are safe for concurrent use and on a nil *Switch, which never
// limits. The zero value passes everything until Set.
type Switch struct {
	st atomic.Pointer[switchState]
}

// switchState is one Limiter of a Switch and the channel that Set closes when
// it replaces it.
type switchState struct {
	l       Limiter
	changed chan struct{}
}

// noSwitchState is the state of a zero Switch: no Limiter, never changed by
// anything but a Set that replaces it.
var noSwitchState = &switchState{}

// NewSwitch returns a Switch that passes charges on to l; a nil l limits
// nothing.
func NewSwitch(l Limiter) *Switch {
	s := &Switch{}
	s.st.Store(&switchState{l: nilLimiter(l), changed: make(chan struct{})})
	return s
}

// Set makes s pass charges on to l from now on, and ends the waits in
// progress on the Rates of the old Limiter, which start over on l; a wait
// inside another kind of Limiter runs to its end there. l must not be or
// contain s.
func (s *Switch) Set(l Limiter) {
	if s == nil {
		return
	}
	if l == Limiter(s) {
		panic("byterate: Switch set to itself")
	}
	if old := s.st.Swap(&switchState{l: nilLimiter(l), changed: make(chan struct{})}); old != nil {
		close(old.changed)
	}
}

// Limiter returns the Limiter s passes charges on to.
func (s *Switch) Limiter() Limiter {
	return s.state().l
}

func (s *Switch) state() *switchState {
	if s != nil {
		if st := s.st.Load(); st != nil {
			return st
		}
	}
	return noSwitchState
}

// Wait charges n bytes to the current Limiter and waits for it; if Set
// replaces it meanwhile, the charge moves to the new one.
func (s *Switch) Wait(ctx context.Context, n int64) error {
	_, err := s.wait(ctx, nil, n)
	return err
}

// wait is Wait in timer t, nil for one per wait. It returns the Limiter that
// kept the charge, which a refund must reach, or nil if none did.
func (s *Switch) wait(ctx context.Context, t *waitTimer, n int64) (Limiter, error) {
	if n <= 0 {
		return nil, nil
	}
	for {
		st := s.state()
		v := levelsOf(st.l)
		var receipt writeCharge
		_, err := v.wait(ctx, t, st.changed, n, true, &receipt)
		if errors.Is(err, errSwitched) {
			receipt.refund(n)
			continue
		}
		if err != nil && (ctx.Err() == nil || !errors.Is(err, ctx.Err())) {
			return nil, err // refused, so nothing kept the charge
		}
		return st.l, err
	}
}

// Ready waits until the current Limiter is ready, as its Ready does, and
// starts over on a new one if Set replaces it meanwhile.
func (s *Switch) Ready(ctx context.Context, n int64) (int64, error) {
	return s.ready(ctx, nil, n)
}

// ready is Ready in timer t, nil for one per wait.
func (s *Switch) ready(ctx context.Context, t *waitTimer, n int64) (int64, error) {
	if n <= 0 {
		return n, nil
	}
	for {
		st := s.state()
		v := levelsOf(st.l)
		granted, err := v.ready(ctx, t, st.changed, n, false)
		if errors.Is(err, errSwitched) {
			continue
		}
		return granted, err
	}
}

// Charge records n transferred bytes on the current Limiter.
func (s *Switch) Charge(n int64) error {
	v := levelsOf(s.state().l)
	return v.charge(n)
}

// Refund gives back n bytes to the current Limiter.
func (s *Switch) Refund(n int64) {
	if l := s.state().l; l != nil {
		l.Refund(n)
	}
}
