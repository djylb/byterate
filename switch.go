package byterate

import (
	"context"
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
// The connection wrappers and a hierarchy that holds the Switch refund a
// canceled or failed charge to the Limiters that took it, Rates only within
// the generation they charged. Refund called directly goes to the current
// Limiter, so call it before Set for bytes charged to the old one.
//
// All methods are safe for concurrent use and on a nil *Switch, which never
// limits. The zero value passes everything until Set.
type Switch struct {
	st atomic.Pointer[switchState]
}

// switchState is one Limiter of a Switch, the channel that Set closes when
// it replaces it, and the Switch itself as a level, for its own waits.
type switchState struct {
	t       target
	self    [1]level
	changed chan struct{}
}

// noSwitchState is the state of a zero Switch: no Limiter, never changed by
// anything but a Set that replaces it.
var noSwitchState = &switchState{}

// NewSwitch returns a Switch that passes charges on to l; a nil l limits
// nothing.
func NewSwitch(l Limiter) *Switch {
	s := &Switch{}
	s.st.Store(s.newState(l))
	return s
}

func (s *Switch) newState(l Limiter) *switchState {
	st := &switchState{self: [1]level{{l: s, kind: kindSwitch}}, changed: make(chan struct{})}
	st.t.init(l)
	return st
}

// Set makes s pass charges on to l from now on, and ends the waits in
// progress on the Rates of the old Limiter, which start over on l; a wait
// inside another kind of Limiter runs to its end there. It panics if l
// passes charges on to s, which would charge s without end.
func (s *Switch) Set(l Limiter) {
	if s == nil {
		return
	}
	configMu.Lock()
	defer configMu.Unlock()
	if reaches(l, func(x Limiter) bool { return x == Limiter(s) }) {
		panic("byterate: Switch set to a Limiter that charges the Switch")
	}
	if old := s.st.Swap(s.newState(l)); old != nil {
		close(old.changed)
	}
}

// Limiter returns the Limiter s passes charges on to.
func (s *Switch) Limiter() Limiter {
	return s.state().t.limiter()
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
	st := s.state()
	if st == noSwitchState || n <= 0 {
		return nil // no Limiter, so nothing to wait for
	}
	var rc receipt
	_, err := waitOn(ctx, st.self[:], nil, n, true, &rc)
	return err
}

// TryWait charges n bytes to the current Limiter if it lets them through
// without waiting, as its TryWait does.
func (s *Switch) TryWait(n int64) error {
	st := s.state()
	if st == noSwitchState || n <= 0 {
		return nil
	}
	var rc receipt
	return st.t.tryWait(n, nowNs(), &rc)
}

// Ready waits until the current Limiter is ready, as its Ready does, and
// starts over on a new one if Set replaces it meanwhile.
func (s *Switch) Ready(ctx context.Context, n int64) (int64, error) {
	st := s.state()
	if st == noSwitchState || n <= 0 {
		return n, nil
	}
	return readyOn(ctx, st.self[:], nil, n, false)
}

// Charge records n transferred bytes on the current Limiter.
func (s *Switch) Charge(n int64) error {
	if n <= 0 {
		return nil
	}
	return s.state().t.charge(n)
}

// Refund gives back n bytes to the current Limiter.
func (s *Switch) Refund(n int64) {
	if l := s.Limiter(); l != nil {
		l.Refund(n)
	}
}
