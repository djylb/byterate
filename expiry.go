package byterate

// expiryFunc is a func that AfterExpiry registered, until it runs or is
// stopped.
type expiryFunc struct{ f func() }

// AfterExpiry arranges for f to run in its own goroutine once b's expiry
// passes, such as to close connections that sit idle and so never meet the
// refusal of their next read or write. An expiry that SetExpiry moves or
// clears before it passes keeps f waiting for the next one; when b has
// already expired, f runs at once. f runs at most once. The timer of the
// expiry runs it; with a Clock set, which no timer follows, the first
// charge, Remaining or Lease that finds the expiry passed does. stop
// unregisters f and reports whether it did so before f was started. A nil
// b never expires, so f never runs.
func (b *Budget) AfterExpiry(f func()) (stop func() bool) {
	if b == nil {
		return func() bool { return false }
	}
	e := &expiryFunc{f: f}
	b.lessees.mu.Lock()
	if b.afterExpiry == nil {
		b.afterExpiry = make(map[*expiryFunc]struct{})
	}
	b.afterExpiry[e] = struct{}{}
	b.nAfterExpiry.Store(int32(len(b.afterExpiry)))
	b.lessees.mu.Unlock()
	if b.expired() {
		b.runAfterExpiry()
	}
	return func() bool {
		b.lessees.mu.Lock()
		defer b.lessees.mu.Unlock()
		if _, ok := b.afterExpiry[e]; !ok {
			return false
		}
		delete(b.afterExpiry, e)
		b.nAfterExpiry.Store(int32(len(b.afterExpiry)))
		return true
	}
}

// runAfterExpiry starts the funcs of AfterExpiry, if b's expiry has passed.
func (b *Budget) runAfterExpiry() {
	b.lessees.mu.Lock()
	if len(b.afterExpiry) == 0 || !b.expiredLocked() {
		b.lessees.mu.Unlock()
		return
	}
	fs := b.afterExpiry
	b.afterExpiry = nil
	b.nAfterExpiry.Store(0)
	b.lessees.mu.Unlock()
	for e := range fs {
		go e.f()
	}
}
