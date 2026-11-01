package byterate

import (
	"errors"
	"slices"
	"sync"
	"sync/atomic"
)

// Lease is a part of a Budget's usage taken ahead of the traffic: Budget.Lease
// adds its Bytes to the usage at once, the holder passes up to that many
// bytes without asking the Budget again, and Return or Settle gives back what
// it did not use. A Lease belongs to the period it was taken in: once a
// Reset or SetUsed ends that period, Return and Settle change nothing.
type Lease struct {
	// Bytes is the number of bytes the lease allows.
	Bytes int64
	per   *budgetPeriod
}

// The wrappers lease about half of what a Budget has left, within these
// bounds, so that leases shrink as the Budget runs out. A Read or Write
// with less credit left than minLease leases more first, so that small
// remainders do not split the I/O.
const (
	minLease = int64(16 << 10)
	maxLease = int64(1 << 20)
)

// Lease takes up to n bytes of b's usage at once and returns them as a Lease:
// n, or fewer when b reaches the next step or a step with Stop first. It
// refuses with ErrBudgetExhausted once b is stopped or expired, and with
// ErrWouldWait while the step that applies has a Limit, whose bytes must be
// charged to b as a Limiter instead. A nil b leases all n; an n <= 0 leases
// nothing.
//
// Leased bytes count as used from the moment they are leased, so that the
// holders of leases together never take the usage past a Stop. Before it
// refuses, b takes back what the wrappers' leases hold unused, so that the
// bytes they hold never keep anyone else from the Budget. OnStep reports a
// step that a Lease taken here reaches as it is taken, since its bytes
// count as used; the wrappers' leases report a step once the bytes that
// passed on them reach it, so that one taken back unused reports none. Leases never span a
// step, so a step's Limit applies from its After.
//
// The wrappers of this package lease from the Budgets among their levels by
// themselves, and Used and Remaining leave out what their leases hold
// unused. Reset, SetUsed, SetSteps, SetExpiry and the expiry itself take
// those leases back at once; a Lease taken here stays with its holder.
func (b *Budget) Lease(n int64) (Lease, error) {
	if b.expired() {
		return Lease{}, ErrBudgetExhausted
	}
	if b == nil || n <= 0 {
		return b.leaseLocked(n, nil)
	}
	b.lessees.mu.Lock()
	l, err := b.leaseLocked(n, nil)
	b.lessees.mu.Unlock()
	if err == nil {
		b.report()
	}
	return l, err
}

// leaseLocked is Lease without the check of the expiry, which reads the
// Clock, and without the OnStep report. Refused,
// it takes back the unused bytes of the wrappers' leases other than self
// and tries once more. b.lessees.mu must be held.
func (b *Budget) leaseLocked(n int64, self *budgetLease) (Lease, error) {
	if n <= 0 {
		return Lease{}, nil
	}
	if b == nil {
		return Lease{Bytes: n}, nil
	}
	set := b.stepSet()
	p := b.per()
	reclaimed := false
	for {
		used := p.used.Load()
		var err error
		if used >= set.stopAt {
			err = ErrBudgetExhausted
		} else if _, s := set.at(used); s != nil && s.Limit != nil {
			err = ErrWouldWait
		}
		if err != nil {
			if !reclaimed {
				reclaimed = true
				if b.reclaimLocked(p, self) > 0 {
					continue
				}
			}
			return Lease{}, err
		}
		take := min(n, set.stopAt-used, set.nextAfter(used)-used)
		if p.used.CompareAndSwap(used, used+take) {
			return Lease{Bytes: take, per: p}, nil
		}
	}
}

// nextAfter returns the After of the first step past used, or maxI64.
func (set *budgetSteps) nextAfter(used int64) int64 {
	for i := range set.steps {
		if a := set.steps[i].After; a > used {
			return a
		}
	}
	return maxI64
}

// Return gives back n unused bytes of l to b's usage, at most l.Bytes,
// unless a Reset or SetUsed since ended the period l was taken in.
func (b *Budget) Return(l Lease, n int64) {
	if b == nil || n <= 0 || l.per == nil {
		return
	}
	l.per.sub(min(n, l.Bytes))
}

// Settle ends l after used of its bytes were transferred, giving back the
// rest as Return does. A used beyond l.Bytes, such as the overshoot of
// concurrent transfers that shared the lease, is added to the usage of l's
// period.
func (b *Budget) Settle(l Lease, used int64) {
	if b == nil || l.per == nil {
		return
	}
	used = max(used, 0)
	if used > l.Bytes {
		l.per.add(used - l.Bytes)
		return
	}
	b.Return(l, l.Bytes-used)
}

// budgetLease is what a wrapper's direction holds of one Budget. Every byte
// the direction counted up to end is accounted for in the Budget: leased
// ahead, or charged once it passed. The bytes from the count to end are
// leased in per and unused; bytes counted past end, by I/O that a change
// overtook, are charged by the next lease or settlement.
type budgetLease struct {
	b   *Budget
	set *leaseSet    // the direction's leases, whose end follows this one's
	end atomic.Int64 // written under b.lessees.mu
	// Under b.lessees.mu: the period of the unused bytes, nil for none,
	// and whether the Budget is charged as a Limiter instead, while a
	// step's Limit applies, which leaves end out of use.
	per     *budgetPeriod
	charged bool
	// Under b.lessees.mu, while per is nil: the period that the bytes up
	// to end were last charged in, nil for none, for unreserve to give
	// bytes counted ahead and not sent back to.
	last *budgetPeriod
}

// total returns the count of the direction's bytes, as leases see it.
func (l *budgetLease) total() int64 { return l.set.side.total.Load() }

// settled records that l's lease left its period, and was settled up to
// end in p, if not nil: a nil p charged nothing, which keeps the period
// of the last charge. b.lessees.mu must be held.
func (l *budgetLease) settled(p *budgetPeriod) {
	if p != nil {
		l.last = p
	}
	l.per = nil
}

// lessees are the budgetLeases of a Budget's wrappers, for Used and
// Remaining to leave out their unused bytes and for changes of the Budget
// to take them back.
type lessees struct {
	mu sync.Mutex
	m  map[*budgetLease]struct{}
	n  atomic.Int32
}

// usedExact returns the usage less what the wrappers' leases hold unused,
// plus what they passed beyond them and is not charged yet.
func (b *Budget) usedExact() int64 {
	for {
		if n, ok := b.usedIn(b.per()); ok {
			return n
		}
	}
}

// usedIn is usedExact in period p, and false once p is not b's period.
func (b *Budget) usedIn(p *budgetPeriod) (int64, bool) {
	if b.lessees.n.Load() == 0 {
		return p.used.Load(), b.cur.Load() == p
	}
	b.lessees.mu.Lock()
	defer b.lessees.mu.Unlock()
	if b.cur.Load() != p {
		return 0, false
	}
	n := p.used.Load()
	for l := range b.lessees.m {
		if !l.charged && (l.per == p || l.per == nil) {
			n -= l.end.Load() - l.total()
		}
	}
	return max(n, 0), true
}

// reclaimLocked takes back the unused bytes of the leases of p other than
// self and returns how many. b.lessees.mu must be held.
func (b *Budget) reclaimLocked(p *budgetPeriod, self *budgetLease) int64 {
	var got int64
	for l := range b.lessees.m {
		if l == self || l.charged || l.per != p {
			continue
		}
		if u := l.cut(); u > 0 {
			p.sub(u)
			got += u
		}
	}
	return got
}

// cut lowers l's end to its direction's count, and returns the unused bytes
// it took. A write counts its bytes before it checks the end once more, so
// cut looks at the count again after lowering the end: if it moved, a write
// may have checked the old end, and cut keeps what that write counted. A
// read counts after it passed, so one in progress can still pass the new
// end by what it reads. b.lessees.mu must be held.
func (l *budgetLease) cut() int64 {
	e := l.end.Load()
	for {
		t := l.total()
		if t >= e {
			l.end.Store(e)
			return 0
		}
		l.end.Store(t)
		l.set.lower(t)
		if l.total() == t {
			return e - t
		}
	}
}

// reclaim takes back the unused bytes of the wrappers' leases and reports
// whether there were any, for a charge that b would refuse otherwise.
func (b *Budget) reclaim() bool {
	if b.lessees.n.Load() == 0 {
		return false
	}
	b.lessees.mu.Lock()
	defer b.lessees.mu.Unlock()
	return b.reclaimLocked(b.per(), nil) > 0
}

// revokeLocked takes back the wrappers' leases, so that their next I/O
// leases again under what b is now, and makes the directions that charge b
// as a Limiter look again whether they may lease. b.lessees.mu must be
// held.
func (b *Budget) revokeLocked() {
	for l := range b.lessees.m {
		if !l.charged {
			if u := l.cut(); l.per != nil && u > 0 {
				l.per.sub(u)
			}
			l.settled(l.per)
			// The next I/O leases again, under what b is now.
			l.set.lower(min(l.end.Load(), l.total()))
		}
		l.set.recheck.Store(true)
	}
}

// revoke is revokeLocked under b.lessees.mu.
func (b *Budget) revoke() {
	b.lessees.mu.Lock()
	defer b.lessees.mu.Unlock()
	b.revokeLocked()
}

// settleLocked gives back what l holds unused and charges what its
// direction passed beyond it, in b's period. b.lessees.mu must be held.
func (l *budgetLease) settleLocked() {
	t, e := l.total(), l.end.Load()
	p := l.per
	switch {
	case p != nil && e > t:
		p.sub(e - t)
	case e < t:
		p = l.b.per()
		p.add(t - e)
	}
	l.end.Store(t)
	l.settled(p)
}

// leaseSet is a direction's leases, of each of its Budgets.
type leaseSet struct {
	side *meterSide // the direction's count, and the end of its leases
	// recheck is set when a Budget that the direction charges as a
	// Limiter changes, for the direction to lease from it again.
	recheck atomic.Bool
	mu      sync.Mutex
	ls      []*budgetLease
}

// set makes bs the Budgets that lease to the direction, and charged those
// that it charges as Limiters, keeping the leases of those it already had
// and settling the others.
func (s *leaseSet) set(bs, charged []*Budget) {
	s.mu.Lock()
	defer s.mu.Unlock()
	keep := make([]*budgetLease, 0, len(bs)+len(charged))
	for i, b := range append(bs[:len(bs):len(bs)], charged...) {
		var found *budgetLease
		for _, l := range s.ls {
			if l.b == b {
				found = l
			}
		}
		if containsLease(keep, found) {
			continue
		}
		b.lessees.mu.Lock()
		if found == nil {
			found = &budgetLease{b: b, set: s}
			found.end.Store(found.total())
			if b.lessees.m == nil {
				b.lessees.m = make(map[*budgetLease]struct{})
			}
			b.lessees.m[found] = struct{}{}
			b.lessees.n.Store(int32(len(b.lessees.m)))
		}
		switch c := i >= len(bs); {
		case c && !found.charged:
			found.settleLocked()
		case !c && found.charged:
			// What passed while b was charged as a Limiter is charged.
			found.end.Store(found.total())
			found.last = nil
		}
		found.charged = i >= len(bs)
		b.lessees.mu.Unlock()
		keep = append(keep, found)
	}
	for _, l := range s.ls {
		if !containsLease(keep, l) {
			l.release()
		}
	}
	s.ls = keep
	s.recompute()
}

func containsLease(ls []*budgetLease, l *budgetLease) bool {
	for _, x := range ls {
		if x == l {
			return true
		}
	}
	return false
}

// release settles l and stops counting it as a lessee.
func (l *budgetLease) release() {
	b := l.b
	b.lessees.mu.Lock()
	if !l.charged {
		l.settleLocked()
	}
	delete(b.lessees.m, l)
	b.lessees.n.Store(int32(len(b.lessees.m)))
	b.lessees.mu.Unlock()
	// The bytes the lease passed may have taken the usage to a step.
	b.report()
}

// lower lowers the end of s's leases to at most e, for a lease that a
// Budget took back.
func (s *leaseSet) lower(e int64) {
	for {
		cur := s.side.end.Load()
		if cur <= e || s.side.end.CompareAndSwap(cur, e) {
			return
		}
	}
}

// recompute sets the end of s's leases from them. A Budget that lowers a
// lease's end meanwhile lowers s's after it, and recompute looks again
// after storing, so that the end it stores never stays above a lease's.
// s.mu must be held.
func (s *leaseSet) recompute() {
	for {
		end := s.minEnd()
		s.side.end.Store(end)
		if s.minEnd() >= end {
			return
		}
	}
}

// minEnd returns the least end of s's leases, maxI64 for none. s.mu must
// be held.
func (s *leaseSet) minEnd() int64 {
	end := maxI64
	for _, l := range s.ls {
		if !l.charged {
			end = min(end, l.end.Load())
		}
	}
	return end
}

// refill leases more from each Budget whose leases cover fewer than want
// bytes past the count, and returns once they cover at least need. On a
// refusal before that it returns the Budget and its error.
func (s *leaseSet) refill(want, need int64) (*Budget, error) {
	var reports []*Budget
	defer func() {
		// The bytes that passed on the leases used up may have taken the
		// usage to a step. OnStep runs under no lock.
		for _, b := range reports {
			b.report()
		}
	}()
	s.mu.Lock()
	defer s.mu.Unlock()
	defer s.recompute()
	for _, l := range s.ls {
		if l.charged {
			continue
		}
		for {
			t := l.total()
			credit := l.end.Load() - t
			if credit >= want {
				break
			}
			err := l.extend(t, want-credit)
			if !slices.Contains(reports, l.b) {
				reports = append(reports, l.b)
			}
			if err != nil {
				if credit >= need {
					break // what is left serves this I/O
				}
				return l.b, err
			}
		}
	}
	return nil, nil
}

// extend leases at least short more bytes, as far as the Budget has them,
// at the count t. A Budget whose
// expiry only its Clock can tell refuses with ErrWouldWait, so that it is
// charged, and its expiry checked, at every read and write.
func (l *budgetLease) extend(t, short int64) error {
	b := l.b
	expired := b.expired()
	b.lessees.mu.Lock()
	defer b.lessees.mu.Unlock()
	if !expired && b.clock.Load() != nil && b.expiry.Load() != nil {
		return ErrWouldWait
	}
	var (
		got Lease
		err = ErrBudgetExhausted
	)
	if !expired {
		left := subSat(b.stepSet().stopAt, b.per().used.Load())
		size := max(short, min(max(left/2, minLease), maxLease))
		got, err = b.leaseLocked(size, l)
	}
	if err == nil {
		l.per = got.per
		l.end.Add(got.Bytes)
		return nil
	}
	if errors.Is(err, ErrBudgetExhausted) {
		// The bytes passed beyond the leases count all the same.
		if e := l.end.Load(); e < t {
			b.per().add(t - e)
			l.end.Store(t)
			if l.per == nil {
				l.last = b.per()
			}
		}
	}
	return err
}
