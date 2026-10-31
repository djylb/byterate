package byterate

import (
	"sync"
	"sync/atomic"
)

// Lease is a part of a Budget's usage taken ahead of the traffic: Budget.Lease
// adds its Bytes to the usage at once, the holder passes up to that many
// bytes without asking the Budget again, and Return or Settle gives back what
// it did not use. A Lease taken before a Reset or SetUsed gives back nothing,
// as the period it was taken in is over.
type Lease struct {
	// Bytes is the number of bytes the lease allows.
	Bytes  int64
	period uint64
}

// The wrappers lease about half of what a Budget has left, within these
// bounds, so that leases shrink as the Budget runs out.
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
// holders of leases together never take the usage past a Stop: a Budget
// shared by many connections is overshot by nothing, and the bytes that open
// leases hold are only kept from the other holders until given back. OnStep
// reports a step when a lease reaches it, before its bytes pass. Leases
// never span a step, so a step's Limit applies from its After.
//
// The wrappers of this package lease from the Budgets among their levels by
// themselves, and Used and Remaining leave out what their leases hold unused.
func (b *Budget) Lease(n int64) (Lease, error) {
	if b.expired() {
		return Lease{}, ErrBudgetExhausted
	}
	l, set, from, err := b.lease(n)
	if err == nil && b != nil {
		b.report(set, from, addSat(from, l.Bytes))
	}
	return l, err
}

// lease is Lease without the check of the expiry, which reads the Clock,
// and the OnStep report, which it returns the arguments of.
func (b *Budget) lease(n int64) (Lease, *budgetSteps, int64, error) {
	if n <= 0 {
		return Lease{}, nil, 0, nil
	}
	if b == nil {
		return Lease{Bytes: n}, nil, 0, nil
	}
	set := b.stepSet()
	period := b.period.Load()
	for {
		used := b.used.Load()
		if used >= set.stopAt {
			return Lease{}, nil, 0, ErrBudgetExhausted
		}
		if _, s := set.at(used); s != nil && s.Limit != nil {
			return Lease{}, nil, 0, ErrWouldWait
		}
		take := min(n, set.stopAt-used, set.nextAfter(used)-used)
		if b.used.CompareAndSwap(used, used+take) {
			return Lease{Bytes: take, period: period}, set, used, nil
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
	if b == nil || n <= 0 {
		return
	}
	b.release(min(n, l.Bytes), l.period)
}

// Settle ends l after used of its bytes were transferred, giving back the
// rest as Return does. A used beyond l.Bytes, such as the overshoot of
// concurrent transfers that shared the lease, is added to the usage.
func (b *Budget) Settle(l Lease, used int64) {
	if b == nil {
		return
	}
	used = max(used, 0)
	if used > l.Bytes {
		if b.period.Load() == l.period {
			b.add(used - l.Bytes)
		}
		return
	}
	b.Return(l, l.Bytes-used)
}

// budgetLease is what a wrapper's direction holds of one Budget: the leases
// of the current period, added up.
type budgetLease struct {
	b     *Budget
	total *atomic.Int64 // the direction's count of bytes
	end   atomic.Int64  // the count at which the leases run out
	held  Lease         // the leases of the period, under b.lessees.mu
	base  int64         // the count when the period's first lease was taken
}

// unused returns the bytes l holds and has not passed, negative for bytes
// passed past it, of b's period. b.lessees.mu must be held.
func (l *budgetLease) unused() int64 {
	if l.held.Bytes == 0 || l.held.period != l.b.period.Load() {
		return 0
	}
	return l.end.Load() - l.total.Load()
}

// lessees are the budgetLeases that hold leases of a Budget, for Used and
// Remaining to leave out their unused bytes.
type lessees struct {
	mu sync.Mutex
	m  map[*budgetLease]struct{}
	n  atomic.Int32
}

// usedExact returns the usage less what the wrappers' leases hold unused.
func (b *Budget) usedExact() int64 {
	if b.lessees.n.Load() == 0 {
		return b.used.Load()
	}
	b.lessees.mu.Lock()
	defer b.lessees.mu.Unlock()
	n := b.used.Load()
	for l := range b.lessees.m {
		n -= l.unused()
	}
	return max(n, 0)
}

// leaseSet is a direction's leases, of each of its Budgets.
type leaseSet struct {
	end atomic.Int64 // the count at which the first lease runs out, maxI64 without
	mu  sync.Mutex
	ls  []*budgetLease
}

// set makes bs the Budgets that lease to the direction counted in total,
// keeping the leases of those it already had and settling the others.
func (s *leaseSet) set(bs []*Budget, total *atomic.Int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	keep := make([]*budgetLease, 0, len(bs))
	for _, b := range bs {
		var found *budgetLease
		for _, l := range s.ls {
			if l.b == b {
				found = l
			}
		}
		if found == nil {
			found = &budgetLease{b: b, total: total}
			found.end.Store(total.Load())
			b.lessees.mu.Lock()
			if b.lessees.m == nil {
				b.lessees.m = make(map[*budgetLease]struct{})
			}
			b.lessees.m[found] = struct{}{}
			b.lessees.n.Store(int32(len(b.lessees.m)))
			b.lessees.mu.Unlock()
		}
		keep = append(keep, found)
	}
	for _, l := range s.ls {
		if !containsLease(keep, l) {
			l.settle()
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

// settle gives back what l holds unused and stops counting it as a lessee.
func (l *budgetLease) settle() {
	b := l.b
	b.lessees.mu.Lock()
	defer b.lessees.mu.Unlock()
	if l.held.Bytes > 0 {
		b.Settle(l.held, l.total.Load()-l.base)
	}
	l.held = Lease{}
	delete(b.lessees.m, l)
	b.lessees.n.Store(int32(len(b.lessees.m)))
}

// recompute sets s.end from its leases. s.mu must be held.
func (s *leaseSet) recompute() {
	end := maxI64
	for _, l := range s.ls {
		end = min(end, l.end.Load())
	}
	s.end.Store(end)
}

// refill leases more from each Budget whose leases cover fewer than need
// bytes past total. On a refusal it returns the Budget and its error.
func (s *leaseSet) refill(total *atomic.Int64, need int64) (*Budget, error) {
	var reports []stepReport
	defer func() {
		// OnStep runs under no lock.
		for _, r := range reports {
			r.b.report(r.set, r.from, r.to)
		}
	}()
	s.mu.Lock()
	defer s.mu.Unlock()
	defer s.recompute()
	for _, l := range s.ls {
		for {
			t := total.Load()
			short := need - (l.end.Load() - t)
			if short <= 0 {
				break
			}
			r, err := l.extend(t, short)
			if err != nil {
				return l.b, err
			}
			reports = append(reports, r)
		}
	}
	return nil, nil
}

// stepReport is an OnStep report to make once no lock is held.
type stepReport struct {
	b        *Budget
	set      *budgetSteps
	from, to int64
}

// extend leases at least short more bytes, as far as the Budget has them,
// at the count t, and returns the OnStep report to make.
func (l *budgetLease) extend(t, short int64) (stepReport, error) {
	b := l.b
	if b.expired() {
		return stepReport{}, ErrBudgetExhausted
	}
	size := max(short, min(max(subSat(b.stepSet().stopAt, b.used.Load())/2, minLease), maxLease))
	b.lessees.mu.Lock()
	got, set, from, err := b.lease(size)
	if err == nil {
		if l.held.Bytes == 0 || got.period != l.held.period {
			// The first lease of a period: what was left of the last one
			// went with it.
			l.held, l.base = got, t
			l.end.Store(t + got.Bytes)
		} else {
			l.held.Bytes += got.Bytes
			l.end.Add(got.Bytes)
		}
	}
	b.lessees.mu.Unlock()
	return stepReport{b: b, set: set, from: from, to: addSat(from, got.Bytes)}, err
}
