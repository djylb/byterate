package byterate

import (
	"cmp"
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"time"
)

// ErrBudgetExhausted is returned for bytes that a Budget stops: once its
// usage reaches a step with Stop, or once its expiry has passed.
var ErrBudgetExhausted = errors.New("byterate: budget exhausted")

// Step is a step of a Budget: what applies once the bytes used in the period
// reach After.
type Step struct {
	// After is the number of bytes used in the period from which the step
	// applies.
	After int64
	// Limit, if set, is charged for the bytes of the step on top of the
	// other limits, so that it sets their pace, such as a slower Rate. A
	// step with Stop needs none.
	Limit Limiter
	// Stop ends the period's traffic: from After on, the Budget refuses all
	// bytes with ErrBudgetExhausted, which ends the connection reads and
	// writes charged to it, and steps after it never apply.
	Stop bool
}

// Budget is a Limiter that counts the bytes used in a period and applies a
// Step once they reach its After: from there on, its Limit sets their pace,
// or Stop refuses them. Steps can slow traffic down in stages and stop it,
// such as to 1 MiB/s past 10 GiB, to 64 KiB/s past 20 GiB and not at all
// past 30 GiB. Before the first step a Budget limits nothing, and without
// steps it only counts.
//
// Reset starts a new period, and SetSteps and SetUsed change the steps and
// the bytes used, at any time and without ending the connections charged to
// the Budget: the next charge sees the change. When periods begin, such as
// each hour, day or month, is up to the caller. SetExpiry makes the Budget
// stop all bytes after a time, until it is set again.
//
// The bytes used count everything charged to the Budget, refunds taken off,
// whichever step applied; a step's Limit sees only the bytes of its step.
// Ready and the connection wrappers end a grant at the next step, so a step
// applies from its After, and a Write never takes the usage past a Stop. A
// Read charges what it read, so concurrent Reads can each pass a Stop by
// their last grant; Charge, which records bytes already transferred, counts
// them in the step that applied before them.
//
// All methods are safe for concurrent use and on a nil *Budget, which never
// limits. The zero value only counts until SetSteps.
type Budget struct {
	used   atomic.Int64
	period atomic.Uint64 // counts Reset and SetUsed, which void earlier charges
	expiry atomic.Pointer[budgetExpiry]
	steps  atomic.Pointer[budgetSteps]
	onStep atomic.Pointer[func(step int, used int64)]
}

// budgetExpiry is the time after which a Budget stops all bytes, as given
// and as nowNs.
type budgetExpiry struct {
	t  time.Time
	at int64
}

// budgetSteps is one set of steps of a Budget, sorted by After, and the After
// of its first step with Stop, or maxI64 for none.
type budgetSteps struct {
	steps  []budgetStep
	stopAt int64
}

// budgetStep is a Step, its Limit as a level, and the bytes charged to its
// Limit and not refunded, which no refund to it may exceed.
type budgetStep struct {
	Step
	t    target // Limit, prepared for charging
	owed atomic.Int64
}

// noSteps is the step set of a Budget without steps.
var noSteps = &budgetSteps{stopAt: maxI64}

// NewBudget returns a Budget with steps, at the start of a period.
func NewBudget(steps ...Step) *Budget {
	b := &Budget{}
	b.SetSteps(steps...)
	return b
}

// SetSteps replaces the steps of b, given in any order, from the next
// charge; Steps returns them sorted. Of steps with the same After, the last
// one given applies, except that a step with Stop always does: from its After
// on, b refuses everything. It panics if a step's Limit passes charges on to
// b, which would charge b without end.
func (b *Budget) SetSteps(steps ...Step) {
	if b == nil {
		return
	}
	configMu.Lock()
	defer configMu.Unlock()
	sorted := slices.Clone(steps)
	slices.SortStableFunc(sorted, func(x, y Step) int { return cmp.Compare(x.After, y.After) })
	set := &budgetSteps{steps: make([]budgetStep, len(sorted)), stopAt: maxI64}
	for i, s := range sorted {
		s.Limit = classify(s.Limit).l
		if reaches(s.Limit, func(x Limiter) bool { return x == Limiter(b) }) {
			panic("byterate: Budget step limited by a Limiter that charges the Budget")
		}
		set.steps[i].Step = s
		set.steps[i].t.init(s.Limit)
		if s.Stop && set.stopAt == maxI64 {
			set.stopAt = s.After
		}
	}
	b.steps.Store(set)
}

// Steps returns b's steps, sorted by After.
func (b *Budget) Steps() []Step {
	set := b.stepSet()
	steps := make([]Step, len(set.steps))
	for i := range set.steps {
		steps[i] = set.steps[i].Step
	}
	return steps
}

// Reset starts a new period: the bytes used go back to zero, and with them
// the step that applies. Refunds of bytes charged before it are dropped.
func (b *Budget) Reset() { b.SetUsed(0) }

// Used returns the bytes used in the period.
func (b *Budget) Used() int64 {
	if b == nil {
		return 0
	}
	return b.used.Load()
}

// SetUsed makes n the bytes used in the period, such as the bytes persisted
// before a restart, or a total that several Budgets share. Refunds of bytes
// charged before it are dropped, and OnStep does not report the step it
// brings b into.
func (b *Budget) SetUsed(n int64) {
	if b == nil {
		return
	}
	b.period.Add(1)
	b.used.Store(max(n, 0))
}

// Step returns the index of the step that applies, in the order of After,
// or -1 before the first step.
func (b *Budget) Step() int {
	if b == nil {
		return -1
	}
	i, _ := b.stepSet().at(b.used.Load())
	return i
}

// SetExpiry makes b stop all bytes after t, as a step with Stop does; a zero
// t means never. The expiry is kept on the monotonic clock, as a deadline
// is, so a later change of the wall clock does not move it.
func (b *Budget) SetExpiry(t time.Time) {
	if b == nil {
		return
	}
	if t.IsZero() {
		b.expiry.Store(nil)
		return
	}
	b.expiry.Store(&budgetExpiry{t: t, at: addSat(nowNs(), int64(time.Until(t)))})
}

// Expiry returns the time SetExpiry set, or the zero time for none.
func (b *Budget) Expiry() time.Time {
	if b != nil {
		if e := b.expiry.Load(); e != nil {
			return e.t
		}
	}
	return time.Time{}
}

// Remaining returns how many more bytes b lets through before it stops
// them: the bytes up to the first step with Stop, 0 once the usage has
// reached it or the expiry has passed, and math.MaxInt64 without such a
// step. It charges nothing, so a concurrent charge can take the bytes
// first.
func (b *Budget) Remaining() int64 {
	if b == nil {
		return maxI64
	}
	if b.expired() {
		return 0
	}
	set := b.stepSet()
	if set.stopAt == maxI64 {
		return maxI64
	}
	return max(set.stopAt-b.used.Load(), 0)
}

// OnStep makes b call f with the index of the step that a charge brought b
// into and the bytes used then, each time a charge reaches a later step; of
// several steps passed at once, it reports the last. f runs on the goroutine
// of that charge, so it must return quickly. Reports of concurrent charges
// may come out of order, and a step reached again after a refund is reported
// again, as is one that a TryWait reached before another level of a
// hierarchy refused the bytes. A nil f stops the reports.
func (b *Budget) OnStep(f func(step int, used int64)) {
	if b == nil {
		return
	}
	if f == nil {
		b.onStep.Store(nil)
		return
	}
	b.onStep.Store(&f)
}

// Wait charges n bytes to b and to the Limit of the step that applies, and
// waits for that Limit. It refuses them with ErrBudgetExhausted, charging
// nothing, when they would take the usage past a step with Stop, or after
// the expiry.
func (b *Budget) Wait(ctx context.Context, n int64) error {
	var rc receipt
	return b.wait(ctx, nil, n, &rc)
}

// wait is Wait in timer t, nil for one per wait, recording the charges in rc:
// b's own, then those of the step's Limit. A refusal leaves rc as it was.
func (b *Budget) wait(ctx context.Context, t *waitTimer, n int64, rc *receipt) error {
	if b == nil || n <= 0 {
		return nil
	}
	m := rc.mark()
	set, used, s, err := b.take(n, rc)
	if err != nil || s == nil {
		return err
	}
	if _, err := s.t.wait(ctx, t, n, true, rc); err != nil {
		if !isCanceled(ctx, err) {
			rc.refundFrom(m, n) // refused, so b keeps no charge either
		}
		return err
	}
	b.report(set, used, addSat(used, n))
	return nil
}

// TryWait charges n bytes to b and to the Limit of the step that applies if
// that Limit lets them through without waiting, and otherwise charges
// nothing and returns ErrWouldWait. It refuses them with ErrBudgetExhausted,
// as Wait does.
func (b *Budget) TryWait(n int64) error {
	var rc receipt
	return b.tryWait(n, nowNs(), &rc)
}

// tryWait is TryWait at now, recording the charges in rc: b's own, then those
// of the step's Limit. A refusal leaves rc as it was.
func (b *Budget) tryWait(n, now int64, rc *receipt) error {
	if b == nil || n <= 0 {
		return nil
	}
	m := rc.mark()
	set, used, s, err := b.take(n, rc)
	if err != nil || s == nil {
		return err
	}
	if err := s.t.tryWait(n, now, rc); err != nil {
		rc.refundFrom(m, n)
		return err
	}
	b.report(set, used, addSat(used, n))
	return nil
}

// check reports, without charging, what tryWait would refuse n bytes at now
// with: ErrBudgetExhausted, or what the Limit of the step that applies
// would refuse them with.
func (b *Budget) check(n, now int64) error {
	if b.expired() {
		return ErrBudgetExhausted
	}
	set := b.stepSet()
	used := b.used.Load()
	if addSat(used, n) > set.stopAt {
		return ErrBudgetExhausted
	}
	if _, s := set.at(used); s != nil && s.Limit != nil {
		return s.t.check(n, now)
	}
	return nil
}

// take adds n bytes to the usage, unless they would take it past a Stop or
// the expiry has passed, and records the charge in rc. It returns the steps,
// the usage before, and the step whose Limit must take the bytes as well,
// owed them, or nil, when it reports the step the bytes reach itself.
func (b *Budget) take(n int64, rc *receipt) (set *budgetSteps, used int64, s *budgetStep, err error) {
	set = b.stepSet()
	period := b.period.Load()
	// Take the bytes before waiting, so that concurrent charges cannot pass
	// a Stop together.
	for {
		if b.expired() {
			return nil, 0, nil, ErrBudgetExhausted
		}
		used = b.used.Load()
		if addSat(used, n) > set.stopAt {
			return nil, 0, nil, ErrBudgetExhausted
		}
		if b.used.CompareAndSwap(used, addSat(used, n)) {
			break
		}
	}
	if _, s = set.at(used); s != nil && s.Limit == nil {
		s = nil
	}
	rc.add(budgetEntry(b, s, period))
	if s != nil {
		s.owed.Add(n)
	} else {
		b.report(set, used, addSat(used, n))
	}
	return set, used, s, nil
}

// Ready returns how many of n bytes may pass at once: up to the next step,
// and as the Limit of the step that applies allows. It refuses with
// ErrBudgetExhausted once the usage reaches a step with Stop, or after the
// expiry, and charges nothing.
func (b *Budget) Ready(ctx context.Context, n int64) (int64, error) {
	return b.ready(ctx, nil, n)
}

// ready is Ready in timer t, nil for one per wait.
func (b *Budget) ready(ctx context.Context, t *waitTimer, n int64) (int64, error) {
	if b == nil || n <= 0 {
		return n, nil
	}
	if b.expired() {
		return 0, ErrBudgetExhausted
	}
	set := b.stepSet()
	used := b.used.Load()
	if used >= set.stopAt {
		return 0, ErrBudgetExhausted
	}
	i, s := set.at(used)
	// Up to the next step, so that a step applies from its After.
	if i+1 < len(set.steps) {
		n = min(n, max(set.steps[i+1].After-used, 1))
	}
	if s == nil || s.Limit == nil {
		return n, nil
	}
	return s.t.ready(ctx, t, n, false)
}

// Charge records n transferred bytes on b and on the Limit of the step that
// applied before them, without waiting. It returns ErrBudgetExhausted when
// the usage has reached a step with Stop, or after the expiry, and otherwise
// the Limit's error; the bytes stay charged either way.
func (b *Budget) Charge(n int64) error {
	if b == nil || n <= 0 {
		return nil
	}
	set := b.stepSet()
	used := b.add(n)
	_, s := set.at(used)
	var err error
	if s != nil && s.Limit != nil {
		s.owed.Add(n)
		err = s.t.charge(n)
	}
	after := addSat(used, n)
	b.report(set, used, after)
	if after >= set.stopAt || b.expired() {
		return ErrBudgetExhausted
	}
	return err
}

// Refund takes back the last n bytes charged, which were not transferred,
// and gives them back to the Limits of the steps they were charged in, as
// far as those took them, from the latest step down. The connection wrappers
// and a hierarchy's Wait refund the exact charges they made instead, in the
// step and period they made them.
func (b *Budget) Refund(n int64) {
	if b == nil || n <= 0 {
		return
	}
	set := b.stepSet()
	was := b.sub(n)
	now := max(was-n, 0)
	i, _ := set.at(was - 1)
	for ; i >= 0 && n > 0; i-- {
		s := &set.steps[i]
		if s.Limit != nil {
			n -= s.refund(n)
		}
		if s.After <= now {
			break
		}
	}
}

// release takes n bytes charged in period off the usage, unless a Reset or
// SetUsed since voided them.
func (b *Budget) release(n int64, period uint64) {
	if b.period.Load() == period {
		b.sub(n)
	}
}

// add adds n bytes to the usage, saturating, and returns the usage before.
func (b *Budget) add(n int64) int64 {
	for {
		used := b.used.Load()
		if b.used.CompareAndSwap(used, addSat(used, n)) {
			return used
		}
	}
}

// sub takes n bytes off the usage, down to zero, and returns the usage
// before.
func (b *Budget) sub(n int64) int64 {
	for {
		used := b.used.Load()
		if b.used.CompareAndSwap(used, max(used-n, 0)) {
			return used
		}
	}
}

// report tells the OnStep callback of a charge that took the usage from
// from to to into a later step of set.
func (b *Budget) report(set *budgetSteps, from, to int64) {
	f := b.onStep.Load()
	if f == nil {
		return
	}
	i, _ := set.at(from)
	if j, _ := set.at(to); j > i {
		(*f)(j, to)
	}
}

// stepSet returns b's steps.
func (b *Budget) stepSet() *budgetSteps {
	if b != nil {
		if set := b.steps.Load(); set != nil {
			return set
		}
	}
	return noSteps
}

// at returns the index of the step of set that applies at used bytes and
// the step, or -1 and nil before the first step.
func (set *budgetSteps) at(used int64) (int, *budgetStep) {
	i := -1
	for j := range set.steps {
		if set.steps[j].After > used {
			break
		}
		i = j
	}
	if i < 0 {
		return -1, nil
	}
	return i, &set.steps[i]
}

// expired reports whether b's expiry has passed.
func (b *Budget) expired() bool {
	e := b.expiry.Load()
	return e != nil && nowNs() >= e.at
}

// refund gives back up to n bytes to the step's Limit, no more than it is
// owed, and returns how many it gave back.
func (s *budgetStep) refund(n int64) int64 {
	for {
		owed := s.owed.Load()
		back := min(n, owed)
		if back <= 0 {
			return 0
		}
		if s.owed.CompareAndSwap(owed, owed-back) {
			s.Limit.Refund(back)
			return back
		}
	}
}

// unowe takes up to n bytes off what the step's Limit is owed, as a receipt
// refunds the Limit's charges itself.
func (s *budgetStep) unowe(n int64) {
	for {
		owed := s.owed.Load()
		back := min(n, owed)
		if back <= 0 || s.owed.CompareAndSwap(owed, owed-back) {
			return
		}
	}
}
