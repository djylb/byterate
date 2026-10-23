package byterate

import (
	"context"
	"errors"
	"sync"
)

// errSwitched ends a wait whose Switch changed limiter, so that the charge
// moves to the new one.
var errSwitched = errors.New("byterate: limiter switched")

// levelKind says how a level is charged. classify is the one place that maps
// the Limiters of this package to their kinds; everything else switches on
// the kind.
type levelKind uint8

const (
	kindNone        levelKind = iota // no Limiter: limits and counts nothing
	kindOther                        // a Limiter of another package, called through its methods
	kindRate                         // a *Rate, charged in place
	kindBudget                       // a *Budget, whose step Limit is charged with the receipt
	kindSwitch                       // a *Switch, whose current Limiter is charged
	kindHierarchy                    // a *hierarchy, whose levels are charged
	kindGroupReads                   // a Group's Reads, whose levels are charged
	kindGroupWrites                  // a Group's Writes, whose levels are charged
	kindCountReads                   // a Counter's Reads view, counted once the others accept
	kindCountWrites                  // a Counter's Writes view, counted once the others accept
)

// level is a Limiter classified for charging: l is the Limiter, nil for
// none, and rate the same one when it is a Rate.
type level struct {
	rate *Rate
	l    Limiter
	kind levelKind
}

// classify returns l as a level. A nil l, also a nil pointer of this package,
// is a level of kind kindNone without a Limiter, so that it does not hide a
// wrapped connection.
func classify(l Limiter) level {
	var kind levelKind
	var isNil bool
	switch t := l.(type) {
	case nil:
		return level{}
	case *Rate:
		if t == nil {
			return level{}
		}
		return level{rate: t, l: t, kind: kindRate}
	case *Budget:
		kind, isNil = kindBudget, t == nil
	case *Switch:
		kind, isNil = kindSwitch, t == nil
	case *hierarchy:
		kind, isNil = kindHierarchy, t == nil
	case *groupReads:
		kind, isNil = kindGroupReads, t == nil
	case *groupWrites:
		kind, isNil = kindGroupWrites, t == nil
	case *counterReads:
		kind, isNil = kindCountReads, t == nil
	case *counterWrites:
		kind, isNil = kindCountWrites, t == nil
	default:
		kind = kindOther
	}
	if isNil {
		return level{}
	}
	return level{l: l, kind: kind}
}

// isView reports whether lv is a view of a Counter, which never limits.
func (lv *level) isView() bool { return lv.kind == kindCountReads || lv.kind == kindCountWrites }

// count adds n bytes charged at now to the Counter of the view lv.
func (lv *level) count(n, now int64) {
	if lv.kind == kindCountReads {
		(*Counter)(lv.l.(*counterReads)).addAt(n, 0, now)
	} else {
		(*Counter)(lv.l.(*counterWrites)).addAt(0, n, now)
	}
}

// levels are the levels that a charge goes to: those that limit, then the
// views of Counters, counted once the others accept. switchCh, when set, is
// the channel that the Switch they came through closes when it changes
// limiter.
type levels struct {
	lv       []level
	views    []level
	switchCh <-chan struct{}
	others   bool // a level of lv is not a Rate
}

// expand returns the levels that charging the level one[0] charges: those of
// a hierarchy or a Group, the current ones of a Switch, or the level itself.
func expand(one []level) levels {
	switch one[0].kind {
	case kindSwitch:
		st := one[0].l.(*Switch).state()
		v := st.t.fixed
		if v.lv == nil {
			v = expandFixed(st.t.one[:])
		}
		v.switchCh = st.changed
		return v
	case kindHierarchy, kindGroupReads, kindGroupWrites:
		return expandFixed(one)
	case kindNone, kindOther, kindRate, kindBudget, kindCountReads, kindCountWrites:
		// A single level, charged as it is.
	}
	return levels{lv: one, others: one[0].kind != kindRate}
}

// expandFixed is expand without a Switch's channel: a Switch, here the
// current Limiter of another, stays a level of its own.
func expandFixed(one []level) levels {
	switch one[0].kind {
	case kindHierarchy:
		return one[0].l.(*hierarchy).levels()
	case kindGroupReads:
		return (*Group)(one[0].l.(*groupReads)).hierarchy(false).levels()
	case kindGroupWrites:
		return (*Group)(one[0].l.(*groupWrites)).hierarchy(true).levels()
	case kindNone, kindOther, kindRate, kindBudget, kindSwitch, kindCountReads, kindCountWrites:
		// A single level, charged as it is.
	}
	return levels{lv: one, others: one[0].kind != kindRate}
}

// target is a Limiter prepared for charging: classified, and expanded once
// unless its levels change, as those of a Switch and of a Group's Reads and
// Writes do. Its levels may refer to itself, so it must not be copied after
// init.
type target struct {
	one   [1]level
	fixed levels // one[0] expanded, with no lv when its levels change
}

func (t *target) init(l Limiter) {
	t.one[0] = classify(l)
	switch t.one[0].kind {
	case kindNone, kindSwitch, kindGroupReads, kindGroupWrites:
	default:
		t.fixed = expandFixed(t.one[:])
	}
}

// limiter returns the Limiter, nil for none.
func (t *target) limiter() Limiter { return t.one[0].l }

// wait charges size bytes and waits, as levels.wait does.
func (t *target) wait(ctx context.Context, tm *waitTimer, size int64, whole bool, rc *receipt) (int64, error) {
	if t.fixed.lv != nil {
		return t.fixed.wait(ctx, tm, size, whole, rc)
	}
	return waitOn(ctx, t.one[:], tm, size, whole, rc)
}

// ready waits until the Limiter is ready, as levels.ready does.
func (t *target) ready(ctx context.Context, tm *waitTimer, size int64, whole bool) (int64, error) {
	if t.fixed.lv != nil {
		return t.fixed.ready(ctx, tm, size, whole)
	}
	return readyOn(ctx, t.one[:], tm, size, whole)
}

// check reports what tryWait would refuse size bytes at now with, as
// levels.check does.
func (t *target) check(size, now int64) error {
	if t.fixed.lv != nil {
		return t.fixed.check(size, now)
	}
	v := expand(t.one[:])
	return v.check(size, now)
}

// tryWait charges size bytes at now unless the Limiter would wait, as
// levels.tryWait does.
func (t *target) tryWait(size, now int64, rc *receipt) error {
	if t.fixed.lv != nil {
		return t.fixed.tryWait(size, now, rc)
	}
	return tryOn(t.one[:], size, now, rc)
}

// charge records n transferred bytes, as levels.charge does.
func (t *target) charge(n int64) error {
	if t.fixed.lv != nil {
		return t.fixed.charge(n)
	}
	return chargeOn(t.one[:], n)
}

// waitOn charges size bytes to the level one[0] and waits, as levels.wait
// does, starting over on the new Limiter when a Switch changes it.
func waitOn(ctx context.Context, one []level, t *waitTimer, size int64, whole bool, rc *receipt) (int64, error) {
	for {
		v := expand(one)
		m := rc.mark()
		n, err := v.wait(ctx, t, size, whole, rc)
		if !errors.Is(err, errSwitched) {
			return n, err
		}
		rc.refundFrom(m, n)
	}
}

// readyOn waits until the level one[0] is ready, as levels.ready does,
// starting over on the new Limiter when a Switch changes it.
func readyOn(ctx context.Context, one []level, t *waitTimer, size int64, whole bool) (int64, error) {
	for {
		v := expand(one)
		n, err := v.ready(ctx, t, size, whole)
		if !errors.Is(err, errSwitched) {
			return n, err
		}
	}
}

// chargeOn records n transferred bytes on the level one[0], as levels.charge
// does.
func chargeOn(one []level, n int64) error {
	v := expand(one)
	return v.charge(n)
}

// allowance returns how much of size the Rate levels let through at now in
// one charge: all of it when every Rate's credit covers it, and otherwise
// the least of what each Rate grants, its credit or its quantum.
func (v *levels) allowance(size, now int64) int64 {
	i := 0
	for ; i < len(v.lv); i++ {
		if r := v.lv[i].rate; r != nil {
			creditNs, limit := r.credit(now)
			if !covers(size, creditNs, limit) {
				break
			}
		}
	}
	if i == len(v.lv) {
		return size
	}
	for i := range v.lv {
		if r := v.lv[i].rate; r != nil {
			size = min(size, r.allowance(size, now))
		}
	}
	return size
}

// quantum returns the least quantum of the Rate levels, or maxI64 if none
// limits.
func (v *levels) quantum() int64 {
	q := maxI64
	for i := range v.lv {
		if r := v.lv[i].rate; r != nil {
			q = min(q, r.quantum())
		}
	}
	return q
}

// requeue returns the quantum q to charge instead of a grant of size bytes
// that must wait, and true, when the credit the allowance saw went to other
// chargers first: a quantum then queues behind them instead of the credit.
func requeue(size, q int64) (int64, bool) {
	return q, size > q
}

// ready waits until the Rate levels have no debt worth a wait and the other
// levels are ready, and returns how much of size a transfer may take: the
// grant of every level or, when whole, all of size, which the other levels'
// Ready then only has to allow. It charges nothing. t is the timer to wait
// in, nil for one per wait; v's switchCh, when closed, ends the wait with
// errSwitched.
func (v *levels) ready(ctx context.Context, t *waitTimer, size int64, whole bool) (int64, error) {
	// The other levels first, so that one refusing, such as an exhausted
	// Budget, does so before the Rates make the caller wait.
	size, err := v.readyOthers(ctx, t, size, whole)
	if err != nil {
		return 0, err
	}
	now := nowNs()
	var waits waitSet
	for i := range v.lv {
		if end, g := v.lv[i].rate.debtEnd(now); end != 0 {
			waits.add(subSat(end, now), now, g)
		}
	}
	if waits.n > 0 {
		if err := waits.sleep(ctx, t, v.switchCh); err != nil {
			return 0, err
		}
		now = nowNs()
	}
	if !whole {
		size = v.allowance(size, now)
	}
	return size, nil
}

// readyOthers asks the levels other than Rates whether they are ready for
// size bytes, and returns the least they grant, at least 1, or all of size
// when whole. A Counter's view is always ready.
// waitRates charges size bytes at once to levels that are Rates alone and
// waits out the longest debt. Rates never refuse a whole charge, so nothing
// is recorded to give back.
func (v *levels) waitRates(ctx context.Context, t *waitTimer, size int64) error {
	var waits waitSet
	v.reserve(size, nowNs(), nil, &waits)
	return waits.sleep(ctx, t, v.switchCh)
}

func (v *levels) readyOthers(ctx context.Context, t *waitTimer, size int64, whole bool) (int64, error) {
	if !v.others {
		return size, nil
	}
	for i := range v.lv {
		lv := &v.lv[i]
		var n int64
		var err error
		switch lv.kind {
		case kindNone, kindRate, kindCountReads, kindCountWrites:
			continue
		case kindBudget:
			n, err = lv.l.(*Budget).ready(ctx, t, size)
		case kindOther:
			n, err = lv.l.Ready(ctx, size)
		default:
			n, err = readyOn(ctx, v.lv[i:i+1], t, size, whole)
		}
		if err != nil {
			return 0, err
		}
		if !whole {
			size = min(size, max(n, 1))
		}
	}
	return size, nil
}

// charge takes n transferred bytes on every level: as debt on the Rates,
// which the next ready waits for, and through Charge on the others. Every
// level keeps the charge; it returns the first error of the others.
func (v *levels) charge(n int64) error {
	now := nowNs()
	var first error
	for i := range v.lv {
		lv := &v.lv[i]
		var err error
		switch lv.kind {
		case kindNone:
		case kindRate:
			lv.rate.reserveAt(n, now)
		case kindBudget, kindOther:
			err = lv.l.Charge(n)
		case kindCountReads, kindCountWrites:
			lv.count(n, now)
		default:
			err = chargeOn(v.lv[i:i+1], n)
		}
		if err != nil && first == nil {
			first = err
		}
	}
	for i := range v.views {
		v.views[i].count(n, now)
	}
	return first
}

// refund gives back n bytes to every level, without a receipt.
func (v *levels) refund(n int64) {
	for i := range v.lv {
		if l := v.lv[i].l; l != nil {
			l.Refund(n)
		}
	}
	for i := range v.views {
		v.views[i].l.Refund(n)
	}
}

// wait charges a transfer of size bytes to every level before it happens and
// waits until each allows it. Unless whole, size is first cut to the grant
// of every level, as ready computes it, and a Rate whose credit went to other
// chargers meanwhile is charged a quantum instead. It returns the bytes
// charged and records each charge in rc for a refund. A level that refuses
// the charge, with an error other than ctx's, takes back what v charged and
// leaves rc as it was. When ctx or v's switchCh ends the wait, the charges
// stay in rc. rc may be nil for a whole charge of Rates alone, which no
// level refuses and nobody refunds.
func (v *levels) wait(ctx context.Context, t *waitTimer, size int64, whole bool, rc *receipt) (int64, error) {
	// The other levels first, also for a whole charge, so that one refusing
	// does so before the others take the bytes and have to give them back.
	size, err := v.readyOthers(ctx, t, size, whole)
	if err != nil {
		return 0, err
	}
	m := rc.mark()
	now := nowNs()
	if !whole {
		size = v.allowance(size, now)
	}
	var waits waitSet
	v.reserve(size, now, rc, &waits)
	if !whole && waits.n > 0 {
		if q, ok := requeue(size, v.quantum()); ok {
			rc.refundFrom(m, size)
			waits = waitSet{}
			size = q
			v.reserve(size, nowNs(), rc, &waits)
		}
	}
	if v.others {
		if err := v.waitOthers(ctx, t, size, now, rc); err != nil {
			if isCanceled(ctx, err) {
				// It kept the charge, so the views count it, for a refund
				// to take off.
				rc.count(v.views, size, now)
				return size, err
			}
			rc.refundFrom(m, size)
			return 0, err
		}
	}
	rc.count(v.views, size, now)
	return size, waits.sleep(ctx, t, v.switchCh)
}

// waitOthers charges size bytes to the levels of v other than Rates, which
// wait already charged at now, and waits for each in turn, recording the
// charges in rc. It returns the first error, ctx's for a charge kept.
func (v *levels) waitOthers(ctx context.Context, t *waitTimer, size, now int64, rc *receipt) error {
	for i := range v.lv {
		lv := &v.lv[i]
		var err error
		switch lv.kind {
		case kindNone, kindRate:
		case kindCountReads, kindCountWrites:
			// A view alone, as the levels of a hierarchy keep views apart.
			lv.count(size, now)
			rc.add(entry{l: lv.l})
		case kindBudget:
			err = lv.l.(*Budget).wait(ctx, t, size, rc)
		case kindOther:
			err = lv.l.Wait(ctx, size)
			if err == nil || isCanceled(ctx, err) {
				rc.add(entry{l: lv.l})
			}
		default:
			_, err = waitOn(ctx, v.lv[i:i+1], t, size, true, rc)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// tryWait charges size bytes to every level at now if none of them makes
// the caller wait or refuses them, recording the charges in rc, and
// otherwise charges nothing and returns the refusal or ErrWouldWait. The
// levels are asked first, as check does, so that a limit that would wait
// costs no charge and refund; a level that refuses meanwhile, such as a
// Rate whose credit went to another charger, still refuses, and the levels
// charged before it take their charge back. A Counter's view counts once
// every other level accepts.
func (v *levels) tryWait(size, now int64, rc *receipt) error {
	if err := v.check(size, now); err != nil {
		return err
	}
	m := rc.mark()
	view := -1
	if v.others {
		for i := range v.lv {
			lv := &v.lv[i]
			var err error
			switch lv.kind {
			case kindNone, kindRate:
			case kindCountReads, kindCountWrites:
				view = i // a view alone, counted last
			case kindBudget:
				err = lv.l.(*Budget).tryWait(size, now, rc)
			case kindOther:
				if err = lv.l.TryWait(size); err == nil {
					rc.add(entry{l: lv.l})
				}
			default:
				err = tryOn(v.lv[i:i+1], size, now, rc)
			}
			if err != nil {
				rc.refundFrom(m, size)
				return err
			}
		}
	}
	for i := range v.lv {
		if r := v.lv[i].rate; r != nil && !r.tryTake(size, now, rc) {
			rc.refundFrom(m, size)
			return ErrWouldWait
		}
	}
	if view >= 0 {
		v.lv[view].count(size, now)
		rc.add(entry{l: v.lv[view].l})
	}
	rc.count(v.views, size, now)
	return nil
}

// check reports, without charging, what tryWait would refuse size bytes at
// now with: the refusal of a level, such as an exhausted Budget, before
// ErrWouldWait for a Rate that would make the caller wait, and nil when the
// levels it can ask without charging let the bytes through. Limiters of
// other packages are asked by tryWait alone.
func (v *levels) check(size, now int64) error {
	wait := false
	for i := range v.lv {
		lv := &v.lv[i]
		var err error
		switch lv.kind {
		case kindRate:
			wait = wait || lv.rate.wouldWait(size, now)
		case kindBudget:
			err = lv.l.(*Budget).check(size, now)
		case kindSwitch, kindGroupReads, kindGroupWrites:
			inner := expand(v.lv[i : i+1])
			err = inner.check(size, now)
		case kindNone, kindOther, kindHierarchy, kindCountReads, kindCountWrites:
			// Checked when charged: other Limiters through TryWait, views
			// never refuse, and a hierarchy is expanded into its levels.
		}
		if errors.Is(err, ErrWouldWait) {
			wait = true
		} else if err != nil {
			return err
		}
	}
	if wait {
		return ErrWouldWait
	}
	return nil
}

// tryOn charges size bytes to the level one[0] at now as levels.tryWait
// does, to the current Limiter of a Switch.
func tryOn(one []level, size, now int64, rc *receipt) error {
	v := expand(one)
	return v.tryWait(size, now, rc)
}

// isCanceled reports whether err is ctx's own error, which a wait that ctx
// ended returns, keeping its charge.
func isCanceled(ctx context.Context, err error) bool {
	ctxErr := ctx.Err()
	return ctxErr != nil && errors.Is(err, ctxErr)
}

// reserve charges size to the Rate levels at now, recording each charge in
// rc and its wait in waits.
func (v *levels) reserve(size, now int64, rc *receipt, waits *waitSet) {
	for i := range v.lv {
		if r := v.lv[i].rate; r != nil {
			wait, g := r.take(size, now, rc)
			waits.add(wait, now, g)
		}
	}
}

// levelWait is the wait one level of a hierarchical charge imposes: when it
// ends, and the generation charged, whose Disable ends it early.
type levelWait struct {
	end int64 // nowNs at which the level allows the bytes
	gen *generation
}

// waitSet holds the waits of the Rate levels of a charge that must wait at
// all, so that Disable on one level ends the wait that level imposes alone.
// Up to eight levels fit without allocating.
type waitSet struct {
	fixed [8]levelWait
	extra []levelWait
	n     int
}

// add records a level's wait of wait from now, which Disable on g ends early.
// A wait short enough to be coalesced into the debt is left out.
func (w *waitSet) add(wait, now int64, g *generation) {
	if w == nil || wait <= coalesceWaitNs {
		return
	}
	lw := levelWait{end: addSat(now, wait), gen: g}
	if w.n < len(w.fixed) {
		w.fixed[w.n] = lw
	} else {
		w.extra = append(w.extra, lw)
	}
	w.n++
}

func (w *waitSet) at(i int) *levelWait {
	if i < len(w.fixed) {
		return &w.fixed[i]
	}
	return &w.extra[i-len(w.fixed)]
}

// sleep waits on t until every level in w allows the bytes. It sleeps until
// the level that ends last, which covers the others; if Disable on that
// level ends its wait, the others still apply. It returns ctx.Err() if ctx
// ends the wait first, and errSwitched if switchCh closes first.
func (w *waitSet) sleep(ctx context.Context, t *waitTimer, switchCh <-chan struct{}) error {
	var done <-chan struct{}
	if w.n > 0 {
		done = ctx.Done()
	}
	for w.n > 0 {
		last := 0
		for i := 1; i < w.n; i++ {
			if w.at(i).end > w.at(last).end {
				last = i
			}
		}
		lw := *w.at(last)
		wait := subSat(lw.end, nowNs())
		if wait <= coalesceWaitNs {
			return nil
		}
		switch t.sleep(wait, lw.gen, done, switchCh) {
		case canceled:
			return ctx.Err()
		case switched:
			return errSwitched
		case slept:
			return nil // the wait elapsed, and the others with it
		case stopped:
			// Disable ended this level's wait alone; the others still apply.
			*w.at(last) = *w.at(w.n - 1)
			w.n--
		}
	}
	return nil
}

// configMu serializes the changes that reaches checks, SetParent, the
// Group's limiter setters, Switch.Set and Budget.SetSteps, so that two of
// them cannot together make a Limiter charge itself when neither does
// alone. With it held, no tree of Groups changes its root.
var configMu sync.Mutex

// reaches reports whether charging l charges a Limiter for which f reports
// true: l itself, or one that l passes charges on to, through hierarchies,
// the current Limiters of Switches, the step Limits of Budgets and the
// levels of Groups. It runs when a Limiter is configured, to keep one from
// charging itself. configMu must be held.
func reaches(l Limiter, f func(Limiter) bool) bool {
	seen := map[Limiter]bool{}
	var visit func(Limiter) bool
	visit = func(l Limiter) bool {
		lv := classify(l)
		if lv.kind == kindNone {
			return false
		}
		if f(lv.l) {
			return true
		}
		if lv.kind == kindOther || seen[lv.l] {
			return false // another package's Limiter may not be comparable
		}
		seen[lv.l] = true
		switch lv.kind {
		case kindHierarchy:
			h := lv.l.(*hierarchy)
			for _, x := range h.lv {
				if visit(x.l) {
					return true
				}
			}
		case kindSwitch:
			return visit(lv.l.(*Switch).Limiter())
		case kindBudget:
			for _, s := range lv.l.(*Budget).Steps() {
				if visit(s.Limit) {
					return true
				}
			}
		case kindGroupReads, kindGroupWrites:
			// The Limiters of the Group's levels as they are now, read
			// without building them, as configMu keeps them.
			g := groupOf(lv.l)
			for a := g; a != nil; a = a.parent.Load() {
				own := a.ReadLimiter()
				if lv.kind == kindGroupWrites {
					own = a.WriteLimiter()
				}
				if visit(own) {
					return true
				}
			}
		case kindNone, kindOther, kindRate, kindCountReads, kindCountWrites:
			// Leaves: they charge no other Limiter of this package.
		}
		return false
	}
	return visit(l)
}
