package byterate

import (
	"context"
	"errors"
)

// errSwitched ends a wait whose Switch changed limiter, so that the charge
// moves to the new one.
var errSwitched = errors.New("byterate: limiter switched")

// level is one level of a limiter: a Rate, whose charges and waits the
// package handles itself, or another Limiter, called through its methods.
type level struct {
	rate  *Rate
	other Limiter
}

// levels views a Limiter as its levels without allocating: a Hierarchy's, or
// the Limiter itself as one level. A nil Limiter has one empty level.
type levels struct {
	one level
	h   []level
}

func levelsOf(l Limiter) levels {
	switch t := l.(type) {
	case *Rate:
		return levels{one: level{rate: t}}
	case *Hierarchy:
		if t != nil {
			return levels{h: t.levels}
		}
		return levels{}
	}
	return levels{one: level{other: l}}
}

func (v *levels) len() int {
	if v.h != nil {
		return len(v.h)
	}
	return 1
}

func (v *levels) at(i int) *level {
	if v.h != nil {
		return &v.h[i]
	}
	return &v.one
}

// allowance returns how much of size the Rate levels let through at now in
// one charge: all of it when every Rate's credit covers it, and otherwise
// the least of what each Rate grants, its credit or its quantum.
func (v *levels) allowance(size, now int64) int64 {
	n := v.len()
	i := 0
	for ; i < n; i++ {
		if r := v.at(i).rate; r != nil {
			creditNs, limit := r.credit(now)
			if !covers(size, creditNs, limit) {
				break
			}
		}
	}
	if i == n {
		return size
	}
	for i = 0; i < n; i++ {
		if r := v.at(i).rate; r != nil {
			size = min(size, r.allowance(size, now))
		}
	}
	return size
}

// quantum returns the least quantum of the Rate levels, or maxI64 if none
// limits.
func (v *levels) quantum() int64 {
	q := maxI64
	for i := range v.len() {
		if r := v.at(i).rate; r != nil {
			q = min(q, r.quantum())
		}
	}
	return q
}

// ready waits until the Rate levels have no debt worth a wait and the other
// levels are ready, and returns how much of size a transfer may take: the
// grant of every level or, when whole, all of size, which the other levels'
// Ready then only has to allow. It charges nothing. t is the timer to wait
// in, nil for one per wait; switchCh, when closed, ends the wait with
// errSwitched.
func (v *levels) ready(ctx context.Context, t *waitTimer, switchCh <-chan struct{}, size int64, whole bool) (int64, error) {
	// The other levels first, so that one refusing, such as an exhausted
	// Budget, does so before the Rates make the caller wait.
	size, err := v.readyOthers(ctx, t, size, whole)
	if err != nil {
		return 0, err
	}
	now := nowNs()
	var waits waitSet
	for i := range v.len() {
		if end, g := v.at(i).rate.debtEnd(now); end != 0 {
			waits.add(subSat(end, now), now, g)
		}
	}
	if waits.n > 0 {
		if err := waits.sleep(t, ctx, switchCh); err != nil {
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
// when whole.
func (v *levels) readyOthers(ctx context.Context, t *waitTimer, size int64, whole bool) (int64, error) {
	for i := range v.len() {
		o := v.at(i).other
		if o == nil {
			continue
		}
		var n int64
		var err error
		switch l := o.(type) {
		case *Switch:
			n, err = l.ready(ctx, t, size)
		case *Budget:
			n, err = l.ready(ctx, t, size)
		default:
			n, err = o.Ready(ctx, size)
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
	for i := range v.len() {
		lv := v.at(i)
		if lv.rate != nil {
			lv.rate.reserveAt(n, now)
		} else if lv.other != nil {
			if err := lv.other.Charge(n); err != nil && first == nil {
				first = err
			}
		}
	}
	return first
}

// wait charges a transfer of size bytes to every level before it happens and
// waits until each allows it. Unless whole, size is first cut to the grant
// of every level, as ready computes it, and a Rate whose credit went to other
// chargers meanwhile is charged a quantum instead. It returns the bytes
// charged, which receipt covers for a refund. A level that refuses the
// charge, with an error other than ctx's, refunds every level and leaves the
// receipt empty. When ctx or switchCh ends the wait, the charges stay in the
// receipt.
func (v *levels) wait(ctx context.Context, t *waitTimer, switchCh <-chan struct{}, size int64, whole bool, receipt *writeCharge) (int64, error) {
	// The other levels first, also for a whole charge, so that one refusing
	// does so before the others take the bytes and have to give them back.
	size, err := v.readyOthers(ctx, t, size, whole)
	if err != nil {
		return 0, err
	}
	now := nowNs()
	n := v.len()
	if !whole {
		size = v.allowance(size, now)
	}
	var waits waitSet
	v.reserve(size, now, receipt, &waits)
	if !whole && waits.n > 0 {
		// The credit the allowance saw went to other chargers first: queue a
		// quantum, not the credit, behind them.
		if q := v.quantum(); size > q {
			receipt.refund(size)
			*receipt = writeCharge{}
			waits = waitSet{}
			size = q
			v.reserve(size, nowNs(), receipt, &waits)
		}
	}
	for i := range n {
		o := v.at(i).other
		if o == nil {
			continue
		}
		charged := o
		var err error
		switch l := o.(type) {
		case *Switch:
			// The Limiter it charged, which a refund must reach after a Set.
			charged, err = l.wait(ctx, t, size)
		case *Budget:
			err = l.wait(ctx, t, size)
		default:
			err = o.Wait(ctx, size)
		}
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil && errors.Is(err, ctxErr) {
				receipt.addOther(charged) // canceled, so it kept the charge
				return size, err
			}
			receipt.refund(size)
			*receipt = writeCharge{}
			return 0, err
		}
		receipt.addOther(charged)
	}
	return size, waits.sleep(t, ctx, switchCh)
}

// reserve charges size to the Rate levels at now, keeping each one's receipt
// in receipt and its wait in waits.
func (v *levels) reserve(size, now int64, receipt *writeCharge, waits *waitSet) {
	n := v.len()
	for i := range n {
		if r := v.at(i).rate; r != nil {
			wait, gen := receipt.at(i, n).reserve(r, size, now)
			waits.add(wait, now, gen)
		}
	}
}

// rateCharge is a charge's receipt for one Rate: the generation it charged,
// and whether the charge added debt or was only metered, as it is while
// unlimited. A Rate disabled at the charge leaves no receipt, and an Enable
// or Reset since the charge voids it.
type rateCharge struct {
	rate *Rate
	gen  *generation
	debt bool
}

// writeCharge holds the receipts of a charge: one per Rate level, the common
// ones on the stack, and the other Limiters charged. A larger hierarchy
// needs a receipt for each additional level as well.
type writeCharge struct {
	fixed      [8]rateCharge
	extra      []rateCharge
	other      [2]Limiter
	moreOthers []Limiter
}

// addOther records that l, a Limiter other than a Rate, took the charge.
func (c *writeCharge) addOther(l Limiter) {
	if l == nil {
		return
	}
	for i := range c.other {
		if c.other[i] == nil {
			c.other[i] = l
			return
		}
	}
	c.moreOthers = append(c.moreOthers, l)
}

// reserve charges size to r at now and fills in c. It takes no lock: the
// charge reports the generation it was metered and charged in, whose Disable
// ends the wait it returns.
func (c *rateCharge) reserve(r *Rate, size, now int64) (int64, *generation) {
	if r == nil {
		return 0, nil
	}
	wait, g, metered, debt := r.reserveCore(size, now)
	if metered {
		c.rate, c.gen, c.debt = r, g, debt
	}
	r.notifyReserved(wait)
	return wait, g
}

// at returns the receipt of the i-th of n levels: in c's fixed array or, for
// more levels than it holds, in c's extra, which it makes on first use.
func (c *writeCharge) at(i, n int) *rateCharge {
	if i < len(c.fixed) {
		return &c.fixed[i]
	}
	if c.extra == nil {
		c.extra = make([]rateCharge, n-len(c.fixed))
	}
	return &c.extra[i-len(c.fixed)]
}

func (c *rateCharge) refund(size int64) {
	r := c.rate
	if r == nil || size <= 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	// Disable, or an Enable that retired the generation, cleared the charge.
	if !r.disabled.Load() && r.gen.Load() == c.gen {
		r.refund(c.gen, size, c.debt)
	}
}

func (c *writeCharge) refund(size int64) {
	for i := range c.fixed {
		c.fixed[i].refund(size)
	}
	for i := range c.extra {
		c.extra[i].refund(size)
	}
	for _, l := range c.other {
		if l != nil {
			l.Refund(size)
		}
	}
	for _, l := range c.moreOthers {
		l.Refund(size)
	}
}

// limiter returns the limiter d charges now: d's own, or the current one of
// a Switch, with the channel that closes when the Switch changes it.
func (d *direction) limiter() (Limiter, <-chan struct{}) {
	if s, ok := d.rate.(*Switch); ok {
		st := s.state()
		return st.l, st.changed
	}
	return d.rate, nil
}

// grantWrite charges d's limiter for the next part of a write of size bytes
// and waits until it may be sent. It returns how many bytes it charged,
// which a refund on error covers: all of size when whole, as for a datagram,
// and otherwise the grant of every level.
func (d *direction) grantWrite(size int64, whole bool, receipt *writeCharge) (int64, error) {
	// Capture the deadline generation before charging, just as reads do.
	g := d.cancel.current()
	if r, ok := d.rate.(*Rate); ok {
		return d.grantRate(r, g, size, whole, receipt)
	}
	for {
		l, switchCh := d.limiter()
		v := levelsOf(l)
		n, err := v.wait(g, &d.timer, switchCh, size, whole, receipt)
		if errors.Is(err, errSwitched) {
			receipt.refund(n)
			*receipt = writeCharge{}
			continue
		}
		return n, g.causeOf(err)
	}
}

// grantRate is grantWrite for a lone Rate, as levels.wait does it but
// without a waitSet.
func (d *direction) grantRate(r *Rate, g *waitGen, size int64, whole bool, receipt *writeCharge) (int64, error) {
	now := nowNs()
	if !whole {
		size = r.allowance(size, now)
	}
	wait, gen := receipt.fixed[0].reserve(r, size, now)
	if !whole && wait > coalesceWaitNs {
		// The credit the allowance saw went to other chargers first: queue a
		// quantum, not the credit, behind them.
		if q := r.quantum(); size > q {
			receipt.refund(size)
			*receipt = writeCharge{}
			size = q
			wait, gen = receipt.fixed[0].reserve(r, size, nowNs())
		}
	}
	if wait > coalesceWaitNs && d.timer.sleep(wait, gen, g.Done(), nil) == canceled {
		return size, g.cause()
	}
	return size, nil
}

// awaitRead waits until d's limiter may let a read through and returns how
// many of size bytes the read may take: all of them when whole, as for a
// datagram, and otherwise the grant of every level. It charges nothing, so a
// reader blocked in the wrapped connection holds no share of a limit;
// chargeRead charges what arrives.
func (d *direction) awaitRead(size int64, whole bool) (int64, error) {
	g := d.cancel.current()
	if r, ok := d.rate.(*Rate); ok {
		// levels.ready for a lone Rate, without a waitSet.
		now := nowNs()
		if end, gen := r.debtEnd(now); end != 0 {
			if d.timer.sleep(subSat(end, now), gen, g.Done(), nil) == canceled {
				return 0, g.cause()
			}
			now = nowNs()
		}
		if whole {
			return size, nil
		}
		return r.allowance(size, now), nil
	}
	for {
		l, switchCh := d.limiter()
		v := levelsOf(l)
		n, err := v.ready(g, &d.timer, switchCh, size, whole)
		if errors.Is(err, errSwitched) {
			continue
		}
		return n, g.causeOf(err)
	}
}

// chargeRead charges n bytes that a read received to d's limiter, as debt
// that the next read waits for, and returns the error of a level that
// refuses them, such as an exhausted Budget.
func (d *direction) chargeRead(n int64) error {
	l, _ := d.limiter()
	v := levelsOf(l)
	return v.charge(n)
}
