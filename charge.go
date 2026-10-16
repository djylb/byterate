package byterate

// rateCharge is a write's receipt for one Rate: the generation it charged,
// and whether the charge added debt or was only metered, as it is while
// unlimited. A Rate disabled at the charge leaves no receipt, and an Enable
// or Reset since the charge voids it.
type rateCharge struct {
	rate *Rate
	gen  *generation
	debt bool
}

// Keep common hierarchies on the write's stack.
// A larger hierarchy needs a receipt for each additional rate as well.
type writeCharge struct {
	fixed  [8]rateCharge
	extra  []rateCharge
	custom Limiter
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

// at returns the receipt of l's i-th Rate: in c's fixed array or, for a
// hierarchy larger than it, in c's extra, which it makes on first use.
func (c *writeCharge) at(i int, l *Hierarchy) *rateCharge {
	if i < len(c.fixed) {
		return &c.fixed[i]
	}
	if c.extra == nil {
		c.extra = make([]rateCharge, 3+len(l.extra)-len(c.fixed))
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
	if c.custom != nil {
		c.custom.Refund(size)
		return
	}
	for i := range c.fixed {
		c.fixed[i].refund(size)
	}
	for i := range c.extra {
		c.extra[i].refund(size)
	}
}

// grantWrite charges d's limiter for the next part of a write of size bytes
// and waits until it may be sent. It returns how many bytes it charged,
// which a refund on error covers: all of size when whole, as for a datagram,
// and otherwise the limiter's allowance, its credit or a quantum. A Limiter
// that cannot grant part of a request is charged all of size.
func (d *direction) grantWrite(size int64, whole bool, receipt *writeCharge) (int64, error) {
	// Capture the deadline generation before charging, just as reads do.
	g := d.cancel.current()
	switch l := d.rate.(type) {
	case *Rate:
		now := nowNs()
		if !whole {
			size = l.allowance(size, now)
		}
		wait, gen := receipt.fixed[0].reserve(l, size, now)
		if !whole && wait > coalesceWaitNs {
			// The credit the allowance saw went to other writers first:
			// queue a quantum, not the credit, behind them.
			if q := l.quantum(); size > q {
				receipt.refund(size)
				*receipt = writeCharge{}
				size = q
				wait, gen = receipt.fixed[0].reserve(l, size, nowNs())
			}
		}
		return size, d.sleep(wait, gen, g)
	case *Hierarchy:
		now := nowNs()
		if !whole {
			size = l.allowance(size, now)
		}
		var waits waitSet
		l.reserveGrant(size, now, receipt, &waits)
		if !whole && waits.n > 0 {
			// As for a Rate: credit seen by the allowance went to others.
			if q := l.quantum(); size > q {
				receipt.refund(size)
				*receipt = writeCharge{}
				waits = waitSet{}
				size = q
				l.reserveGrant(size, nowNs(), receipt, &waits)
			}
		}
		return size, d.sleepSet(&waits, g)
	default:
		receipt.custom = l
		return size, d.waitLimiter(g, size)
	}
}

// reserveGrant charges size to every Rate of l at now, keeping each Rate's
// receipt in receipt and its wait in waits.
func (l *Hierarchy) reserveGrant(size, now int64, receipt *writeCharge, waits *waitSet) {
	for i := range l.len() {
		wait, gen := receipt.at(i, l).reserve(l.rate(i), size, now)
		waits.add(wait, now, gen)
	}
}

// awaitRead waits until d's limiter may let a read through and returns how
// many of size bytes the read may take: all of them when whole, as for a
// datagram, and otherwise the limiter's allowance once its debt is repaid.
// It charges nothing, so a reader blocked in the wrapped connection holds no
// share of a limit; chargeRead charges what arrives. A Limiter that cannot
// grant part of a request lets size through and waits in chargeRead.
func (d *direction) awaitRead(size int64, whole bool) (int64, error) {
	g := d.cancel.current()
	switch l := d.rate.(type) {
	case *Rate:
		now := nowNs()
		if end, gen := l.debtEnd(now); end != 0 {
			if err := d.sleep(subSat(end, now), gen, g); err != nil {
				return 0, err
			}
			now = nowNs()
		}
		if whole {
			return size, nil
		}
		return l.allowance(size, now), nil
	case *Hierarchy:
		now := nowNs()
		var waits waitSet
		l.debts(now, &waits)
		if waits.n > 0 {
			if err := d.sleepSet(&waits, g); err != nil {
				return 0, err
			}
			now = nowNs()
		}
		if whole {
			return size, nil
		}
		return l.allowance(size, now), nil
	default:
		return size, nil
	}
}

// chargeRead charges n bytes that a read received. A *Rate or
// *Hierarchy takes them as debt, which the next read waits for
// before reading; another Limiter waits now, and the error that ends its wait
// is returned with the bytes.
func (d *direction) chargeRead(n int64) error {
	switch l := d.rate.(type) {
	case *Rate:
		l.reserveAt(n, nowNs())
	case *Hierarchy:
		l.reserve(n, nil)
	default:
		return d.waitLimiter(d.cancel.current(), n)
	}
	return nil
}

// waitLimiter charges size to d's limiter with Wait in generation g,
// returning g's cause if Close or a deadline ended the wait and any other
// error as it is.
func (d *direction) waitLimiter(g *waitGen, size int64) error {
	if err := d.rate.Wait(g, size); err != nil {
		if g.ended() {
			return g.cause()
		}
		return err
	}
	return nil
}
