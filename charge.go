package byterate

// rateCharge is a write's receipt for one Rate: the generation it charged,
// and whether the charge added debt or was only metered, as it is while
// unlimited. A Rate stopped at the charge leaves no receipt, and a restart
// since the charge voids it.
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
// charge reports the generation it was metered and charged in, whose Stop
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

func (c *rateCharge) refund(size int64) {
	r := c.rate
	if r == nil || size <= 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	// Stop, or a Start that retired the generation, cleared the charge.
	if r.enabled.Load() && r.gen.Load() == c.gen {
		r.refund(c.gen, size, c.debt)
	}
}

func (c *writeCharge) refund(size int64) {
	if c.custom != nil {
		c.custom.ReturnBucket(size)
		return
	}
	for i := range c.fixed {
		c.fixed[i].refund(size)
	}
	for i := range c.extra {
		c.extra[i].refund(size)
	}
}

func (s *rateConn) chargeWrite(size int64, receipt *writeCharge) error {
	d := &s.write
	// Capture the deadline generation before charging, just as Read does.
	switch l := d.rate.(type) {
	case *Rate:
		g := d.cancel.current()
		wait, gen := receipt.fixed[0].reserve(l, size, nowNs())
		return d.sleep(wait, gen, g)
	case *HierarchicalLimiter:
		g := d.cancel.current()
		now := nowNs()
		var waits waitSet
		charge := func(c *rateCharge, r *Rate) {
			wait, gen := c.reserve(r, size, now)
			waits.add(wait, now, gen)
		}
		charge(&receipt.fixed[0], l.first)
		charge(&receipt.fixed[1], l.second)
		charge(&receipt.fixed[2], l.third)
		if len(l.extra) > 0 {
			const inlineExtra = len(receipt.fixed) - 3
			if len(l.extra) > inlineExtra {
				receipt.extra = make([]rateCharge, len(l.extra)-inlineExtra)
			}
			for i, r := range l.extra {
				if i < inlineExtra {
					charge(&receipt.fixed[i+3], r)
				} else {
					charge(&receipt.extra[i-inlineExtra], r)
				}
			}
		}
		return d.sleepSet(&waits, g)
	default:
		receipt.custom = l
		return s.wait(d, size)
	}
}
