package byterate

// rateCharge is a write's receipt for one Rate: the generation it charged,
// and whether the charge added debt or was only metered, as it is while
// unlimited. A Rate stopped at the charge leaves no receipt, and a restart
// since the charge voids it.
type rateCharge struct {
	rate *Rate
	gen  *stopSignal
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
// generation is read before and after the charge, and a charge that a
// Start came between leaves no receipt, since it may have landed on either
// side of the reset. That forgoes the refund of a write racing with a
// restart, which wakes its waiters anyway.
func (c *rateCharge) reserve(r *Rate, size, now int64) (int64, <-chan struct{}) {
	if r == nil {
		return 0, nil
	}
	gen := r.stop.Load()
	wait, metered, debt := r.reserveCore(size, now)
	if metered && gen != nil && r.stop.Load() == gen {
		c.rate, c.gen, c.debt = r, gen, debt
	}
	r.notifyReserved(wait)
	var stop <-chan struct{}
	if gen != nil {
		stop = gen.ch
	}
	return wait, r.startedStopCh(stop)
}

func (c *rateCharge) refund(size int64) {
	r := c.rate
	if r == nil || size <= 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.enabled.Load() || r.stop.Load() != c.gen {
		return
	}
	if c.debt {
		r.refund(size)
	} else {
		r.bytesAcc.Add(-size)
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
	var g *waitGen
	var wait int64
	var stop <-chan struct{}
	switch l := d.rate.(type) {
	case *Rate:
		g = d.cancel.current()
		wait, stop = receipt.fixed[0].reserve(l, size, nowNs())
	case *HierarchicalLimiter:
		g = d.cancel.current()
		now := nowNs()
		charge := func(c *rateCharge, r *Rate) {
			if delay, signal := c.reserve(r, size, now); delay > wait {
				wait, stop = delay, signal
			}
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
	default:
		receipt.custom = l
		return s.wait(d, size)
	}
	return d.sleep(wait, stop, g)
}
