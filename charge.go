package byterate

// rateCharge is a write's receipt. Unlimited charges affect the meter only;
// stopped rates affect neither it nor the debt. A restart invalidates both.
type rateCharge struct {
	rate    *Rate
	epoch   uint64
	metered bool
	debt    bool
}

// Keep common hierarchies on the write's stack.
// A larger hierarchy needs a receipt for each additional rate as well.
type writeCharge struct {
	fixed  [8]rateCharge
	extra  []rateCharge
	custom Limiter
}

func (c *rateCharge) reserve(r *Rate, size, now int64) (int64, <-chan struct{}) {
	if r == nil {
		return 0, nil
	}
	r.mu.Lock()
	c.rate, c.epoch = r, r.startEpoch.Load()
	c.metered = r.enabled.Load()
	c.debt = c.metered && r.rate.Load() > 0
	stop := r.stopCh()
	wait := r.reserveCore(size, now)
	r.mu.Unlock()
	r.notifyReserved(wait)
	return wait, stop
}

func (c *rateCharge) refund(size int64) {
	r := c.rate
	if r == nil || !c.metered || size <= 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.enabled.Load() || c.epoch != r.startEpoch.Load() {
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
	if wait > coalesceWaitNs && !sleepNs(wait, stop, g.Done()) {
		return g.cause()
	}
	return nil
}
