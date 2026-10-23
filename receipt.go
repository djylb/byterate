package byterate

// entry records one charge of a receipt: what took it and what a refund
// must check first. Its type tells a Rate's charge from a Budget's and from
// that of any other Limiter, which takes a plain Refund.
type entry struct {
	l    Limiter     // the Rate, Budget, Counter view or other Limiter charged
	gen  *generation // a Rate's generation charged, which an Enable since voids
	step *budgetStep // a Budget's step whose Limit took the charge, if any
	// A Rate's charge: 1 if it added debt rather than only being metered. A
	// Budget's: the period charged, which a Reset since voids.
	aux uint64
}

func budgetEntry(b *Budget, s *budgetStep, period uint64) entry {
	return entry{l: b, step: s, aux: period}
}

// receipt records the charges of one transfer, of the same size on every
// level, in the order they were made, so that a refund reaches each exactly
// once, also inside a Switch or a Budget's step: up to eight on the stack,
// more in extra. views are the Counter views of a hierarchy that counted the
// transfer, kept as their slice so that they take no entries.
type receipt struct {
	fixed [8]entry
	extra []entry
	n     int
	views []level
}

// mark is a point in a receipt to take back charges to.
type mark struct {
	n     int
	views bool
}

func (rc *receipt) mark() mark {
	if rc == nil {
		return mark{}
	}
	return mark{n: rc.n, views: rc.views != nil}
}

// reset empties rc for the next transfer, keeping extra's memory.
func (rc *receipt) reset() {
	rc.n = 0
	rc.extra = rc.extra[:0]
	rc.views = nil
}

// add records a charge.
func (rc *receipt) add(e entry) { *rc.next() = e }

// addRate records a Rate's charge in generation g, which added debt or was
// only metered, in place, as every charge of a Rate makes one.
func (rc *receipt) addRate(r *Rate, g *generation, debt bool) {
	e := rc.next()
	e.l, e.gen, e.step, e.aux = r, g, nil, 0
	if debt {
		e.aux = 1
	}
}

// next returns the entry for the next charge.
func (rc *receipt) next() *entry {
	rc.n++
	if rc.n <= len(rc.fixed) {
		return &rc.fixed[rc.n-1]
	}
	rc.extra = append(rc.extra, entry{})
	return &rc.extra[len(rc.extra)-1]
}

func (rc *receipt) at(i int) *entry {
	if i < len(rc.fixed) {
		return &rc.fixed[i]
	}
	return &rc.extra[i-len(rc.fixed)]
}

// count counts n bytes charged at now on views and records them.
func (rc *receipt) count(views []level, n, now int64) {
	if len(views) == 0 {
		return
	}
	for i := range views {
		views[i].count(n, now)
	}
	if rc == nil {
		return
	}
	if rc.views == nil {
		rc.views = views
		return
	}
	for i := range views {
		rc.add(entry{l: views[i].l})
	}
}

// refund gives back size bytes of every charge in rc and empties it.
func (rc *receipt) refund(size int64) { rc.refundFrom(mark{}, size) }

// refundFrom gives back size bytes of the charges made since m, latest
// first, and takes them out of rc.
func (rc *receipt) refundFrom(m mark, size int64) {
	// A nil receipt records nothing, so it has nothing to give back; it is
	// only used for whole charges to Rates alone, which never refuse.
	if rc == nil {
		return
	}
	if size > 0 {
		for i := rc.n - 1; i >= m.n; i-- {
			rc.at(i).refund(size)
		}
		if !m.views {
			for i := range rc.views {
				rc.views[i].l.Refund(size)
			}
		}
	}
	rc.n = m.n
	if len(rc.extra) > 0 {
		rc.extra = rc.extra[:max(m.n-len(rc.fixed), 0)]
	}
	if !m.views {
		rc.views = nil
	}
}

// refund gives back size bytes of the charge e records.
func (e *entry) refund(size int64) {
	switch l := e.l.(type) {
	case *Rate:
		l.mu.Lock()
		defer l.mu.Unlock()
		// An Enable that retired the generation cleared the charge; while
		// disabled, only its meter counts.
		if l.gen.Load() == e.gen {
			l.refund(e.gen, size, e.aux != 0 && !l.disabled.Load())
		}
	case *Budget:
		// The step's Limit is refunded by the entries after this one.
		l.release(size, e.aux)
		if e.step != nil {
			e.step.unowe(size)
		}
	default:
		l.Refund(size)
	}
}
