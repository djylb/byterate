package byterate

import "context"

// NewHierarchy combines limiters into one Limiter of nested levels, such as
// a connection's Rate within its user's Rate and Budget within a global
// Rate. It charges every level and waits until every one allows the bytes,
// so the strictest sets the pace. Every Rate also meters the traffic,
// whether it is limited, unlimited or disabled, so each level reports its
// own Throughput, and a limit set on any of them later applies to the next
// charge. Disable on a Rate ends the wait it imposes, and the others still
// apply; a disabled Rate limits nothing until Enable.
//
// The levels of a hierarchy given as a level take its place. Nil limiters,
// also nil pointers of this package's types, are left out, and so are
// repeats of a limiter of this package, so that a level given twice is
// charged once; unlimited Rates are kept, as they meter and can be limited
// later. NewHierarchy returns nil when no level remains and that level
// itself when one does. Pass nil for levels that should neither limit nor
// meter: an unlimited Rate still costs a charge per call.
//
// A level that refuses a charge, such as an exhausted Budget, is asked first
// and refuses it for the whole: Wait then gives the bytes back to every
// level that took them, and the others neither take them nor make the
// caller wait. TryWait charges every level or none: if any refuses, or would
// make the caller wait, the others take nothing, and a refusal is reported
// before ErrWouldWait. Charge, which records bytes already transferred,
// leaves them on every level and returns the first refusal. Refund gives
// the bytes back to every level; Ready returns the least that any level
// grants, at least 1.
// The views of a Counter are counted once the other levels accept the bytes.
//
// Every level is charged when the bytes are, not when the strictest lets them
// pass, so a looser Rate counts bytes still waiting on a stricter one. Each
// keeps its long-run limit, but when many waits on stricter levels end
// together, a looser Rate they share can pass more than its burst at once:
// up to the bytes of those waits.
func NewHierarchy(limiters ...Limiter) Limiter {
	h := &hierarchy{}
	for _, l := range limiters {
		h.add(l)
	}
	switch len(h.lv) + len(h.views) {
	case 0:
		return nil
	case 1:
		if len(h.lv) == 1 {
			return h.lv[0].l
		}
		return h.views[0].l
	}
	return h
}

// hierarchy is the Limiter of NewHierarchy: its levels that limit, in order,
// and the views of Counters, counted once the others accept a charge.
type hierarchy struct {
	lv     []level
	views  []level
	others bool // a level of lv is not a Rate
}

// add adds the levels of l, unless they are nil or already in h.
func (h *hierarchy) add(l Limiter) {
	lv := classify(l)
	switch lv.kind {
	case kindNone:
		return
	case kindHierarchy:
		inner := lv.l.(*hierarchy)
		for _, x := range inner.lv {
			h.add(x.l)
		}
		for _, x := range inner.views {
			h.add(x.l)
		}
		return
	case kindOther:
		// Not looked up: comparing another package's Limiters could panic
		// on an incomparable dynamic type.
	default:
		if h.has(lv.l) {
			return
		}
	}
	if lv.isView() {
		h.views = append(h.views, lv)
	} else {
		h.lv = append(h.lv, lv)
		h.others = h.others || lv.kind != kindRate
	}
}

// has reports whether l, a Limiter of this package, is a level of h.
func (h *hierarchy) has(l Limiter) bool {
	for _, x := range h.lv {
		if x.l == l {
			return true
		}
	}
	for _, x := range h.views {
		if x.l == l {
			return true
		}
	}
	return false
}

func (h *hierarchy) levels() levels { return levels{lv: h.lv, views: h.views, others: h.others} }

func (h *hierarchy) Wait(ctx context.Context, n int64) error {
	if n <= 0 {
		return nil
	}
	v := h.levels()
	if !h.others && h.views == nil {
		return v.waitRates(ctx, nil, n)
	}
	var rc receipt
	_, err := v.wait(ctx, nil, n, true, &rc)
	return err
}

func (h *hierarchy) TryWait(n int64) error {
	if n <= 0 {
		return nil
	}
	var rc receipt
	v := h.levels()
	return v.tryWait(n, nowNs(), &rc)
}

func (h *hierarchy) Refund(n int64) {
	if n > 0 {
		v := h.levels()
		v.refund(n)
	}
}

func (h *hierarchy) Ready(ctx context.Context, n int64) (int64, error) {
	if n <= 0 {
		return n, nil
	}
	v := h.levels()
	return v.ready(ctx, nil, n, false)
}

func (h *hierarchy) Charge(n int64) error {
	if n <= 0 {
		return nil
	}
	v := h.levels()
	return v.charge(n)
}
