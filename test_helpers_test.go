package byterate

import "context"

// bg is the context of waits that nothing cancels.
var bg = context.Background()

// limiters returns rates as Limiters, for NewHierarchy.
func limiters(rates ...*Rate) []Limiter {
	l := make([]Limiter, len(rates))
	for i, r := range rates {
		l[i] = r
	}
	return l
}

// reserve charges n to every Rate of l at one point in time and returns the
// longest wait, without waiting.
func (h *hierarchy) reserve(n int64) int64 {
	now := nowNs()
	var longest int64
	for _, lv := range h.lv {
		if lv.rate != nil {
			wait, _ := lv.rate.reserveAt(n, now)
			longest = max(longest, wait)
		}
	}
	return longest
}

// genOf returns r's generation, making it as a first charge would.
func genOf(r *Rate) *generation { return r.generation(nowNs()) }

// reserve charges size to r without waiting and returns how long a Wait
// would wait.
func (r *Rate) reserve(size int64) int64 {
	wait, _ := r.reserveAt(size, nowNs())
	return wait
}

// stopCh returns a channel that Disable closes.
func (r *Rate) stopCh() <-chan struct{} {
	if r == nil {
		return nil
	}
	return r.gen.Load().waitCh(0)
}
