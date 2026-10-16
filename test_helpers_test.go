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
func (l *Hierarchy) reserve(n int64, _ *waitSet) int64 {
	now := nowNs()
	var longest int64
	for _, lv := range l.levels {
		if lv.rate != nil {
			wait, _ := lv.rate.reserveAt(n, now)
			longest = max(longest, wait)
		}
	}
	return longest
}

// genOf returns r's generation, making it as a first charge would.
func genOf(r *Rate) *generation { return r.generation(nowNs()) }
