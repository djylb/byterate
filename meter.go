package byterate

import "sync"

// window meters the throughput of a Counter in fixed sampling windows of
// sampleIntervalNs, whole seconds of its clock. Counting never reads the
// clock: the totals are sampled, by Throughput or Sample, and a sample once
// the clock has passed the open window's end closes it. The bytes since the
// last sample count evenly over the whole seconds since then, so a reader
// who samples every second sees each second on its own, one who samples
// every N seconds their average. The windows are the same for every reader,
// so what is reported does not depend on how many read it.
//
// Refunds that outweigh a window's counts, of bytes counted in an earlier
// one, report it as 0 and carry the rest forward.
type window struct {
	mu      sync.Mutex // serializes sampling and reading
	end     int64      // when the open window ends
	sampled bool       // a first sample was taken
	closed  bool       // a span was closed, ending at startAt
	bytes   [2]int64   // its bytes per second, per direction
	start   [2]int64   // the totals at which the open span began
	startAt int64      // when it began
}

// roll closes the open window, if now is past it, at totals, and opens the
// one that holds now. w.mu must be held.
func (w *window) roll(now int64, totals [2]int64) {
	if w.sampled && now < w.end {
		return
	}
	m := now % sampleIntervalNs
	if m < 0 {
		m += sampleIntervalNs
	}
	begin := now - m
	if !w.sampled {
		// The first sample: bytes counted before it are no throughput of
		// any second.
		w.start = totals
	} else {
		span := max(begin-w.startAt, sampleIntervalNs)
		for i := range totals {
			b := totals[i] - w.start[i]
			if b < 0 {
				b = 0 // more refunded than counted: carry the rest forward
			} else {
				w.start[i] = totals[i]
			}
			w.bytes[i] = mulDiv(b, sampleIntervalNs, span)
		}
		w.closed = true
	}
	w.sampled = true
	w.startAt = begin
	w.end = addSat(begin, sampleIntervalNs)
}

// last returns the bytes per second of the span that ended where the open
// window began, or 0s before the second sample. w.mu must be held.
func (w *window) last() [2]int64 {
	if w.closed {
		return w.bytes
	}
	return [2]int64{}
}
