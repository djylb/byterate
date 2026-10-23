package byterate

import (
	"sync"
	"sync/atomic"
)

// window meters throughput in fixed sampling windows of sampleIntervalNs,
// slices of the package's clock from epoch on, for Rates and Counters alike: a window holds the bytes
// counted while the clock was in it, so what Throughput reports depends on
// when the bytes passed, and not on who reads it, how often or when. A count
// that carries its time closes the open window once that time is past its
// end, before counting; Throughput closes it the same way and reports the
// window just before the current one, or nothing if no bytes passed then.
//
// The bytes of the open window are the totals counted, which the owner
// keeps, less the totals at which the window began; the owner reads the
// totals with w.mu held, so that they are not older than the window. Refunds that outweigh a
// window's counts, of bytes counted in an earlier one, report it as 0 and
// carry the rest forward.
type window struct {
	end      atomic.Int64 // nowNs at which the open window ends, ^minI64 so that the zero value is before any time
	mu       sync.Mutex   // serializes closing and reading
	closedAt int64        // nowNs at which the last closed window ended
	bytes    [2]int64     // its bytes, per direction
	start    [2]int64     // the totals at which the open window began
}

// due reports whether a count at now must close the open window first. The
// counter then closes it unless w.mu is held, by another counter closing it
// or a reader that closes it as well.
func (w *window) due(now int64) bool { return now >= w.end.Load()^minI64 }

// roll closes the open window, if due at now, at totals, and opens the one
// that holds now. w.mu must be held.
func (w *window) roll(now int64, totals [2]int64) {
	end := w.end.Load() ^ minI64
	if now < end {
		return
	}
	if end == minI64 {
		// The first window: bytes counted before it, without a time, are
		// no throughput of any second.
		w.start = totals
	} else {
		// Bytes counted in the open window belong to it, however late it
		// closes: counts with a later time would have closed it first.
		for i := range totals {
			b := totals[i] - w.start[i]
			if b < 0 {
				b = 0 // more refunded than counted: carry the rest forward
			} else {
				w.start[i] = totals[i]
			}
			w.bytes[i] = b
		}
		w.closedAt = end
	}
	m := now % sampleIntervalNs
	if m < 0 {
		m += sampleIntervalNs
	}
	w.end.Store(addSat(now-m, sampleIntervalNs) ^ minI64)
}

// last returns the bytes per second of the window before the open one, or
// 0s if none was closed there. w.mu must be held.
func (w *window) last() [2]int64 {
	if end := w.end.Load() ^ minI64; end != minI64 && w.closedAt == end-sampleIntervalNs {
		return w.bytes // a window is a second long, so its bytes are its bytes per second
	}
	return [2]int64{}
}

// copyFrom copies src's state into w, which nothing else uses yet.
func (w *window) copyFrom(src *window) {
	src.mu.Lock()
	defer src.mu.Unlock()
	w.end.Store(src.end.Load())
	w.closedAt, w.bytes, w.start = src.closedAt, src.bytes, src.start
}
