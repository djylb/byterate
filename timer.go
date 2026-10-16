package byterate

import (
	"math/bits"
	"math/rand/v2"
	"sync/atomic"
	"time"
	"unsafe"
)

const (
	maxI64 = int64(^uint64(0) >> 1)
	minI64 = -maxI64 - 1

	sampleIntervalNs = int64(time.Second)
	coalesceWaitNs   = int64(200 * time.Microsecond)
	shortWaitNs      = int64(2 * time.Millisecond)
)

// epoch is the shared monotonic origin for Rate and Meter timestamps, so zero
// values and clones need no per-instance clock base and wall-clock steps do
// not affect sampling. It is set one second in the past so nowNs is never 0,
// which Meter reserves for "no window started": on coarse clocks (Windows
// InterruptTime) time.Since(time.Now()) stays 0 until the next tick. Inside
// a testing/synctest bubble, whose clock starts in 2000, nowNs is negative;
// arithmetic on it saturates rather than wraps.
var epoch = time.Now().Add(-time.Second) // Add keeps the monotonic reading

// nowNs returns monotonic nanoseconds since epoch.
func nowNs() int64 {
	return int64(time.Since(epoch))
}

// cacheLineSize covers the 128-byte lines of Apple silicon and the
// adjacent-line prefetch of x86. Objects whose size is a multiple of it are
// aligned to it by the allocator.
const cacheLineSize = 128

// cacheLinePad separates fields written by different cores.
type cacheLinePad [cacheLineSize]byte

// sleepNs waits waitNs, returning early when stopCh or done is closed.
// It reports false only when done was closed before the wait elapsed.
//
// Without done, a wait of up to shortWaitNs sleeps without a timer: only
// Stop could end it, and it ends within 2 ms anyway. A wait with done always
// watches it, however short, since a deadline or Close must end it.
func sleepNs(waitNs int64, stopCh, done <-chan struct{}) bool {
	return (*waitTimer)(nil).sleep(waitNs, stopCh, done)
}

// waitTimer keeps the timer of a connection direction's last wait for the
// next one, so that a throttled connection does not make a timer for every
// Read and Write. It is not shared between connections: a timer belongs to
// the testing/synctest bubble that made it, and fails fatally when used from
// another or from outside one. A nil *waitTimer makes a timer for each wait.
type waitTimer struct {
	t atomic.Pointer[time.Timer] // idle timer, nil while a wait holds it
}

// sleep is sleepNs with w's timer.
func (w *waitTimer) sleep(waitNs int64, stopCh, done <-chan struct{}) bool {
	if waitNs <= 0 {
		return true
	}
	if done == nil && (stopCh == nil || waitNs <= shortWaitNs) {
		time.Sleep(time.Duration(waitNs))
		return true
	}
	// A canceled operation needs no timer, including repeated writes after
	// Close. Keep the check before taking one, but do not skip a live
	// cancellation signal even for a sub-millisecond wait. A receive from one
	// channel takes no lock while the channel is open, unlike a select of
	// two, and the stop channel is shared with other waiters.
	if signaled(done) {
		return false
	}
	if signaled(stopCh) {
		return true
	}

	t := w.take(time.Duration(waitNs))
	defer w.put(t)
	select {
	case <-t.C:
		return true
	case <-stopCh:
		return true
	case <-done:
		return false
	}
}

// key spreads the waiters of a shared Rate over its generation's stop
// channels while keeping a connection direction on one, so that a Rate of
// its own makes no more channels than it has directions waiting. A nil w,
// the waiter of a plain Get, takes one at random.
func (w *waitTimer) key() uint32 {
	if w == nil {
		return rand.Uint32()
	}
	return uint32(uint64(uintptr(unsafe.Pointer(w))) * 0x9E3779B97F4A7C15 >> 32)
}

// signaled reports whether ch is closed.
func signaled(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// take returns a timer set to fire after d: w's idle one, or a new one if a
// concurrent wait holds it.
func (w *waitTimer) take(d time.Duration) *time.Timer {
	if w != nil {
		if t := w.t.Swap(nil); t != nil {
			// put left the channel empty, so a receive sees this setting
			// alone.
			t.Reset(d)
			return t
		}
	}
	return time.NewTimer(d)
}

// put stops t and keeps it for w's next wait.
func (w *waitTimer) put(t *time.Timer) {
	if !t.Stop() {
		// A timer that fired keeps its value in the channel when the program
		// runs with asynchronous timer channels (GODEBUG asynctimerchan=1),
		// which would end the next wait at once.
		select {
		case <-t.C:
		default:
		}
	}
	if w != nil {
		w.t.CompareAndSwap(nil, t)
	}
}

// bytesToNsCeil returns ceil(bytes*1e9/rate), saturated to maxI64.
func bytesToNsCeil(bytes, rate int64) int64 {
	if bytes <= 0 || rate <= 0 {
		return 0
	}
	hi, lo := bits.Mul64(uint64(bytes), uint64(time.Second))
	if hi >= uint64(rate) { // quotient does not fit in 64 bits
		return maxI64
	}
	q, r := bits.Div64(hi, lo, uint64(rate))
	if q >= uint64(maxI64) {
		return maxI64
	}
	if r != 0 {
		q++
	}
	return int64(q)
}

// bytesPerSec returns floor(bytes*1e9/dtNs), saturated to maxI64.
func bytesPerSec(bytes, dtNs int64) int64 {
	if bytes <= 0 || dtNs <= 0 {
		return 0
	}
	return mulDiv(bytes, int64(time.Second), dtNs)
}

// mulDiv returns floor(a*b/c) for a, b >= 0 and c > 0, saturated to maxI64.
func mulDiv(a, b, c int64) int64 {
	hi, lo := bits.Mul64(uint64(a), uint64(b))
	if hi >= uint64(c) { // quotient does not fit in 64 bits
		return maxI64
	}
	q, _ := bits.Div64(hi, lo, uint64(c))
	if q > uint64(maxI64) {
		return maxI64
	}
	return int64(q)
}

func clampAdd(a, b int64) int64 {
	if b <= 0 {
		return a
	}
	if a > maxI64-b {
		return maxI64
	}
	return a + b
}

// subSat returns a-b, saturated to the int64 range.
func subSat(a, b int64) int64 {
	d := a - b
	if (a < 0) != (b < 0) && (d < 0) != (a < 0) {
		if a < 0 {
			return minI64
		}
		return maxI64
	}
	return d
}

func clampSub(a, b int64) int64 {
	if b <= 0 {
		return a
	}
	if a < minI64+b {
		return minI64
	}
	return a - b
}
