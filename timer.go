package byterate

import (
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

	// A stream wrapper without credit lets about quantumNs of traffic through
	// per charge, at least minQuantum and at most maxQuantum bytes, so that a
	// large Read or Write neither bursts nor holds a shared limit for long.
	quantumNs  = int64(20 * time.Millisecond)
	minQuantum = int64(512)
	maxQuantum = int64(64 << 10)
)

// epoch is the shared monotonic origin for the timestamps of Rates, Counters
// and Budgets, so zero values and clones need no per-instance clock base and
// wall-clock steps do not affect them. It is set one second in the past, so
// that nowNs is positive outside a testing/synctest bubble. Inside one, whose
// clock starts in 2000, nowNs is negative; arithmetic on it saturates rather
// than wraps.
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

// sleepResult says how a wait ended.
type sleepResult uint8

const (
	slept    sleepResult = iota // the wait elapsed
	stopped                     // Disable on the generation ended it
	canceled                    // done was closed first
	switched                    // a Switch the wait went through changed limiter
)

// waitTimer keeps the timer of a connection direction's last wait for the
// next one, so that a throttled connection does not make a timer for every
// Read and Write. It is not shared between connections: a timer belongs to
// the testing/synctest bubble that made it, and fails fatally when used from
// another or from outside one. A nil *waitTimer makes a timer for each wait.
type waitTimer struct {
	t atomic.Pointer[time.Timer] // idle timer, nil while a wait holds it
}

// sleep waits waitNs in w's timer, returning early when Disable on g, done
// or switched ends the wait. A nil g has no Disable.
//
// Without done and switched, a wait of up to shortWaitNs sleeps without a
// timer and without taking one of g's stop channels: only Disable could end
// it, and it ends within 2 ms anyway. A wait with done always watches it,
// however short, since a deadline or Close must end it.
func (w *waitTimer) sleep(waitNs int64, g *generation, done, switchCh <-chan struct{}) sleepResult {
	if waitNs <= 0 {
		return slept
	}
	if done == nil && switchCh == nil && (g == nil || waitNs <= shortWaitNs) {
		time.Sleep(time.Duration(waitNs))
		return slept
	}
	// A canceled operation needs no timer, including repeated writes after
	// Close. Keep the check before taking one, but do not skip a live
	// cancellation signal even for a sub-millisecond wait. A receive from one
	// channel takes no lock while the channel is open, unlike a select of
	// two, and the stop channel is shared with other waiters.
	if signaled(done) {
		return canceled
	}
	if signaled(switchCh) {
		return switched
	}
	stopCh := g.waitCh(w.key())
	if signaled(stopCh) {
		return stopped
	}

	t := w.take(time.Duration(waitNs))
	defer w.put(t)
	select {
	case <-t.C:
		return slept
	case <-stopCh:
		return stopped
	case <-done:
		return canceled
	case <-switchCh:
		return switched
	}
}

// key spreads the waiters of a shared Rate over its generation's stop
// channels while keeping a connection direction on one, so that a Rate of
// its own makes no more channels than it has directions waiting. A nil w,
// the waiter of a plain Wait, takes one at random.
func (w *waitTimer) key() uint32 {
	if w == nil {
		return rand.Uint32()
	}
	return mix32(uintptr(unsafe.Pointer(w)))
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
