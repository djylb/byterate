package byterate

import (
	"math/rand/v2"
	"sync/atomic"
	"time"
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

// cacheLinePad separates fields written by different cores. cacheLineSize,
// set per architecture, is the size of the lines it pads to; objects whose
// size is a multiple of it are aligned to it by the allocator. A field set
// padded with [(cacheLineSize - unsafe.Sizeof(f)%cacheLineSize) %
// cacheLineSize]byte fills whole lines at the sizes of the architecture.
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
	// The direction as a flow of fair Rates (Rate.SetFair): when its last
	// charge was allowed, as nowNs, the bytes it charged since it last
	// paused, and whether its last ready went through a sparse lane.
	flowEnd   atomic.Int64
	flowBytes atomic.Int64
	flowLane  atomic.Bool
}

// sparseGapNs is how long a flow must pause after its last charge was
// allowed to count as sparse again: longer than a busy flow takes to come
// back for its next grant.
const sparseGapNs = shortWaitNs

// sparse reports whether w's flow is sparse at now for a charge of size
// bytes on a Rate of quantum q: since it last paused, its charges with
// this one stay within a quantum. A nil w is no flow.
func (w *waitTimer) sparse(now, size, q int64) bool {
	if w == nil {
		return false
	}
	b := w.flowBytes.Load()
	if subSat(now, w.flowEnd.Load()) >= sparseGapNs {
		b = 0 // paused
	}
	return addSat(b, size) <= q
}

// noteFlow records a charge of n bytes of w's flow at now that waits
// waitNs, and whether it went through a sparse lane, for a read to charge
// what it receives there too.
func (w *waitTimer) noteFlow(now, waitNs, n int64, lane bool) {
	if w == nil {
		return
	}
	if subSat(now, w.flowEnd.Load()) >= sparseGapNs {
		w.flowBytes.Store(n)
	} else {
		w.flowBytes.Add(n)
	}
	w.flowEnd.Store(addSat(now, max(waitNs, 0)))
	w.flowLane.Store(lane)
}

// received records n bytes that w's flow received after a ready, which
// noteFlow recorded with none.
func (w *waitTimer) received(n int64) {
	if w != nil && n > 0 {
		w.flowBytes.Add(n)
	}
}

// laneCharge reports whether the charge of what w's flow received goes to
// the sparse lanes of its fair Rates too.
func (w *waitTimer) laneCharge() bool { return w != nil && w.flowLane.Load() }

// sleep waits waitNs in w's timer, returning early when Disable on g, done
// or switched ends the wait. A nil g has no Disable.
//
// Without done and switched, a wait of up to shortWaitNs sleeps without a
// timer and without taking one of g's stop channels: only Disable could end
// it, and it ends within 2 ms anyway. A wait with done always watches it,
// however short, since a deadline or Close must end it.
func (w *waitTimer) sleep(waitNs int64, g *generation, done, switchCh <-chan struct{}) sleepResult {
	return w.sleepOn(waitNs, g.stops(), done, switchCh)
}

// sleepOn is sleep with the channels of g, nil for none, which stopped
// says closed.
func (w *waitTimer) sleepOn(waitNs int64, g *stopper, done, switchCh <-chan struct{}) sleepResult {
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
		return rand.Uint32() //nolint:gosec // spreads waiters over channels, no secret
	}
	return mix32(addrOf(w))
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
