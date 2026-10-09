package byterate

import (
	"math/bits"
	"sync"
	"time"
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
// InterruptTime) time.Since(time.Now()) stays 0 until the next tick.
var epoch = time.Now().Add(-time.Second) // Add keeps the monotonic reading

// nowNs returns monotonic nanoseconds since epoch.
func nowNs() int64 {
	return int64(time.Since(epoch))
}

// cacheLinePad separates fields written by different cores. 128 bytes covers
// the 128-byte lines of Apple silicon and the adjacent-line prefetch of x86.
type cacheLinePad [128]byte

var timerPool = sync.Pool{
	New: func() any {
		t := time.NewTimer(0)
		if !t.Stop() {
			select {
			case <-t.C:
			default:
			}
		}
		return t
	},
}

func getTimer(d time.Duration) *time.Timer {
	t := timerPool.Get().(*time.Timer)
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
	t.Reset(d)
	return t
}

func putTimer(t *time.Timer) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
	timerPool.Put(t)
}

// sleepNs waits waitNs, returning early when stopCh or done is closed.
// It reports false only when done was closed before the wait elapsed.
func sleepNs(waitNs int64, stopCh, done <-chan struct{}) bool {
	if waitNs <= 0 {
		return true
	}
	if waitNs <= shortWaitNs || (stopCh == nil && done == nil) {
		time.Sleep(time.Duration(waitNs))
		return true
	}

	t := getTimer(time.Duration(waitNs))
	defer putTimer(t)
	select {
	case <-t.C:
		return true
	case <-stopCh:
		return true
	case <-done:
		return false
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
	hi, lo := bits.Mul64(uint64(bytes), uint64(time.Second))
	if hi >= uint64(dtNs) { // quotient does not fit in 64 bits
		return maxI64
	}
	q, _ := bits.Div64(hi, lo, uint64(dtNs))
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

func clampSub(a, b int64) int64 {
	if b <= 0 {
		return a
	}
	if a < minI64+b {
		return minI64
	}
	return a - b
}
