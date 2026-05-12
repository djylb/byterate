package byterate

import (
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

func sleepNs(waitNs int64, stopCh <-chan struct{}) {
	if waitNs <= 0 {
		return
	}
	if waitNs <= shortWaitNs || stopCh == nil {
		time.Sleep(time.Duration(waitNs))
		return
	}

	t := getTimer(time.Duration(waitNs))
	select {
	case <-t.C:
	case <-stopCh:
	}
	putTimer(t)
}

func bytesToNsCeil(bytes, rate int64) int64 {
	if bytes <= 0 || rate <= 0 {
		return 0
	}
	if bytes > maxI64/1e9 {
		return maxI64
	}
	num := bytes * 1e9
	return (num + rate - 1) / rate
}

func bytesPerSec(bytes, dtNs int64) int64 {
	if bytes <= 0 || dtNs <= 0 {
		return 0
	}
	q := bytes / dtNs
	rem := bytes % dtNs

	if q > maxI64/1e9 {
		return maxI64
	}
	res := q * 1e9

	if rem > 0 {
		if rem > maxI64/1e9 {
			add := maxI64 / dtNs
			if add > maxI64-res {
				return maxI64
			}
			res += add
		} else {
			add := (rem * 1e9) / dtNs
			if add > maxI64-res {
				return maxI64
			}
			res += add
		}
	}
	if res < 0 {
		return maxI64
	}
	return res
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
