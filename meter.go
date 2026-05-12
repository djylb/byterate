package byterate

import (
	"sync/atomic"
	"time"
)

type Meter struct {
	lastSampleNs int64
	inAcc        int64
	outAcc       int64
	inBps        int64
	outBps       int64
}

func NewMeter() *Meter {
	now := time.Now().UnixNano()
	return &Meter{lastSampleNs: now}
}

func (m *Meter) Clone() *Meter {
	if m == nil {
		return nil
	}
	cloned := &Meter{}
	atomic.StoreInt64(&cloned.lastSampleNs, atomic.LoadInt64(&m.lastSampleNs))
	atomic.StoreInt64(&cloned.inAcc, atomic.LoadInt64(&m.inAcc))
	atomic.StoreInt64(&cloned.outAcc, atomic.LoadInt64(&m.outAcc))
	atomic.StoreInt64(&cloned.inBps, atomic.LoadInt64(&m.inBps))
	atomic.StoreInt64(&cloned.outBps, atomic.LoadInt64(&m.outBps))
	return cloned
}

func (m *Meter) Add(in, out int64) {
	if m == nil {
		return
	}
	if in != 0 {
		atomic.AddInt64(&m.inAcc, in)
	}
	if out != 0 {
		atomic.AddInt64(&m.outAcc, out)
	}
	m.roll(time.Now().UnixNano())
}

func (m *Meter) Snapshot() (int64, int64, int64) {
	if m == nil {
		return 0, 0, 0
	}
	m.roll(time.Now().UnixNano())
	inBps := atomic.LoadInt64(&m.inBps)
	outBps := atomic.LoadInt64(&m.outBps)
	return inBps, outBps, inBps + outBps
}

func (m *Meter) Reset() {
	if m == nil {
		return
	}
	now := time.Now().UnixNano()
	atomic.StoreInt64(&m.lastSampleNs, now)
	atomic.StoreInt64(&m.inAcc, 0)
	atomic.StoreInt64(&m.outAcc, 0)
	atomic.StoreInt64(&m.inBps, 0)
	atomic.StoreInt64(&m.outBps, 0)
}

func (m *Meter) roll(nowNs int64) {
	if m == nil {
		return
	}
	last := atomic.LoadInt64(&m.lastSampleNs)
	if nowNs <= last || nowNs-last < sampleIntervalNs {
		return
	}
	if !atomic.CompareAndSwapInt64(&m.lastSampleNs, last, nowNs) {
		return
	}
	delta := nowNs - last
	if delta <= 0 {
		return
	}
	inBytes := atomic.SwapInt64(&m.inAcc, 0)
	outBytes := atomic.SwapInt64(&m.outAcc, 0)
	atomic.StoreInt64(&m.inBps, inBytes*sampleIntervalNs/delta)
	atomic.StoreInt64(&m.outBps, outBytes*sampleIntervalNs/delta)
}
