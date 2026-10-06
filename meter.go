package byterate

import "sync/atomic"

// Meter measures inbound and outbound throughput without limiting it. All
// methods are safe for concurrent use and on a nil *Meter. The zero value is
// ready to use and starts its first sampling window on first use. A Meter must
// not be copied after first use; use Clone.
type Meter struct {
	lastSampleNs atomic.Int64 // ns since epoch, 0 => no window started yet
	inAcc        atomic.Int64
	outAcc       atomic.Int64
	inBps        atomic.Int64
	outBps       atomic.Int64
}

// NewMeter returns a Meter whose first sampling window starts now.
func NewMeter() *Meter {
	m := &Meter{}
	m.lastSampleNs.Store(nowNs())
	return m
}

// Clone returns an independent copy of m's counters and rates.
func (m *Meter) Clone() *Meter {
	if m == nil {
		return nil
	}
	cloned := &Meter{}
	cloned.lastSampleNs.Store(m.lastSampleNs.Load())
	cloned.inAcc.Store(m.inAcc.Load())
	cloned.outAcc.Store(m.outAcc.Load())
	cloned.inBps.Store(m.inBps.Load())
	cloned.outBps.Store(m.outBps.Load())
	return cloned
}

// Add records in inbound and out outbound bytes and closes the current
// sampling window once it is at least one second old.
func (m *Meter) Add(in, out int64) {
	if m == nil {
		return
	}
	if in != 0 {
		m.inAcc.Add(in)
	}
	if out != 0 {
		m.outAcc.Add(out)
	}
	m.roll(nowNs())
}

// Snapshot returns the inbound, outbound and total throughput in bytes per
// second over the last sampling window of at least one second, closing the
// current window if it is due.
func (m *Meter) Snapshot() (inBps, outBps, totalBps int64) {
	if m == nil {
		return 0, 0, 0
	}
	m.roll(nowNs())
	inBps = m.inBps.Load()
	outBps = m.outBps.Load()
	return inBps, outBps, clampAdd(inBps, outBps)
}

// Reset clears all counters and rates and starts a new sampling window.
func (m *Meter) Reset() {
	if m == nil {
		return
	}
	m.lastSampleNs.Store(nowNs())
	m.inAcc.Store(0)
	m.outAcc.Store(0)
	m.inBps.Store(0)
	m.outBps.Store(0)
}

func (m *Meter) roll(now int64) {
	if m == nil {
		return
	}
	last := m.lastSampleNs.Load()
	if last == 0 {
		m.lastSampleNs.CompareAndSwap(0, now) // zero value: first window starts now
		return
	}
	if now-last < sampleIntervalNs {
		return
	}
	if !m.lastSampleNs.CompareAndSwap(last, now) {
		return
	}
	delta := now - last
	m.inBps.Store(bytesPerSec(m.inAcc.Swap(0), delta))
	m.outBps.Store(bytesPerSec(m.outAcc.Swap(0), delta))
}
