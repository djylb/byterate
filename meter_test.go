package byterate

import (
	"testing"
	"time"
)

func TestMeterSnapshotReportsRecentRates(t *testing.T) {
	meter := NewMeter()
	meter.Add(300, 500)
	time.Sleep(1100 * time.Millisecond)
	inBps, outBps, totalBps := meter.Snapshot()
	if inBps <= 0 || outBps <= 0 || totalBps != inBps+outBps {
		t.Fatalf("Snapshot() = %d/%d/%d, want positive rates with total sum", inBps, outBps, totalBps)
	}
}

func TestMeterLargeWindowDoesNotOverflow(t *testing.T) {
	m := NewMeter()
	m.Add(10<<30, 9<<30)
	m.roll(m.lastSampleNs.Load() + int64(1100*time.Millisecond))
	inBps, outBps, totalBps := m.Snapshot()
	if inBps != 9_761_289_309 || outBps != 8_785_160_378 || totalBps != inBps+outBps {
		t.Fatalf("Snapshot() = %d/%d/%d, want 9761289309/8785160378/sum", inBps, outBps, totalBps)
	}

	m.Reset()
	m.Add(-500, 0)
	m.roll(m.lastSampleNs.Load() + int64(time.Second))
	if inBps, _, _ := m.Snapshot(); inBps != 0 {
		t.Fatalf("Snapshot() in after negative Add = %d, want 0", inBps)
	}
}

func TestZeroValueMeterStartsWindowOnFirstUse(t *testing.T) {
	var m Meter
	m.Add(100, 50)
	last := m.lastSampleNs.Load()
	if last < int64(time.Second) || last > nowNs() {
		t.Fatalf("lastSampleNs after first Add = %d, want a monotonic timestamp in [1s, %d]", last, nowNs())
	}
	m.roll(last + int64(time.Second))
	inBps, outBps, totalBps := m.Snapshot()
	if inBps != 100 || outBps != 50 || totalBps != 150 {
		t.Fatalf("Snapshot() = %d/%d/%d, want 100/50/150", inBps, outBps, totalBps)
	}
}

func TestMeterUsesMonotonicEpoch(t *testing.T) {
	// Wall-clock UnixNano stamps (~1.7e18) would exceed nowNs, which counts
	// monotonic time since the package epoch.
	m := NewMeter()
	if got := m.lastSampleNs.Load(); got < int64(time.Second) || got > nowNs() {
		t.Fatalf("NewMeter lastSampleNs = %d, want monotonic ns since epoch", got)
	}
	m.Reset()
	if got := m.lastSampleNs.Load(); got < int64(time.Second) || got > nowNs() {
		t.Fatalf("Reset lastSampleNs = %d, want monotonic ns since epoch", got)
	}
}

func TestNowNsNeverHitsMeterSentinel(t *testing.T) {
	// Meter treats lastSampleNs==0 as "no window started"; nowNs must never
	// return 0, even on clocks that only tick every few milliseconds.
	if got := nowNs(); got < int64(time.Second) {
		t.Fatalf("nowNs() = %d, want >= 1s so it never equals Meter's 0 sentinel", got)
	}
}
