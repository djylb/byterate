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
