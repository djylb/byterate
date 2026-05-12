package byterate

import (
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"
)

func TestBytesToNsCeil(t *testing.T) {
	t.Parallel()

	if got := bytesToNsCeil(0, 100); got != 0 {
		t.Fatalf("bytesToNsCeil(0,100)=%d, want 0", got)
	}
	if got := bytesToNsCeil(100, 0); got != 0 {
		t.Fatalf("bytesToNsCeil(100,0)=%d, want 0", got)
	}
	if got := bytesToNsCeil(1, 2); got != 500000000 {
		t.Fatalf("bytesToNsCeil(1,2)=%d, want 500000000", got)
	}
	if got := bytesToNsCeil(3, 2); got != 1500000000 {
		t.Fatalf("bytesToNsCeil(3,2)=%d, want 1500000000", got)
	}
	if got := bytesToNsCeil(maxI64, 1); got != maxI64 {
		t.Fatalf("bytesToNsCeil(maxI64,1)=%d, want maxI64", got)
	}
}

func TestBytesPerSec(t *testing.T) {
	t.Parallel()

	if got := bytesPerSec(0, 100); got != 0 {
		t.Fatalf("bytesPerSec(0,100)=%d, want 0", got)
	}
	if got := bytesPerSec(100, 0); got != 0 {
		t.Fatalf("bytesPerSec(100,0)=%d, want 0", got)
	}
	if got := bytesPerSec(5, int64(time.Second)); got != 5 {
		t.Fatalf("bytesPerSec(5,1s)=%d, want 5", got)
	}
	if got := bytesPerSec(3, int64(2*time.Second)); got != 1 {
		t.Fatalf("bytesPerSec(3,2s)=%d, want 1", got)
	}
	if got := bytesPerSec(maxI64, 1); got != maxI64 {
		t.Fatalf("bytesPerSec(maxI64,1)=%d, want maxI64", got)
	}
}

func TestClampHelpers(t *testing.T) {
	t.Parallel()

	if got := clampAdd(1, 2); got != 3 {
		t.Fatalf("clampAdd(1,2)=%d, want 3", got)
	}
	if got := clampAdd(maxI64-1, 10); got != maxI64 {
		t.Fatalf("clampAdd overflow=%d, want maxI64", got)
	}
	if got := clampSub(5, 2); got != 3 {
		t.Fatalf("clampSub(5,2)=%d, want 3", got)
	}
	if got := clampSub(minI64+1, 10); got != minI64 {
		t.Fatalf("clampSub underflow=%d, want minI64", got)
	}
}

func TestRateLifecycleAndJSON(t *testing.T) {
	r := NewRate(1024)
	if r == nil {
		t.Fatal("NewRate returned nil")
	}
	if got := r.Limit(); got != 1024 {
		t.Fatalf("Limit()=%d, want 1024", got)
	}

	r.Stop()
	r.Get(1024)
	if got := r.Now(); got != 0 {
		t.Fatalf("Now() after Stop/Get=%d, want 0", got)
	}

	r.Start()
	r.SetLimit(2048)
	if got := r.Limit(); got != 2048 {
		t.Fatalf("Limit() after SetLimit=%d, want 2048", got)
	}

	r.ResetLimit(0)
	if got := r.Limit(); got != 0 {
		t.Fatalf("Limit() after ResetLimit(0)=%d, want 0", got)
	}

	b, err := r.MarshalJSON()
	if err != nil {
		t.Fatalf("MarshalJSON error: %v", err)
	}
	var out map[string]int64
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("json.Unmarshal error: %v", err)
	}
	if out["Limit"] != 0 {
		t.Fatalf("MarshalJSON Limit=%d, want 0", out["Limit"])
	}
}

func TestNilRateSafety(t *testing.T) {
	var r *Rate
	r.SetLimit(1)
	r.ResetLimit(1)
	r.Start()
	r.Stop()
	r.Get(1)
	r.ReturnBucket(1)

	if got := r.Limit(); got != 0 {
		t.Fatalf("nil Limit()=%d, want 0", got)
	}
	if got := r.Now(); got != 0 {
		t.Fatalf("nil Now()=%d, want 0", got)
	}
	b, err := r.MarshalJSON()
	if err != nil {
		t.Fatalf("nil MarshalJSON error: %v", err)
	}
	if string(b) != "null" {
		t.Fatalf("nil MarshalJSON=%s, want null", string(b))
	}
}

func TestRateCloneDetachesRuntimeState(t *testing.T) {
	original := NewRate(2048)
	original.Get(64)

	cloned := original.Clone()
	if cloned == nil {
		t.Fatal("Clone() = nil, want detached rate copy")
	}
	if cloned == original {
		t.Fatal("Clone() returned original pointer, want detached copy")
	}
	if cloned.Limit() != original.Limit() {
		t.Fatalf("Clone().Limit() = %d, want %d", cloned.Limit(), original.Limit())
	}
	if cloned.stopCh() == original.stopCh() {
		t.Fatal("Clone() should not reuse the original stop channel")
	}

	cloned.ResetLimit(0)
	if got := original.Limit(); got != 2048 {
		t.Fatalf("original limit after clone mutation = %d, want 2048", got)
	}

	original.Stop()
	if cloned.stopCh() == nil {
		t.Fatal("clone stop channel = nil after original Stop(), want independent channel")
	}
	select {
	case <-cloned.stopCh():
		t.Fatal("clone stop channel closed when original stopped")
	default:
	}
}

func TestMeterCloneDetachesMutableState(t *testing.T) {
	original := NewMeter()
	original.Add(3, 4)

	cloned := original.Clone()
	if cloned == nil {
		t.Fatal("Clone() = nil, want detached meter copy")
	}
	if cloned == original {
		t.Fatal("Clone() returned original pointer, want detached copy")
	}
	if atomic.LoadInt64(&cloned.inAcc) != atomic.LoadInt64(&original.inAcc) || atomic.LoadInt64(&cloned.outAcc) != atomic.LoadInt64(&original.outAcc) {
		t.Fatalf("Clone() accumulators = %d/%d, want %d/%d",
			atomic.LoadInt64(&cloned.inAcc),
			atomic.LoadInt64(&cloned.outAcc),
			atomic.LoadInt64(&original.inAcc),
			atomic.LoadInt64(&original.outAcc),
		)
	}

	cloned.Add(5, 6)
	if got := atomic.LoadInt64(&original.inAcc); got != 3 {
		t.Fatalf("original inAcc after clone mutation = %d, want 3", got)
	}
	if got := atomic.LoadInt64(&original.outAcc); got != 4 {
		t.Fatalf("original outAcc after clone mutation = %d, want 4", got)
	}
}
