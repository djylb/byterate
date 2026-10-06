package byterate

import (
	"context"
	"encoding/json"
	"errors"
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

	// Inputs whose intermediate product exceeds int64 must stay exact.
	for _, tc := range []struct{ bytes, rate, want int64 }{
		{10_000_000_000, 10_000_000_000, 1_000_000_000},
		{10 << 30, 10 << 30, 1_000_000_000},
		{9_000_000_000, 1 << 62, 2},
		{32768, maxI64, 1},
		{maxI64, maxI64, 1_000_000_000},
		{maxI64, 1_000_000_000, maxI64},
		{maxI64, 1_000_000_001, 9_223_372_027_631_403_780},
	} {
		if got := bytesToNsCeil(tc.bytes, tc.rate); got != tc.want {
			t.Fatalf("bytesToNsCeil(%d,%d)=%d, want %d", tc.bytes, tc.rate, got, tc.want)
		}
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

	// Inputs whose intermediate product exceeds int64 must stay exact.
	for _, tc := range []struct{ bytes, dtNs, want int64 }{
		{9_500_000_000, 10_000_000_000, 950_000_000},
		{99_000_000_000, 100_000_000_000, 990_000_000},
		{10_000_000_000, 20_000_000_000, 500_000_000},
		{10 << 30, int64(1100 * time.Millisecond), 9_761_289_309},
		{maxI64, int64(time.Second), maxI64},
		{maxI64, maxI64, 1_000_000_000},
	} {
		if got := bytesPerSec(tc.bytes, tc.dtNs); got != tc.want {
			t.Fatalf("bytesPerSec(%d,%d)=%d, want %d", tc.bytes, tc.dtNs, got, tc.want)
		}
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
	if cloned.inAcc.Load() != original.inAcc.Load() || cloned.outAcc.Load() != original.outAcc.Load() {
		t.Fatalf("Clone() accumulators = %d/%d, want %d/%d",
			cloned.inAcc.Load(),
			cloned.outAcc.Load(),
			original.inAcc.Load(),
			original.outAcc.Load(),
		)
	}

	cloned.Add(5, 6)
	if got := original.inAcc.Load(); got != 3 {
		t.Fatalf("original inAcc after clone mutation = %d, want 3", got)
	}
	if got := original.outAcc.Load(); got != 4 {
		t.Fatalf("original outAcc after clone mutation = %d, want 4", got)
	}
}

func TestRateLargeGetDoesNotPoisonLimiter(t *testing.T) {
	r := NewRate(10 << 30)
	if wait := r.reserve(10 << 30); wait != 0 {
		t.Fatalf("reserve(10GiB) at 10GiB/s with full burst wait=%s, want 0", time.Duration(wait))
	}
	if wait := r.reserve(20 << 30); wait < int64(900*time.Millisecond) || wait > int64(time.Second) {
		t.Fatalf("reserve(20GiB) after burst wait=%s, want about 1s", time.Duration(wait))
	}
	if wait := r.reserve(1); wait > int64(2*time.Second) {
		t.Fatalf("reserve(1) after large reservations wait=%s, want about 1s", time.Duration(wait))
	}
}

func TestNewRateStartsWithFullBurst(t *testing.T) {
	r := NewRate(1000)
	if wait := r.reserve(2000); wait != 0 {
		t.Fatalf("reserve(2000) within burst wait=%s, want 0", time.Duration(wait))
	}
	if wait := r.reserve(1000); wait < int64(900*time.Millisecond) || wait > int64(time.Second) {
		t.Fatalf("reserve(1000) after burst wait=%s, want about 1s", time.Duration(wait))
	}
}

func TestZeroValueRateLimitsAfterStart(t *testing.T) {
	var r Rate
	if wait := r.reserve(1 << 20); wait != 0 {
		t.Fatalf("zero Rate before Start wait=%s, want 0", time.Duration(wait))
	}
	r.SetLimit(1000)
	r.Start()
	if wait := r.reserve(2000); wait != 0 {
		t.Fatalf("reserve(2000) within burst wait=%s, want 0", time.Duration(wait))
	}
	if wait := r.reserve(1000); wait < int64(900*time.Millisecond) || wait > int64(time.Second) {
		t.Fatalf("reserve(1000) after burst wait=%s, want about 1s", time.Duration(wait))
	}
	last := r.lastSampleNs.Load()
	r.updateRateWithNow(last + int64(2*time.Second))
	if got := r.nowBps.Load(); got != 1500 {
		t.Fatalf("nowBps after 3000 bytes over 2s = %d, want 1500", got)
	}

	cloned := (&Rate{}).Clone()
	cloned.ResetLimit(1000)
	cloned.reserve(2000)
	if wait := cloned.reserve(1000); wait < int64(900*time.Millisecond) || wait > int64(time.Second) {
		t.Fatalf("clone of zero Rate reserve(1000) after burst wait=%s, want about 1s", time.Duration(wait))
	}
}

func TestRateSetLimitKeepsDebtResetLimitClearsIt(t *testing.T) {
	r := NewRate(1)
	r.reserve(100)
	r.SetLimit(1 << 30)
	if wait := r.reserve(1); wait < int64(90*time.Second) {
		t.Fatalf("reserve(1) after SetLimit wait=%s, want the old debt kept", time.Duration(wait))
	}
	r.ResetLimit(1 << 30)
	if wait := r.reserve(1); wait != 0 {
		t.Fatalf("reserve(1) after ResetLimit wait=%s, want 0", time.Duration(wait))
	}
}

func TestRateMarshalJSONReportsZeroWhenIdle(t *testing.T) {
	r := NewRate(0)
	r.reserve(500)
	r.lastSampleNs.Store(nowNs() - int64(2*time.Second))

	nowRate := func() int64 {
		b, err := r.MarshalJSON()
		if err != nil {
			t.Fatalf("MarshalJSON error: %v", err)
		}
		var out map[string]int64
		if err := json.Unmarshal(b, &out); err != nil {
			t.Fatalf("json.Unmarshal error: %v", err)
		}
		return out["NowRate"]
	}
	if got := nowRate(); got < 200 || got > 250 {
		t.Fatalf("MarshalJSON NowRate=%d, want about 250", got)
	}

	// An idle window must read 0, not the earlier bytes spread over more time.
	r.lastSampleNs.Store(nowNs() - int64(time.Second))
	if got := nowRate(); got != 0 {
		t.Fatalf("idle MarshalJSON NowRate=%d, want 0", got)
	}
}

func TestRateGetContextCancel(t *testing.T) {
	r := NewRate(1024)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- r.GetContext(ctx, 64<<10)
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("GetContext() error = %v, want %v", err, context.Canceled)
		}
	case <-time.After(500 * time.Millisecond):
		r.Stop()
		t.Fatal("GetContext() did not return promptly after cancel")
	}
	if got := r.bytesAcc.Load(); got != 64<<10 {
		t.Fatalf("bytesAcc after canceled GetContext = %d, want charge kept", got)
	}

	// No wait needed: a done context does not turn an allowed charge into an error.
	r.ResetLimit(1024)
	if err := r.GetContext(ctx, 1); err != nil {
		t.Fatalf("GetContext() within burst error = %v, want nil", err)
	}
}
