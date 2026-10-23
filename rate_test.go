package byterate

import (
	"context"
	"errors"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
	"unsafe"
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

func TestClampHelpers(t *testing.T) {
	t.Parallel()

	if got := addSat(1, 2); got != 3 {
		t.Fatalf("addSat(1,2)=%d, want 3", got)
	}
	if got := addSat(maxI64-1, 10); got != maxI64 {
		t.Fatalf("addSat overflow=%d, want maxI64", got)
	}
	if got := addSat(minI64+1, -10); got != minI64 {
		t.Fatalf("addSat underflow=%d, want minI64", got)
	}
	if got := subSat(5, 2); got != 3 {
		t.Fatalf("subSat(5,2)=%d, want 3", got)
	}
	if got := subSat(minI64+1, 10); got != minI64 {
		t.Fatalf("subSat underflow=%d, want minI64", got)
	}
}

func TestRateLifecycle(t *testing.T) {
	r := NewRate(1024)
	if r == nil {
		t.Fatal("NewRate returned nil")
	}
	if got := r.Limit(); got != 1024 {
		t.Fatalf("Limit()=%d, want 1024", got)
	}

	r.Disable()
	_ = r.Wait(bg, 1024)
	if got := metered(r); got != 1024 {
		t.Fatalf("metered after Disable/Wait=%d, want 1024: a disabled Rate still meters", got)
	}

	r.Enable()
	r.SetLimit(2048)
	if got := r.Limit(); got != 2048 {
		t.Fatalf("Limit() after SetLimit=%d, want 2048", got)
	}

	r.Reset(0)
	if got := r.Limit(); got != 0 {
		t.Fatalf("Limit() after Reset(0)=%d, want 0", got)
	}
}

func TestNilRateSafety(t *testing.T) {
	var r *Rate
	r.SetLimit(1)
	r.Reset(1)
	r.Enable()
	r.Disable()
	_ = r.Wait(bg, 1)
	r.Refund(1)

	if got := r.Limit(); got != 0 {
		t.Fatalf("nil Limit()=%d, want 0", got)
	}
	if got := r.Throughput(); got != 0 {
		t.Fatalf("nil Throughput()=%d, want 0", got)
	}
}

func TestRateCloneDetachesRuntimeState(t *testing.T) {
	original := NewRate(2048)
	_ = original.Wait(bg, 64)

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

	cloned.Reset(0)
	if got := original.Limit(); got != 2048 {
		t.Fatalf("original limit after clone mutation = %d, want 2048", got)
	}

	original.Disable()
	if cloned.stopCh() == nil {
		t.Fatal("clone stop channel = nil after original Disable(), want independent channel")
	}
	select {
	case <-cloned.stopCh():
		t.Fatal("clone stop channel closed when original disabled")
	default:
	}
}

func TestRateLargeWaitDoesNotPoisonLimiter(t *testing.T) {
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
	expectFullBurst(t, NewRate(1000))
}

// expectFullBurst checks that r, limited to 1000 bytes per second, starts
// with its whole burst of two seconds and then waits.
func expectFullBurst(t *testing.T, r *Rate) {
	t.Helper()
	if wait := r.reserve(2000); wait != 0 {
		t.Fatalf("reserve(2000) within burst wait=%s, want 0", time.Duration(wait))
	}
	if wait := r.reserve(1000); wait < int64(900*time.Millisecond) || wait > int64(time.Second) {
		t.Fatalf("reserve(1000) after burst wait=%s, want about 1s", time.Duration(wait))
	}
}

// The zero value is an enabled, unlimited Rate: it meters at once, and a
// limit set on it applies with a full burst, without Enable.
func TestZeroValueRateIsReady(t *testing.T) {
	var unlimited Rate
	if wait := unlimited.reserve(1 << 20); wait != 0 || metered(&unlimited) != 1<<20 {
		t.Fatalf("zero Rate reserve(1 MiB) wait=%s metered=%d, want 0 and 1 MiB", time.Duration(wait), metered(&unlimited))
	}

	var r Rate
	r.SetLimit(1000)
	expectFullBurst(t, &r)
	if got := metered(&r); got != 3000 {
		t.Fatalf("metered after 3000 bytes = %d, want 3000", got)
	}

	cloned := (&Rate{}).Clone()
	cloned.SetLimit(1000)
	cloned.reserve(2000)
	if wait := cloned.reserve(1000); wait < int64(900*time.Millisecond) || wait > int64(time.Second) {
		t.Fatalf("clone of zero Rate reserve(1000) after burst wait=%s, want about 1s", time.Duration(wait))
	}
}

// metered returns the bytes r has metered since its generation began, net of
// refunds.
func metered(r *Rate) int64 { return genOf(r).total.Load() }

// TestRateSetLimitDebt checks that debt is kept as bytes across limit
// changes, also through a time without a limit, and that Reset drops it.
func TestRateSetLimitDebt(t *testing.T) {
	inDebt := func() *Rate {
		r := NewRate(1000)
		r.reserve(2000)  // spend the burst
		r.reserve(10000) // 10s of debt
		return r
	}
	expectWait := func(r *Rate, lo, hi time.Duration) {
		t.Helper()
		if wait := time.Duration(r.reserve(1)); wait < lo || wait > hi {
			t.Fatalf("reserve(1) wait=%s, want %s to %s", wait, lo, hi)
		}
	}

	r := inDebt()
	r.SetLimit(500)
	expectWait(r, 19*time.Second, 21*time.Second) // 10000 bytes at 500 B/s
	r.SetLimit(1 << 30)
	expectWait(r, 0, time.Duration(coalesceWaitNs))

	r = inDebt()
	r.SetLimit(0)
	r.SetLimit(2000)
	expectWait(r, 4*time.Second, 6*time.Second) // 10000 bytes at 2000 B/s

	r = inDebt()
	r.Reset(1000)
	expectFullBurst(t, r)
}

// TestRateRaisedLimitKeepsQueue raises the limit of a Rate with callers
// queued: they keep their wait, so new charges must still wait behind their
// bytes rather than go out alongside them.
func TestRateRaisedLimitKeepsQueue(t *testing.T) {
	r := NewRate(100_000)
	r.reserve(200_000) // spend the burst
	for range 100 {
		r.reserve(10_000) // a 10s queue
	}
	r.SetLimit(101_000)
	if wait := r.reserve(1); wait < int64(9800*time.Millisecond) {
		t.Fatalf("reserve(1) after a 1%% raise wait=%s, want the queue's 9.9s", time.Duration(wait))
	}
}

// TestRateRefundAfterLoweredLimit refunds one of several queued charges
// after the limit was lowered: the refund must cancel that charge alone, not
// the debt of the others.
func TestRateRefundAfterLoweredLimit(t *testing.T) {
	r := NewRate(1_000_000)
	r.reserve(2_000_000) // spend the burst
	for range 10 {
		r.reserve(1_000_000) // 10s of queued writes
	}
	r.SetLimit(100_000)
	r.Refund(1_000_000) // one write is canceled
	if wait := r.reserve(1); wait < int64(85*time.Second) {
		t.Fatalf("reserve(1) after the refund wait=%s, want the other writes' 90s", time.Duration(wait))
	}
}

// TestRateRefundAfterRaisedLimit refunds a large blocked charge, such as a
// write that Close canceled, after its limit was raised: no debt for the
// unsent bytes may remain to stall the Rate's other users.
func TestRateRefundAfterRaisedLimit(t *testing.T) {
	r := NewRate(1024)
	r.reserve(2048)    // spend the burst
	r.reserve(1 << 20) // a 1 MiB write waiting about 17 minutes
	r.SetLimit(1 << 30)
	r.Refund(1 << 20)
	if wait := r.reserve(1); wait > int64(time.Millisecond) {
		t.Fatalf("reserve(1) after the refund wait=%s, want about 0", time.Duration(wait))
	}
}

// TestRateSetLimitRacesCharges changes a limit while other goroutines charge
// at the old one: no charge may have its cost converted to the new limit
// twice, or not at all.
func TestRateSetLimitRacesCharges(t *testing.T) {
	charge := func(r *Rate, change func()) {
		var stop atomic.Bool
		var wg sync.WaitGroup
		for range 4 {
			wg.Go(func() {
				for !stop.Load() {
					r.reserve(1 << 10)
					// Lets the test go on where goroutines are not
					// preempted, as on js/wasm.
					runtime.Gosched()
				}
			})
		}
		time.Sleep(20 * time.Microsecond)
		change()
		stop.Store(true)
		wg.Wait()
	}
	for range 50 {
		r := NewRate(1)
		charge(r, func() { r.SetLimit(1 << 40) })
		// Charges at 1 TiB/s add nanoseconds of debt; one 1 KiB charge at
		// 1 B/s left unconverted would add about 17 minutes.
		if wait := r.reserve(1); wait > int64(time.Second) {
			t.Fatalf("reserve(1) after raising the limit under load wait=%s, want about 0", time.Duration(wait))
		}

		r = NewRate(1 << 40)
		charge(r, func() { r.SetLimit(1 << 10) })
		// Each charge after the change adds 1s of debt at 1 KiB/s; one
		// converted from 1 TiB/s on top would add about 34,000 years.
		if wait := r.reserve(1); wait > int64(100*365*24*time.Hour) {
			t.Fatalf("reserve(1) after lowering the limit under load wait=%s, want the charges' own debt", time.Duration(wait))
		}
	}
}

func TestRateWaitCancel(t *testing.T) {
	r := NewRate(1024)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- r.Wait(ctx, 64<<10)
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Wait() error = %v, want %v", err, context.Canceled)
		}
	case <-time.After(500 * time.Millisecond):
		r.Disable()
		t.Fatal("Wait() did not return promptly after cancel")
	}
	if got := metered(r); got != 64<<10 {
		t.Fatalf("metered after canceled Wait = %d, want charge kept", got)
	}

	// No wait needed: a done context does not turn an allowed charge into an
	// error.
	r.Reset(1024)
	if err := r.Wait(ctx, 1); err != nil {
		t.Fatalf("Wait() within burst error = %v, want nil", err)
	}
}

// TestRateStartsWithFullBurstForLaterLimit checks that an unlimited Rate
// given a limit later starts with the full burst, also early in the process,
// when the monotonic clock is still below the burst window, and that an
// unlimited Rate takes no generation until something is charged to it.
func TestRateStartsWithFullBurstForLaterLimit(t *testing.T) {
	unlimited := NewRate(0)
	if unlimited.gen.Load() != nil {
		t.Fatal("NewRate(0) made a generation before any charge")
	}
	var enabled Rate
	enabled.Enable()
	want := nowNs() - burstWindowNs
	if got := genOf(&enabled).tat.Load(); got > want || got < want-int64(time.Second) {
		t.Fatalf("tat = %d, want about %d (a full burst)", got, want)
	}
	for _, r := range []*Rate{unlimited, &enabled} {
		r.SetLimit(1000)
		if wait := r.reserve(2000); wait > int64(time.Millisecond) {
			t.Fatalf("first 2s of bytes waited %v, want the burst", time.Duration(wait))
		}
	}
}

// TestRateCloneDuringEnableDisable clones a Rate that is being disabled and
// enabled: every clone must be either disabled or able to wait.
func TestRateCloneDuringEnableDisable(t *testing.T) {
	r := NewRate(1000)
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			r.Disable()
			r.Enable()
			runtime.Gosched() // js/wasm does not preempt this loop
		}
	}()
	defer func() { close(stop); <-done }()
	for range 100000 {
		c := r.Clone()
		if !c.disabled.Load() && isClosed(c.stopCh()) {
			t.Fatal("Clone() is enabled with a closed stop channel, so it would never wait")
		}
	}
}

// TestRateChargeRefundDropsChargesBeforeReset refunds, after Reset, a
// charge made before it, as a write that Reset woke and that then
// failed does: the cleared meter must not go below 0, and the refund must
// not credit the debt charged since.
func TestRateChargeRefundDropsChargesBeforeReset(t *testing.T) {
	r := NewRate(1000)
	var charge receipt
	r.take(1<<20, nowNs(), &charge)
	r.Reset(1000)
	r.reserve(2000)  // spend the new burst
	r.reserve(10000) // 10s of debt since the reset
	charge.refund(1 << 20)
	if got := metered(r); got != 12000 {
		t.Fatalf("metered after the stale refund = %d, want 12000", got)
	}
	if wait := r.reserve(1); wait < int64(9*time.Second) {
		t.Fatalf("reserve(1) after the stale refund wait=%s, want the 10s of debt kept", time.Duration(wait))
	}

	// A refund of a charge made since the last Enable is applied.
	r.take(1000, nowNs(), &charge)
	charge.refund(1000)
	if got := metered(r); got != 12001 {
		t.Fatalf("metered after a current refund = %d, want 12001", got)
	}
}

// TestLayoutFillsCacheLines checks the sizes that keep a Rate's fields off
// other data's cache lines: a Rate fills one line, and a generation two,
// with the counters every charge writes alone on the second.
func TestLayoutFillsCacheLines(t *testing.T) {
	if got := unsafe.Sizeof(Rate{}); got != cacheLineSize {
		t.Errorf("Rate is %d bytes, want %d", got, cacheLineSize)
	}
	var g generation
	if got := unsafe.Sizeof(g); got != 2*cacheLineSize {
		t.Errorf("generation is %d bytes, want %d", got, 2*cacheLineSize)
	}
	if got := unsafe.Offsetof(g.tat); got != cacheLineSize {
		t.Errorf("generation.tat at offset %d, want %d", got, cacheLineSize)
	}
	var gr Group
	if got := unsafe.Sizeof(gr); got != 2*cacheLineSize {
		t.Errorf("Group is %d bytes, want %d", got, 2*cacheLineSize)
	}
	if got := unsafe.Offsetof(gr.conns); got != cacheLineSize {
		t.Errorf("Group.conns at offset %d, want %d", got, cacheLineSize)
	}
}

// A zero value makes its first generation on the first charge: Disable racing
// with those charges must still wake every caller that went on to wait, and
// Enable must bring the limit back.
func TestZeroValueDisableRacesFirstCharge(t *testing.T) {
	for range 200 {
		var r Rate
		r.SetLimit(1000)
		start := make(chan struct{})
		var wg sync.WaitGroup
		for range 4 {
			wg.Go(func() {
				<-start
				_ = r.Wait(bg, 10000) // 10s of debt unless Disable wakes it
			})
		}
		close(start)
		runtime.Gosched()
		r.Disable()
		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("Disable left a caller of a zero Rate waiting")
		}
		r.Enable()
		r.reserve(2000)
		if wait := r.reserve(1000); wait < int64(900*time.Millisecond) {
			t.Fatalf("reserve(1000) after Enable wait=%s, want the limit back", time.Duration(wait))
		}
	}
}

// Enable on an enabled zero value racing with its first charge must keep the
// generation that charge made, and with it the charge's debt.
func TestZeroValueEnableRacesFirstCharge(t *testing.T) {
	for range 50000 {
		var r Rate
		r.SetLimit(1000)
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Go(func() {
			<-start
			r.reserve(12000) // 10s of debt past the 2000 B burst
		})
		wg.Go(func() {
			<-start
			r.Enable()
		})
		close(start)
		wg.Wait()
		if wait := r.reserve(1); wait < int64(9*time.Second) {
			t.Fatalf("Enable racing with the first charge dropped its debt: wait %v", time.Duration(wait))
		}
	}
}

// Ready waits for the debt alone and grants the credit or a quantum; Charge
// takes the bytes as debt without waiting, as a relay's hooks use them.
func TestRateReadyAndCharge(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := NewRate(1000)
		if n, err := r.Ready(bg, 1<<20); n != 2000 || err != nil {
			t.Fatalf("Ready on a full burst = %d, %v; want the 2000 B burst", n, err)
		}
		start := time.Now()
		if err := r.Charge(4000); err != nil || time.Since(start) != 0 {
			t.Fatalf("Charge(4000) = %v after %v; want no wait", err, time.Since(start))
		}
		n, err := r.Ready(bg, 1<<20)
		if n != 512 || err != nil || time.Since(start) != 2*time.Second {
			t.Fatalf("Ready after 2s of debt = %d, %v after %v; want a 512 B quantum after 2s", n, err, time.Since(start))
		}
		// A relay's loop passes the limit, however large its buffer.
		start = time.Now()
		sent := int64(0)
		for sent < 10000 {
			n, _ := r.Ready(bg, 32<<10)
			sent += n
			_ = r.Charge(n)
		}
		if got := time.Since(start); got < 9*time.Second || got > 11*time.Second {
			t.Fatalf("10000 B through Ready and Charge at 1000 B/s took %v, want about 10s", got)
		}
	})
}

// Ready ends at once with ctx's error when ctx ends during the wait, as a
// relay that stops cancels the context of the hook it is blocked in.
func TestReadyEndsWithContext(t *testing.T) {
	for name, wrap := range map[string]func(*Rate) Limiter{
		"Rate":      func(r *Rate) Limiter { return r },
		"Hierarchy": func(r *Rate) Limiter { return NewHierarchy(r, stopAfter(1<<30)) },
		"Switch":    func(r *Rate) Limiter { return NewSwitch(r) },
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				r := NewRate(1000)
				r.reserve(12000) // 10s of debt past the burst
				l := wrap(r)
				ctx, cancel := context.WithCancel(bg)
				done := make(chan error, 1)
				go func() {
					_, err := l.Ready(ctx, 1000)
					done <- err
				}()
				time.Sleep(time.Second)
				start := time.Now()
				cancel()
				if err := <-done; !errors.Is(err, context.Canceled) || time.Since(start) != 0 {
					t.Fatalf("Ready after cancel = %v after %v, want context.Canceled at once", err, time.Since(start))
				}
			})
		})
	}
}

// SetBurst sets how much passes at once after an idle period, and the
// grants of the connection wrappers follow it.
func TestRateSetBurst(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := NewRate(1000)
		if r.Burst() != 2*time.Second {
			t.Fatalf("default Burst() = %v, want 2s", r.Burst())
		}
		r.SetBurst(100 * time.Millisecond)
		time.Sleep(time.Second) // idle long past the burst
		if wait := r.reserve(100); wait != 0 {
			t.Fatalf("reserve(100) within a 100 B burst waited %v", time.Duration(wait))
		}
		if wait := r.reserve(100); wait != int64(100*time.Millisecond) {
			t.Fatalf("reserve(100) past the burst waits %v, want 100ms", time.Duration(wait))
		}
		inner := &dataConn{}
		conn := NewConn(inner, r)
		time.Sleep(time.Second)
		_, _ = conn.Write(make([]byte, 1000))
		if largest := slices.Max(inner.runs); largest > 100 {
			t.Fatalf("largest write with a 100 ms burst at 1000 B/s = %d B, want at most 100", largest)
		}
		if c := r.Clone(); c.Burst() != 100*time.Millisecond {
			t.Fatalf("Clone() Burst() = %v, want 100ms", c.Burst())
		}
		r.SetBurst(0)
		if r.Burst() != 2*time.Second {
			t.Fatalf("Burst() after SetBurst(0) = %v, want the default 2s", r.Burst())
		}
	})
}

// A huge burst saturates instead of wrapping, also inside a synctest bubble,
// where the clock reads below zero.
func TestRateHugeBurstInBubble(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := NewRate(1000)
		r.SetBurst(time.Duration(1<<63 - 1))
		r.reserve(2000)
		time.Sleep(10 * time.Second) // credit builds up past the default burst
		if wait := r.reserve(10000); wait != 0 {
			t.Fatalf("reserve(10000) after 10s with a huge burst waits %v, want 0", time.Duration(wait))
		}
		if wait := r.reserve(1000); wait != int64(time.Second) {
			t.Fatalf("reserve(1000) then waits %v, want 1s", time.Duration(wait))
		}
	})
}
