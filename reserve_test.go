package byterate

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// try is TryAcquire that fails the test on an unexpected error.
func try(t *testing.T, r Reserver, minBytes, maxBytes int64) int64 {
	t.Helper()
	n, err := r.TryAcquire(minBytes, maxBytes)
	if err != nil && !errors.Is(err, ErrWouldWait) {
		t.Fatalf("TryAcquire(%d, %d): %v", minBytes, maxBytes, err)
	}
	return n
}

func TestAcquireUnlimited(t *testing.T) {
	var zero *Rate
	var g Group
	for name, r := range map[string]Reserver{
		"nil": zero, "unlimited": NewRate(0), "budget": NewBudget(), "switch": NewSwitch(nil),
		"hierarchy": NewHierarchy(NewRate(0), NewRate(0)).(Reserver), "group": g.Writes().(Reserver),
	} {
		if n, err := r.Acquire(bg, 10, 1<<20); n != 1<<20 || err != nil {
			t.Errorf("%s: Acquire = %d, %v, want %d", name, n, err, 1<<20)
		}
		if n, err := r.TryAcquire(1<<20, 1<<20); n != 1<<20 || err != nil {
			t.Errorf("%s: TryAcquire = %d, %v", name, n, err)
		}
		if n, err := r.Acquire(bg, 1, 0); n != 0 || err != nil {
			t.Errorf("%s: Acquire(max 0) = %d, %v", name, n, err)
		}
		if n, err := r.TryAcquire(5, -1); n != 0 || err != nil {
			t.Errorf("%s: TryAcquire(max -1) = %d, %v", name, n, err)
		}
	}
}

func TestAcquireSemantics(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := NewRate(1000) // burst 2000 bytes
		if n := try(t, r, 1, 500); n != 500 {
			t.Fatalf("TryAcquire(1, 500) = %d", n)
		}
		if n := try(t, r, 1600, 1600); n != 0 {
			t.Fatalf("TryAcquire(1600, 1600) with 1500 of credit = %d, want none", n)
		}
		if n := try(t, r, 1, 5000); n != 1500 {
			t.Fatalf("TryAcquire(1, 5000) = %d, want the remaining credit 1500", n)
		}
		if n, err := r.TryAcquire(1, 1); n != 0 || !errors.Is(err, ErrWouldWait) {
			t.Fatalf("TryAcquire on empty = %d, %v", n, err)
		}
		start := time.Now()
		n, err := r.Acquire(bg, 300, 5000)
		if err != nil || n < 300 || n > 5000 {
			t.Fatalf("Acquire(300, 5000) = %d, %v", n, err)
		}
		if el := time.Since(start); el < 250*time.Millisecond {
			t.Fatalf("Acquire(min 300) returned after %v", el)
		}
		// A min above the burst is granted whole, after its debt.
		time.Sleep(10 * time.Second)
		start = time.Now()
		if n, err := r.Acquire(bg, 3000, 3000); n != 3000 || err != nil {
			t.Fatalf("Acquire(3000, 3000) = %d, %v", n, err)
		}
		if el := time.Since(start); el < time.Second {
			t.Fatalf("Acquire(3000) above a 2000 burst returned after %v", el)
		}
		// min above max counts as max.
		time.Sleep(10 * time.Second)
		if n := try(t, r, 900, 100); n != 100 {
			t.Fatalf("TryAcquire(900, 100) = %d, want 100", n)
		}
		// Refunding more than was taken stops at the burst.
		r.Refund(1 << 30)
		if n := try(t, r, 1, 1<<30); n != 2000 {
			t.Fatalf("TryAcquire after over-Refund = %d, want 2000", n)
		}
	})
}

func TestAcquireCancelKeepsNothing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := NewRate(1000)
		_ = try(t, r, 2000, 2000)
		for _, minBytes := range []int64{1, 500, 1000} {
			ctx, cancel := context.WithTimeout(bg, 10*time.Millisecond)
			if n, err := r.Acquire(ctx, minBytes, 1000); n != 0 || !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("Acquire(%d, 1000) = %d, %v", minBytes, n, err)
			}
			cancel()
		}
		// Nothing stays taken: 1s after the burst, 1000 bytes of credit.
		time.Sleep(time.Second - 30*time.Millisecond)
		if n := try(t, r, 1, 5000); n != 1000 {
			t.Fatalf("TryAcquire after canceled Acquires = %d, want 1000", n)
		}
	})
}

func TestHierarchyAcquireRollback(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := NewRate(1000)
		b := NewBudget(Step{After: 100, Stop: true})
		h := NewHierarchy(r, b).(Reserver)
		if _, err := h.Acquire(bg, 200, 200); !errors.Is(err, ErrBudgetExhausted) {
			t.Fatalf("Acquire past Stop = %v", err)
		}
		if b.Used() != 0 || try(t, r, 2000, 2000) != 2000 {
			t.Fatalf("refused Acquire left charges: used %d", b.Used())
		}
		r.Refund(2000)
		if n := try(t, h, 1, 500); n != 100 {
			t.Fatalf("TryAcquire(1, 500) = %d, want 100 up to the Stop", n)
		}
		if n, err := h.TryAcquire(1, 1); n != 0 || !errors.Is(err, ErrBudgetExhausted) || b.Used() != 100 {
			t.Fatalf("TryAcquire past Stop = %d, %v, used %d", n, err, b.Used())
		}
		// A canceled wait on the Rate gives back the Budget's charge too.
		b.Reset()
		_ = try(t, r, 1900, 1900)
		ctx, cancel := context.WithTimeout(bg, 10*time.Millisecond)
		defer cancel()
		if _, err := h.Acquire(ctx, 100, 100); err == nil {
			t.Fatal("Acquire did not wait")
		}
		if b.Used() != 0 {
			t.Fatalf("canceled Acquire left %d on the Budget", b.Used())
		}
	})
}

func TestBudgetAcquireStepLimit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		step := NewRate(1000)
		b := NewBudget(Step{After: 0, Limit: step})
		if n := try(t, b, 1, 1<<20); n != 2000 {
			t.Fatalf("TryAcquire = %d, want the step's burst 2000", n)
		}
		start := time.Now()
		if n, err := b.Acquire(bg, 500, 500); n != 500 || err != nil {
			t.Fatalf("Acquire = %d, %v", n, err)
		}
		if el := time.Since(start); el < 450*time.Millisecond {
			t.Fatalf("Acquire on an empty step returned after %v", el)
		}
		if b.Used() != 2500 {
			t.Fatalf("used %d, want 2500", b.Used())
		}
	})
}

func TestAcquireConcurrentNeverOvergrants(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const limit = 100_000
		r := NewRate(limit)
		h := NewHierarchy(r, NewRate(1<<30)).(Reserver)
		var got atomic.Int64
		ctx, cancel := context.WithTimeout(bg, 2*time.Second)
		defer cancel()
		var wg sync.WaitGroup
		for i := range 64 {
			wg.Go(func() {
				var l Reserver = r
				if i%2 == 1 {
					l = h
				}
				for ctx.Err() == nil {
					var n int64
					switch i % 4 {
					case 0, 1:
						n, _ = l.TryAcquire(1, 1500)
						if n == 0 {
							time.Sleep(time.Millisecond)
						}
					default:
						n, _ = l.Acquire(ctx, 500, 4000)
					}
					got.Add(n)
				}
			})
		}
		wg.Wait()
		// 2 s at the limit plus the 2 s burst at the start.
		if most := int64(4 * limit); got.Load() > most {
			t.Fatalf("granted %d bytes in 2s, more than %d", got.Load(), most)
		}
		if got.Load() < 2*limit {
			t.Fatalf("granted only %d bytes", got.Load())
		}
	})
}

// otherLimiter is a Limiter of another package, which is not a Reserver.
type otherLimiter struct{ Limiter }

func TestReserverOfEveryLimiter(t *testing.T) {
	var g Group
	for _, l := range []Limiter{
		NewHierarchy(otherLimiter{NewRate(0)}), NewHierarchy(NewRate(0)),
		g.Reads(), g.Writes(), NewSwitch(NewRate(0)), NewSharedCounter().reads(),
	} {
		r, ok := l.(Reserver)
		if !ok {
			t.Fatalf("%T is not a Reserver", l)
		}
		if n, err := r.Acquire(bg, 1, 100); n != 100 || err != nil {
			t.Fatalf("%T: Acquire = %d, %v", l, n, err)
		}
	}
}

func BenchmarkRateTryAcquire(b *testing.B) {
	r := NewRate(1 << 50)
	b.ReportAllocs()
	for b.Loop() {
		_, _ = r.TryAcquire(1, 1500)
	}
}

func BenchmarkRateAcquire(b *testing.B) {
	r := NewRate(1 << 50)
	b.ReportAllocs()
	for b.Loop() {
		_, _ = r.Acquire(bg, 1, 1500)
	}
}

func BenchmarkHierarchyAcquire(b *testing.B) {
	h := NewHierarchy(NewRate(1<<50), NewRate(1<<50)).(Reserver)
	b.ReportAllocs()
	for b.Loop() {
		_, _ = h.Acquire(bg, 1, 1500)
	}
}

func BenchmarkHierarchyTryAcquire(b *testing.B) {
	h := NewHierarchy(NewRate(1<<50), NewRate(1<<50)).(Reserver)
	b.ReportAllocs()
	for b.Loop() {
		_, _ = h.TryAcquire(1, 1500)
	}
}

func BenchmarkBudgetAcquire(b *testing.B) {
	bu := NewBudget(Step{After: 0, Limit: NewRate(1 << 50)})
	b.ReportAllocs()
	for b.Loop() {
		_, _ = bu.Acquire(bg, 1, 1500)
	}
}
