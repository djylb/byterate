package byterate

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestBudgetTakesAndRefuses(t *testing.T) {
	b := NewBudget(1000)
	if err := b.Wait(bg, 600); err != nil {
		t.Fatalf("Wait(600) of 1000 = %v", err)
	}
	if err := b.Wait(bg, 600); !errors.Is(err, ErrBudgetExhausted) || b.Left() != 400 {
		t.Fatalf("Wait(600) of 400 = %v with %d left; want ErrBudgetExhausted and nothing taken", err, b.Left())
	}
	if n, err := b.Ready(bg, 1000); n != 400 || err != nil {
		t.Fatalf("Ready(1000) = %d, %v; want the 400 left", n, err)
	}
	if err := b.Charge(500); !errors.Is(err, ErrBudgetExhausted) || b.Left() != -100 {
		t.Fatalf("Charge(500) of 400 = %v with %d left; want ErrBudgetExhausted and -100", err, b.Left())
	}
	if _, err := b.Ready(bg, 1); !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("Ready of an overdrawn Budget = %v, want ErrBudgetExhausted", err)
	}
	b.Refund(100)
	b.Grant(1000, time.Time{})
	if b.Left() != 1000 {
		t.Fatalf("Left() after refund and grant = %d, want 1000", b.Left())
	}
	var zero Budget
	if _, err := zero.Ready(bg, 1); !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("Ready of a zero Budget = %v, want ErrBudgetExhausted", err)
	}
	var none *Budget
	if err := none.Charge(1 << 30); err != nil {
		t.Fatalf("Charge of a nil Budget = %v, want nil", err)
	}
}

func TestBudgetLapses(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := NewBudget(0)
		b.Grant(1000, time.Now().Add(time.Minute))
		time.Sleep(time.Minute - time.Second)
		if err := b.Wait(bg, 10); err != nil || b.Left() != 990 {
			t.Fatalf("Wait before the lapse = %v with %d left", err, b.Left())
		}
		time.Sleep(time.Second)
		if err := b.Wait(bg, 10); !errors.Is(err, ErrBudgetExhausted) || b.Left() != 0 {
			t.Fatalf("Wait after the lapse = %v with %d left; want ErrBudgetExhausted and 0", err, b.Left())
		}
		if err := b.Charge(10); !errors.Is(err, ErrBudgetExhausted) {
			t.Fatalf("Charge after the lapse = %v, want ErrBudgetExhausted", err)
		}
		b.Grant(100, time.Now().Add(time.Minute)) // the 980 left count again
		if b.Left() != 1080 {
			t.Fatalf("Left() after a later grant = %d, want 1080", b.Left())
		}
	})
}

func TestBudgetOnLowReportsOnce(t *testing.T) {
	b := NewBudget(1000)
	var reports atomic.Int64
	var last atomic.Int64
	b.OnLow(300, func(left int64) { reports.Add(1); last.Store(left) })
	for range 10 {
		_ = b.Charge(100)
	}
	if reports.Load() != 1 || last.Load() != 200 {
		t.Fatalf("OnLow ran %d times, last with %d left; want once with 200", reports.Load(), last.Load())
	}
	b.Grant(1000, time.Time{})
	_ = b.Charge(800)
	if reports.Load() != 2 {
		t.Fatalf("OnLow ran %d times after a grant lifted the Budget, want 2", reports.Load())
	}
}

// Many charges racing past the mark report once.
func TestBudgetOnLowRacesReportOnce(t *testing.T) {
	b := NewBudget(1 << 20)
	var reports atomic.Int64
	b.OnLow(1<<19, func(int64) { reports.Add(1) })
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 1 << 10 {
				_ = b.Charge(128)
			}
		})
	}
	wg.Wait()
	if reports.Load() != 1 || b.Left() != 0 {
		t.Fatalf("OnLow ran %d times with %d left; want once and 0", reports.Load(), b.Left())
	}
}

// A connection limited by a Budget sends what is left, then fails with
// ErrBudgetExhausted; reads take what is left and fail once it is gone.
func TestConnStopsAtExhaustedBudget(t *testing.T) {
	b := NewBudget(1000)
	inner := &dataConn{}
	conn := NewConn(inner, NewHierarchy(NewRate(1<<20), b))
	n, err := conn.Write(make([]byte, 4000))
	if n != 1000 || !errors.Is(err, ErrBudgetExhausted) || len(inner.data) != 1000 {
		t.Fatalf("Write(4000) on a 1000 B Budget = %d, %v with %d sent; want 1000 and ErrBudgetExhausted", n, err, len(inner.data))
	}

	rb := NewBudget(1000)
	rc := NewConn(&recordingConn{}, rb)
	if n, err := rc.Read(make([]byte, 4000)); n != 1000 || err != nil {
		t.Fatalf("Read on a 1000 B Budget = %d, %v; want the 1000 left", n, err)
	}
	if n, err := rc.Read(make([]byte, 4000)); n != 0 || !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("Read on an exhausted Budget = %d, %v; want 0 and ErrBudgetExhausted", n, err)
	}
}

// A level that refuses, such as an exhausted Budget, does so before the
// others take the bytes or make the caller wait: no Budget is charged and
// refunded, which would report it low each time, and no debt is waited out
// only to be refused.
func TestRefusalComesFirst(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		user, global := NewBudget(1000), NewBudget(0)
		var reports atomic.Int64
		user.OnLow(500, func(int64) { reports.Add(1) })
		l := NewHierarchy(user, global)
		for range 5 {
			if err := l.Wait(bg, 800); !errors.Is(err, ErrBudgetExhausted) {
				t.Fatalf("Wait = %v, want ErrBudgetExhausted", err)
			}
		}
		if reports.Load() != 0 || user.Left() != 1000 {
			t.Fatalf("user budget reported low %d times with %d left; want 0 and 1000", reports.Load(), user.Left())
		}

		r := NewRate(1000)
		r.reserve(12000) // 10s of debt
		start := time.Now()
		if _, err := NewHierarchy(r, global).Ready(bg, 100); !errors.Is(err, ErrBudgetExhausted) || time.Since(start) != 0 {
			t.Fatalf("Ready = %v after %v; want ErrBudgetExhausted at once", err, time.Since(start))
		}
	})
}

// A Grant racing a charge that crosses the mark must not keep the next
// crossing from being reported.
func TestBudgetOnLowAfterRacingGrant(t *testing.T) {
	for range 1000 {
		b := NewBudget(150)
		var reports atomic.Int64
		b.OnLow(100, func(int64) { reports.Add(1) })
		var wg sync.WaitGroup
		wg.Go(func() { _ = b.Wait(bg, 60) })         // 150 -> 90 crosses
		wg.Go(func() { b.Grant(1000, time.Time{}) }) // lifts it back
		wg.Wait()
		before := reports.Load()
		_ = b.Charge(b.Left() - 50) // down to 50: crosses again
		if reports.Load() != before+1 {
			t.Fatalf("crossing after a racing Grant reported %d times, want once", reports.Load()-before)
		}
	}
}
