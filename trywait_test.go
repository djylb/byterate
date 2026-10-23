package byterate

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

// TryWait lets through what Wait would without sleeping, and charges
// nothing else.
func TestRateTryWait(t *testing.T) {
	r := NewRate(1000) // a burst of 2000 bytes
	if err := r.TryWait(1500); err != nil {
		t.Fatalf("TryWait(1500) within the burst = %v, want nil", err)
	}
	if err := r.TryWait(1000); !errors.Is(err, ErrWouldWait) {
		t.Fatalf("TryWait(1000) past the burst = %v, want ErrWouldWait", err)
	}
	if got := metered(r); got != 1500 {
		t.Fatalf("metered = %d, want the 1500 bytes let through alone", got)
	}
	if err := r.TryWait(500); err != nil {
		t.Fatalf("TryWait(500) of the rest of the burst = %v, want nil", err)
	}
	var unlimited, disabled, nilRate *Rate = NewRate(0), NewRate(1), nil
	disabled.Disable()
	for _, r := range []*Rate{unlimited, disabled, nilRate} {
		if err := r.TryWait(1 << 30); err != nil {
			t.Fatalf("TryWait on a Rate that does not limit = %v, want nil", err)
		}
	}
}

// TryWait charges every level or none: a level that would wait or refuses
// leaves the others as they were, Counter views count only what passes, and
// the Limits of Budgets give back their usage.
func TestTryWaitChargesAllOrNothing(t *testing.T) {
	loose := NewRate(1 << 30)
	var c Counter
	other := &hookLimiter{}
	var budget *Budget
	for _, tc := range []struct {
		name string
		l    func() Limiter
	}{
		{"hierarchy", func() Limiter { return NewHierarchy(loose, other, NewRate(1000), c.Writes()) }},
		{"budget", func() Limiter {
			budget = NewBudget(Step{After: 0, Limit: NewRate(1000)}, Step{After: 10_000, Stop: true})
			return NewHierarchy(loose, other, budget, c.Writes())
		}},
		{"switch", func() Limiter { return NewHierarchy(loose, NewSwitch(NewRate(1000)), c.Writes()) }},
		{"group", func() Limiter {
			g := NewGroup(nil)
			g.SetWriteLimiter(NewRate(1000))
			return NewHierarchy(loose, other, g.Writes())
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l := tc.l()
			// The first 2000 bytes pass, the next would wait on the strict
			// level, here or in the budget's step.
			if err := l.TryWait(2000); err != nil {
				t.Fatalf("TryWait(2000) = %v, want nil", err)
			}
			looseBefore, otherBefore := metered(loose), other.taken-other.back
			_, outBefore := c.Total()
			usedBefore := budget.Used()
			if err := l.TryWait(100); !errors.Is(err, ErrWouldWait) {
				t.Fatalf("TryWait(100) = %v, want ErrWouldWait", err)
			}
			if got := metered(loose); got != looseBefore {
				t.Fatalf("loose Rate metered %d, want %d as before the refusal", got, looseBefore)
			}
			if got := other.taken - other.back; got != otherBefore {
				t.Fatalf("other Limiter kept %d, want %d", got, otherBefore)
			}
			if _, out := c.Total(); out != outBefore {
				t.Fatalf("Counter counted %d, want %d", out, outBefore)
			}
			if got := budget.Used(); got != usedBefore {
				t.Fatalf("Budget used %d, want %d", got, usedBefore)
			}
		})
	}

	b := stopAfter(100)
	if err := b.TryWait(100); err != nil {
		t.Fatalf("TryWait up to the stop = %v, want nil", err)
	}
	if err := NewHierarchy(loose, b).TryWait(1); !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("TryWait past the stop = %v, want ErrBudgetExhausted", err)
	}
	if got := b.Remaining(); got != 0 {
		t.Fatalf("Remaining() past the stop = %d, want 0", got)
	}
	// The refusal comes before a Rate that would wait, as for Wait.
	spent := NewRate(1000)
	spent.reserve(2000)
	if err := NewHierarchy(spent, b).TryWait(1); !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("TryWait past the stop and the Rate's burst = %v, want ErrBudgetExhausted", err)
	}
}

// Concurrent TryWaits never let through more than the limit: the credit
// goes to one charge at a time, and one that loses it charges nothing.
func TestTryWaitConcurrentNeverOvercharges(t *testing.T) {
	for range 20 {
		conn, shared := NewRate(1<<30), NewRate(1000) // a burst of 2000 bytes
		l := NewHierarchy(conn, shared)
		var admitted atomic.Int64
		var wg sync.WaitGroup
		for range 8 {
			wg.Go(func() {
				for range 100 {
					if l.TryWait(100) == nil {
						admitted.Add(100)
					}
				}
			})
		}
		wg.Wait()
		// 2000 bytes of burst, and what the time the test took adds.
		if got := admitted.Load(); got < 2000 || got > 2200 {
			t.Fatalf("admitted %d bytes, want the 2000 of the burst", got)
		}
		if got := metered(conn); got != admitted.Load() {
			t.Fatalf("the other level metered %d, want the %d admitted: refused charges must be taken back", got, admitted.Load())
		}
	}
}
