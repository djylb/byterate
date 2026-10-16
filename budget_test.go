package byterate

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// stopAfter returns a Budget that stops all bytes past n.
func stopAfter(n int64) *Budget { return NewBudget(Step{After: n, Stop: true}) }

func TestBudgetCountsAndStops(t *testing.T) {
	b := stopAfter(1000)
	if err := b.Wait(bg, 600); err != nil || b.Used() != 600 || b.Step() != -1 {
		t.Fatalf("Wait(600) = %v, used %d, step %d; want nil, 600, -1", err, b.Used(), b.Step())
	}
	if err := b.Wait(bg, 600); !errors.Is(err, ErrBudgetExhausted) || b.Used() != 600 {
		t.Fatalf("Wait(600) past the stop = %v, used %d; want ErrBudgetExhausted and nothing taken", err, b.Used())
	}
	if n, err := b.Ready(bg, 1000); n != 400 || err != nil {
		t.Fatalf("Ready(1000) = %d, %v; want the 400 up to the stop", n, err)
	}
	if err := b.Charge(500); !errors.Is(err, ErrBudgetExhausted) || b.Used() != 1100 || b.Step() != 0 {
		t.Fatalf("Charge(500) into the stop = %v, used %d, step %d; want ErrBudgetExhausted, 1100, 0", err, b.Used(), b.Step())
	}
	if _, err := b.Ready(bg, 1); !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("Ready past the stop = %v, want ErrBudgetExhausted", err)
	}
	b.Refund(300)
	if b.Used() != 800 {
		t.Fatalf("Used() after Refund(300) = %d, want 800", b.Used())
	}
	b.Reset()
	if b.Used() != 0 || b.Step() != -1 {
		t.Fatalf("after Reset: used %d, step %d; want 0, -1", b.Used(), b.Step())
	}
	b.SetUsed(999)
	if err := b.Wait(bg, 1); err != nil {
		t.Fatalf("Wait(1) up to the stop exactly = %v", err)
	}
	if err := b.Wait(bg, 1); !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("Wait(1) at the stop = %v, want ErrBudgetExhausted", err)
	}

	var counting Budget // no steps: counts and limits nothing
	if err := counting.Wait(bg, 1<<40); err != nil || counting.Used() != 1<<40 {
		t.Fatalf("a Budget without steps = %v with %d used", err, counting.Used())
	}
	var none *Budget
	if err := none.Charge(1 << 30); err != nil || none.Step() != -1 {
		t.Fatalf("Charge of a nil Budget = %v", err)
	}
}

// Steps slow traffic down in stages and then stop it, each from its After.
func TestBudgetStepsInStages(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		first, second := NewRate(1000), NewRate(500)
		first.reserve(2000) // spend the bursts, so that each step paces at once
		second.reserve(1000)
		b := NewBudget(
			Step{After: 3000, Stop: true},
			Step{After: 1000, Limit: first},
			Step{After: 2000, Limit: second},
		) // in any order
		var reached []int
		b.OnStep(func(step int, _ int64) { reached = append(reached, step) })
		inner := &dataConn{}
		conn := NewConn(inner, NewHierarchy(NewRate(1<<30), b))
		start := time.Now()
		n, err := conn.Write(make([]byte, 10000))
		// 1000 B at once, 1000 B at 1000 B/s, 1000 B at 500 B/s, then stop.
		if n != 3000 || !errors.Is(err, ErrBudgetExhausted) || len(inner.data) != 3000 {
			t.Fatalf("Write(10000) = %d, %v with %d sent; want 3000 and ErrBudgetExhausted", n, err, len(inner.data))
		}
		// 1s in the first step; the second step's Rate gathered 500 B of
		// credit meanwhile, so its 1000 B take 1s more.
		if got := time.Since(start); got != 2*time.Second {
			t.Fatalf("Write took %v, want 1s in each step", got)
		}
		if b.Used() != 3000 || b.Step() != 2 || len(reached) != 3 || reached[2] != 2 {
			t.Fatalf("used %d, step %d, steps reached %v; want 3000, 2 and every step once", b.Used(), b.Step(), reached)
		}
	})
}

// A Read takes up to a step, and the Read that reaches a stop returns its
// bytes with ErrBudgetExhausted.
func TestBudgetStopsReads(t *testing.T) {
	b := stopAfter(1000)
	conn := NewConn(&recordingConn{}, b)
	if n, err := conn.Read(make([]byte, 4000)); n != 1000 || !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("Read on a 1000 B Budget = %d, %v; want 1000 and ErrBudgetExhausted", n, err)
	}
	if n, err := conn.Read(make([]byte, 4000)); n != 0 || !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("Read past the stop = %d, %v; want 0 and ErrBudgetExhausted", n, err)
	}
}

func TestBudgetExpiry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var b Budget
		b.SetExpiry(time.Now().Add(time.Minute))
		time.Sleep(time.Minute - time.Second)
		if err := b.Wait(bg, 10); err != nil {
			t.Fatalf("Wait before the expiry = %v", err)
		}
		time.Sleep(time.Second)
		if err := b.Wait(bg, 10); !errors.Is(err, ErrBudgetExhausted) {
			t.Fatalf("Wait after the expiry = %v, want ErrBudgetExhausted", err)
		}
		if err := b.Charge(10); !errors.Is(err, ErrBudgetExhausted) || b.Used() != 20 {
			t.Fatalf("Charge after the expiry = %v with %d used; want ErrBudgetExhausted and 20", err, b.Used())
		}
		b.SetExpiry(time.Time{})
		time.Sleep(time.Hour)
		if err := b.Wait(bg, 10); err != nil {
			t.Fatalf("Wait after clearing the expiry = %v", err)
		}
	})
}

// Many charges racing past a step report it once.
func TestBudgetOnStepRacesReportOnce(t *testing.T) {
	b := NewBudget(Step{After: 1 << 19})
	var reports atomic.Int64
	b.OnStep(func(int, int64) { reports.Add(1) })
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 1 << 10 {
				_ = b.Charge(128)
			}
		})
	}
	wg.Wait()
	if reports.Load() != 1 || b.Used() != 1<<20 {
		t.Fatalf("OnStep ran %d times with %d used; want once and 1 MiB", reports.Load(), b.Used())
	}
	b.Reset()
	_ = b.Charge(1 << 20)
	if reports.Load() != 2 {
		t.Fatalf("OnStep ran %d times after a new period reached the step again, want 2", reports.Load())
	}
}

// A level that refuses, such as a stopped Budget, does so before the others
// take the bytes or make the caller wait.
func TestRefusalComesFirst(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		user, stopped := NewBudget(), stopAfter(0)
		l := NewHierarchy(user, stopped)
		for range 5 {
			if err := l.Wait(bg, 800); !errors.Is(err, ErrBudgetExhausted) {
				t.Fatalf("Wait = %v, want ErrBudgetExhausted", err)
			}
		}
		if user.Used() != 0 {
			t.Fatalf("the other Budget took %d B of refused charges", user.Used())
		}

		r := NewRate(1000)
		r.reserve(12000) // 10s of debt
		start := time.Now()
		if _, err := NewHierarchy(r, stopped).Ready(bg, 100); !errors.Is(err, ErrBudgetExhausted) || time.Since(start) != 0 {
			t.Fatalf("Ready = %v after %v; want ErrBudgetExhausted at once", err, time.Since(start))
		}
	})
}

// hookLimiter is a Limiter whose Wait runs a hook and then takes or refuses
// the bytes, keeping count of what it took and got back.
type hookLimiter struct {
	hook        func()
	refuse      bool
	taken, back int64
}

func (l *hookLimiter) Wait(_ context.Context, n int64) error {
	if l.hook != nil {
		l.hook()
	}
	if l.refuse {
		return errors.New("refused")
	}
	l.taken += n
	return nil
}
func (l *hookLimiter) Refund(n int64)                                  { l.back += n }
func (l *hookLimiter) Ready(_ context.Context, n int64) (int64, error) { return n, nil }
func (l *hookLimiter) Charge(n int64) error                            { l.taken += n; return nil }

// A step's Limit that refuses leaves the Budget as it was, and refunds never
// give a step's Limit back more than it took.
func TestBudgetStepLimitRefunds(t *testing.T) {
	lim := &hookLimiter{}
	b := NewBudget(Step{After: 100, Limit: lim})
	if err := b.Wait(bg, 100); err != nil || lim.taken != 0 { // before the step
		t.Fatalf("Wait(100) before the step = %v, step limit took %d", err, lim.taken)
	}
	if err := b.Wait(bg, 50); err != nil || lim.taken != 50 {
		t.Fatalf("Wait(50) in the step = %v, step limit took %d; want 50", err, lim.taken)
	}
	lim.refuse = true
	lim.hook = func() { b.Refund(50) } // the last charge is undone meanwhile
	if err := b.Wait(bg, 30); err == nil {
		t.Fatal("Wait through a refusing step limit succeeded")
	}
	if b.Used() != 100 || lim.taken-lim.back != 0 {
		t.Fatalf("used %d, step limit holds %d; want 100 and 0", b.Used(), lim.taken-lim.back)
	}
	b.Refund(100) // bytes from before the step: none go to its limit
	if lim.back != 50 {
		t.Fatalf("step limit got back %d, want only the 50 it took", lim.back)
	}
	lim.refuse, lim.hook = false, nil
	b.SetUsed(150)
	if err := b.Wait(bg, 100); err != nil || lim.taken != 150 { // in the step
		t.Fatalf("Wait(100) in the step = %v, step limit took %d in all; want 150", err, lim.taken)
	}
	b.Refund(200) // back to before the step: the last 100 go back to it
	if lim.taken-lim.back != 0 {
		t.Fatalf("step limit holds %d after a refund across its After, want 0", lim.taken-lim.back)
	}

	other := &hookLimiter{}
	b.SetUsed(200)
	_ = b.Charge(10) // to lim
	b.SetSteps(Step{After: 100, Limit: other})
	b.Refund(10)
	if other.back != 0 {
		t.Fatalf("a refund after SetSteps gave the new step limit %d it never took", other.back)
	}
	defer func() {
		if recover() == nil {
			t.Fatal("a step limited by its own Budget did not panic")
		}
	}()
	b.SetSteps(Step{Limit: b})
}

// Changing the steps, their limits or the period while a connection is slowed
// down never ends it: a Reset brings back the full speed from the next grant,
// and a new step limit applies at once.
func TestBudgetChangesKeepConnections(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		slow := NewRate(1000)
		slow.reserve(2000) // spend the step limit's burst
		b := NewBudget(Step{After: 1000, Limit: slow})
		conn := NewConn(&dataConn{}, NewHierarchy(NewRate(1<<20), b))
		done := make(chan ioResult, 1)
		start := time.Now()
		go func() {
			n, err := conn.Write(make([]byte, 100000))
			done <- ioResult{n, err}
		}()
		time.Sleep(time.Second)
		slow.SetLimit(2000) // a new slow-down speed
		time.Sleep(time.Second)
		b.SetSteps(Step{After: 1 << 20, Limit: slow}) // a larger allowance
		b.Reset()                                     // and a new period
		res := <-done
		if res.n != 100000 || res.err != nil {
			t.Fatalf("Write across the changes = %d, %v; want all of it", res.n, res.err)
		}
		if got := time.Since(start); got > 2100*time.Millisecond {
			t.Fatalf("Write took %v, want the full speed back right after the Reset at 2s", got)
		}
		b.SetSteps(Step{After: 0, Stop: true}) // a stop from now on
		if _, err := conn.Write(make([]byte, 10)); !errors.Is(err, ErrBudgetExhausted) {
			t.Fatalf("Write after a stop step was set = %v, want ErrBudgetExhausted", err)
		}
	})
}

// A Budget slowing writes down through a step's Rate does not allocate per
// Write.
func TestBudgetStepAllocatesNothing(t *testing.T) {
	if runtime.GOOS == "js" {
		t.Skip("time.Sleep allocates on js")
	}
	slow := NewRate(1 << 20)
	slow.reserve(2 << 20) // spend the burst, so that each Write below waits
	b := NewBudget(Step{After: 0, Limit: slow})
	conn := NewConn(&nopNetConn{}, NewHierarchy(NewRate(1<<30), b))
	buf := make([]byte, 8<<10) // about 8 ms at the step's pace
	_, _ = conn.Write(buf)
	if allocs := testing.AllocsPerRun(20, func() { _, _ = conn.Write(buf) }); allocs != 0 {
		t.Fatalf("a Write slowed by a step allocated %v times, want 0", allocs)
	}
}

// A Stop holds once reached, also when steps follow it or share its After.
func TestBudgetStopHolds(t *testing.T) {
	for name, steps := range map[string][]Step{
		"followed": {{After: 10, Stop: true}, {After: 20, Limit: NewRate(1 << 20)}},
		"same":     {{After: 10, Stop: true}, {After: 10, Limit: NewRate(1 << 20)}},
	} {
		t.Run(name, func(t *testing.T) {
			b := NewBudget(steps...)
			if err := b.Charge(25); !errors.Is(err, ErrBudgetExhausted) {
				t.Fatalf("Charge(25) past the stop = %v, want ErrBudgetExhausted", err)
			}
			if _, err := b.Ready(bg, 100); !errors.Is(err, ErrBudgetExhausted) {
				t.Fatalf("Ready past the stop = %v, want ErrBudgetExhausted", err)
			}
			if err := b.Wait(bg, 1); !errors.Is(err, ErrBudgetExhausted) {
				t.Fatalf("Wait past the stop = %v, want ErrBudgetExhausted", err)
			}
		})
	}
}

// Concurrent Waits never take the usage past a Stop, also while a step's
// Limit makes them wait.
func TestBudgetConcurrentWaitsStop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		slow := NewRate(1000)
		b := NewBudget(Step{After: 0, Limit: slow}, Step{After: 1000, Stop: true})
		var ok atomic.Int64
		var wg sync.WaitGroup
		for range 10 {
			wg.Go(func() {
				if b.Wait(bg, 500) == nil {
					ok.Add(1)
				}
			})
		}
		wg.Wait()
		if ok.Load() != 2 || b.Used() != 1000 {
			t.Fatalf("%d Waits of 500 B passed a stop at 1000 B with %d used, want 2 and 1000", ok.Load(), b.Used())
		}
	})
}

// Refunds reach the step Limits that took the bytes: a refund of a charge
// that crossed into a step goes to the step before, and a connection's
// refund to the step it charged, also after SetSteps.
func TestBudgetRefundsReachTheirSteps(t *testing.T) {
	a, c := &hookLimiter{}, &hookLimiter{}
	b := NewBudget(Step{After: 0, Limit: a}, Step{After: 10, Limit: c})
	_ = b.Charge(5)
	_ = b.Charge(10) // crosses into c's step, charged to a
	b.Refund(10)
	if a.taken-a.back != 5 || c.taken-c.back != 0 || b.Used() != 5 {
		t.Fatalf("after refunding the crossing charge: a holds %d, c %d, used %d; want 5, 0, 5",
			a.taken-a.back, c.taken-c.back, b.Used())
	}

	synctest.Test(t, func(t *testing.T) {
		slow := NewRate(1000)
		slow.reserve(2000) // spend the burst, so that the write waits
		b := NewBudget(Step{After: 0, Limit: slow})
		failed := errors.New("write failed")
		conn := NewReadWriteCloser(&scriptedConn{writeErr: failed}, b)
		done := make(chan error, 1)
		go func() {
			_, err := conn.Write(make([]byte, 100))
			done <- err
		}()
		synctest.Wait()
		other := NewRate(1000)
		b.SetSteps(Step{After: 0, Limit: other}) // new steps while the write waits
		if err := <-done; !errors.Is(err, failed) {
			t.Fatalf("Write = %v, want %v", err, failed)
		}
		if metered(slow) != 2000 || metered(other) != 0 || b.Used() != 0 {
			t.Fatalf("after the failed write: slow metered %d, other %d, used %d; want 2000, 0, 0",
				metered(slow), metered(other), b.Used())
		}
	})
}

// The usage never goes below zero or wraps, and a refund of bytes charged
// before a Reset does not reach the new period.
func TestBudgetUsageBounds(t *testing.T) {
	b := stopAfter(100)
	b.Refund(50)
	if b.Used() != 0 {
		t.Fatalf("Refund on an empty Budget left %d used, want 0", b.Used())
	}
	if n, _ := b.Ready(bg, 1000); n != 100 {
		t.Fatalf("Ready after a stray refund = %d, want the 100 up to the stop", n)
	}
	if err := b.Charge(1<<63 - 1); !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("Charge(MaxInt64) = %v, want ErrBudgetExhausted", err)
	}
	if err := b.Charge(1<<63 - 1); !errors.Is(err, ErrBudgetExhausted) || b.Used() != 1<<63-1 {
		t.Fatalf("a second Charge(MaxInt64) = %v with %d used, want ErrBudgetExhausted and a saturated count", err, b.Used())
	}

	failed := errors.New("write failed")
	c := NewBudget()
	conn := NewReadWriteCloser(&scriptedConn{writeErr: failed}, NewHierarchy(NewRate(1<<20), c,
		&hookLimiter{hook: func() { c.SetUsed(500) }})) // a new count while the write is charged
	_, _ = conn.Write(make([]byte, 100))
	if c.Used() != 500 {
		t.Fatalf("a refund of a write charged before SetUsed left %d used, want 500", c.Used())
	}
}

func TestBudgetRejectsItselfInAHierarchy(t *testing.T) {
	b := NewBudget()
	defer func() {
		if recover() == nil {
			t.Fatal("a step limited by a Hierarchy holding its Budget did not panic")
		}
	}()
	b.SetSteps(Step{Limit: NewHierarchy(NewRate(1), b)})
}

// SetSteps, Reset and SetUsed racing with charges and refunds keep the usage
// in bounds and every step limit owed no more than it took.
func TestBudgetChangesRaceCharges(t *testing.T) {
	limits := []*Rate{NewRate(1 << 30), NewRate(1 << 30)}
	b := NewBudget(Step{After: 1000, Limit: limits[0]})
	var stop atomic.Bool
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for !stop.Load() {
				if b.Wait(bg, 100) == nil {
					b.Refund(50)
				}
				_ = b.Charge(30)
				_, _ = b.Ready(bg, 1000)
				runtime.Gosched() // js/wasm does not preempt this loop
			}
		})
	}
	for i := range 2000 {
		switch i % 3 {
		case 0:
			b.SetSteps(Step{After: int64(i), Limit: limits[i%2]}, Step{After: 1 << 40, Stop: true})
		case 1:
			b.Reset()
		default:
			b.SetUsed(int64(i))
		}
		if b.Used() < 0 {
			t.Fatalf("Used() = %d, want never below zero", b.Used())
		}
		runtime.Gosched()
	}
	stop.Store(true)
	wg.Wait()
}
