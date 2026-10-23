package byterate

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

func TestNewHierarchyCollapses(t *testing.T) {
	cases := map[string]Limiter{
		"none":  NewHierarchy(),
		"nil":   NewHierarchy(nil),
		"two":   NewHierarchy(nil, nil),
		"three": NewHierarchy(nil, nil, nil),
		"four":  NewHierarchy(nil, nil, nil, nil),
	}
	for name, l := range cases {
		if l != nil {
			t.Fatalf("%s: got %T(%v), want untyped nil", name, l, l)
		}
	}

	// A single Rate, limited or not, is returned itself.
	for _, r := range []*Rate{NewRate(0), NewRate(1024)} {
		if l := NewHierarchy(nil, r, nil); l != Limiter(r) {
			t.Fatalf("single rate: got %#v, want the rate itself", l)
		}
	}
	unlimited, limited := NewRate(0), NewRate(1024)
	for _, l := range []Limiter{
		NewHierarchy(nil, unlimited, limited),
		NewHierarchy(unlimited, limited),
		NewHierarchy(unlimited, nil, limited),
		NewHierarchy(unlimited, limited, NewRate(0), NewRate(0)),
	} {
		if _, ok := l.(*hierarchy); !ok {
			t.Fatalf("two or more rates: got %T, want *hierarchy", l)
		}
	}
}

// TestHierarchyAppliesLaterLimit checks that a limit set on a level
// after the limiter was built applies to it.
func TestHierarchyAppliesLaterLimit(t *testing.T) {
	later := NewRate(0)
	l := NewHierarchy(later, NewRate(0)).(*hierarchy)
	later.SetLimit(1000)
	// Two seconds' burst is free; the third second of bytes must wait.
	if wait := l.reserve(3000); wait < int64(900*time.Millisecond) {
		t.Fatalf("wait = %v, want about a second", time.Duration(wait))
	}
}

func TestHierarchyRefundsAllChildrenOnShortWrite(t *testing.T) {
	first := NewRate(1 << 20)
	second := NewRate(2 << 20)

	limiter := NewHierarchy(first, second)
	if _, ok := limiter.(*hierarchy); !ok {
		t.Fatalf("NewHierarchy() type = %T, want *hierarchy", limiter)
	}

	wantErr := errors.New("short write")
	conn := NewReadWriteCloser(&scriptedConn{writeN: 2, writeErr: wantErr}, limiter)
	n, err := conn.Write([]byte("hello"))
	if !errors.Is(err, wantErr) {
		t.Fatalf("Write() error = %v, want %v", err, wantErr)
	}
	if n != 2 {
		t.Fatalf("Write() n = %d, want 2", n)
	}
	if got := metered(first); got != 2 {
		t.Fatalf("first metered after Write() = %d, want 2", got)
	}
	if got := metered(second); got != 2 {
		t.Fatalf("second metered after Write() = %d, want 2", got)
	}
}

// TestHierarchyDisableEndsOnlyThatLevelsWait restarts the strictest level of
// a hierarchy while a caller waits: the wait that level imposed ends, but
// the other level's still applies, in every way of charging a hierarchy.
func TestHierarchyDisableEndsOnlyThatLevelsWait(t *testing.T) {
	// A Read waits for the debt alone and takes on the bytes it reads as
	// debt for the next one; the others wait for the 1000 bytes too.
	cases := map[string]struct {
		charge func(l Limiter) error
		want   time.Duration
	}{
		"Wait": {func(l Limiter) error {
			return l.Wait(context.Background(), 1000)
		}, 6 * time.Second},
		"Read": {func(l Limiter) error {
			_, err := NewConn(&nopNetConn{}, l).Read(make([]byte, 1000))
			return err
		}, 5 * time.Second},
		"Write": {func(l Limiter) error {
			_, err := NewConn(&nopNetConn{}, l).Write(make([]byte, 1000))
			return err
		}, 6 * time.Second},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				global, user := NewRate(1000), NewRate(1000)
				global.reserve(2000 + 5000) // the burst and 5s of debt
				user.reserve(2000 + 10000)  // the burst and 10s of debt
				l := NewHierarchy(global, user)
				start := time.Now()
				done := make(chan time.Duration, 1)
				go func() {
					if err := tc.charge(l); err != nil {
						t.Error(err)
					}
					done <- time.Since(start)
				}()
				time.Sleep(time.Second)
				user.Reset(1000)
				if got := <-done; got != tc.want {
					t.Fatalf("waited %v after the user level was restarted, want the global level's %v", got, tc.want)
				}
			})
		})
	}
}

// A disabled level is skipped, and Disable on the level that imposes the
// wait ends it.
func TestHierarchySkipsDisabledLevelAndWakesOnDisable(t *testing.T) {
	disabled := NewRate(1)
	disabled.Disable()
	enabled := NewRate(1)

	limiter := NewHierarchy(disabled, enabled)
	done := make(chan struct{})
	start := time.Now()
	go func() {
		_ = limiter.Wait(bg, 10)
		close(done)
	}()

	time.Sleep(20 * time.Millisecond)
	enabled.Disable()

	select {
	case <-done:
		if elapsed := time.Since(start); elapsed > 250*time.Millisecond {
			t.Fatalf("Hierarchy.Wait() returned after %s, want fast wake on Disable", elapsed)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Hierarchy.Wait() did not wake after Disable on the waiting level")
	}
}

func TestHierarchyWakeOnMaxWaitLimiterDisable(t *testing.T) {
	fast := NewRate(1 << 20)
	slow := NewRate(1)

	limiter := NewHierarchy(fast, slow)
	done := make(chan struct{})
	start := time.Now()
	go func() {
		_ = limiter.Wait(bg, 10)
		close(done)
	}()

	time.Sleep(20 * time.Millisecond)
	slow.Disable()

	select {
	case <-done:
		if elapsed := time.Since(start); elapsed > 250*time.Millisecond {
			t.Fatalf("Hierarchy.Wait() returned after %s, want fast wake on Disable of the max-wait level", elapsed)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Hierarchy.Wait() did not wake after Disable of the max-wait level")
	}
}

func TestHierarchyWaitCancel(t *testing.T) {
	first := NewRate(1024)
	second := NewRate(2048)
	limiter := NewHierarchy(first, second).(*hierarchy)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- limiter.Wait(ctx, 64<<10)
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Wait() error = %v, want %v", err, context.Canceled)
		}
	case <-time.After(500 * time.Millisecond):
		first.Disable()
		second.Disable()
		t.Fatal("Hierarchy.Wait() did not return promptly after cancel")
	}
}

// TestDisableBetweenChargeAndWait disables the Rate imposing the wait right
// after the charge, before the caller sleeps: Disable must still end the wait.
func TestDisableBetweenChargeAndWait(t *testing.T) {
	cases := map[string]func(strict *Rate) func(){
		"Rate.Wait": func(strict *Rate) func() {
			return func() { _ = strict.Wait(bg, 3000) }
		},
		"Rate.Wait cancelable": func(strict *Rate) func() {
			return func() {
				ctx, cancel := context.WithCancel(bg)
				defer cancel()
				_ = strict.Wait(ctx, 3000)
			}
		},
		"Hierarchy.Wait": func(strict *Rate) func() {
			l := NewHierarchy(NewRate(1<<30), strict, NewRate(0))
			return func() { _ = l.Wait(bg, 3000) }
		},
		"Hierarchy.Wait cancelable": func(strict *Rate) func() {
			l := NewHierarchy(NewRate(1<<30), strict)
			return func() {
				ctx, cancel := context.WithCancel(bg)
				defer cancel()
				_ = l.Wait(ctx, 3000)
			}
		},
	}
	for name, get := range cases {
		t.Run(name, func(t *testing.T) {
			strict := NewRate(1000)
			strict.reserve(2000) // spend the burst: 3000 more bytes wait 3s
			op := get(strict)

			var once sync.Once
			hook := func(r *Rate) {
				if r == strict {
					once.Do(func() { strict.Reset(0) })
				}
			}
			testHooks.reserved.Store(&hook)
			defer testHooks.reserved.Store(nil)

			done := make(chan struct{})
			go func() {
				op()
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(time.Second):
				strict.Disable() // release the stale wait
				<-done
				t.Fatal("Reset between the charge and the wait did not end the wait")
			}
			hookRan := true
			once.Do(func() { hookRan = false })
			if !hookRan {
				t.Fatal("the test hook did not run")
			}
		})
	}
}

// TestHierarchyChargesRepeatedRateOnce checks that a Rate given at
// more than one level, such as a user's that doubles as its group's, is not
// charged twice.
func TestHierarchyChargesRepeatedRateOnce(t *testing.T) {
	r := NewRate(1000)
	if l := NewHierarchy(r, nil, r); l != Limiter(r) {
		t.Fatalf("a Rate given twice: got %T, want the Rate itself", l)
	}
	other := NewRate(0)
	l := NewHierarchy(r, other, r, other, r).(*hierarchy)
	if wait := l.reserve(2000); wait != 0 {
		t.Fatalf("reserve(2000) within the burst wait=%s, want 0", time.Duration(wait))
	}
	for _, rate := range []*Rate{r, other} {
		if got := metered(rate); got != 2000 {
			t.Fatalf("metered = %d, want 2000 charged once", got)
		}
	}
}

// TestHierarchyRefundSkipsRestartedLevels refunds a charge to a
// limiter one of whose levels was disabled at the charge and enabled before
// the refund: that level was not charged, so it must not be refunded.
func TestHierarchyRefundSkipsRestartedLevels(t *testing.T) {
	conn, user := NewRate(1000), NewRate(1000)
	l := NewHierarchy(conn, user).(*hierarchy)
	user.Disable()
	c := NewReadWriteCloser(&changingWriteConn{change: user.Enable}, l)
	_, _ = c.Write(make([]byte, 500))
	if got := metered(conn); got != 0 {
		t.Fatalf("metered of the charged level after the refund = %d, want 0", got)
	}
	if got := metered(user); got != 0 {
		t.Fatalf("metered of the restarted level after the refund = %d, want 0", got)
	}
}

// A level that refuses a charge refuses it for the Hierarchy: Wait gives the
// bytes back to every level, while Charge keeps them and reports the refusal.
func TestHierarchyRefusalRefundsEveryLevel(t *testing.T) {
	r := NewRate(1 << 20)
	b := stopAfter(100)
	l := NewHierarchy(r, b)
	if err := l.Wait(bg, 1000); !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("Wait(1000) on a 100 B Budget = %v, want ErrBudgetExhausted", err)
	}
	if got := metered(r); got != 0 || b.Used() != 0 {
		t.Fatalf("after a refused Wait the Rate metered %d and the Budget used %d; want 0 and 0", got, b.Used())
	}
	if n, err := l.Ready(bg, 1000); n != 100 || err != nil {
		t.Fatalf("Ready(1000) = %d, %v; want the Budget's 100", n, err)
	}
	if err := l.Charge(1000); !errors.Is(err, ErrBudgetExhausted) || metered(r) != 1000 || b.Used() != 1000 {
		t.Fatalf("Charge(1000) = %v, metered %d, used %d; want ErrBudgetExhausted and the bytes kept", err, metered(r), b.Used())
	}
}

// NewHierarchy takes any Limiter: it flattens a Hierarchy given as a level,
// leaves out nil and repeated levels and collapses to a single one.
func TestNewHierarchyFlattens(t *testing.T) {
	r1, r2, b := NewRate(1), NewRate(2), stopAfter(1)
	s := NewSwitch(nil)
	inner := NewHierarchy(r1, b)
	l := NewHierarchy(inner, r2, b, (*Budget)(nil), (*Switch)(nil), s, s, nil).(*hierarchy)
	if len(l.lv) != 4 || l.lv[0].rate != r1 || l.lv[1].l != b || l.lv[2].rate != r2 || l.lv[3].l != s {
		t.Fatalf("levels = %+v, want r1, b, r2, s", l.lv)
	}
	if got := NewHierarchy(b, (*Rate)(nil)); got != Limiter(b) {
		t.Fatalf("NewHierarchy(b, nil) = %v, want b", got)
	}
}
