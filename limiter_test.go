package byterate

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestNewHierarchicalLimiterCollapses(t *testing.T) {
	cases := map[string]Limiter{
		"none":  NewHierarchicalLimiter(),
		"nil":   NewHierarchicalLimiter(nil),
		"two":   NewHierarchicalLimiter2(nil, nil),
		"three": NewHierarchicalLimiter3(nil, nil, nil),
		"four":  NewHierarchicalLimiter(nil, nil, nil, nil),
	}
	for name, l := range cases {
		if l != nil {
			t.Fatalf("%s: got %T(%v), want untyped nil", name, l, l)
		}
	}

	// A single Rate, limited or not, is returned itself.
	for _, r := range []*Rate{NewRate(0), NewRate(1024)} {
		if l := NewHierarchicalLimiter(nil, r, nil); l != Limiter(r) {
			t.Fatalf("single rate: got %#v, want the rate itself", l)
		}
	}
	unlimited, limited := NewRate(0), NewRate(1024)
	for _, l := range []Limiter{
		NewHierarchicalLimiter(nil, unlimited, limited),
		NewHierarchicalLimiter2(unlimited, limited),
		NewHierarchicalLimiter3(unlimited, nil, limited),
		NewHierarchicalLimiter(unlimited, limited, NewRate(0), NewRate(0)),
	} {
		if _, ok := l.(*HierarchicalLimiter); !ok {
			t.Fatalf("two or more rates: got %T, want *HierarchicalLimiter", l)
		}
	}
}

// TestHierarchicalLimiterMetersUnlimitedRates checks that an unlimited level,
// such as a global one kept only for its throughput, is charged too.
func TestHierarchicalLimiterMetersUnlimitedRates(t *testing.T) {
	global := NewRate(0)
	conn := NewRate(1 << 30)
	l := NewHierarchicalLimiter(conn, global)
	l.Get(64 << 10)
	for _, r := range []*Rate{global, conn} {
		r.lastSampleNs.Store(nowNs() - int64(time.Second))
		if got := r.Now(); got < 32<<10 {
			t.Fatalf("Now() = %d B/s, want the charged 64 KiB over about a second", got)
		}
	}
}

// TestHierarchicalLimiterAppliesLaterLimit checks that a limit set on a level
// after the limiter was built applies to it.
func TestHierarchicalLimiterAppliesLaterLimit(t *testing.T) {
	later := NewRate(0)
	l := NewHierarchicalLimiter(later, NewRate(0)).(*HierarchicalLimiter)
	later.SetLimit(1000)
	// Two seconds' burst is free; the third second of bytes must wait.
	if wait, _ := l.reserve(3000); wait < int64(900*time.Millisecond) {
		t.Fatalf("wait = %v, want about a second", time.Duration(wait))
	}
}

func TestHierarchicalLimiterRefundsAllChildrenOnShortWrite(t *testing.T) {
	first := NewRate(1 << 20)
	first.Start()
	second := NewRate(2 << 20)
	second.Start()

	limiter := NewHierarchicalLimiter(first, second)
	if _, ok := limiter.(*HierarchicalLimiter); !ok {
		t.Fatalf("NewHierarchicalLimiter() type = %T, want *HierarchicalLimiter", limiter)
	}

	wantErr := errors.New("short write")
	conn := NewRateReadWriteCloser(&scriptedConn{writeN: 2, writeErr: wantErr}, limiter)
	n, err := conn.Write([]byte("hello"))
	if !errors.Is(err, wantErr) {
		t.Fatalf("Write() error = %v, want %v", err, wantErr)
	}
	if n != 2 {
		t.Fatalf("Write() n = %d, want 2", n)
	}
	if got := first.bytesAcc.Load(); got != 2 {
		t.Fatalf("first bytesAcc after Write() = %d, want 2", got)
	}
	if got := second.bytesAcc.Load(); got != 2 {
		t.Fatalf("second bytesAcc after Write() = %d, want 2", got)
	}
}

func TestHierarchicalLimiterWakeOnPrimaryRateStop(t *testing.T) {
	first := NewRate(1)
	first.Start()
	second := NewRate(1)
	second.Start()

	limiter := NewHierarchicalLimiter(first, second)
	done := make(chan struct{})
	start := time.Now()
	go func() {
		limiter.Get(10)
		close(done)
	}()

	time.Sleep(20 * time.Millisecond)
	first.Stop()

	select {
	case <-done:
		if elapsed := time.Since(start); elapsed > 250*time.Millisecond {
			t.Fatalf("HierarchicalLimiter.Get() returned after %s, want fast wake on stop", elapsed)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("HierarchicalLimiter.Get() did not wake after primary rate stop")
	}
}

func TestHierarchicalLimiterWakeOnFirstEnabledRateStop(t *testing.T) {
	disabled := NewRate(0)
	disabled.Start()
	enabled := NewRate(1)
	enabled.Start()

	limiter := NewHierarchicalLimiter2(disabled, enabled)
	done := make(chan struct{})
	start := time.Now()
	go func() {
		limiter.Get(10)
		close(done)
	}()

	time.Sleep(20 * time.Millisecond)
	enabled.Stop()

	select {
	case <-done:
		if elapsed := time.Since(start); elapsed > 250*time.Millisecond {
			t.Fatalf("HierarchicalLimiter.Get() returned after %s, want fast wake on enabled stop", elapsed)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("HierarchicalLimiter.Get() did not wake after enabled rate stop")
	}
}

func TestHierarchicalLimiterWakeOnMaxWaitLimiterStop(t *testing.T) {
	fast := NewRate(1 << 20)
	fast.Start()
	slow := NewRate(1)
	slow.Start()

	limiter := NewHierarchicalLimiter2(fast, slow)
	done := make(chan struct{})
	start := time.Now()
	go func() {
		limiter.Get(10)
		close(done)
	}()

	time.Sleep(20 * time.Millisecond)
	slow.Stop()

	select {
	case <-done:
		if elapsed := time.Since(start); elapsed > 250*time.Millisecond {
			t.Fatalf("HierarchicalLimiter.Get() returned after %s, want fast wake on max-wait limiter stop", elapsed)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("HierarchicalLimiter.Get() did not wake after max-wait limiter stop")
	}
}

func TestHierarchicalLimiterGetContextCancel(t *testing.T) {
	first := NewRate(1024)
	second := NewRate(2048)
	limiter := NewHierarchicalLimiter2(first, second).(*HierarchicalLimiter)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- limiter.GetContext(ctx, 64<<10)
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("GetContext() error = %v, want %v", err, context.Canceled)
		}
	case <-time.After(500 * time.Millisecond):
		first.Stop()
		second.Stop()
		t.Fatal("HierarchicalLimiter.GetContext() did not return promptly after cancel")
	}
}

// TestStopBetweenChargeAndWait stops the Rate imposing the wait right after
// the charge, before the caller sleeps: Stop must still end the wait.
func TestStopBetweenChargeAndWait(t *testing.T) {
	cases := map[string]func(strict *Rate) func(){
		"Rate.Get": func(strict *Rate) func() {
			return func() { strict.Get(3000) }
		},
		"Rate.GetContext": func(strict *Rate) func() {
			return func() { _ = strict.GetContext(context.Background(), 3000) }
		},
		"HierarchicalLimiter.Get": func(strict *Rate) func() {
			l := NewHierarchicalLimiter3(NewRate(1<<30), strict, NewRate(0))
			return func() { l.Get(3000) }
		},
		"HierarchicalLimiter.GetContext": func(strict *Rate) func() {
			l := NewHierarchicalLimiter2(NewRate(1<<30), strict).(*HierarchicalLimiter)
			return func() { _ = l.GetContext(context.Background(), 3000) }
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
					once.Do(func() { strict.ResetLimit(0) })
				}
			}
			testHookReserved.Store(&hook)
			defer testHookReserved.Store(nil)

			done := make(chan struct{})
			go func() {
				op()
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(time.Second):
				strict.Stop() // release the stale wait
				<-done
				t.Fatal("ResetLimit between the charge and the wait did not end the wait")
			}
			hookRan := true
			once.Do(func() { hookRan = false })
			if !hookRan {
				t.Fatal("the test hook did not run")
			}
		})
	}
}

// TestHierarchicalLimiterChargesRepeatedRateOnce checks that a Rate given at
// more than one level, such as a user's that doubles as its group's, is not
// charged twice.
func TestHierarchicalLimiterChargesRepeatedRateOnce(t *testing.T) {
	r := NewRate(1000)
	if l := NewHierarchicalLimiter(r, nil, r); l != Limiter(r) {
		t.Fatalf("a Rate given twice: got %T, want the Rate itself", l)
	}
	other := NewRate(0)
	l := NewHierarchicalLimiter(r, other, r, other, r).(*HierarchicalLimiter)
	if wait, _ := l.reserve(2000); wait != 0 {
		t.Fatalf("reserve(2000) within the burst wait=%s, want 0", time.Duration(wait))
	}
	for _, rate := range []*Rate{r, other} {
		if got := rate.bytesAcc.Load(); got != 2000 {
			t.Fatalf("bytesAcc = %d, want 2000 charged once", got)
		}
	}
}
