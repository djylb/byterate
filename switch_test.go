package byterate

import (
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

// Set ends a wait on the old Limiter, refunds it there, and charges the new
// one instead.
func TestSwitchMovesWaitToNewLimiter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		slow, fast := NewRate(1000), NewRate(1<<20)
		slow.reserve(2000) // spend the burst
		s := NewSwitch(slow)
		done := make(chan time.Duration, 1)
		start := time.Now()
		go func() {
			_ = s.Wait(bg, 10000) // 10s on slow
			done <- time.Since(start)
		}()
		synctest.Wait()
		s.Set(fast)
		if got := <-done; got != 0 {
			t.Fatalf("Wait took %v after Set, want it moved to the fast limiter at once", got)
		}
		if metered(slow) != 2000 || metered(fast) != 10000 {
			t.Fatalf("metered slow/fast = %d/%d, want the charge moved: 2000/10000", metered(slow), metered(fast))
		}
	})
}

// A connection limited through a Switch follows Set for the bytes it has not
// sent yet.
func TestConnFollowsSwitch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		slow, fast := NewRate(1000), NewRate(1<<20)
		s := NewSwitch(slow)
		inner := &dataConn{}
		conn := NewConn(inner, s)
		done := make(chan time.Duration, 1)
		start := time.Now()
		go func() {
			_, _ = conn.Write(make([]byte, 100000))
			done <- time.Since(start)
		}()
		time.Sleep(time.Second)
		s.Set(fast)
		if got := <-done; got > time.Second+time.Millisecond {
			t.Fatalf("Write took %v, want it to finish on the new limiter at once after 1s", got)
		}
		if len(inner.data) != 100000 {
			t.Fatalf("connection got %d B, want the 100000 written", len(inner.data))
		}
		// slow kept its burst and what it passed in the second before the
		// switch, not the grant the switch ended.
		if got := metered(slow); got > 3000 {
			t.Fatalf("slow limiter kept %d B, want only what it passed", got)
		}
	})
}

func TestZeroSwitchPasses(t *testing.T) {
	var s Switch
	if err := s.Wait(bg, 1<<30); err != nil {
		t.Fatal(err)
	}
	if n, err := s.Ready(bg, 1<<30); n != 1<<30 || err != nil {
		t.Fatalf("Ready of a zero Switch = %d, %v", n, err)
	}
	s.Set(NewRate(1000))
	if s.Limiter() == nil {
		t.Fatal("Set on a zero Switch kept no Limiter")
	}
}

// A Switch inside a Hierarchy refunds a failed write to the Limiter it
// charged, not to the one Set put in its place since.
func TestNestedSwitchRefundsWhereItCharged(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := NewRate(1000)
		r.reserve(2000) // spend the burst, so that the grant waits
		a, b := NewBudget(), NewBudget()
		s := NewSwitch(a)
		failed := errors.New("write failed")
		conn := NewReadWriteCloser(&scriptedConn{writeErr: failed}, NewHierarchy(r, s))
		done := make(chan error, 1)
		go func() {
			_, err := conn.Write(make([]byte, 100))
			done <- err
		}()
		synctest.Wait() // charged to a, waiting on r
		s.Set(b)
		if err := <-done; !errors.Is(err, failed) {
			t.Fatalf("Write() = %v, want %v", err, failed)
		}
		if a.Used() != 0 || b.Used() != 0 {
			t.Fatalf("budgets used a/b = %d/%d, want the failed write refunded to a: 0/0", a.Used(), b.Used())
		}
	})
}

func TestSwitchZeroCountsDoNotWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := NewRate(1000)
		r.reserve(12000) // 10s of debt
		s := NewSwitch(r)
		start := time.Now()
		if n, err := s.Ready(bg, 0); n != 0 || err != nil || time.Since(start) != 0 {
			t.Fatalf("Ready(0) = %d, %v after %v; want 0 at once", n, err, time.Since(start))
		}
		if err := s.Wait(bg, 0); err != nil || time.Since(start) != 0 {
			t.Fatalf("Wait(0) = %v after %v; want nil at once", err, time.Since(start))
		}
	})
}

func TestSwitchSetToItselfPanics(t *testing.T) {
	s := NewSwitch(nil)
	defer func() {
		if recover() == nil {
			t.Fatal("Set(s) on s did not panic")
		}
	}()
	s.Set(s)
}
