package byterate

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

func TestWriteReceiptRefundsEveryLevel(t *testing.T) {
	for _, count := range []int{1, 3, 5, 10} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			rates := make([]*Rate, count)
			for i := range rates {
				rates[i] = NewRate(1000)
				switch i % 3 {
				case 1:
					rates[i].SetLimit(0)
				case 2:
					rates[i].Disable()
				}
			}
			c := NewReadWriteCloser(&scriptedConn{writeN: 50}, NewHierarchy(rates...))
			_, _ = c.Write(make([]byte, 200))
			for i, r := range rates {
				want := int64(50)
				if i%3 == 2 {
					want = 0
				}
				if got := metered(r); got != want {
					t.Fatalf("level %d: short write left %d bytes, want %d", i, got, want)
				}
			}
		})
	}
}

type changingWriteConn struct {
	scriptedConn
	change func()
}

func (c *changingWriteConn) Write([]byte) (int, error) {
	c.change()
	return 0, errors.New("write failed")
}

// A write charged while unlimited incurred no debt. Refunding its bytes
// must not cancel another write's debt, even when the limit has changed.
func TestUnlimitedWriteRefundPreservesOtherDebt(t *testing.T) {
	for _, temporary := range []bool{false, true} {
		t.Run(map[bool]string{false: "new_limit", true: "temporary_unlimited"}[temporary], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				r := NewRate(0)
				if temporary {
					r.SetLimit(1000)
					_ = r.Wait(bg, 2000)
					r.reserve(10000)
					r.SetLimit(0)
				}
				c := NewReadWriteCloser(&changingWriteConn{change: func() {
					r.SetLimit(1000)
					if !temporary {
						_ = r.Wait(bg, 2000)
						r.reserve(10000)
					}
				}}, r)
				_, _ = c.Write(make([]byte, 10000))
				if wait := time.Duration(r.reserve(1)); wait < 10*time.Second {
					t.Fatalf("unlimited write's refund erased another charge: wait = %v, want >=10s", wait)
				}
			})
		})
	}
}

func TestHierarchyWriteReceipts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Exercise limited, unlimited and disabled levels, including receipts
		// beyond the inline storage.
		rates := []*Rate{NewRate(0), NewRate(1000), NewRate(0), NewRate(1000), NewRate(1000)}
		for range 5 {
			rates = append(rates, NewRate(1000))
		}
		rates[2].Disable()
		l := NewHierarchy(rates...)
		c := NewReadWriteCloser(&changingWriteConn{change: func() {
			for _, r := range rates {
				r.Reset(1000)
				_ = r.Wait(bg, 2000)
				r.reserve(10000)
			}
		}}, l)
		_, _ = c.Write(make([]byte, 500))
		for i, r := range rates {
			if got := metered(r); got != 12000 {
				t.Fatalf("level %d stale refund changed new meter: %d", i, got)
			}
			if wait := r.reserve(1); wait < int64(10*time.Second) {
				t.Fatalf("level %d stale refund changed new debt: %v", i, time.Duration(wait))
			}
		}
	})
}

func TestHierarchyResetBeforeLevelCharge(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		first, second := NewRate(1), NewRate(1000)
		_ = first.Wait(bg, 2)
		hook := func(r *Rate) {
			if r == first {
				// The second level restarts after the write begins but before
				// it is charged. Its later charge must still be refundable.
				second.Reset(1000)
				first.Disable()
			}
		}
		testHooks.reserved.Store(&hook)
		defer testHooks.reserved.Store(nil)
		c := NewReadWriteCloser(&scriptedConn{writeErr: errors.New("failed")}, NewHierarchy(first, second))
		_, _ = c.Write(make([]byte, 4))
		if got := metered(second); got != 0 {
			t.Fatalf("refund of new-generation charge lost: meter=%d", got)
		}
	})
}

// restartOnCharge restarts r once from inside its next charge, after the
// charger has read r's generation, as a Reset on another goroutine can.
func restartOnCharge(t *testing.T, r *Rate) {
	var once sync.Once
	hook := func(c *Rate) {
		if c == r {
			once.Do(func() { r.Reset(r.Limit()) })
		}
	}
	testHooks.charging.Store(&hook)
	t.Cleanup(func() { testHooks.charging.Store(nil) })
}

// TestChargeAfterConcurrentReset charges a Rate that is restarted between
// the charger reading its generation and charging it: the charge lands in
// the new generation, so it must wait there, and a write's refund must
// reach it.
func TestChargeAfterConcurrentReset(t *testing.T) {
	failed := errors.New("write failed")
	// The charges of 4000 B at 1000 B/s that wait for all of it take 2s on
	// the 2000 B burst the restart restores and leave 2s of debt. A Read
	// takes a quantum, 512 B, as debt without waiting. A failed Write fails
	// after its first grant, whose refund restores the burst. The Writes
	// span sampling windows, so only the others' meters show the charge.
	cases := map[string]struct {
		charge  func(r *Rate) error
		elapsed time.Duration
		meter   int64 // -1: not checked
		after   time.Duration
	}{
		"Wait": {func(r *Rate) error {
			return r.Wait(context.Background(), 4000)
		}, 2 * time.Second, 4000, 2 * time.Second},
		"Hierarchical": {func(r *Rate) error {
			return NewHierarchy(NewRate(0), r).Wait(bg, 4000)
		}, 2 * time.Second, 4000, 2 * time.Second},
		"Read": {func(r *Rate) error {
			_, err := NewConn(&nopNetConn{}, r).Read(make([]byte, 4000))
			return err
		}, 0, 512, 512 * time.Millisecond},
		"Write": {func(r *Rate) error {
			_, err := NewConn(&nopNetConn{}, r).Write(make([]byte, 4000))
			return err
		}, 2 * time.Second, -1, 2 * time.Second},
		"HierarchicalWrite": {func(r *Rate) error {
			_, err := NewConn(&nopNetConn{}, NewHierarchy(NewRate(0), r)).Write(make([]byte, 4000))
			return err
		}, 2 * time.Second, -1, 2 * time.Second},
		"FailedWrite": {func(r *Rate) error {
			_, err := NewReadWriteCloser(&scriptedConn{writeErr: failed}, r).Write(make([]byte, 4000))
			if !errors.Is(err, failed) {
				return err
			}
			return nil
		}, 0, 0, 0},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				r := NewRate(1000)
				_ = r.Wait(bg, 2000) // the burst, which the restart restores
				restartOnCharge(t, r)
				start := time.Now()
				if err := tc.charge(r); err != nil {
					t.Fatal(err)
				}
				if got := time.Since(start); got != tc.elapsed {
					t.Fatalf("charge after a restart restoring a 2000 B burst took %v, want %v", got, tc.elapsed)
				}
				// The new generation meters the charge once, and its refund.
				if got := metered(r); tc.meter >= 0 && got != tc.meter {
					t.Fatalf("meter after the charge = %d, want %d", got, tc.meter)
				}
				if wait := r.reserve(2000); wait != int64(tc.after) {
					t.Fatalf("reserve(2000) afterwards waits %v, want %v", time.Duration(wait), tc.after)
				}
			})
		})
	}
}

// TestResetRaceLandsInOneGeneration races a charge that must wait with a
// restart on a clock that does not move, as within one tick of a coarse
// clock, where a new generation starts with the same debt the old one had.
// The charge must land in one generation and act in that one alone: if its
// debt is in the new generation, it must wait there.
func TestResetRaceLandsInOneGeneration(t *testing.T) {
	for _, refunded := range []bool{false, true} {
		t.Run(map[bool]string{false: "fresh", true: "refunded"}[refunded], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				for range 20000 {
					r := NewRate(1000)
					if refunded {
						r.reserve(1000)
						r.Refund(1000) // tat back at the burst floor
					}
					var elapsed time.Duration
					var wg sync.WaitGroup
					start := make(chan struct{})
					wg.Go(func() {
						<-start
						s := time.Now()
						_ = r.Wait(bg, 5000) // 3s after the 2s burst
						elapsed = time.Since(s)
					})
					wg.Go(func() {
						<-start
						r.Disable()
						r.Enable()
					})
					close(start)
					wg.Wait()
					if inNew := r.gen.Load().tat.Load() > nowNs(); inNew && elapsed != 3*time.Second {
						t.Fatalf("charge in the new generation waited %v, want 3s", elapsed)
					}
				}
			})
		})
	}
}

// TestSaturatedDebtInBubble saturates a Rate's debt inside a synctest bubble,
// whose clock is below the package epoch: waits must stay long, not wrap.
func TestSaturatedDebtInBubble(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := NewRate(1000)
		r.reserve(maxI64)
		r.reserve(maxI64)
		if wait := r.reserve(1000); wait < int64(time.Hour) {
			t.Fatalf("wait after saturating the debt = %v, want it saturated too", time.Duration(wait))
		}
		r.SetLimit(500) // converts the saturated debt
		if wait := r.reserve(1000); wait < int64(time.Hour) {
			t.Fatalf("wait after converting a saturated debt = %v, want it saturated too", time.Duration(wait))
		}
	})
}

// TestConvertDebtAlwaysChangesTat lowers the limit of a Rate with 1 ns of
// debt, which the conversion leaves at 1 ns: tat must still change, so that
// a charge that loaded it before fails its CAS and retries at the new limit.
func TestConvertDebtAlwaysChangesTat(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := NewRate(1000)
		g := r.gen.Load()
		before := nowNs() + 1
		g.tat.Store(before)
		r.SetLimit(400) // 1 ns at 1000 B/s is 2 ns at 400 B/s, less the 1 ns step
		if g.tat.Load() == before {
			t.Fatal("SetLimit left tat unchanged")
		}
	})
}

// TestDisableWakesEveryWaiter disables a Rate that many connections and
// callers of Wait wait on, spread over the channels of its generation: every
// one must wake at once.
func TestDisableWakesEveryWaiter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := NewRate(1000)
		r.reserve(2000) // spend the burst
		var wg sync.WaitGroup
		for range 200 {
			c := NewConn(&nopNetConn{}, r)
			wg.Go(func() { _, _ = c.Write(make([]byte, 1000)) })
			wg.Go(func() { _ = r.Wait(bg, 1000) })
		}
		synctest.Wait()
		used := 0
		for i := range r.gen.Load().stop.Load().shards {
			if r.gen.Load().stop.Load().shards[i].Load() != nil {
				used++
			}
		}
		if used < 2 {
			t.Fatalf("400 waiters share %d channel(s)", used)
		}
		// The first waiters took the channels of the small first set, which
		// Disable must close too once a larger set has replaced it.
		if set := r.gen.Load().stop.Load(); sharedStopShards() > initialStopShards && set.prev == nil {
			t.Fatalf("stop set of %d channels did not grow for 400 waiters", len(set.shards))
		}
		start := time.Now()
		r.Disable()
		wg.Wait()
		if elapsed := time.Since(start); elapsed != 0 {
			t.Fatalf("Disable woke the waiters after %v, want at once", elapsed)
		}
	})
}

// TestPrivateRateKeepsSmallStopSet throttles both directions of a connection
// with a Rate of its own, as one per connection is used in a hierarchy: its
// waits must keep to the small first stop set, a channel per direction,
// instead of growing it to the size of a shared Rate's.
func TestPrivateRateKeepsSmallStopSet(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := NewRate(1000)
		r.reserve(2000) // spend the burst
		c := NewConn(&nopNetConn{}, r)
		var wg sync.WaitGroup
		for range 50 {
			wg.Go(func() { _, _ = c.Write(make([]byte, 100)) })
			wg.Go(func() { _, _ = c.Read(make([]byte, 100)) })
			synctest.Wait()
		}
		set := r.gen.Load().stop.Load()
		if set == nil || len(set.shards) != initialStopShards || set.made.Load() > 2 {
			t.Fatalf("a connection's waits made a stop set of %d channels, %d of them used",
				len(set.shards), set.made.Load())
		}
		r.Disable()
		wg.Wait()
	})
}
