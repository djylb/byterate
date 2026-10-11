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
					rates[i].Stop()
				}
			}
			c := NewRateReadWriteCloser(&scriptedConn{writeN: 50}, NewHierarchicalLimiter(rates...))
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
					r.Get(2000)
					r.reserve(10000)
					r.SetLimit(0)
				}
				c := NewRateReadWriteCloser(&changingWriteConn{change: func() {
					r.SetLimit(1000)
					if !temporary {
						r.Get(2000)
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

func TestHierarchicalWriteReceipts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Exercise limited, unlimited and stopped levels, including receipts
		// beyond the inline storage.
		rates := []*Rate{NewRate(0), NewRate(1000), NewRate(0), NewRate(1000), NewRate(1000)}
		for range 5 {
			rates = append(rates, NewRate(1000))
		}
		rates[2].Stop()
		l := NewHierarchicalLimiter(rates...)
		c := NewRateReadWriteCloser(&changingWriteConn{change: func() {
			for _, r := range rates {
				r.ResetLimit(1000)
				r.Get(2000)
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

func TestHierarchyRestartBeforeLevelCharge(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		first, second := NewRate(1), NewRate(1000)
		first.Get(2)
		hook := func(r *Rate) {
			if r == first {
				// The second level restarts after the write begins but before
				// it is charged. Its later charge must still be refundable.
				second.ResetLimit(1000)
				first.Stop()
			}
		}
		testHookReserved.Store(&hook)
		defer testHookReserved.Store(nil)
		c := NewRateReadWriteCloser(&scriptedConn{writeErr: errors.New("failed")}, NewHierarchicalLimiter(first, second))
		_, _ = c.Write(make([]byte, 4))
		if got := metered(second); got != 0 {
			t.Fatalf("refund of new-generation charge lost: meter=%d", got)
		}
	})
}

// restartOnCharge restarts r once from inside its next charge, after the
// charger has read r's generation, as a ResetLimit on another goroutine can.
func restartOnCharge(t *testing.T, r *Rate) {
	var once sync.Once
	hook := func(c *Rate) {
		if c == r {
			once.Do(func() { r.ResetLimit(r.Limit()) })
		}
	}
	testHookCharging.Store(&hook)
	t.Cleanup(func() { testHookCharging.Store(nil) })
}

// TestChargeAfterConcurrentRestart charges a Rate that is restarted between
// the charger reading its generation and charging it: the charge lands in
// the new generation, so it must wait there, and a write's refund must
// reach it.
func TestChargeAfterConcurrentRestart(t *testing.T) {
	type charger func(r *Rate) error
	failed := errors.New("write failed")
	cases := map[string]charger{
		"Get": func(r *Rate) error { r.Get(4000); return nil },
		"GetContext": func(r *Rate) error {
			return r.GetContext(context.Background(), 4000)
		},
		"Hierarchical": func(r *Rate) error {
			NewHierarchicalLimiter2(NewRate(0), r).Get(4000)
			return nil
		},
		"Read": func(r *Rate) error {
			_, err := NewRateConn(&nopNetConn{}, r).Read(make([]byte, 4000))
			return err
		},
		"Write": func(r *Rate) error {
			_, err := NewRateConn(&nopNetConn{}, r).Write(make([]byte, 4000))
			return err
		},
		"HierarchicalWrite": func(r *Rate) error {
			_, err := NewRateConn(&nopNetConn{}, NewHierarchicalLimiter2(NewRate(0), r)).Write(make([]byte, 4000))
			return err
		},
		"FailedWrite": func(r *Rate) error {
			_, err := NewRateReadWriteCloser(&scriptedConn{writeErr: failed}, r).Write(make([]byte, 4000))
			if !errors.Is(err, failed) {
				return err
			}
			return nil
		},
	}
	for name, charge := range cases {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				r := NewRate(1000)
				r.Get(2000) // the burst, which the restart restores
				restartOnCharge(t, r)
				start := time.Now()
				if err := charge(r); err != nil {
					t.Fatal(err)
				}
				if got := time.Since(start); got != 2*time.Second {
					t.Fatalf("4000 B at 1000 B/s after a restart restoring a 2000 B burst waited %v, want 2s", got)
				}
				want := int64(0) // the refund restores the burst
				if name != "FailedWrite" {
					want = int64(2 * time.Second) // the 2000 B on top of the 4000
				}
				if wait := r.reserve(2000); wait != want {
					t.Fatalf("reserve(2000) afterwards waits %v, want %v", time.Duration(wait), time.Duration(want))
				}
			})
		})
	}
}

// TestChargeWaitsForResettingStart charges a Rate whose Start has stored a
// new generation but not yet reset tat: the charge must wait for the reset
// and land after it, in the new generation. It runs on the real clock, as a
// goroutine blocked on a mutex is not durably blocked in a synctest bubble.
func TestChargeWaitsForResettingStart(t *testing.T) {
	r := NewRate(1000)
	r.reserve(10000) // 10s of debt in the old generation
	r.mu.Lock()      // a Start in progress
	gen := &stopSignal{ch: make(chan struct{})}
	r.stop.Store(gen)
	type result struct {
		wait int64
		stop <-chan struct{}
	}
	done := make(chan result, 1)
	go func() {
		wait, stop := r.reserveAt(4000, nowNs())
		done <- result{wait, stop}
	}()
	time.Sleep(20 * time.Millisecond)
	select {
	case <-done:
		r.mu.Unlock()
		t.Fatal("charge returned while Start was resetting tat")
	default:
	}
	r.tat.Store(nowNs() - burstWindowNs)
	gen.ready.Store(true)
	r.mu.Unlock()
	got := <-done
	if wait := time.Duration(got.wait); wait > 3*time.Second || wait < time.Second {
		t.Fatalf("charge after the reset waits %v, want about 2s", wait)
	}
	if got.stop != gen.ch {
		t.Fatal("charge after the reset waits on another generation's channel")
	}
}
