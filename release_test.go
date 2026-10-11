package byterate

import (
	"errors"
	"fmt"
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
