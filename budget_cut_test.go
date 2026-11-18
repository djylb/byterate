package byterate

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
)

// lowerOnReady sets b's steps on its first Ready: after the levels cut the
// write to b's grant, before b is charged. Without steps, it sets a Stop at
// stop.
type lowerOnReady struct {
	b     *Budget
	steps []Step
	stop  int64
	done  bool
}

func (*lowerOnReady) Wait(context.Context, int64) error { return nil }
func (*lowerOnReady) TryWait(int64) error               { return nil }
func (*lowerOnReady) Refund(int64)                      {}
func (*lowerOnReady) Charge(int64) error                { return nil }

func (l *lowerOnReady) Ready(_ context.Context, n int64) (int64, error) {
	if !l.done {
		l.done = true
		if l.steps == nil {
			l.steps = []Step{{After: l.stop, Stop: true}}
		}
		l.b.SetSteps(l.steps...) //nolint:contextcheck // SetSteps takes no context
	}
	return n, nil
}

// netCharge is a Limiter that keeps the bytes charged to it, refunds taken
// off.
type netCharge struct{ n atomic.Int64 }

func (c *netCharge) Wait(_ context.Context, n int64) error { c.n.Add(n); return nil }
func (c *netCharge) TryWait(n int64) error                 { c.n.Add(n); return nil }
func (c *netCharge) Refund(n int64)                        { c.n.Add(-n) }
func (c *netCharge) Charge(n int64) error                  { c.n.Add(n); return nil }
func (*netCharge) Ready(_ context.Context, n int64) (int64, error) {
	return n, nil
}

// A stop lowered between the grant and the charge still lets the bytes
// below it through, and the charge that reaches it reports the step once.
func TestBudgetStopLoweredBeforeCharge(t *testing.T) {
	before := new(netCharge)
	for name, wrap := range map[string]func(*Budget, Limiter) Limiter{
		"group": func(b *Budget, l Limiter) Limiter {
			g := NewGroup(nil)
			g.SetLimiters(nil, NewHierarchy(b, l))
			return g.Writes()
		},
		"group-rate": func(b *Budget, l Limiter) Limiter {
			g := NewGroup(nil)
			g.SetLimiters(nil, NewHierarchy(NewRate(1<<30), before, b, l))
			return g.Writes()
		},
		"nested": func(b *Budget, l Limiter) Limiter {
			g := NewGroup(nil)
			g.SetLimiters(nil, NewHierarchy(NewRate(1<<30), NewHierarchy(b), l))
			return g.Writes()
		},
	} {
		t.Run(name, func(t *testing.T) {
			before.n.Store(0)
			b := NewBudget(Step{After: 1000, Stop: true})
			var steps []int64
			b.OnStep(func(_ int, u int64) { steps = append(steps, u) })
			c := NewConn(&nopNetConn{}, WithLimiters(nil, wrap(b, &lowerOnReady{b: b, stop: 100})))
			n, err := c.Write(make([]byte, 400))
			if n != 100 || !errors.Is(err, ErrBudgetExhausted) || b.Used() != 100 || len(steps) != 1 || steps[0] != 100 {
				t.Fatalf("wrote %d (%v), used %d, steps %v; want 100, ErrBudgetExhausted, 100, [100]", n, err, b.Used(), steps)
			}
			// A level charged before the Budget cut the write gives back
			// the bytes cut.
			if name == "group-rate" && before.n.Load() != 100 {
				t.Fatalf("the level before the Budget kept %d bytes, want 100", before.n.Load())
			}
		})
	}
}

// Connections sharing a Budget whose step has a Limit, so that every Write
// charges it rather than a lease, stop exactly at the Stop: no Write is
// refused before the usage reaches it, the bytes written are the bytes
// used, and the Stop is reported once.
func TestBudgetSharedStopExact(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const stop = 1_000_003
		b := NewBudget(Step{After: 0, Limit: NewRate(1 << 30)}, Step{After: stop, Stop: true})
		var stops atomic.Int64
		b.OnStep(func(s int, u int64) {
			if s == 1 && u == stop {
				stops.Add(1)
			}
		})
		g := NewGroup(nil)
		g.SetLimiters(nil, b)
		var written, early atomic.Int64
		var wg sync.WaitGroup
		for i := range 16 {
			wg.Go(func() {
				c := NewConn(&nopNetConn{}, WithLimiters(nil, g.Writes()))
				defer func() { _ = c.Close() }()
				buf := make([]byte, 4<<10+i*97)
				for {
					n, err := c.Write(buf)
					written.Add(int64(n))
					if err != nil {
						if !errors.Is(err, ErrBudgetExhausted) || b.Used() != stop {
							early.Add(1)
						}
						return
					}
				}
			})
		}
		wg.Wait()
		if early.Load() != 0 || written.Load() != stop || b.Used() != stop || stops.Load() != 1 {
			t.Fatalf("early refusals %d, wrote %d, used %d, Stop reported %d times; want 0, %d, %d, 1",
				early.Load(), written.Load(), b.Used(), stops.Load(), stop, stop)
		}
	})
}

// A step moved below a grant between the grant and the charge applies from
// its After: the charge stops there and the step is reported at it.
func TestBudgetStepLoweredBeforeCharge(t *testing.T) {
	b := NewBudget(Step{After: 1000, Stop: true})
	var steps []int64
	b.OnStep(func(_ int, u int64) { steps = append(steps, u) })
	g := NewGroup(nil)
	g.SetLimiters(nil, NewHierarchy(b, &lowerOnReady{b: b, steps: []Step{
		{After: 100, Limit: NewRate(1 << 30)}, {After: 1000, Stop: true},
	}}))
	c := NewConn(&nopNetConn{}, WithLimiters(nil, g.Writes()))
	n, err := c.Write(make([]byte, 400))
	if n != 400 || err != nil || b.Used() != 400 || len(steps) != 1 || steps[0] != 100 {
		t.Fatalf("wrote %d (%v), used %d, steps %v; want 400, nil, 400, [100]", n, err, b.Used(), steps)
	}
}
