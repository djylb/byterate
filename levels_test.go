package byterate

import (
	"errors"
	"testing"
)

// A Write refunds the Rates it charged inside a Switch, a Budget's step and a
// Group exactly, within the generation charged: a Reset between the charge
// and the refund leaves the new generation alone, while the Budget and the
// Counter get their bytes back.
func TestNestedRefundsSkipRestartedRates(t *testing.T) {
	type setup struct {
		l      Limiter
		budget *Budget
		count  *Counter
	}
	cases := map[string]func(r *Rate) setup{
		"switch": func(r *Rate) setup { return setup{l: NewSwitch(r)} },
		"budget step": func(r *Rate) setup {
			b := NewBudget(Step{After: 0, Limit: r})
			return setup{l: NewHierarchy(NewRate(0), b), budget: b}
		},
		"switch of hierarchy": func(r *Rate) setup {
			var c Counter
			return setup{l: NewHierarchy(NewRate(0), NewSwitch(NewHierarchy(r, c.Writes()))), count: &c}
		},
		"group in hierarchy": func(r *Rate) setup {
			g := NewGroup(nil)
			g.SetWriteLimiter(r)
			return setup{l: NewHierarchy(NewRate(0), g.Writes()), count: g.Traffic()}
		},
	}
	for name, build := range cases {
		t.Run(name, func(t *testing.T) {
			r := NewRate(1000)
			s := build(r)
			c := NewReadWriteCloser(&changingWriteConn{change: func() { r.Reset(1000) }}, s.l)
			if _, err := c.Write(make([]byte, 500)); err == nil {
				t.Fatal("Write through a failing connection succeeded")
			}
			if got := metered(r); got != 0 {
				t.Fatalf("metered of the reset Rate after the refund = %d, want 0", got)
			}
			if s.budget != nil && s.budget.Used() != 0 {
				t.Fatalf("Budget used %d after the refund, want 0", s.budget.Used())
			}
			if s.count != nil {
				if _, out := s.count.Total(); out != 0 {
					t.Fatalf("Counter counted %d after the refund, want 0", out)
				}
			}
		})
	}
}

// A Limiter that would charge itself is refused wherever it is set, also
// through other Limiters.
func TestLimiterCyclesRefused(t *testing.T) {
	mustPanic := func(name string, f func()) {
		t.Helper()
		defer func() {
			if recover() == nil {
				t.Errorf("%s did not panic", name)
			}
		}()
		f()
	}
	s := NewSwitch(nil)
	mustPanic("Switch set to itself", func() { s.Set(s) })
	mustPanic("Switch set to a hierarchy of itself", func() { s.Set(NewHierarchy(NewRate(1), s)) })
	b := NewBudget()
	mustPanic("Budget step limited by itself", func() { b.SetSteps(Step{Limit: b}) })
	s.Set(b)
	mustPanic("Budget step limited by a Switch of itself", func() { b.SetSteps(Step{Limit: s}) })
	b.SetSteps(Step{After: 10, Limit: NewRate(1)})
	mustPanic("Switch set to a Budget stepping to the Switch", func() {
		other := NewBudget(Step{Limit: s})
		s.Set(other)
	})

	root := NewGroup(nil)
	child := NewGroup(root)
	mustPanic("Group limited by a descendant's Writer", func() { root.SetWriteLimiter(child.Writes()) })
	mustPanic("Group limited by its Reader through a Switch", func() { child.SetReadLimiter(NewSwitch(child.Reads())) })
	other := NewGroup(nil)
	other.SetWriteLimiter(child.Writes()) // child's writes are not other's
	if err := root.SetParent(other); !errors.Is(err, ErrGroupCycle) {
		t.Fatalf("SetParent under a Group limited by a descendant's Writer = %v, want ErrGroupCycle", err)
	}
	if err := child.SetParent(NewGroup(nil)); err != nil {
		t.Fatalf("SetParent to a new root = %v", err)
	}
}
