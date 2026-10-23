package byterate

import (
	"context"
	"errors"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// tree returns a root, a user under it, a client under the user and a
// tunnel under the client.
func tree() (root, user, client, tunnel *Group) {
	root = NewGroup(nil)
	user = NewGroup(root)
	client = NewGroup(user)
	tunnel = NewGroup(client)
	return root, user, client, tunnel
}

// Acquire takes a slot at every level or none, and a full level refuses.
func TestGroupConnSlots(t *testing.T) {
	root, user, client, tunnel := tree()
	root.SetMaxConns(3)
	user.SetMaxConns(2)
	for range 2 {
		if err := tunnel.Acquire(); err != nil {
			t.Fatal(err)
		}
	}
	if err := client.Acquire(); !errors.Is(err, ErrTooManyConns) {
		t.Fatalf("third Acquire under a user capped at 2 = %v, want ErrTooManyConns", err)
	}
	for _, g := range []*Group{root, user, client} {
		if g.Conns() != 2 {
			t.Fatalf("Conns() after a refused Acquire = %d, want the 2 taken, at every level", g.Conns())
		}
	}
	if tunnel.Conns() != 2 {
		t.Fatalf("tunnel Conns() = %d, want 2", tunnel.Conns())
	}
	other := NewGroup(root)
	if err := other.Acquire(); err != nil { // the root's third
		t.Fatal(err)
	}
	if err := NewGroup(root).Acquire(); !errors.Is(err, ErrTooManyConns) {
		t.Fatalf("Acquire past the root's cap = %v, want ErrTooManyConns", err)
	}
	tunnel.Release()
	other.Release()
	if root.Conns() != 1 || user.Conns() != 1 || tunnel.Conns() != 1 {
		t.Fatalf("Conns() after releases = %d/%d/%d, want 1 each", root.Conns(), user.Conns(), tunnel.Conns())
	}
	user.SetMaxConns(0)
	if user.MaxConns() != 0 {
		t.Fatalf("MaxConns() after SetMaxConns(0) = %d, want 0", user.MaxConns())
	}
}

// A connection opened in a Group is limited by every level and counted in
// every level's traffic, bytes read and written; closing it gives its slot
// back once.
func TestGroupOpenLimitsAndCounts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		root, user, client, tunnel := tree()
		user.SetWriteLimiter(NewRate(1000)) // 1000 B/s of writes for the user
		conn, err := tunnel.Open(&recordingConn{})
		if err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		if n, err := conn.Write(make([]byte, 3000)); n != 3000 || err != nil {
			t.Fatalf("Write = %d, %v", n, err)
		}
		if got := time.Since(start); got != time.Second {
			t.Fatalf("Write of 3000 B under a 1000 B/s user took %v, want 1s past the burst", got)
		}
		if n, err := conn.Read(make([]byte, 500)); n != 500 || err != nil {
			t.Fatalf("Read = %d, %v", n, err)
		}
		for name, g := range map[string]*Group{"root": root, "user": user, "client": client, "tunnel": tunnel} {
			if in, out := g.Traffic().Load(); in != 500 || out != 3000 || g.Conns() != 1 {
				t.Fatalf("%s: traffic %d in, %d out, %d conns; want 500, 3000, 1", name, in, out, g.Conns())
			}
		}
		_ = conn.Close()
		_ = conn.Close()
		if root.Conns() != 0 || tunnel.Conns() != 0 {
			t.Fatalf("Conns() after closing twice = %d/%d, want 0", root.Conns(), tunnel.Conns())
		}

		user.SetMaxConns(1)
		first, _ := tunnel.Open(&recordingConn{})
		if c, err := client.Open(&recordingConn{}); !errors.Is(err, ErrTooManyConns) || c != nil {
			t.Fatalf("Open past the cap = %v, %v; want nil and ErrTooManyConns", c, err)
		}
		_ = first.Close()
	})
}

// SetParent moves a Group's connections with it: their slots, their later
// bytes and their limits go to the new ancestors, while the bytes counted
// before stay with the old ones.
func TestGroupSetParentMovesConnections(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		root := NewGroup(nil)
		a, b := NewGroup(root), NewGroup(root)
		b.SetWriteLimiter(NewRate(1000))
		client := NewGroup(a)
		c1, _ := client.Open(&recordingConn{})
		c2, _ := client.Open(&recordingConn{})
		_, _ = c1.Write(make([]byte, 500))
		_, _ = c2.Write(make([]byte, 500)) // with the levels as built before the move

		b.SetMaxConns(1)
		if err := client.SetParent(b); err != nil {
			t.Fatal(err)
		}
		if a.Conns() != 0 || b.Conns() != 2 || root.Conns() != 2 {
			t.Fatalf("Conns() after the move: a %d, b %d, root %d; want 0, 2, 2", a.Conns(), b.Conns(), root.Conns())
		}
		if err := NewGroup(b).Acquire(); !errors.Is(err, ErrTooManyConns) {
			t.Fatalf("Acquire under b over its cap after the move = %v, want ErrTooManyConns", err)
		}
		start := time.Now()
		_, _ = c2.Write(make([]byte, 3000)) // b's limit now applies
		if got := time.Since(start); got != time.Second {
			t.Fatalf("Write after the move took %v, want 1s under b's limit", got)
		}
		if _, out := a.Traffic().Load(); out != 1000 {
			t.Fatalf("a counted %d B out, want the 1000 B from before the move", out)
		}
		if _, out := b.Traffic().Load(); out != 3000 {
			t.Fatalf("b counted %d B out, want the 3000 B after the move", out)
		}
		_ = c1.Close()
		_ = c2.Close()
		if b.Conns() != 0 || root.Conns() != 0 || client.Conns() != 0 {
			t.Fatalf("Conns() after closing: b %d, root %d, client %d; want 0", b.Conns(), root.Conns(), client.Conns())
		}

		if err := root.SetParent(client); !errors.Is(err, ErrGroupCycle) {
			t.Fatalf("SetParent onto a descendant = %v, want ErrGroupCycle", err)
		}
		if err := root.SetParent(root); !errors.Is(err, ErrGroupCycle) {
			t.Fatalf("SetParent onto itself = %v, want ErrGroupCycle", err)
		}
		if client.Parent() != b || root.Parent() != nil {
			t.Fatal("a refused SetParent moved a Group")
		}
	})
}

// A Group's Reads and Writes serve a relay's hooks as any Limiter does.
func TestGroupRelayHooks(t *testing.T) {
	root := NewGroup(nil)
	user := NewGroup(root)
	user.SetReadLimiter(NewBudget(Step{After: 1000, Stop: true}))
	reader := user.Reads()
	sent := int64(0)
	var err error
	for err == nil {
		var n int64
		if n, err = reader.Ready(bg, 400); err == nil {
			err = reader.Charge(n)
			sent += n
		}
	}
	if !errors.Is(err, ErrBudgetExhausted) || sent != 1000 {
		t.Fatalf("relay through a capped Group sent %d, ended with %v; want 1000 and ErrBudgetExhausted", sent, err)
	}
	if in, _ := root.Traffic().Load(); in != 1000 {
		t.Fatalf("root counted %d B in, want 1000", in)
	}
	var zero Group
	if err := zero.Writes().Wait(bg, 10); err != nil {
		t.Fatal(err)
	}
	if _, out := zero.Traffic().Load(); out != 10 {
		t.Fatalf("a zero Group counted %d B out, want 10", out)
	}
	var none *Group
	if none.Reads() != nil || none.Writes() != nil {
		t.Fatal("a nil Group has limiters")
	}
}

// Moves within and between trees, of leaves, inner Groups and roots, race
// with Acquire, AcquireContext and Release in both trees: every slot taken
// is given back where its Group is then, nothing deadlocks, and the bytes
// of every level add up.
func TestGroupMovesRaceSlots(t *testing.T) {
	r1, r2 := NewGroup(nil), NewGroup(nil)
	a1, a2 := NewGroup(r1), NewGroup(r2)
	l1, l2 := NewGroup(a1), NewGroup(a2)
	a2.SetMaxConns(2)
	all := []*Group{r1, r2, a1, a2, l1, l2}
	var stop atomic.Bool
	var wg sync.WaitGroup
	for i := range 6 {
		wg.Go(func() {
			g := all[2+i%4]
			for !stop.Load() {
				var err error
				if i%2 == 0 {
					err = g.Acquire()
				} else {
					ctx, cancel := context.WithTimeout(bg, time.Millisecond)
					err = g.AcquireContext(ctx)
					cancel()
				}
				if err == nil {
					_ = g.Writes().Charge(10)
					g.Release()
				}
				runtime.Gosched() // js/wasm does not preempt this loop
			}
		})
	}
	moves := []struct {
		g, p *Group
	}{
		{l1, a2}, {l1, r2}, {l1, nil}, {l1, a1},
		{a1, r2}, {r2, a1}, {r2, nil}, {a1, r1},
		{r1, l2}, {r1, nil}, {l2, l1}, {l2, a2},
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range 3000 {
			m := moves[i%len(moves)]
			if err := m.g.SetParent(m.p); err != nil && !errors.Is(err, ErrGroupCycle) {
				t.Error(err)
			}
			runtime.Gosched()
		}
	}()
	select {
	case <-done:
	case <-time.After(time.Minute):
		t.Fatal("SetParent deadlocked with Acquire and Release")
	}
	stop.Store(true)
	wg.Wait()
	var leaves int64
	for _, g := range all {
		if g.Conns() != 0 {
			t.Fatalf("Conns() = %d after every slot was released, want 0", g.Conns())
		}
		if a := g.queue(); a.n.Load() != 0 || len(a.q) != 0 {
			t.Fatalf("%d waiters left after every AcquireContext returned", a.n.Load())
		}
	}
	for _, g := range []*Group{a1, a2, l1, l2} {
		_, out := g.Traffic().Load()
		leaves += out
	}
	_, out1 := r1.Traffic().Load()
	_, out2 := r2.Traffic().Load()
	if out1+out2 > leaves || out1+out2 == 0 {
		t.Fatalf("counted %d B in the roots, more than the %d B of the levels below or none", out1+out2, leaves)
	}
}

// AcquireContext waits for a slot: a Release wakes the waiters in the order
// they came, one per slot, and a canceled wait takes nothing and leaves no
// waiter behind.
func TestGroupAcquireContextWaits(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		root := NewGroup(nil)
		root.SetMaxConns(1)
		user := NewGroup(root)
		if err := user.Acquire(); err != nil {
			t.Fatal(err)
		}
		var tme *TooManyConnsError
		if err := user.Acquire(); !errors.Is(err, ErrTooManyConns) || !errors.As(err, &tme) || tme.Group() != root {
			t.Fatalf("Acquire past the root's cap = %v, want a TooManyConnsError of the root", err)
		}
		var order []int
		var mu sync.Mutex
		for i := range 3 {
			go func() {
				if err := user.AcquireContext(bg); err != nil {
					t.Error(err)
				}
				mu.Lock()
				order = append(order, i)
				mu.Unlock()
			}()
			synctest.Wait() // queued before the next
		}
		canceled, cancel := context.WithCancel(bg)
		errc := make(chan error, 1)
		go func() { errc <- user.AcquireContext(canceled) }()
		synctest.Wait()
		cancel()
		if err := <-errc; !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled AcquireContext = %v, want context.Canceled", err)
		}
		for i := range 3 {
			user.Release()
			synctest.Wait()
			mu.Lock()
			if len(order) != i+1 || order[i] != i {
				t.Fatalf("waiters acquired in order %v, want the order they came", order)
			}
			mu.Unlock()
		}
		if root.Conns() != 1 || root.queue().n.Load() != 0 {
			t.Fatalf("Conns() = %d with %d waiters, want 1 and none", root.Conns(), root.queue().n.Load())
		}
		user.Release()

		// Raising the cap, and moving away from the full Group, wake waiters.
		root.SetMaxConns(1)
		_ = root.Acquire()
		got := make(chan error, 2)
		go func() { got <- user.AcquireContext(bg) }()
		synctest.Wait()
		root.SetMaxConns(2)
		if err := <-got; err != nil {
			t.Fatal(err)
		}
		go func() { got <- user.AcquireContext(bg) }()
		synctest.Wait()
		if err := user.SetParent(nil); err != nil {
			t.Fatal(err)
		}
		if err := <-got; err != nil || user.Conns() != 2 {
			t.Fatalf("AcquireContext after the move = %v with %d slots, want nil and 2", err, user.Conns())
		}
	})
}

// Callers of AcquireContext at several levels of capped Groups, holding
// slots briefly, all get theirs: no freed slot is left without a waiter
// woken to take it, also when a waiter woken for one is refused by
// another level.
func TestGroupAcquireContextLosesNoWakeup(t *testing.T) {
	root := NewGroup(nil)
	root.SetMaxConns(3)
	a, b := NewGroup(root), NewGroup(root)
	a.SetMaxConns(2)
	b.SetMaxConns(2)
	leaves := []*Group{a, b, NewGroup(a), NewGroup(b)}
	var wg sync.WaitGroup
	for i := range 16 {
		wg.Go(func() {
			g := leaves[i%len(leaves)]
			for range 200 {
				if err := g.AcquireContext(bg); err != nil {
					t.Error(err)
					return
				}
				runtime.Gosched()
				g.Release()
			}
		})
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Minute):
		t.Fatalf("callers of AcquireContext still waiting with %d of 3 slots taken", root.Conns())
	}
	if root.Conns() != 0 {
		t.Fatalf("Conns() = %d, want 0", root.Conns())
	}
}

// Open can wait for a slot and charge limiters of the connection's own
// within the Group.
func TestGroupOpenOptions(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		g := NewGroup(nil)
		g.SetMaxConns(1)
		g.SetLimiters(NewRate(1<<30), NewRate(1<<30))
		perConn := NewRate(1000)
		c, err := g.Open(&recordingConn{}, WithLimiters(nil, perConn))
		if err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		if _, err := c.Write(make([]byte, 3000)); err != nil {
			t.Fatal(err)
		}
		if elapsed := time.Since(start); elapsed < time.Second {
			t.Fatalf("Write of 3000 bytes at the connection's 1000 B/s took %v, want 1s past the burst", elapsed)
		}
		if _, out := g.Traffic().Total(); out != 3000 || metered(perConn) != 3000 {
			t.Fatalf("counted %d in the Group and %d on the connection, want 3000", out, metered(perConn))
		}
		ctx, cancel := context.WithTimeout(bg, time.Minute)
		defer cancel()
		opened := make(chan net.Conn)
		go func() {
			c, err := g.Open(&recordingConn{}, WithWait(ctx))
			if err != nil {
				t.Error(err)
			}
			opened <- c
		}()
		synctest.Wait()
		_ = c.Close()
		_ = (<-opened).Close()
		if g.Conns() != 0 {
			t.Fatalf("Conns() = %d, want 0", g.Conns())
		}
	})
}

// A throttled Write through a tree of Groups allocates nothing, with a Rate
// at each of eight levels.
func TestGroupAllocatesNothing(t *testing.T) {
	if runtime.GOOS == "js" {
		t.Skip("time.Sleep allocates on js")
	}
	leaf := NewGroup(nil)
	leaf.SetWriteLimiter(NewRate(0))
	for range 7 {
		leaf = NewGroup(leaf)
		leaf.SetWriteLimiter(NewRate(0)) // metering
	}
	slow := NewRate(1 << 20)
	slow.reserve(2 << 20) // spend the burst, so that each Write below waits
	leaf.Parent().SetWriteLimiter(slow)
	conn, err := leaf.Open(&nopNetConn{})
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 8<<10) // about 8 ms at the user's pace
	_, _ = conn.Write(buf)
	if allocs := testing.AllocsPerRun(20, func() { _, _ = conn.Write(buf) }); allocs != 0 {
		t.Fatalf("a Write through a tree of Groups allocated %v times, want 0", allocs)
	}
}

// A wait that its context ends keeps its charge on every level, Counters
// too, so that the Refund the Limiter contract calls for balances them.
func TestGroupCanceledWaitKeepsCount(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		g := NewGroup(nil)
		g.SetWriteLimiter(NewBudget(Step{Limit: NewRate(100)}))
		var c Counter
		for name, l := range map[string]Limiter{
			"group":     g.Writes(),
			"hierarchy": NewHierarchy(c.Writes(), NewBudget(Step{Limit: NewRate(100)})),
		} {
			ctx, cancel := context.WithTimeout(bg, 20*time.Millisecond)
			err := l.Wait(ctx, 1000)
			cancel()
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("%s: Wait = %v, want its deadline", name, err)
			}
			l.Refund(1000)
		}
		if _, out := g.Traffic().Load(); out != 0 {
			t.Fatalf("group counted %d B out after the refund, want 0", out)
		}
		if _, out := c.Load(); out != 0 {
			t.Fatalf("hierarchy counted %d B out after the refund, want 0", out)
		}
	})
}

// A refused charge counts nowhere, and one a Switch moves elsewhere counts
// only where it went.
func TestGroupRefusalsAndSwitchesCountOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		stopped := NewGroup(nil)
		stopped.SetWriteLimiter(NewBudget(Step{Stop: true}))
		if err := stopped.Writes().Wait(bg, 100); !errors.Is(err, ErrBudgetExhausted) {
			t.Fatalf("Wait under a stopped Budget = %v, want ErrBudgetExhausted", err)
		}
		if _, out := stopped.Traffic().Load(); out != 0 {
			t.Fatalf("a refused Wait counted %d B, want 0", out)
		}

		a, b := NewGroup(nil), NewGroup(nil)
		slow := NewRate(100)
		slow.reserve(1000) // so that a Wait under a blocks
		a.SetWriteLimiter(slow)
		sw := NewSwitch(a.Writes())
		done := make(chan error)
		go func() { done <- sw.Wait(bg, 100) }()
		synctest.Wait()
		sw.Set(b.Writes())
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		_, aOut := a.Traffic().Load()
		_, bOut := b.Traffic().Load()
		if aOut != 0 || bOut != 100 {
			t.Fatalf("a Wait switched from a to b counted %d B in a and %d in b, want 0 and 100", aOut, bOut)
		}
	})
}

// A Limiter or Counter given to several levels is charged once, and
// SetTraffic sends later bytes to the new Counter.
func TestGroupSharedLevelsChargeOnce(t *testing.T) {
	user := NewGroup(nil)
	tunnel := NewGroup(user)
	budget := NewBudget()
	var traffic Counter
	for _, g := range []*Group{user, tunnel} {
		g.SetWriteLimiter(budget)
		g.SetTraffic(&traffic)
	}
	if err := tunnel.Writes().Charge(100); err != nil {
		t.Fatal(err)
	}
	if _, out := traffic.Load(); budget.Used() != 100 || out != 100 {
		t.Fatalf("a Budget and a Counter at two levels took %d B and %d B, want 100 each", budget.Used(), out)
	}
	fresh := &Counter{}
	user.SetTraffic(fresh)
	_ = tunnel.Writes().Charge(10)
	if _, out := fresh.Load(); out != 10 {
		t.Fatalf("the new Counter counted %d B, want 10", out)
	}
	if user.WriteLimiter() != budget || user.ReadLimiter() != nil {
		t.Fatal("ReadLimiter and WriteLimiter do not return what was set")
	}
}

// A change to a Group rebuilds the levels of its descendants alone.
func TestGroupChangesRebuildOnlyBelow(t *testing.T) {
	root, user, _, tunnel := tree()
	other := NewGroup(root)
	_ = tunnel.Writes().Charge(1)
	built := tunnel.cache.Load()
	other.SetWriteLimiter(NewRate(1000))
	other.SetTraffic(nil)
	_ = tunnel.Writes().Charge(1)
	if tunnel.cache.Load() != built {
		t.Fatal("a change to an unrelated Group rebuilt the levels")
	}
	for _, change := range []func(){
		func() { _ = user.SetParent(NewGroup(nil)) }, // a parent as unchanged as the root
		func() { user.SetWriteLimiter(NewRate(1000)) },
		func() { _ = user.SetParent(root) },
		func() { root.SetTraffic(nil) },
		func() { _ = root.SetParent(NewGroup(nil)) }, // a new ancestor past the chain
	} {
		change()
		_ = tunnel.Writes().Charge(1)
		if tunnel.cache.Load() == built {
			t.Fatal("a change to an ancestor left the levels as they were")
		}
		built = tunnel.cache.Load()
	}
}

// Changes racing charges lose no bytes: every byte counts in the Counter of
// the moment.
func TestGroupChangesRaceCharges(t *testing.T) {
	root := NewGroup(nil)
	leaf := NewGroup(NewGroup(root))
	var mu sync.Mutex
	counters := []*Counter{root.Traffic()}
	var stop atomic.Bool
	var wg sync.WaitGroup
	var charged atomic.Int64
	for range 4 {
		wg.Go(func() {
			for !stop.Load() {
				_ = leaf.Writes().Charge(10)
				charged.Add(10)
				runtime.Gosched() // js/wasm does not preempt this loop
			}
		})
	}
	for i := range 1000 {
		if i%2 == 0 {
			root.SetWriteLimiter(NewRate(0))
		} else {
			c := &Counter{}
			mu.Lock()
			counters = append(counters, c)
			mu.Unlock()
			root.SetTraffic(c)
		}
		runtime.Gosched()
	}
	stop.Store(true)
	wg.Wait()
	var total int64
	for _, c := range counters {
		_, out := c.Load()
		total += out
	}
	if total != charged.Load() {
		t.Fatalf("the root's Counters counted %d B, want the %d charged", total, charged.Load())
	}
}

// Open refuses a nil connection without taking a slot; OpenDatagram keeps
// messages whole, and OpenReadWriteCloser gives its slot back on Close.
func TestGroupOpenVariants(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		g := NewGroup(nil)
		if c, err := g.Open(nil); c != nil || !errors.Is(err, ErrNilConn) || g.Conns() != 0 {
			t.Fatalf("Open(nil) = %v, %v with %d slots taken; want nil, ErrNilConn, 0", c, err, g.Conns())
		}
		if _, err := g.OpenReadWriteCloser(nil); !errors.Is(err, ErrNilConn) || g.Conns() != 0 {
			t.Fatalf("OpenReadWriteCloser(nil) = %v with %d slots taken", err, g.Conns())
		}
		g.SetWriteLimiter(NewRate(1000))
		rc := &recordingConn{}
		dc, err := g.OpenDatagram(rc)
		if err != nil {
			t.Fatal(err)
		}
		if n, err := dc.Write(make([]byte, 3000)); n != 3000 || err != nil || rc.maxWrite() != 3000 {
			t.Fatalf("datagram Write = %d, %v, largest write %d; want 3000 whole", n, err, rc.maxWrite())
		}
		rwc, err := g.OpenReadWriteCloser(&recordingConn{})
		if err != nil || g.Conns() != 2 {
			t.Fatalf("OpenReadWriteCloser = %v with %d slots taken, want 2", err, g.Conns())
		}
		_ = rwc.Close()
		_ = rwc.Close()
		_ = dc.Close()
		if g.Conns() != 0 {
			t.Fatalf("Conns() after closing = %d, want 0", g.Conns())
		}
	})
}
