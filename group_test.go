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
// every level's traffic, read bytes as inbound and written ones as
// outbound; closing it gives its slot back once.
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
		if c, err := client.Open(&recordingConn{}); !errors.Is(err, ErrTooManyConns) || c == nil {
			t.Fatalf("Open past the cap = %v, %v; want ErrTooManyConns and the connection back", c, err)
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

// A Group's Reader and Writer serve a relay's hooks as any Limiter does.
func TestGroupRelayHooks(t *testing.T) {
	root := NewGroup(nil)
	user := NewGroup(root)
	user.SetReadLimiter(NewBudget(Step{After: 1000, Stop: true}))
	reader := user.Reader()
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
	if err := zero.Writer().Wait(bg, 10); err != nil {
		t.Fatal(err)
	}
	if _, out := zero.Traffic().Load(); out != 10 {
		t.Fatalf("a zero Group counted %d B out, want 10", out)
	}
	var none *Group
	if none.Reader() != nil || none.Writer() != nil {
		t.Fatal("a nil Group has limiters")
	}
}

// Acquire, Release and SetParent racing leave every count exact.
func TestGroupMovesRaceSlots(t *testing.T) {
	root := NewGroup(nil)
	a, b := NewGroup(root), NewGroup(root)
	client := NewGroup(a)
	var stop atomic.Bool
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for !stop.Load() {
				if client.Acquire() == nil {
					_ = client.Writer().Charge(10)
					client.Release()
				}
				runtime.Gosched() // js/wasm does not preempt this loop
			}
		})
	}
	for i := range 2000 {
		target := a
		if i%2 == 0 {
			target = b
		}
		if err := client.SetParent(target); err != nil {
			t.Fatal(err)
		}
		runtime.Gosched()
	}
	stop.Store(true)
	wg.Wait()
	for name, g := range map[string]*Group{"root": root, "a": a, "b": b, "client": client} {
		if g.Conns() != 0 {
			t.Fatalf("%s Conns() = %d after every slot was released, want 0", name, g.Conns())
		}
	}
	_, rootOut := root.Traffic().Load()
	_, aOut := a.Traffic().Load()
	_, bOut := b.Traffic().Load()
	_, clientOut := client.Traffic().Load()
	if rootOut != clientOut || aOut+bOut != clientOut {
		t.Fatalf("counted %d B out in the root, %d+%d in a and b, %d in the client; want them equal", rootOut, aOut, bOut, clientOut)
	}
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
			"group":     g.Writer(),
			"hierarchy": NewHierarchy(c.Outbound(), NewBudget(Step{Limit: NewRate(100)})),
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
		if err := stopped.Writer().Wait(bg, 100); !errors.Is(err, ErrBudgetExhausted) {
			t.Fatalf("Wait under a stopped Budget = %v, want ErrBudgetExhausted", err)
		}
		if _, out := stopped.Traffic().Load(); out != 0 {
			t.Fatalf("a refused Wait counted %d B, want 0", out)
		}

		a, b := NewGroup(nil), NewGroup(nil)
		slow := NewRate(100)
		slow.reserve(1000) // so that a Wait under a blocks
		a.SetWriteLimiter(slow)
		sw := NewSwitch(a.Writer())
		done := make(chan error)
		go func() { done <- sw.Wait(bg, 100) }()
		synctest.Wait()
		sw.Set(b.Writer())
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
	if err := tunnel.Writer().Charge(100); err != nil {
		t.Fatal(err)
	}
	if _, out := traffic.Load(); budget.Used() != 100 || out != 100 {
		t.Fatalf("a Budget and a Counter at two levels took %d B and %d B, want 100 each", budget.Used(), out)
	}
	fresh := &Counter{}
	user.SetTraffic(fresh)
	_ = tunnel.Writer().Charge(10)
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
	_ = tunnel.Writer().Charge(1)
	built := tunnel.cache.Load()
	other.SetWriteLimiter(NewRate(1000))
	other.SetTraffic(nil)
	_ = tunnel.Writer().Charge(1)
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
		_ = tunnel.Writer().Charge(1)
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
				_ = leaf.Writer().Charge(10)
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
