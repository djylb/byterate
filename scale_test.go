package byterate

import (
	"errors"
	"runtime"
	"slices"
	"testing"
	"time"
	"unsafe"
)

// within fails t unless f returns within a second, as a call that waits for
// a lock held by the test never does.
func within(t *testing.T, what string, f func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		f()
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("%s waited for a lock held elsewhere", what)
		<-done
	}
}

// Opening and closing connections, alone, with a Counter or in a Group,
// takes no lock shared by all, and neither do changes to Limiters that
// pass charges on to no other: only the checks against cycles share one.
func TestNoGlobalLockOnConnsOrLeafChanges(t *testing.T) {
	root := NewGroup(nil)
	g := NewGroup(root)
	var ctr Counter
	sw := NewSwitch(nil)
	b := NewBudget()
	cycleMu.Lock()
	within(t, "NewConn and Close", func() {
		_ = NewConn(&nopNetConn{}).Close()
		_ = NewConn(&nopNetConn{}, WithCounter(&ctr)).Close()
	})
	within(t, "Open and Close", func() {
		c, err := g.Open(&nopNetConn{})
		if err != nil {
			t.Error(err)
			return
		}
		_, _ = c.Write(make([]byte, 10))
		_ = c.Close()
	})
	within(t, "Acquire and Release", func() {
		if g.Acquire() == nil {
			g.Release()
		}
	})
	within(t, "setting Rates", func() {
		g.SetLimiters(NewRate(1000), nil)
		sw.Set(NewRate(1000))
		b.SetSteps(Step{After: 10, Limit: NewRate(1000)})
	})
	cycleMu.Unlock()
	if _, w := root.Traffic().Total(); w != 10 {
		t.Fatalf("root Traffic %d, want 10", w)
	}
}

// A connection registers with its own Group alone: the Groups above count
// it through the sums of those below, and a change of an ancestor's
// Limiters reaches it at its next read or write without touching it.
func TestOpenRegistersAtOwnGroup(t *testing.T) {
	root, user, _, tunnel := tree()
	c, err := tunnel.Open(&nopNetConn{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	for _, x := range []struct {
		g    *Group
		want int
	}{{root, 0}, {user, 0}, {tunnel, 1}} {
		if n := meterSources(&x.g.node().sum); n != x.want {
			t.Fatalf("%d Meters attached to a Group, want %d", n, x.want)
		}
	}
	setWrite(root, NewRate(1<<40))
	if c.write.state.Load() != 0 {
		t.Fatal("SetLimiters on an ancestor changed the connection itself")
	}
	_, _ = c.Write(make([]byte, 7))
	if c.write.state.Load() != dirLimited {
		t.Fatal("the ancestor's limit does not apply from the next write")
	}
	if _, w := root.Traffic().Total(); w != 7 {
		t.Fatalf("root Traffic %d, want 7", w)
	}
}

// A Counter that one goroutine at a time adds to stays one pair of counters,
// and its spread grows with contention to one cell per processor at most.
func TestCounterSpreadsOnlyOnContention(t *testing.T) {
	var c Counter
	for range 1000 {
		c.Add(1, 1)
	}
	if c.added.cells.Load() != nil {
		t.Fatal("a Counter without contention spread its Adds")
	}
	set := c.added.grow(nil)
	for {
		next := c.added.grow(set)
		if next == set {
			break
		}
		set = next
	}
	if n := len(set.cells); n > max(2, pow2AtLeast(runtime.GOMAXPROCS(0))) {
		t.Fatalf("%d cells, want at most one per processor", n)
	}
	for range 1000 {
		c.Add(1, 1)
	}
	if r, w := c.Total(); r != 2000 || w != 2000 {
		t.Fatalf("Total() = %d, %d after spreading; want 2000, 2000", r, w)
	}
}

// A tally spread over shards takes an attach to one shard while a reader
// holds another.
func TestTallyAttachDoesNotWaitForReader(t *testing.T) {
	var tl tally
	tl.attach(newMeter(), [2]int64{})
	tl.grow(tl.srcs.Load())
	tl.grow(tl.srcs.Load())
	tab := tl.srcs.Load()
	held := newMeter()
	other := newMeter()
	for i := 0; shardIndex(srcKey(other), len(tab.shards)) == shardIndex(srcKey(held), len(tab.shards)); i++ {
		if i == 1000 {
			t.Fatal("the tally did not spread its sources over shards")
		}
		other = newMeter()
	}
	sh := &tab.shards[shardIndex(srcKey(held), len(tab.shards))]
	sh.mu.Lock() // a reader going through the shard
	within(t, "an attach to another shard", func() { tl.attach(other, [2]int64{}) })
	sh.mu.Unlock()
	if tl.nsrc.Load() != 2 {
		t.Fatalf("%d sources, want 2", tl.nsrc.Load())
	}
}

// The waiters of a Group join and leave the queue in any order, the woken
// in the order they came, a first one in front.
func TestSlotQueueOrder(t *testing.T) {
	var q slotQueue
	a, b, c := q.add(false), q.add(false), q.add(false)
	front := q.add(true)
	q.leave(b)
	var order []*slotWaiter
	for range 3 {
		q.wake(false)
		for _, w := range []*slotWaiter{a, b, c, front} {
			if w.woken && !slices.Contains(order, w) {
				order = append(order, w)
			}
		}
	}
	if len(order) != 3 || order[0] != front || order[1] != a || order[2] != c || q.n.Load() != 0 || q.head != nil || q.tail != nil {
		t.Fatalf("woken %d in the wrong order, %d left", len(order), q.n.Load())
	}
	if b.woken {
		t.Fatal("a waiter that left was woken")
	}
}

// A read or write that applies a change of its Group's levels after Close
// leases nothing: Close settled the leases for good.
func TestNoLeaseAfterClose(t *testing.T) {
	g := NewGroup(nil)
	b := stopAfter(1 << 30)
	setWrite(g, b)
	c, err := g.Open(&nopNetConn{})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = c.Write(make([]byte, 10))
	_ = c.Close()
	setWrite(g, NewHierarchy(b, NewRate(1<<40))) // a change the next I/O applies
	c.write.recheck()                            // as a Write racing Close does
	if n := b.lessees.n.Load(); n != 0 {
		t.Fatalf("%d leases after Close", n)
	}
}

// A Group dropped leaves its parent's sum once collected, keeping the bytes
// it counted there, so that Groups made and dropped do not pile up.
func TestDroppedGroupLeavesParent(t *testing.T) {
	root := NewGroup(nil)
	func() {
		g := NewGroup(root)
		_ = g.Writes().Charge(5)
	}()
	deadline := time.Now().Add(10 * time.Second)
	for root.node().sum.nsrc.Load() != 0 {
		if time.Now().After(deadline) {
			t.Fatal("a dropped Group stays in its parent's sum")
		}
		runtime.GC()
		time.Sleep(time.Millisecond)
	}
	if _, w := root.Traffic().Total(); w != 5 {
		t.Fatalf("root Traffic %d after its child was dropped, want 5", w)
	}
}

// The fields that cores write apart fill whole cache lines of the
// architecture, and no more than they need.
func TestCacheLinePadding(t *testing.T) {
	for _, x := range []struct {
		name       string
		size, need uintptr
	}{
		{"Rate", unsafe.Sizeof(Rate{}), unsafe.Sizeof(rateFields{})},
		{"meterSide", unsafe.Sizeof(meterSide{}), 16},
		{"stripeCell", unsafe.Sizeof(stripeCell{}), 16},
	} {
		lines := (x.need + cacheLineSize - 1) / cacheLineSize
		if x.size != lines*cacheLineSize {
			t.Errorf("%s takes %d B, want %d lines of %d", x.name, x.size, lines, cacheLineSize)
		}
	}
}

// A Counter that counts a connection by Attach cannot become the Traffic of
// the connection's Group or of an ancestor, which counts it already.
func TestTrafficOverlapsAttachedMeter(t *testing.T) {
	root := NewGroup(nil)
	g := NewGroup(root)
	c, err := g.Open(&nopNetConn{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	var ctr Counter
	if err := ctr.Attach(c.Meter()); err != nil {
		t.Fatal(err)
	}
	for _, x := range []*Group{g, root} {
		if err := x.SetTraffic(&ctr); !errors.Is(err, ErrCounterOverlap) {
			t.Fatalf("SetTraffic of a Counter that counts a connection below: %v", err)
		}
	}
	if err := NewGroup(nil).SetTraffic(&ctr); err != nil {
		t.Fatalf("SetTraffic elsewhere: %v", err)
	}
}

// A Traffic Counter shared by sibling Groups lets go of each Group once it
// is collected, keeping what it counted, so that Groups made and dropped do
// not pile up in it.
func TestSharedTrafficLetsGoOfDroppedGroups(t *testing.T) {
	root := NewGroup(nil)
	shared := &Counter{}
	for range 100 {
		g := NewGroup(root)
		if err := g.SetTraffic(shared); err != nil {
			t.Fatal(err)
		}
		c, err := g.Open(&nopNetConn{})
		if err != nil {
			t.Fatal(err)
		}
		_, _ = c.Read(make([]byte, 1))
		_ = c.Close()
	}
	deadline := time.Now().Add(10 * time.Second)
	for shared.nsrc.Load() != 0 || root.node().sum.nsrc.Load() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("%d Groups left in the shared Counter, %d in the root", shared.nsrc.Load(), root.node().sum.nsrc.Load())
		}
		runtime.GC()
		time.Sleep(time.Millisecond)
	}
	if r, _ := shared.Total(); r != 100 {
		t.Fatalf("shared Counter read %d, want 100", r)
	}
	if r, _ := root.Traffic().Total(); r != 100 {
		t.Fatalf("root read %d, want 100", r)
	}
}

// A change in one tree of Groups leaves the connections of other trees
// alone, and a connection moved to another tree takes on its levels.
func TestTreeVersions(t *testing.T) {
	a, b := NewGroup(nil), NewGroup(nil)
	g := NewGroup(a)
	c, err := g.Open(&nopNetConn{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	setWrite(NewGroup(b), NewRate(1000))
	if c.write.load()&dirStale != 0 {
		t.Fatal("a change in another tree made the connection look again")
	}
	setWrite(b, NewRate(1<<40))
	if err := g.SetParent(b); err != nil {
		t.Fatal(err)
	}
	_, _ = c.Write(make([]byte, 10))
	if c.write.state.Load() != dirLimited {
		t.Fatal("the connection moved to another tree does not take on its limit")
	}
	setWrite(b, nil)
	_, _ = c.Write(make([]byte, 10))
	if c.write.state.Load() != 0 {
		t.Fatal("the connection does not follow a change in its new tree")
	}
}

// Reading a Group's Traffic does not hold the lock of its shard while it
// reads the Groups below, so attaches to it go on meanwhile.
func TestTallyReadDoesNotHoldParentLock(t *testing.T) {
	root := NewGroup(nil)
	child := NewGroup(root)
	cn, err := child.Open(&nopNetConn{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cn.Close() }()
	csh := &child.node().sum.srcs.Load().shards[0]
	csh.mu.Lock() // the child busy
	read := make(chan struct{})
	go func() {
		defer close(read)
		_, _ = root.Traffic().Total()
	}()
	time.Sleep(10 * time.Millisecond) // let the read reach the child
	made := make(chan struct{})
	go func() {
		defer close(made)
		_ = NewGroup(root)
	}()
	select {
	case <-made:
		csh.mu.Unlock()
	case <-time.After(time.Second):
		csh.mu.Unlock()
		<-made
		<-read
		t.Fatal("a Group made under a root being read waited for the read")
	}
	<-read
}

// A wrapper closed registers no cleanup later, when a change it applies
// after Close gives it leases.
func TestNoCleanupAfterClose(t *testing.T) {
	c := NewConn(&nopNetConn{})
	_ = c.Close()
	c.SetLimiters(nil, stopAfter(1<<20))
	if c.settles {
		t.Fatal("a closed wrapper registered a cleanup")
	}
}

// A shard's map lets go of its memory once most of its sources left.
func TestShardMapShrinks(t *testing.T) {
	var tl tally
	ms := make([]*Meter, 1000)
	for i := range ms {
		ms[i] = newMeter()
		tl.attach(ms[i], [2]int64{})
	}
	for _, m := range ms[10:] {
		tl.detach(m, [2]int64{})
	}
	tab := tl.srcs.Load()
	for i := range tab.shards {
		if sh := &tab.shards[i]; sh.peak > 4*len(sh.m)+minShrink {
			t.Fatalf("shard %d keeps a map of %d for %d sources", i, sh.peak, len(sh.m))
		}
	}
	if tl.nsrc.Load() != 10 {
		t.Fatalf("%d sources, want 10", tl.nsrc.Load())
	}
}
