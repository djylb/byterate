package byterate

import (
	"context"
	"errors"
	"io"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"
)

var (
	// ErrTooManyConns is matched by the *TooManyConnsError of Acquire and
	// Open, for a Group, or one of its ancestors, that has as many
	// connections as its MaxConns.
	ErrTooManyConns = errors.New("byterate: too many connections")
	// ErrGroupCycle is returned by SetParent for a parent that is the Group
	// itself or one of its descendants.
	ErrGroupCycle = errors.New("byterate: group would be its own ancestor")
	// ErrCounterOverlap is returned where a Counter would count the same
	// bytes twice: given as the Traffic of a Group and of one of its
	// ancestors or descendants, or to a connection by WithCounter or Attach
	// while it is the Traffic of the connection's Group or of an ancestor.
	// Groups that share a Counter must not be above one another; siblings
	// may share one.
	ErrCounterOverlap = errors.New("byterate: counter would count the same bytes twice")
)

// TooManyConnsError is the error of Acquire and Open when a Group refuses a
// connection slot: errors.Is reports it as ErrTooManyConns, and Group
// returns the Group that refused, which is the one opened or an ancestor.
type TooManyConnsError struct{ g *Group }

func (e *TooManyConnsError) Error() string { return ErrTooManyConns.Error() }

// Is reports whether target is ErrTooManyConns.
func (e *TooManyConnsError) Is(target error) bool { return target == ErrTooManyConns }

// Group returns the Group at its MaxConns that refused the slot.
func (e *TooManyConnsError) Group() *Group { return e.g }

// Group is a level of a tree of traffic controls, such as a whole server,
// one of its users, a user's client and one of its tunnels: the connections
// opened in a Group are charged to the limiters of the Group and of each of
// its ancestors, count in each one's Traffic and Conns, and each level can
// cap its connections. A Group with a nil parent is a root.
//
// Open takes a connection slot and wraps a connection; a connection opened
// so costs its reads and writes no more than counting while no level
// limits it, as its Meter adds up into the Traffic of each level only when
// that is read. Acquire and Release take and give back a slot alone, and
// Reads and Writes return the Limiters of a Group's reads and writes, for
// relays that call a hook per read and write, which count in Traffic as
// they are charged. SetParent moves a Group and its connections under another
// parent at any time: from then on, their bytes go to the new ancestors, as
// do their slots, while the bytes counted before stay where they are.
//
// Acquire and Release take no lock: SetParent waits for those in progress in
// the trees it changes, and they wait for it. Charges take no lock, and a
// connection registers with its own Group alone, so opening and closing
// connections in different Groups does not contend. A Group's Traffic adds
// up those of its connections and of its children's Traffic when it is
// read, so a read costs in proportion to the Groups and connections below.
//
// All methods are safe for concurrent use. The zero value is a root without
// limits.
type Group struct {
	// The fields fill two cache lines: the first is read by every charge of
	// the Group's connections and its descendants' and written only when the
	// Group changes, the second is written by every Acquire and Release
	// below it.
	groupLinks
	_ [(cacheLineSize - unsafe.Sizeof(groupLinks{})%cacheLineSize) % cacheLineSize]byte
	groupSlots
	_ [(cacheLineSize - unsafe.Sizeof(groupSlots{})%cacheLineSize) % cacheLineSize]byte
}

// groupLinks are the fields of a Group that its charges read.
type groupLinks struct {
	parent      atomic.Pointer[Group]
	read, write atomic.Pointer[groupLimiter]
	traffic     atomic.Pointer[Counter]
	epoch       atomic.Uint64 // changed with the limiters and the Counter
	// The levels that the Group's connections are charged to, kept until
	// the Group or an ancestor changes, so that charges walk no tree and
	// allocate nothing.
	cache atomic.Pointer[groupLevels]
	nd    atomic.Pointer[groupNode] // made on first use
}

// groupNode is the sum of a Group's traffic, which its parent's sum, its
// Traffic and its connections' Meters attach to. It does not refer to the
// Group, so that a Group dropped can be collected while its sum stays
// counted in its parent's.
type groupNode struct {
	// up is the node of the parent, under cycleMu, which the sum is
	// attached to; cleanup is set once the Group has a cleanup that
	// detaches it when the Group is collected.
	up      *groupNode
	sum     tally
	cleanup bool
}

// totals makes a node the source of the sums and Counters it is attached to.
func (n *groupNode) totals() [2]int64 { return n.sum.total() }

// within reports whether n is a, or below a. cycleMu must be held.
func (n *groupNode) within(a *groupNode) bool {
	for x := n; x != nil; x = x.up {
		if x == a {
			return true
		}
	}
	return false
}

// groupSlots are the fields of a Group that Acquire and Release write.
type groupSlots struct {
	conns    atomic.Int64              // slots taken in the Group and its descendants
	maxConns atomic.Int64              // 0 for no cap
	waiters  atomic.Pointer[slotQueue] // made by the first AcquireContext refused here
	// guard, while the Group is a root, keeps SetParent from changing its
	// tree while Acquire or Release walk it, so that a moved Group takes
	// its connections along exactly.
	guard treeGuard
}

// groupLimiter holds a Limiter of a Group's own level.
type groupLimiter struct {
	l Limiter
}

// NewGroup returns a Group under parent, or a root for a nil parent.
func NewGroup(parent *Group) *Group {
	g := &Group{}
	if parent != nil {
		g.parent.Store(parent)
		g.adopt(parent, [2]int64{})
	}
	return g
}

// node returns g's node, making it on first use.
func (g *Group) node() *groupNode {
	if n := g.nd.Load(); n != nil {
		return n
	}
	g.nd.CompareAndSwap(nil, &groupNode{})
	return g.nd.Load()
}

// adopt attaches g's sum to that of p, with tot its totals now, and makes
// sure that g, once collected, takes it out again. cycleMu must be held,
// unless g is not yet shared.
func (g *Group) adopt(p *Group, tot [2]int64) {
	n, up := g.node(), p.node()
	up.sum.attach(n, tot)
	n.up = up
	if !n.cleanup {
		n.cleanup = true
		runtime.AddCleanup(g, (*groupNode).orphan, n)
	}
}

// orphan takes the sum of a collected Group out of its parent's, keeping
// the bytes it counted there.
func (n *groupNode) orphan() {
	cycleMu.Lock()
	defer cycleMu.Unlock()
	if up := n.up; up != nil {
		up.sum.detach(n, n.sum.total())
		n.up = nil
	}
}

// Parent returns g's parent, or nil for a root.
func (g *Group) Parent() *Group { return g.parent.Load() }

// root returns the root of g's tree.
func (g *Group) root() *Group {
	a := g
	for p := a.parent.Load(); p != nil; p = a.parent.Load() {
		a = p
	}
	return a
}

// treeGuard is how SetParent and the Acquires and Releases of a tree keep
// out of each other's way without a lock that every Acquire would write: an
// Acquire marks itself busy in the root, on a counter that spreads as they
// contend, and checks that no move is under way; SetParent announces its
// move, then waits until none is busy. Either sees the other, as both
// write before they read.
type treeGuard struct {
	moving atomic.Pointer[chan struct{}] // while SetParent moves, closed when done
	busy   stripes                       // sum 0: Acquires and Releases under way
}

// enterTree marks an Acquire or Release in g's tree busy, once no move is
// under way there, and returns the counter to take it off again with
// leaveTree.
func (g *Group) enterTree() *atomic.Int64 {
	for {
		r := g.root()
		busy := r.guard.busy.add(0, 1)
		ch := r.guard.moving.Load()
		if ch == nil && g.root() == r {
			return busy
		}
		busy.Add(-1)
		if ch != nil {
			<-*ch
		}
	}
}

func leaveTree(busy *atomic.Int64) { busy.Add(-1) }

// beginMove announces a move in the tree of g, a root, and waits until no Acquire or
// Release is under way there. cycleMu must be held, so that one move runs
// at a time.
func (g *Group) beginMove() {
	ch := make(chan struct{})
	g.guard.moving.Store(&ch)
	for g.guard.busy.sum(0) != 0 {
		runtime.Gosched()
	}
}

// endMove ends the move that beginMove announced in g's tree.
func (g *Group) endMove() {
	if ch := g.guard.moving.Swap(nil); ch != nil {
		close(*ch)
	}
}

// SetParent moves g under p, or makes it a root for a nil p, with the
// connection slots taken in g and its descendants: they leave g's old
// ancestors and count in p and its ancestors, even beyond their MaxConns,
// which then refuse new connections until enough close. Charges after the
// move go to the new ancestors; a wait in progress ends on the old ones, and
// so does the Refund of a wrapped connection, while a direct Refund on
// Reads or Writes goes to the new ones. Callers of AcquireContext waiting
// on an old ancestor try again. It returns ErrGroupCycle, and moves
// nothing, when p is g or a descendant of g, or when a Limiter of p or of
// one of its ancestors charges the Reads or Writes of g or of a descendant.
func (g *Group) SetParent(p *Group) error {
	cycleMu.Lock()
	defer cycleMu.Unlock()
	// No other SetParent runs, so the roots stay.
	from := g.root()
	from.beginMove()
	defer from.endMove()
	if p != nil {
		if to := p.root(); to != from {
			to.beginMove()
			defer to.endMove()
		}
	}
	for a := p; a != nil; a = a.parent.Load() {
		if a == g || g.chargedBy(a.readLimiter()) || g.chargedBy(a.writeLimiter()) {
			return ErrGroupCycle
		}
	}
	// A Traffic of the new ancestors that counts a Group or connection of
	// g's subtree already would count its bytes twice once g is below.
	for a := p; a != nil; a = a.parent.Load() {
		if t := a.traffic.Load(); t != nil && t.countsWithin(g.node()) {
			return ErrCounterOverlap
		}
	}
	old := g.parent.Load()
	n := g.conns.Load()
	for a := old; a != nil; a = a.parent.Load() {
		a.conns.Add(-n)
	}
	for a := p; a != nil; a = a.parent.Load() {
		a.conns.Add(n)
	}
	// g's bytes from now on count in the new ancestors, and those it counted
	// stay with the old ones: one reading of its totals ends the one and
	// starts the other. The ancestors in both chains count them all along.
	nd := g.node()
	tot := nd.sum.total()
	if old != nil {
		old.node().sum.detach(nd, tot)
		nd.up = nil
	}
	if p != nil {
		g.adopt(p, tot)
	}
	g.parent.Store(p)
	groupGen.Add(1)
	// The waiters of the old ancestors may now find a slot, or a chain
	// without them; the chain of old does not change.
	for a := old; a != nil; a = a.parent.Load() {
		a.wakeAll()
	}
	return nil
}

// SetLimiters sets the Limiters that g's own level charges the reads and
// the writes of its connections to, on top of its ancestors' and
// descendants', nil for none. A Rate, a Budget or a NewHierarchy of them
// suits; the same Limiter for both directions shares one limit between
// them, and one set at several levels is charged once. It applies from the
// next read or write, also of the connections open in g and below, which
// take it on at their next grant: a wait in progress ends on the Limiters
// it began with. It
// panics if a Limiter charges the Reads or Writes of g or of a descendant,
// which would charge itself without end.
func (g *Group) SetLimiters(read, write Limiter) { g.setLimiters(&read, &write) }

// setLimiters sets the Limiters that read and write point to, leaving a
// direction with a nil pointer as it is.
func (g *Group) setLimiters(read, write *Limiter) {
	check := false
	for _, l := range [2]*Limiter{read, write} {
		if l != nil {
			*l = classify(*l).l
			check = check || mayReach(*l)
		}
	}
	if check {
		cycleMu.Lock()
		defer cycleMu.Unlock()
		for _, l := range [2]*Limiter{read, write} {
			if l != nil && g.chargedBy(*l) {
				panic("byterate: Group limiter charges the Group itself")
			}
		}
	}
	if read != nil {
		g.read.Store(&groupLimiter{l: *read})
	}
	if write != nil {
		g.write.Store(&groupLimiter{l: *write})
	}
	g.epoch.Add(1)
	groupGen.Add(1)
}

// groupGen changes with the limiters and the parent of any Group, for the
// connections opened in Groups to look whether their levels changed: one
// load per read or write tells them they did not.
var groupGen atomic.Uint64

// chargedBy reports whether l charges the Reads or Writes of g or of one of
// its descendants. cycleMu must be held.
func (g *Group) chargedBy(l Limiter) bool {
	return reaches(l, func(x Limiter) bool {
		for a := groupOf(x); a != nil; a = a.parent.Load() {
			if a == g {
				return true
			}
		}
		return false
	})
}

// groupOf returns the Group of a Reads or Writes Limiter, and nil for any
// other Limiter.
func groupOf(l Limiter) *Group {
	switch t := l.(type) {
	case *groupReads:
		return (*Group)(t)
	case *groupWrites:
		return (*Group)(t)
	}
	return nil
}

// Limiters returns the Limiters of g's own level, as SetLimiters set them.
func (g *Group) Limiters() (read, write Limiter) {
	return loadLimiter(&g.read), loadLimiter(&g.write)
}

func (g *Group) readLimiter() Limiter  { return loadLimiter(&g.read) }
func (g *Group) writeLimiter() Limiter { return loadLimiter(&g.write) }

func loadLimiter(p *atomic.Pointer[groupLimiter]) Limiter {
	if l := p.Load(); l != nil {
		return l.l
	}
	return nil
}

// Traffic returns the Counter of the bytes that the connections of g and its
// descendants read and wrote, and their throughput. A moved Group's bytes
// count where it was when they passed.
func (g *Group) Traffic() *Counter {
	if c := g.traffic.Load(); c != nil {
		return c
	}
	// The first Counter counts all that g's sum did, and is made once.
	c := &Counter{}
	c.attach(g.node(), [2]int64{})
	g.traffic.CompareAndSwap(nil, c)
	return g.traffic.Load()
}

// SetTraffic makes c the Counter of g's traffic from now on, such as a
// Counter restored after a restart, or one that several Groups share to add
// up their traffic; nil makes a new one. The bytes counted before stay in
// the Counter it replaces. It returns ErrCounterOverlap, and changes
// nothing, when c already counts an ancestor or descendant of g, or a
// connection in g or below, whose bytes it would count twice.
func (g *Group) SetTraffic(c *Counter) error {
	if c == nil {
		c = &Counter{}
	}
	cycleMu.Lock()
	defer cycleMu.Unlock()
	n := g.node()
	old := g.Traffic()
	if old == c {
		return nil
	}
	if c.countsAround(n) {
		return ErrCounterOverlap
	}
	tot := n.sum.total()
	old.detach(n, tot)
	c.attach(n, tot)
	g.traffic.Store(c)
	return nil
}

// countsWithin reports whether c counts a Group at or below n, or a
// connection opened there. cycleMu must be held.
func (c *Counter) countsWithin(n *groupNode) bool {
	return c.anySource(func(s source) bool {
		switch v := s.(type) {
		case *groupNode:
			return v.within(n)
		case *Meter:
			return v.node != nil && v.node.within(n)
		}
		return false
	})
}

// countsAbove reports whether c counts a Group at or above n. cycleMu must
// be held.
func (c *Counter) countsAbove(n *groupNode) bool {
	return c.anySource(func(s source) bool {
		v, ok := s.(*groupNode)
		return ok && n.within(v)
	})
}

// countsAround reports whether c counts a Group other than n's above or
// below it, or a connection opened at or below n, which as n's Traffic it
// would count again. cycleMu must be held.
func (c *Counter) countsAround(n *groupNode) bool {
	return c.anySource(func(s source) bool {
		switch v := s.(type) {
		case *groupNode:
			return v != n && (v.within(n) || n.within(v))
		case *Meter:
			return v.node != nil && v.node.within(n)
		}
		return false
	})
}

// SetMaxConns caps the connections of g and its descendants at n, from the
// next Acquire; 0 or less means no cap. Connections over a lowered cap stay
// open; callers of AcquireContext waiting on g try again.
func (g *Group) SetMaxConns(n int64) {
	g.maxConns.Store(max(n, 0))
	g.wakeAll()
}

// MaxConns returns g's cap on connections, or 0 for none.
func (g *Group) MaxConns() int64 { return g.maxConns.Load() }

// Conns returns the connection slots taken in g and its descendants.
func (g *Group) Conns() int64 { return g.conns.Load() }

// Acquire takes a connection slot in g and each of its ancestors, or none
// and a *TooManyConnsError when any of them has as many as its MaxConns; a
// racing Acquire that takes a slot and gives it back can cause such a
// refusal too. Each successful Acquire must be matched by one Release.
func (g *Group) Acquire() error {
	if a := g.acquire(); a != nil {
		return &TooManyConnsError{g: a}
	}
	return nil
}

// AcquireContext is Acquire that waits for a slot instead of refusing it,
// until ctx is done, when it returns ctx.Err() and takes no slot. Waiters
// are woken in the order they came as slots free up, but are not served in
// that order: an Acquire, or a woken waiter that a level above refuses,
// can take a freed slot first.
func (g *Group) AcquireContext(ctx context.Context) error {
	var woke *Group // the Group that woke the caller, to wait at its front again
	for {
		a := g.acquire()
		if a != nil && woke != nil && a != woke {
			// Another level refused the caller, so the slot that woke it
			// may still be free: wake the next waiter for it.
			woke.wakeOne()
		}
		if a == nil {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		q := a.queue()
		w := q.add(a == woke)
		woke = nil
		// A slot that freed up before w was queued woke nobody.
		if b := g.acquire(); b != a {
			q.leave(w)
			if b == nil {
				return nil
			}
			continue
		}
		select {
		case <-w.ch:
			woke = a
		case <-ctx.Done():
			q.leave(w)
			return ctx.Err()
		}
	}
}

// acquire takes a slot in g and each of its ancestors, or none, and returns
// the one that refused, or nil.
func (g *Group) acquire() *Group {
	defer leaveTree(g.enterTree())
	for a := g; a != nil; a = a.parent.Load() {
		if !a.take() {
			// The slots taken below a may have refused a waiter meanwhile.
			for b := g; b != a; b = b.parent.Load() {
				b.give()
			}
			return a
		}
	}
	return nil
}

// take takes a slot in g alone, unless g is at its MaxConns.
func (g *Group) take() bool {
	if g.maxConns.Load() == 0 {
		g.conns.Add(1) // no cap to check: one add, which never retries
		return true
	}
	for {
		n, limit := g.conns.Load(), g.maxConns.Load()
		if limit > 0 && n >= limit {
			return false
		}
		if g.conns.CompareAndSwap(n, n+1) {
			return true
		}
	}
}

// Release gives back a connection slot that Acquire took, in g and in each
// of its ancestors now. It panics, and gives back nothing, when g holds no
// slot, as a Release without its Acquire finds: the slot of the caller
// keeps g's count above zero whatever else runs.
func (g *Group) Release() {
	defer leaveTree(g.enterTree())
	if g.conns.Load() < 1 {
		panic("byterate: Group.Release without Acquire")
	}
	for a := g; a != nil; a = a.parent.Load() {
		a.give()
	}
}

// give gives back a slot in g alone and wakes the waiter that has waited
// longest for one.
func (g *Group) give() {
	g.conns.Add(-1)
	g.wakeOne()
}

// wakeOne wakes the caller of AcquireContext that has waited longest on g.
func (g *Group) wakeOne() {
	if q := g.waiters.Load(); q != nil && q.n.Load() > 0 {
		q.wake(false)
	}
}

// wakeAll wakes every caller of AcquireContext waiting on g.
func (g *Group) wakeAll() {
	if q := g.waiters.Load(); q != nil && q.n.Load() > 0 {
		q.wake(true)
	}
}

// queue returns g's queue of waiters, making it on first use.
func (g *Group) queue() *slotQueue {
	if q := g.waiters.Load(); q != nil {
		return q
	}
	g.waiters.CompareAndSwap(nil, &slotQueue{})
	return g.waiters.Load()
}

// slotQueue holds the callers of AcquireContext that a Group refused, in the
// order they came, so that a freed slot wakes the one that has waited
// longest. It is a list linked both ways, so that a caller joins and leaves
// it at once however many wait.
type slotQueue struct {
	head, tail *slotWaiter
	mu         sync.Mutex
	n          atomic.Int32 // waiters queued, for Release to skip the lock
}

// slotWaiter is a caller waiting in a slotQueue, woken by closing ch.
type slotWaiter struct {
	ch         chan struct{}
	prev, next *slotWaiter // under the queue's mu
	woken      bool        // under the queue's mu
}

// add queues a new waiter, at the front if first, and returns it.
func (q *slotQueue) add(first bool) *slotWaiter {
	w := &slotWaiter{ch: make(chan struct{})}
	q.mu.Lock()
	defer q.mu.Unlock()
	switch {
	case q.head == nil:
		q.head, q.tail = w, w
	case first:
		w.next, q.head.prev = q.head, w
		q.head = w
	default:
		w.prev, q.tail.next = q.tail, w
		q.tail = w
	}
	q.n.Add(1)
	return w
}

// unlink takes w out of q. q.mu must be held.
func (q *slotQueue) unlink(w *slotWaiter) {
	if w.prev != nil {
		w.prev.next = w.next
	} else {
		q.head = w.next
	}
	if w.next != nil {
		w.next.prev = w.prev
	} else {
		q.tail = w.prev
	}
	w.prev, w.next = nil, nil
	q.n.Add(-1)
}

// leave takes w out of q. A wake that w took and did not use goes to the
// next waiter, so that no freed slot is left without one trying it.
func (q *slotQueue) leave(w *slotWaiter) {
	q.mu.Lock()
	if !w.woken {
		q.unlink(w)
		q.mu.Unlock()
		return
	}
	q.mu.Unlock()
	q.wake(false)
}

// wake wakes the waiter that has waited longest, or with all every waiter.
func (q *slotQueue) wake(all bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for w := q.head; w != nil; w = q.head {
		q.unlink(w)
		w.woken = true
		close(w.ch)
		if !all {
			return
		}
	}
}

// Reads returns the Limiter of the reads of g's connections: it charges the
// read Limiters of g and its ancestors and counts the bytes read in their
// Traffic, along the ancestors of the moment. It is nil for a nil g.
func (g *Group) Reads() Reserver {
	if g == nil {
		return nil
	}
	return (*groupReads)(g)
}

// Writes returns the Limiter of the writes of g's connections, as Reads
// does for reads, counting the bytes written.
func (g *Group) Writes() Reserver {
	if g == nil {
		return nil
	}
	return (*groupWrites)(g)
}

// Open takes a connection slot in g, as Acquire does, and wraps c as
// NewConn does, limited by the Limiters of g and each of its ancestors, on
// top of any WithLimiter of its own, and counted in their Traffic; closing
// the result gives the slot back. Later changes to the levels, such as
// SetLimiters or SetParent, apply to it as well. When a slot cannot be
// taken it returns nil and the error, a *TooManyConnsError or, WithWait,
// the context's, leaving c open for the caller to close; it returns
// ErrNilConn for a nil c.
func (g *Group) Open(c net.Conn, opts ...Option) (*Conn, error) {
	if c == nil {
		return nil, ErrNilConn
	}
	o := mergeOptions(opts)
	if err := g.admit(o); err != nil {
		return nil, err
	}
	defer g.admitted(o)
	return newConn(c, o, g), nil
}

// OpenPacketConn is Open for a net.PacketConn, wrapped as NewPacketConn
// does.
func (g *Group) OpenPacketConn(c net.PacketConn, opts ...Option) (*PacketConn, error) {
	if c == nil {
		return nil, ErrNilConn
	}
	o := mergeOptions(opts)
	if err := g.admit(o); err != nil {
		return nil, err
	}
	defer g.admitted(o)
	return newPacketConn(c, o, g), nil
}

// OpenStream is Open for an io.ReadWriteCloser, wrapped as NewStream does.
func (g *Group) OpenStream(rwc io.ReadWriteCloser, opts ...Option) (*Stream, error) {
	if rwc == nil {
		return nil, ErrNilConn
	}
	o := mergeOptions(opts)
	if err := g.admit(o); err != nil {
		return nil, err
	}
	defer g.admitted(o)
	s := &Stream{}
	s.init(rwc, rwc, rwc, withDatagram(o, rwc), g)
	return s, nil
}

// admit takes a slot for a connection opened with o and, when o gives
// Counters, checks them against the Traffic of g and its ancestors and
// holds cycleMu, for admitted to release once the connection is attached:
// a Counter that already counts g or an ancestor would count the
// connection twice.
func (g *Group) admit(o options) error {
	if err := g.acquireFor(o.ctx); err != nil {
		return err
	}
	if len(o.counters) == 0 {
		return nil
	}
	cycleMu.Lock()
	n := g.node()
	for _, c := range o.counters {
		if c.countsAbove(n) {
			cycleMu.Unlock()
			g.Release()
			return ErrCounterOverlap
		}
	}
	return nil
}

// admitted ends what admit began for o.
func (g *Group) admitted(o options) {
	if len(o.counters) > 0 {
		cycleMu.Unlock()
	}
}

// acquireFor takes a slot for Open, waiting for it until ctx is done if
// ctx is set.
func (g *Group) acquireFor(ctx context.Context) error {
	if ctx != nil {
		return g.AcquireContext(ctx)
	}
	return g.Acquire()
}

// groupLevels are the levels of both directions of a Group, as built from
// the Groups of chain.
//
// A Group counts in two ways on purpose: the wrappers opened in it add their
// Meters to each level's Traffic, which costs their reads and writes nothing,
// while the hook-style relays that charge Reads and Writes have no Meter and
// count through the Counter views among these levels.
type groupLevels struct {
	chain       []groupStamp
	read, write hierarchy
	// The Limiters of the levels of each direction, from g up, without the
	// view of g's sum: those of the wrappers opened in g, whose Meters
	// count their bytes.
	own [2][]Limiter
}

// groupStamp is a Group of a chain and its epoch when levels were built.
type groupStamp struct {
	g     *Group
	epoch uint64
}

// hierarchy returns the levels that the reads, or the writes, of g's
// connections are charged to: the Limiters of that direction of g and each
// ancestor, flattened, and the views of their Traffic.
func (g *Group) hierarchy(write bool) *hierarchy {
	c := g.levels()
	if write {
		return &c.write
	}
	return &c.read
}

// levels returns g's levels as they are now.
func (g *Group) levels() *groupLevels {
	c := g.cache.Load()
	if c == nil || !c.current(g) {
		c = g.build()
	}
	return c
}

// current reports whether c was built from g and its ancestors as they are.
func (c *groupLevels) current(g *Group) bool {
	a := g
	for _, s := range c.chain {
		if a != s.g || a.epoch.Load() != s.epoch {
			return false
		}
		a = a.parent.Load()
	}
	return a == nil
}

// build builds g's levels and keeps them in its cache. It takes no lock: a
// chain that SetParent changes while it is read is not current, which the
// next charge sees, so it builds them again.
func (g *Group) build() *groupLevels {
	c := &groupLevels{}
	for a := g; a != nil; a = a.parent.Load() {
		// The epoch first: a change after it makes the next charge build
		// the levels again.
		c.chain = append(c.chain, groupStamp{g: a, epoch: a.epoch.Load()})
		r, w := a.readLimiter(), a.writeLimiter()
		c.read.add(r)
		c.write.add(w)
		c.own[0] = append(c.own[0], r)
		c.own[1] = append(c.own[1], w)
	}
	// The ancestors' sums add up g's.
	sum := &g.node().sum
	c.read.add(sum.reads())
	c.write.add(sum.writes())
	g.cache.Store(c)
	return c
}

// groupReads and groupWrites are the Limiters of the reads and the writes
// of a Group's connections, the hierarchy of its levels of the moment.
type (
	groupReads  Group
	groupWrites Group
)

func (l *groupReads) hier() *hierarchy  { return (*Group)(l).hierarchy(false) }
func (l *groupWrites) hier() *hierarchy { return (*Group)(l).hierarchy(true) }

func (l *groupReads) Wait(ctx context.Context, n int64) error { return l.hier().Wait(ctx, n) }
func (l *groupReads) TryWait(n int64) error                   { return l.hier().TryWait(n) }
func (l *groupReads) Refund(n int64)                          { l.hier().Refund(n) }
func (l *groupReads) Ready(ctx context.Context, n int64) (int64, error) {
	return l.hier().Ready(ctx, n)
}
func (l *groupReads) Charge(n int64) error { return l.hier().Charge(n) }

func (l *groupWrites) Wait(ctx context.Context, n int64) error { return l.hier().Wait(ctx, n) }
func (l *groupWrites) TryWait(n int64) error                   { return l.hier().TryWait(n) }
func (l *groupWrites) Refund(n int64)                          { l.hier().Refund(n) }
func (l *groupWrites) Ready(ctx context.Context, n int64) (int64, error) {
	return l.hier().Ready(ctx, n)
}
func (l *groupWrites) Charge(n int64) error { return l.hier().Charge(n) }
