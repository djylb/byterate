package byterate

import (
	"context"
	"errors"
	"io"
	"maps"
	"net"
	"sync"
	"sync/atomic"
	"unsafe"
	"weak"
)

var (
	// ErrTooManyConns is matched by the *TooManyConnsError of Acquire and
	// Open, for a Group, or one of its ancestors, that has as many
	// connections as its MaxConns.
	ErrTooManyConns = errors.New("byterate: too many connections")
	// ErrGroupCycle is returned by SetParent for a parent that is the Group
	// itself or one of its descendants.
	ErrGroupCycle = errors.New("byterate: group would be its own ancestor")
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
// Each tree has a lock of its own, which its root holds: Acquire and Release
// share it, and SetParent takes it, with that of the tree it moves a Group
// to, so that trees do not contend with each other. Charges take no lock.
//
// All methods are safe for concurrent use. The zero value is a root without
// limits.
type Group struct {
	// The fields fill two cache lines: the first is read by every charge of
	// the Group's connections and its descendants' and written only when the
	// Group changes, the second is written by every Acquire and Release
	// below it.
	groupLinks
	_ [cacheLineSize - unsafe.Sizeof(groupLinks{})]byte
	groupSlots
	_ [cacheLineSize - unsafe.Sizeof(groupSlots{})]byte
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
	// The wrappers opened in the Group and its descendants, and their
	// Meters, under configMu: the Group's own record of them, as Traffic
	// may be shared with other Groups or wrappers.
	members map[weak.Pointer[rateConn]]*Meter
}

// groupSlots are the fields of a Group that Acquire and Release write.
type groupSlots struct {
	conns    atomic.Int64 // slots taken in the Group and its descendants
	maxConns atomic.Int64 // 0 for no cap
	// tree, while the Group is a root, serializes SetParent with Acquire and
	// Release in its tree, so that a moved Group takes its connections
	// along exactly.
	tree    sync.RWMutex
	waiters atomic.Pointer[slotQueue] // made by the first AcquireContext refused here
}

// groupLimiter holds a Limiter of a Group's own level.
type groupLimiter struct {
	l Limiter
}

// NewGroup returns a Group under parent, or a root for a nil parent.
func NewGroup(parent *Group) *Group {
	g := &Group{}
	g.parent.Store(parent)
	return g
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

// lockTree read-locks the tree of g and returns its root, which holds the
// lock: SetParent can move g to another tree before the lock is taken, but
// not while it is held.
func (g *Group) lockTree() *Group {
	for {
		r := g.root()
		r.tree.RLock()
		if g.root() == r {
			return r
		}
		r.tree.RUnlock()
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
	configMu.Lock()
	defer configMu.Unlock()
	// No other SetParent runs, so the roots stay; Acquire and Release hold
	// one tree's lock at a time, so taking two cannot deadlock.
	from := g.root()
	from.tree.Lock()
	defer from.tree.Unlock()
	if p != nil {
		if to := p.root(); to != from {
			to.tree.Lock()
			defer to.tree.Unlock()
		}
	}
	for a := p; a != nil; a = a.parent.Load() {
		if a == g || g.chargedBy(a.readLimiter()) || g.chargedBy(a.writeLimiter()) {
			return ErrGroupCycle
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
	g.parent.Store(p)
	// The wrappers below g count in the new ancestors from now on, and the
	// bytes they counted stay with the old ones. The ancestors in both
	// chains, such as the root of a move within a tree, keep them as they
	// are.
	stay := make(map[*Group]bool)
	for a := old; a != nil; a = a.parent.Load() {
		stay[a] = true
	}
	var left, joined []*Counter
	for a := p; a != nil; a = a.parent.Load() {
		if stay[a] {
			stay[a] = false // in both chains
			continue
		}
		joined = append(joined, a.Traffic())
		if a.members == nil {
			a.members = make(map[weak.Pointer[rateConn]]*Meter)
		}
		maps.Copy(a.members, g.members)
	}
	for a := old; a != nil; a = a.parent.Load() {
		if !stay[a] {
			continue
		}
		left = append(left, a.Traffic())
		for k := range g.members {
			delete(a.members, k)
		}
	}
	moveMeters(left, joined, g.meters())
	g.update()
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
// next read or write, also of the connections open in g and below. It
// panics if a Limiter charges the Reads or Writes of g or of a descendant,
// which would charge itself without end.
func (g *Group) SetLimiters(read, write Limiter) { g.setLimiters(&read, &write) }

// setLimiters sets the Limiters that read and write point to, leaving a
// direction with a nil pointer as it is.
func (g *Group) setLimiters(read, write *Limiter) {
	configMu.Lock()
	defer configMu.Unlock()
	for _, l := range [2]*Limiter{read, write} {
		if l != nil {
			if *l = classify(*l).l; g.chargedBy(*l) {
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
	g.update()
}

// chargedBy reports whether l charges the Reads or Writes of g or of one of
// its descendants. configMu must be held.
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
// descendants read and wrote, and their throughput. A moved connection's
// bytes count where it was when they passed. Unless SetTraffic gives it
// another, it is a plain Counter: the connections opened in the Group add
// to it without contending anyway, and a NewSharedCounter suits a level
// that many relays charge through Reads and Writes at once.
func (g *Group) Traffic() *Counter {
	if c := g.traffic.Load(); c != nil {
		return c
	}
	// The first Counter is made once, so every level built with it counts
	// in the one that stays.
	g.traffic.CompareAndSwap(nil, &Counter{})
	return g.traffic.Load()
}

// SetTraffic makes c the Counter of g's traffic from the next read or write,
// such as a NewSharedCounter for a Group that many connections share, or a
// Counter restored after a restart; nil makes a new one. A read or write that
// was charged before may still count in the Counter it replaces.
func (g *Group) SetTraffic(c *Counter) {
	if c == nil {
		c = &Counter{}
	}
	configMu.Lock()
	defer configMu.Unlock()
	old := g.Traffic()
	if old == c {
		return
	}
	// Only the wrappers of g's subtree move; others that share old stay.
	moveMeters([]*Counter{old}, []*Counter{c}, g.meters())
	g.traffic.Store(c)
	g.epoch.Add(1)
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
	r := g.lockTree()
	defer r.tree.RUnlock()
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
	r := g.lockTree()
	defer r.tree.RUnlock()
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
// longest.
type slotQueue struct {
	mu sync.Mutex
	q  []*slotWaiter
	n  atomic.Int32 // len(q), for Release to skip the lock
}

// slotWaiter is a caller waiting in a slotQueue, woken by closing ch.
type slotWaiter struct {
	ch    chan struct{}
	woken bool // under the queue's mu
}

// add queues a new waiter, at the front if first, and returns it.
func (q *slotQueue) add(first bool) *slotWaiter {
	w := &slotWaiter{ch: make(chan struct{})}
	q.mu.Lock()
	defer q.mu.Unlock()
	if first {
		q.q = append(q.q, nil)
		copy(q.q[1:], q.q)
		q.q[0] = w
	} else {
		q.q = append(q.q, w)
	}
	q.n.Add(1)
	return w
}

// leave takes w out of q. A wake that w took and did not use goes to the
// next waiter, so that no freed slot is left without one trying it.
func (q *slotQueue) leave(w *slotWaiter) {
	q.mu.Lock()
	if !w.woken {
		i := 0
		for q.q[i] != w {
			i++
		}
		q.q = append(q.q[:i], q.q[i+1:]...)
		q.n.Add(-1)
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
	n := min(len(q.q), 1)
	if all {
		n = len(q.q)
	}
	for _, w := range q.q[:n] {
		w.woken = true
		close(w.ch)
	}
	clear(q.q[:n])
	q.q = q.q[n:]
	if len(q.q) == 0 {
		q.q = nil // drop the memory the woken took
	}
	q.n.Add(int32(-n))
}

// Reads returns the Limiter of the reads of g's connections: it charges the
// read Limiters of g and its ancestors and counts the bytes read in their
// Traffic, along the ancestors of the moment. It is nil for a nil g.
func (g *Group) Reads() Limiter {
	if g == nil {
		return nil
	}
	return (*groupReads)(g)
}

// Writes returns the Limiter of the writes of g's connections, as Reads
// does for reads, counting the bytes written.
func (g *Group) Writes() Limiter {
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
	if err := g.acquireFor(o.ctx); err != nil {
		return nil, err
	}
	return newConn(c, o, g), nil
}

// OpenPacketConn is Open for a net.PacketConn, wrapped as NewPacketConn
// does.
func (g *Group) OpenPacketConn(c net.PacketConn, opts ...Option) (*PacketConn, error) {
	if c == nil {
		return nil, ErrNilConn
	}
	o := mergeOptions(opts)
	if err := g.acquireFor(o.ctx); err != nil {
		return nil, err
	}
	return newPacketConn(c, o, g), nil
}

// OpenStream is Open for an io.ReadWriteCloser, wrapped as NewStream does.
func (g *Group) OpenStream(rwc io.ReadWriteCloser, opts ...Option) (*Stream, error) {
	if rwc == nil {
		return nil, ErrNilConn
	}
	o := mergeOptions(opts)
	if err := g.acquireFor(o.ctx); err != nil {
		return nil, err
	}
	s := &Stream{}
	s.init(rwc, rwc, rwc, withDatagram(o, rwc), g)
	return s, nil
}

// acquireFor takes a slot for Open, waiting for it until ctx is done if
// ctx is set.
func (g *Group) acquireFor(ctx context.Context) error {
	if ctx != nil {
		return g.AcquireContext(ctx)
	}
	return g.Acquire()
}

// meters returns the Meters of the wrappers opened in g and its
// descendants. configMu must be held.
func (g *Group) meters() []*Meter {
	ms := make([]*Meter, 0, len(g.members))
	for _, m := range g.members {
		ms = append(ms, m)
	}
	return ms
}

// update gives the wrappers opened in g and its descendants the Limiters
// of their levels as they are now. configMu must be held.
func (g *Group) update() {
	for k := range g.members {
		if s := k.Value(); s != nil {
			s.applyLocked()
		}
	}
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
	c := g.cache.Load()
	if c == nil || !c.current(g) {
		c = g.build()
	}
	if write {
		return &c.write
	}
	return &c.read
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
		c.read.add(a.readLimiter())
		c.write.add(a.writeLimiter())
		t := a.Traffic()
		c.read.add(t.reads())
		c.write.add(t.writes())
	}
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
