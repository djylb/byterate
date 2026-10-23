package byterate

import (
	"context"
	"errors"
	"io"
	"net"
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
// Open takes a connection slot and wraps a connection; Acquire and Release
// take and give back a slot alone, and Reads and Writes return the
// Limiters of a Group's reads and writes, for relays and wrappers of the
// caller's choice. SetParent moves a Group and its connections under another
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
		if a == g || g.chargedBy(a.ReadLimiter()) || g.chargedBy(a.WriteLimiter()) {
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
	// The waiters of the old ancestors may now find a slot, or a chain
	// without them; the chain of old does not change.
	for a := old; a != nil; a = a.parent.Load() {
		a.wakeAll()
	}
	return nil
}

// SetReadLimiter sets the Limiter that g's own level charges the reads of
// its connections to, on top of its ancestors' and descendants', or none for
// nil. A Rate, a Budget or a NewHierarchy of them suits; the same Limiter as
// the write one shares one limit between the two directions, and one set at
// several levels is charged once. It applies from the next read. It panics
// if l charges the Reads or Writes of g or of a descendant, which would
// charge itself without end.
func (g *Group) SetReadLimiter(l Limiter) { g.setLimiters(&l, nil) }

// SetWriteLimiter sets the Limiter of the writes of g's connections, as
// SetReadLimiter does for reads.
func (g *Group) SetWriteLimiter(l Limiter) { g.setLimiters(nil, &l) }

// SetLimiters sets both of g's own Limiters at once, as SetReadLimiter and
// SetWriteLimiter do, so that no read or write is charged with one of them
// changed and not the other.
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

// ReadLimiter returns the Limiter of g's own level for reads, as
// SetReadLimiter set it, or nil.
func (g *Group) ReadLimiter() Limiter { return loadLimiter(&g.read) }

// WriteLimiter returns the Limiter of g's own level for writes, as
// SetWriteLimiter set it, or nil.
func (g *Group) WriteLimiter() Limiter { return loadLimiter(&g.write) }

func loadLimiter(p *atomic.Pointer[groupLimiter]) Limiter {
	if l := p.Load(); l != nil {
		return l.l
	}
	return nil
}

// Traffic returns the Counter of the bytes that the connections of g and its
// descendants read and wrote. A moved connection's bytes count where it was
// when they passed. Unless SetTraffic gives it another, it is a plain
// Counter: a NewSharedCounter suits a level that many connections pass
// through at once, such as a root.
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
// of its ancestors now.
func (g *Group) Release() {
	r := g.lockTree()
	defer r.tree.RUnlock()
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

// OpenOption changes how the Open methods of a Group open a connection, and
// WithDrop how NewPacketConn and NewDuplexPacketConn wrap one.
type OpenOption struct {
	read, write Limiter
	limiters    bool
	drop        bool
	ctx         context.Context
}

// WithLimiters charges the reads and writes of the connection to read and
// write as well as to the Group's levels, as limits of the connection's own
// within the Group, such as a Rate per connection. A nil Limiter leaves its
// direction to the Group's levels.
func WithLimiters(read, write Limiter) OpenOption {
	return OpenOption{read: read, write: write, limiters: true}
}

// WithWait makes Open wait for a connection slot, as AcquireContext does,
// until ctx is done, instead of refusing it with a *TooManyConnsError.
func WithWait(ctx context.Context) OpenOption {
	return OpenOption{ctx: ctx}
}

// WithDrop makes a wrapped net.PacketConn drop the datagrams that its limit
// does not let through at once, as TryWait decides, instead of waiting for
// them: such a datagram is not charged, a WriteTo of it returns len(b) and
// nil, as a UDP socket whose buffer is full does, and a ReadFrom discards it
// and reads the next one. A limit that refuses a datagram, such as an
// exhausted Budget, still ends the WriteTo or ReadFrom with its error. It
// applies to NewPacketConn, NewDuplexPacketConn and OpenPacketConn alone.
func WithDrop() OpenOption {
	return OpenOption{drop: true}
}

// dropping reports whether opts has WithDrop.
func dropping(opts []OpenOption) bool {
	for _, o := range opts {
		if o.drop {
			return true
		}
	}
	return false
}

// open takes a connection slot in g as opts say and returns the Limiters of
// the connection's reads and writes.
func (g *Group) open(opts []OpenOption) (read, write Limiter, err error) {
	read, write = g.Reads(), g.Writes()
	var ctx context.Context
	for _, o := range opts {
		if o.limiters {
			read, write = NewHierarchy(o.read, read), NewHierarchy(o.write, write)
		}
		if o.ctx != nil {
			ctx = o.ctx
		}
	}
	if ctx != nil {
		err = g.AcquireContext(ctx)
	} else {
		err = g.Acquire()
	}
	return read, write, err
}

// Open takes a connection slot in g, as Acquire does, and wraps c as
// NewDuplexConn does with g's Reads and Writes; closing the result gives
// the slot back. When a slot cannot be taken it returns nil and the error,
// a *TooManyConnsError or, WithWait, the context's, leaving c open for the
// caller to close; it returns ErrNilConn for a nil c.
func (g *Group) Open(c net.Conn, opts ...OpenOption) (net.Conn, error) {
	return g.openConn(c, isDatagramConn(c), opts)
}

// OpenDatagram is Open for a connection whose every Read and Write carries
// one whole message, wrapped as NewDuplexDatagramConn does.
func (g *Group) OpenDatagram(c net.Conn, opts ...OpenOption) (net.Conn, error) {
	return g.openConn(c, true, opts)
}

func (g *Group) openConn(c net.Conn, datagram bool, opts []OpenOption) (net.Conn, error) {
	if c == nil {
		return nil, ErrNilConn
	}
	read, write, err := g.open(opts)
	if err != nil {
		return nil, err
	}
	nc := newRateNetConn(c, read, write, datagram)
	nc.group = g
	return nc, nil
}

// OpenPacketConn is Open for a net.PacketConn, wrapped as
// NewDuplexPacketConn does.
func (g *Group) OpenPacketConn(c net.PacketConn, opts ...OpenOption) (net.PacketConn, error) {
	if c == nil {
		return nil, ErrNilConn
	}
	read, write, err := g.open(opts)
	if err != nil {
		return nil, err
	}
	pc := newRatePacketConn(c, read, write, dropping(opts))
	pc.s.group = g
	return pc, nil
}

// OpenReadWriteCloser is Open for an io.ReadWriteCloser, wrapped as
// NewDuplexReadWriteCloser does.
func (g *Group) OpenReadWriteCloser(rwc io.ReadWriteCloser, opts ...OpenOption) (io.ReadWriteCloser, error) {
	if rwc == nil {
		return nil, ErrNilConn
	}
	read, write, err := g.open(opts)
	if err != nil {
		return nil, err
	}
	s := &rateConn{}
	s.init(rwc, rwc, rwc, read, write, isDatagramConn(rwc))
	s.group = g
	return s, nil
}

// groupLevels are the levels of both directions of a Group, as built from
// the Groups of chain.
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
		c.read.add(a.ReadLimiter())
		c.write.add(a.WriteLimiter())
		t := a.Traffic()
		c.read.add(t.Reads())
		c.write.add(t.Writes())
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
