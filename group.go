package byterate

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
)

var (
	// ErrTooManyConns is returned by Acquire and Open when a Group, or one
	// of its ancestors, has as many connections as its MaxConns.
	ErrTooManyConns = errors.New("byterate: too many connections")
	// ErrGroupCycle is returned by SetParent for a parent that is the Group
	// itself or one of its descendants.
	ErrGroupCycle = errors.New("byterate: group would be its own ancestor")
)

// groupTree serializes the moves of SetParent with Acquire and Release, so
// that a moved Group takes its connections along exactly, and with the
// building of levels, so that they are built from one tree. Charges take no
// lock.
var groupTree sync.RWMutex

// Group is a level of a tree of traffic controls, such as a whole server,
// one of its users, a user's client and one of its tunnels: the connections
// opened in a Group are charged to the limiters of the Group and of each of
// its ancestors, count in each one's Traffic and Conns, and each level can
// cap its connections. A Group with a nil parent is a root.
//
// Open takes a connection slot and wraps a connection; Acquire and Release
// take and give back a slot alone, and Reader and Writer return the limiters
// of a Group's reads and writes, for relays and wrappers of the caller's
// choice. SetParent moves a Group and its connections under another parent
// at any time: from then on, their bytes go to the new ancestors, as do
// their slots, while the bytes counted before stay where they are.
//
// All methods are safe for concurrent use. The zero value is a root without
// limits.
type Group struct {
	parent      atomic.Pointer[Group]
	read, write atomic.Pointer[groupLimiter]
	traffic     atomic.Pointer[Counter]
	epoch       atomic.Uint64 // changed with the limiters and the Counter
	conns       atomic.Int64  // slots taken in the Group and its descendants
	maxConns    atomic.Int64  // 0 for no cap
	// The levels that the Group's connections are charged to, kept until
	// the Group or an ancestor changes, so that charges walk no tree and
	// allocate nothing.
	cache atomic.Pointer[groupLevels]
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

// SetParent moves g under p, or makes it a root for a nil p, with the
// connection slots taken in g and its descendants: they leave g's old
// ancestors and count in p and its ancestors, even beyond their MaxConns,
// which then refuse new connections until enough close. Charges after the
// move go to the new ancestors; a wait in progress ends on the old ones, and
// so does the Refund of a wrapped connection, while a direct Refund on
// Reader or Writer goes to the new ones. It returns ErrGroupCycle, and moves
// nothing, when p is g or a descendant of g.
func (g *Group) SetParent(p *Group) error {
	groupTree.Lock()
	defer groupTree.Unlock()
	for a := p; a != nil; a = a.parent.Load() {
		if a == g {
			return ErrGroupCycle
		}
	}
	n := g.conns.Load()
	for a := g.parent.Load(); a != nil; a = a.parent.Load() {
		a.conns.Add(-n)
	}
	for a := p; a != nil; a = a.parent.Load() {
		a.conns.Add(n)
	}
	g.parent.Store(p)
	return nil
}

// SetReadLimiter sets the Limiter that g's own level charges the reads of
// its connections to, on top of its ancestors' and descendants', or none for
// nil. A Rate, a Budget or a Hierarchy of them suits; the same Limiter as
// the write one shares one limit between the two directions, and one set at
// several levels is charged once. It applies from the next read. It must not
// charge the Reader or Writer of g or of its descendants, which would charge
// itself without end.
func (g *Group) SetReadLimiter(l Limiter) { g.setLimiter(&g.read, l) }

// SetWriteLimiter sets the Limiter of the writes of g's connections, as
// SetReadLimiter does for reads.
func (g *Group) SetWriteLimiter(l Limiter) { g.setLimiter(&g.write, l) }

func (g *Group) setLimiter(p *atomic.Pointer[groupLimiter], l Limiter) {
	p.Store(&groupLimiter{l: nilLimiter(l)})
	g.epoch.Add(1)
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
// descendants read, as inbound, and wrote, as outbound. A moved connection's
// bytes count where it was when they passed. Unless SetTraffic gives it
// another, it is a plain Counter: a NewSharedCounter suits a level that many
// connections pass through at once, such as a root.
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
// open.
func (g *Group) SetMaxConns(n int64) { g.maxConns.Store(max(n, 0)) }

// MaxConns returns g's cap on connections, or 0 for none.
func (g *Group) MaxConns() int64 { return g.maxConns.Load() }

// Conns returns the connection slots taken in g and its descendants.
func (g *Group) Conns() int64 { return g.conns.Load() }

// Acquire takes a connection slot in g and each of its ancestors, or none
// and ErrTooManyConns when any of them has as many as its MaxConns; a racing
// Acquire that takes a slot and gives it back can cause such a refusal too.
// Each successful Acquire must be matched by one Release.
func (g *Group) Acquire() error {
	groupTree.RLock()
	defer groupTree.RUnlock()
	for a := g; a != nil; a = a.parent.Load() {
		if !a.take() {
			for b := g; b != a; b = b.parent.Load() {
				b.conns.Add(-1)
			}
			return ErrTooManyConns
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
	groupTree.RLock()
	defer groupTree.RUnlock()
	for a := g; a != nil; a = a.parent.Load() {
		a.conns.Add(-1)
	}
}

// Reader returns the Limiter of the reads of g's connections: it charges the
// read Limiters of g and its ancestors and counts the bytes as inbound in
// their Traffic, along the ancestors of the moment. It is nil for a nil g.
func (g *Group) Reader() Limiter {
	if g == nil {
		return nil
	}
	return (*groupReader)(g)
}

// Writer returns the Limiter of the writes of g's connections, as Reader
// does for reads, counting the bytes as outbound.
func (g *Group) Writer() Limiter {
	if g == nil {
		return nil
	}
	return (*groupWriter)(g)
}

// Open takes a connection slot in g, as Acquire does, and wraps c as
// NewDuplexConn does with g's Reader and Writer; closing the result gives
// the slot back. It returns ErrTooManyConns, and c unwrapped, when a slot
// cannot be taken, leaving c open, and ErrNilConn for a nil c.
func (g *Group) Open(c net.Conn) (net.Conn, error) {
	return g.open(c, isDatagramConn(c))
}

// OpenDatagram is Open for a connection whose every Read and Write carries
// one whole message, wrapped as NewDatagramConn does.
func (g *Group) OpenDatagram(c net.Conn) (net.Conn, error) {
	return g.open(c, true)
}

func (g *Group) open(c net.Conn, datagram bool) (net.Conn, error) {
	if c == nil {
		return nil, ErrNilConn
	}
	if err := g.Acquire(); err != nil {
		return c, err
	}
	nc := &rateNetConn{conn: c}
	nc.init(c, g.Reader(), g.Writer(), datagram)
	nc.group = g
	return nc, nil
}

// OpenReadWriteCloser is Open for an io.ReadWriteCloser, wrapped as
// NewDuplexReadWriteCloser does.
func (g *Group) OpenReadWriteCloser(rwc io.ReadWriteCloser) (io.ReadWriteCloser, error) {
	if rwc == nil {
		return nil, ErrNilConn
	}
	if err := g.Acquire(); err != nil {
		return rwc, err
	}
	s := &rateConn{}
	s.init(rwc, g.Reader(), g.Writer(), isDatagramConn(rwc))
	s.group = g
	return s, nil
}

// groupLevels are the levels of both directions of a Group, as built from
// the Groups of chain.
type groupLevels struct {
	chain       []groupStamp
	read, write []level
}

// groupStamp is a Group of a chain and its epoch when levels were built.
type groupStamp struct {
	g     *Group
	epoch uint64
}

// levels returns the levels that the reads, or the writes, of g's
// connections are charged to: the Limiters of that direction of g and each
// ancestor, flattened, then the views of their Traffic. Counted last, the
// views take no receipt of a Rate's.
func (g *Group) levels(write bool) []level {
	c := g.cache.Load()
	if c == nil || !c.current(g) {
		c = g.build()
	}
	if write {
		return c.write
	}
	return c.read
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

// build builds g's levels and keeps them in its cache.
func (g *Group) build() *groupLevels {
	groupTree.RLock()
	defer groupTree.RUnlock()
	c := &groupLevels{}
	for a := g; a != nil; a = a.parent.Load() {
		// The epoch first: a change after it makes the next charge build
		// the levels again.
		c.chain = append(c.chain, groupStamp{g: a, epoch: a.epoch.Load()})
		c.read = addLevels(c.read, a.ReadLimiter())
		c.write = addLevels(c.write, a.WriteLimiter())
	}
	for _, s := range c.chain {
		t := s.g.Traffic()
		c.read = addLevels(c.read, t.Inbound())
		c.write = addLevels(c.write, t.Outbound())
	}
	g.cache.Store(c)
	return c
}

// groupReader and groupWriter are the Limiters of the reads and the writes
// of a Group's connections.
type (
	groupReader Group
	groupWriter Group
)

func (l *groupReader) Wait(ctx context.Context, n int64) error {
	return waitLevels(ctx, (*Group)(l).levels(false), n)
}
func (l *groupReader) Refund(n int64) { refundLevels((*Group)(l).levels(false), n) }
func (l *groupReader) Ready(ctx context.Context, n int64) (int64, error) {
	return readyLevels(ctx, (*Group)(l).levels(false), n)
}
func (l *groupReader) Charge(n int64) error { return chargeLevels((*Group)(l).levels(false), n) }

func (l *groupWriter) Wait(ctx context.Context, n int64) error {
	return waitLevels(ctx, (*Group)(l).levels(true), n)
}
func (l *groupWriter) Refund(n int64) { refundLevels((*Group)(l).levels(true), n) }
func (l *groupWriter) Ready(ctx context.Context, n int64) (int64, error) {
	return readyLevels(ctx, (*Group)(l).levels(true), n)
}
func (l *groupWriter) Charge(n int64) error { return chargeLevels((*Group)(l).levels(true), n) }

func waitLevels(ctx context.Context, lv []level, n int64) error {
	if n <= 0 {
		return nil
	}
	v := levels{h: lv}
	var receipt writeCharge
	_, err := v.wait(ctx, nil, nil, n, true, &receipt)
	return err
}

func refundLevels(lv []level, n int64) {
	if n <= 0 {
		return
	}
	for _, l := range lv {
		if l.rate != nil {
			l.rate.Refund(n)
		} else {
			l.other.Refund(n)
		}
	}
}

func readyLevels(ctx context.Context, lv []level, n int64) (int64, error) {
	if n <= 0 {
		return n, nil
	}
	v := levels{h: lv}
	return v.ready(ctx, nil, nil, n, false)
}

func chargeLevels(lv []level, n int64) error {
	if n <= 0 {
		return nil
	}
	v := levels{h: lv}
	return v.charge(n)
}
