package byterate

import (
	"context"
	"sync"
	"sync/atomic"
	"unsafe"
)

// pauseGate is what the charges of a Budget wait on while a step with Pause
// applies. A change that may end the pause, such as SetSteps, opens the
// gate and puts a new one in its place for waiters to come. A blocking wait
// sleeps on one of the gate's stop channels, which spread the waiters as a
// Rate generation's do; a NotifyReadable of a wrapper registers its call on
// one of the gate's notify shards instead, so that a reader paused while
// idle holds no goroutine. Opening the gate closes every channel and
// advances every registered notify once, on one goroutine for them all:
// the work is proportional to the waiters.
type pauseGate struct {
	stopper
	notes atomic.Pointer[noteSet] // made by the first notify
}

// noteSet holds the notify shards of a pauseGate. A gate makes a set of one
// shard for its first notify and replaces it with one sized for the
// processors once registrations contend on it, keeping the small one for
// open to empty, so that the many gates of Budgets that pause few readers
// each stay small.
type noteSet struct {
	prev   *noteSet
	shards []noteShard // a power of two of them
}

// noteShard is a list of the notifies registered on a gate, on a cache
// line of its own.
type noteShard struct {
	noteList
	_ [(cacheLineSize - unsafe.Sizeof(noteList{})%cacheLineSize) % cacheLineSize]byte
}

// noteList is a list of notifies under mu; open says that the gate opened,
// after which nothing joins it.
type noteList struct {
	mu   sync.Mutex
	head *pendingNotify
	open bool
}

// openNotes is left in an opened gate.
var openNotes = &noteSet{}

// gate returns b's current pauseGate, making one if it has none.
func (b *Budget) gate() *pauseGate {
	if g := b.pause.Load(); g != nil {
		return g
	}
	b.pause.CompareAndSwap(nil, &pauseGate{})
	return b.pause.Load()
}

// wakePaused opens b's pauseGate, if it has one, waking the charges that
// wait on a step with Pause, to look at b again. Every change that may end
// a pause calls it once the change is visible and without b's locks held:
// a waiter takes the gate before it looks whether b is paused, so that it
// either sees the change or is woken by it.
func (b *Budget) wakePaused() {
	if g := b.pause.Swap(nil); g != nil {
		g.open()
	}
}

// paused returns b's gate when the step that applies has Pause, and nil
// otherwise, also when b refuses the bytes, which the caller then finds.
func (b *Budget) paused() *pauseGate {
	if !b.stepSet().pause {
		return nil
	}
	g := b.gate()
	if b.expiredLocked() {
		return nil
	}
	set := b.stepSet()
	used := b.per().used.Load()
	if used >= set.stopAt {
		return nil
	}
	if _, s := set.at(used); s != nil && s.Pause {
		return g
	}
	return nil
}

// awaitResume waits until gate g opens, ctx is done or switchCh closes. It
// returns nil when g opened, errSwitched for switchCh, and otherwise ctx's
// error: for the probe of a wrapper's TryRead and NotifyReadable, a
// pausedError that wraps it and carries g, for the NotifyReadable to
// register on.
func awaitResume(ctx context.Context, t *waitTimer, g *pauseGate, switchCh <-chan struct{}) error {
	switch t.sleepOn(maxI64, &g.stopper, ctx.Done(), switchCh) {
	case canceled:
		if ctx == probeCtx {
			return &pausedError{gate: g, err: ctx.Err()}
		}
		return ctx.Err()
	case switched:
		return errSwitched
	case slept, stopped:
	}
	return nil
}

// pausedError is the error of a probe on a paused Budget. It is the probe
// context's error, as the callers of a wait expect, and carries the gate
// that the probe would wait on.
type pausedError struct {
	gate *pauseGate
	err  error
}

func (e *pausedError) Error() string { return e.err.Error() }
func (e *pausedError) Unwrap() error { return e.err }

// open wakes g's waiters: it closes its channels and calls its notifies.
func (g *pauseGate) open() {
	g.closeStop()
	var woken []*pendingNotify
	for set := g.notes.Swap(openNotes); set != nil && set != openNotes; set = set.prev {
		for i := range set.shards {
			l := &set.shards[i].noteList
			l.mu.Lock()
			for n := l.head; n != nil; n = n.gnext {
				n.linked = false
				woken = append(woken, n)
			}
			l.head = nil
			l.open = true
			l.mu.Unlock()
		}
	}
	if len(woken) > 0 {
		// One goroutine for all, so that the change that opened g neither
		// waits for them nor runs the readers' calls under its own locks.
		go func() {
			for _, n := range woken {
				n.advance()
			}
		}()
	}
}

// add registers n on g, keyed by key, and reports whether it did: false
// once g has opened. n.mu must be held.
func (g *pauseGate) add(n *pendingNotify, key uint32) bool {
	set := g.notes.Load()
	if set == nil {
		g.notes.CompareAndSwap(nil, &noteSet{shards: make([]noteShard, 1)})
		set = g.notes.Load()
	}
	if set == openNotes {
		return false
	}
	l := &set.shards[shardIndex(key, len(set.shards))].noteList
	if !l.mu.TryLock() {
		l.mu.Lock()
		if n := sharedStopShards(); n > len(set.shards) {
			g.notes.CompareAndSwap(set, &noteSet{shards: make([]noteShard, n), prev: set})
		}
	}
	defer l.mu.Unlock()
	if l.open {
		return false
	}
	n.gnext, n.gprev = l.head, nil
	if l.head != nil {
		l.head.gprev = n
	}
	l.head = n
	n.linked = true
	n.glist = l
	return true
}

// unparkGate takes n off the gate list it registered on, if open has not
// taken it off already. n.mu must be held.
func (n *pendingNotify) unparkGate() {
	l := n.glist
	n.gate = nil
	if l == nil {
		return
	}
	n.glist = nil
	l.mu.Lock()
	defer l.mu.Unlock()
	if !n.linked {
		return // open took it off
	}
	if n.gprev != nil {
		n.gprev.gnext = n.gnext
	} else {
		l.head = n.gnext
	}
	if n.gnext != nil {
		n.gnext.gprev = n.gprev
	}
	n.gnext, n.gprev, n.linked = nil, nil, false
}
