package byterate

import (
	"runtime"
	"sync"
	"sync/atomic"
)

// tally is a pair of totals, of bytes read and written: what is added to it
// and what its sources count after they were attached. A source is a Meter
// or another tally, such as a Group's of each of its children, so that a
// sum over a tree is read from its levels rather than written at each of
// them by every byte.
//
// The sources sit in shards, each under a lock of its own that Attach,
// Detach and the readers of the totals take, so that attaching connections
// do not contend with each other or wait for a reader going through others
// than theirs. There is one shard to begin with, and a lock found taken
// doubles them, so that a tally few attach to takes one small table and
// one that many attach to at once spreads over as many shards as it needs.
//
// A tally read takes the locks of its shards one at a time, and while it
// holds one, those of the tallies in it: a tally is only ever a source of
// tallies above it in a tree, so the locks are always taken from the top
// down and cannot deadlock.
type tally struct {
	srcs  atomic.Pointer[srcTable]
	added stripes
	nsrc  atomic.Int64 // sources attached, so that a Detach of none skips the table
}

// source is what a tally adds up: a Meter, or a Group's node.
type source interface {
	totals() [2]int64
}

// totals returns the bytes of m.
func (m *Meter) totals() [2]int64 { return [2]int64{m.read.count(), m.written.count()} }

// srcTable holds the sources of a tally in shards. The first table has
// one shard, in the table itself.
type srcTable struct {
	shards []srcShard // a power of two of them
	first  [1]srcShard
	// retired is set, with the lock of every shard held, once a bigger
	// table replaced this one: whoever then locks a shard looks again.
	retired bool
}

func newSrcTable() *srcTable {
	t := &srcTable{}
	t.shards = t.first[:]
	return t
}

// srcShard is one shard of a srcTable: its sources, with their totals when
// attached and their Attaches not yet detached, and the bytes that its
// detached sources counted while attached. Its first source sits in one,
// so that a shard of one source, as a Group's Traffic has, needs no map.
// Shards are not padded apart: they are many only where many sources come
// and go, and a lock shared with a neighbour's line costs less there than
// the memory padding would take from the many small tallies.
type srcShard struct {
	m    map[source]srcEntry
	one  srcSlot // its src nil for none
	mu   sync.Mutex
	kept [2]int64
}

// srcSlot is a source and its entry.
type srcSlot struct {
	src source
	srcEntry
}

// srcEntry is a source attached to a tally: its totals when attached, and
// the attaches not yet detached.
type srcEntry struct {
	base [2]int64
	refs int
}

// sum returns the bytes that the sources of sh counted while attached.
// sh.mu must be held.
func (sh *srcShard) sum() [2]int64 {
	v := sh.kept
	if sh.one.src != nil {
		v = addCounted(v, sh.one.src, &sh.one.srcEntry)
	}
	for s, e := range sh.m {
		v = addCounted(v, s, &e)
	}
	return v
}

func addCounted(v [2]int64, s source, e *srcEntry) [2]int64 {
	tot := s.totals()
	return [2]int64{v[0] + tot[0] - e.base[0], v[1] + tot[1] - e.base[1]}
}

// put adds an attach of s with base tot to sh, and reports whether s is
// new. sh.mu must be held.
func (sh *srcShard) put(s source, e srcEntry) bool {
	if sh.one.src == s {
		sh.one.refs += e.refs
		return false
	}
	if old, ok := sh.m[s]; ok {
		old.refs += e.refs
		sh.m[s] = old
		return false
	}
	if sh.one.src == nil {
		sh.one = srcSlot{src: s, srcEntry: e}
		return true
	}
	if sh.m == nil {
		sh.m = make(map[source]srcEntry)
	}
	sh.m[s] = e
	return true
}

// drop ends an attach of s with tot its totals now, and reports whether
// that was its last one, which keeps its bytes in sh. sh.mu must be held.
func (sh *srcShard) drop(s source, tot [2]int64) bool {
	var e srcEntry
	switch old, ok := sh.m[s]; {
	case sh.one.src == s:
		if sh.one.refs--; sh.one.refs > 0 {
			return false
		}
		e, sh.one = sh.one.srcEntry, srcSlot{}
	case ok:
		if old.refs--; old.refs > 0 {
			sh.m[s] = old
			return false
		}
		e = old
		delete(sh.m, s)
	default:
		return false
	}
	sh.kept[0] += tot[0] - e.base[0]
	sh.kept[1] += tot[1] - e.base[1]
	return true
}

// add adds read and written bytes.
func (t *tally) add(read, written int64) {
	if read != 0 {
		t.added.add(0, read)
	}
	if written != 0 {
		t.added.add(1, written)
	}
}

// total returns the totals of t.
func (t *tally) total() [2]int64 {
	v := [2]int64{t.added.sum(0), t.added.sum(1)}
	if t.srcs.Load() == nil {
		return v // never attached to
	}
	for {
		s, ok := t.sourced()
		if ok {
			return [2]int64{v[0] + s[0], v[1] + s[1]}
		}
	}
}

// sourced returns the bytes of t's sources, and false when a bigger table
// replaced the one it read meanwhile. Each shard is read whole under its
// lock, so a Detach that moves a source's bytes into the shard is seen
// whole or not at all.
func (t *tally) sourced() ([2]int64, bool) {
	var v [2]int64
	tab := t.srcs.Load()
	if tab == nil {
		return v, true
	}
	for i := range tab.shards {
		sh := &tab.shards[i]
		sh.mu.Lock()
		if tab.retired {
			sh.mu.Unlock()
			return v, false
		}
		n := sh.sum()
		sh.mu.Unlock()
		v[0] += n[0]
		v[1] += n[1]
	}
	return v, true
}

// attach adds the bytes s counts beyond tot, its totals now, to t. A
// source attached more than once counts once, until as many detaches.
func (t *tally) attach(s source, tot [2]int64) {
	sh := t.lock(s)
	defer sh.mu.Unlock()
	if sh.put(s, srcEntry{base: tot, refs: 1}) {
		t.nsrc.Add(1)
	}
}

// detach ends an attach of s, and with the last one stops adding its bytes
// beyond tot, its totals now, keeping those it counted until then.
func (t *tally) detach(s source, tot [2]int64) {
	if t.nsrc.Load() == 0 {
		return
	}
	sh := t.lock(s)
	defer sh.mu.Unlock()
	if sh.drop(s, tot) {
		t.nsrc.Add(-1)
	}
}

// anySource reports whether f reports true for a source of t.
func (t *tally) anySource(f func(source) bool) bool {
	tab := t.srcs.Load()
	for tab != nil {
		found, retired := false, false
		for i := range tab.shards {
			sh := &tab.shards[i]
			sh.mu.Lock()
			if retired = tab.retired; !retired {
				found = sh.one.src != nil && f(sh.one.src)
				for s := range sh.m {
					found = found || f(s)
				}
			}
			sh.mu.Unlock()
			if found || retired {
				break
			}
		}
		if !retired {
			return found
		}
		tab = t.srcs.Load()
	}
	return false
}

// lock locks and returns the shard of s, making the table on first use and
// doubling it when the lock is taken.
func (t *tally) lock(s source) *srcShard {
	for {
		tab := t.srcs.Load()
		if tab == nil {
			t.srcs.CompareAndSwap(nil, newSrcTable())
			continue
		}
		sh := &tab.shards[shardIndex(srcKey(s), len(tab.shards))]
		if !sh.mu.TryLock() {
			if len(tab.shards) < pow2AtLeast(maxSrcShards()) {
				t.grow(tab)
				continue
			}
			sh.mu.Lock()
		}
		if !tab.retired {
			return sh
		}
		sh.mu.Unlock()
	}
}

// maxSrcShards is the most shards a tally spreads its sources over: a few
// per processor, as only those that attach at once contend.
func maxSrcShards() int { return 4 * runtime.GOMAXPROCS(0) }

// grow replaces tab, unless another did, by a table of twice its shards,
// keeping the sources and their bytes.
func (t *tally) grow(tab *srcTable) {
	for i := range tab.shards {
		tab.shards[i].mu.Lock()
	}
	defer func() {
		for i := range tab.shards {
			tab.shards[i].mu.Unlock()
		}
	}()
	if tab.retired {
		return
	}
	next := &srcTable{shards: make([]srcShard, 2*len(tab.shards))}
	for i := range tab.shards {
		old := &tab.shards[i]
		next.shards[0].kept[0] += old.kept[0]
		next.shards[0].kept[1] += old.kept[1]
		if old.one.src != nil {
			next.shards[shardIndex(srcKey(old.one.src), len(next.shards))].put(old.one.src, old.one.srcEntry)
		}
		for s, e := range old.m {
			next.shards[shardIndex(srcKey(s), len(next.shards))].put(s, e)
		}
	}
	tab.retired = true
	t.srcs.Store(next)
}

// srcKey spreads sources over shards by their address.
func srcKey(s source) uint32 {
	switch v := s.(type) {
	case *Meter:
		return mix32(addrOf(v) >> 4)
	case *groupNode:
		return mix32(addrOf(v) >> 4)
	}
	return 0
}
