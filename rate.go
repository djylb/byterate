package byterate

import (
	"context"
	"math/bits"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// burstWindowNs is the burst allowance: after an idle period a Rate lets up
// to two seconds' worth of bytes through without waiting.
const burstWindowNs = int64(2 * time.Second)

// generation is the state of a Rate from one Enable that clears it to the
// next: its debt, its meter and the channels that Disable closes. Enable
// replaces the generation instead of clearing it in place, and a new one is
// complete before it is published, so a charge, a refund and a sampling
// window each act on the one generation they loaded, and one that an Enable
// retired keeps its effects to itself.
//
// It fills two cache lines of its size class exactly: the first is read by
// every charge, the second written by every charge.
type generation struct {
	stop         atomic.Pointer[stopSet] // made by the first waiter
	lastSampleNs atomic.Int64            // start of the sampling window, written once per window
	nowBps       atomic.Int64            // throughput of the last sampling window, bytes/s
	_            [cacheLineSize - 24]byte
	tat          atomic.Int64 // theoretical arrival time (ns since epoch), can be negative
	bytesAcc     atomic.Int64 // bytes charged in the current sampling window, net of refunds
	_            [cacheLineSize - 16]byte
}

// stopSet holds the channels of a generation that Disable closes. Waiters
// spread over them, as a wait locks the channel it selects on, and one
// channel for every waiter of a shared Rate would serialize them. A
// generation makes a small set for its first waiter and each channel for the
// first waiter keyed to it, so that a Rate that never makes callers wait pays
// for none, and one of a single connection, whose directions keep a key
// each, pays for a channel per direction. Once every channel of the small set
// is made, the Rate is shared, and a set sized for the processors replaces
// it, keeping the small one for Disable to close.
type stopSet struct {
	shards []atomic.Pointer[stopChan] // a power of two of them
	made   atomic.Int32               // channels made in shards
	prev   *stopSet                   // the set this one replaced, still to close
}

// initialStopShards is the size of a generation's first stop set.
const initialStopShards = 8

func newStopSet(n int, prev *stopSet) *stopSet {
	return &stopSet{shards: make([]atomic.Pointer[stopChan], n), prev: prev}
}

// sharedStopShards is the size of the set that replaces a full first one:
// eight channels per processor, at most 256.
func sharedStopShards() int {
	n := initialStopShards
	for n < 8*runtime.GOMAXPROCS(0) && n < 256 {
		n *= 2
	}
	return n
}

// stopChan is one of the channels of a generation that Disable closes.
type stopChan struct{ ch chan struct{} }

var (
	// stoppedSet is left by Disable in a generation that no waiter had given a
	// set, and closedStop in the shards of a set that no waiter had made.
	stoppedSet = &stopSet{}
	closedStop = &stopChan{ch: closedChan}
)

// newGeneration returns a generation with a full burst and a meter whose
// first window starts at now.
func newGeneration(now int64) *generation {
	g := &generation{}
	g.tat.Store(now - burstWindowNs)
	g.lastSampleNs.Store(now)
	return g
}

// waitCh returns the channel of g that Disable closes for the waiter keyed by
// key, or nil for no generation.
func (g *generation) waitCh(key uint32) <-chan struct{} {
	if g == nil {
		return nil
	}
	set := g.stop.Load()
	if set == nil {
		set = newStopSet(initialStopShards, nil)
		if !g.stop.CompareAndSwap(nil, set) {
			set = g.stop.Load() // another waiter's, or stoppedSet
		}
	}
	if set == stoppedSet {
		return closedChan
	}
	s := &set.shards[key&uint32(len(set.shards)-1)]
	c := s.Load()
	if c == nil {
		c = &stopChan{ch: make(chan struct{})}
		if !s.CompareAndSwap(nil, c) {
			return s.Load().ch // another waiter's, or closedStop
		}
		if int(set.made.Add(1)) == len(set.shards) {
			g.grow(set)
		}
	}
	return c.ch
}

// grow replaces g's full stop set with a larger one, unless Disable or another
// waiter replaced it first. Waiters keep the channels they took from the
// full set, which Disable closes through the new set's prev.
func (g *generation) grow(full *stopSet) {
	if n := sharedStopShards(); n > len(full.shards) {
		g.stop.CompareAndSwap(full, newStopSet(n, full))
	}
}

// closeStop closes g's channels, waking its waiters, and leaves stoppedSet
// and closedStop for waiters to come. It runs once, for Disable.
func (g *generation) closeStop() {
	for set := g.stop.Swap(stoppedSet); set != nil; set = set.prev {
		for i := range set.shards {
			if c := set.shards[i].Swap(closedStop); c != nil {
				close(c.ch)
			}
		}
	}
}

// Rate is a bytes-per-second limiter (GCRA, a token-bucket variant) with a
// built-in throughput meter. A limit <= 0 means unlimited, and after an idle
// period up to two seconds' worth of bytes pass without waiting. The meter
// counts up to 2^63-1 bytes in a sampling window.
//
// All methods are safe for concurrent use and on a nil *Rate, which never
// limits. The zero value is an enabled, unlimited Rate, ready for SetLimit. A
// Rate must not be copied after first use; use Clone.
type Rate struct {
	// The fields fill the size class's cache line exactly, so that those
	// every charge reads share it with nothing that others write.
	gen      atomic.Pointer[generation] // nil until a zero value's first charge or Enable
	rate     atomic.Int64               // bytes/s, <=0 => unlimited
	debtRate int64                      // protected by mu: the limit tat's debt is reckoned at, the last one above 0
	mu       sync.Mutex                 // serializes Enable, Disable, SetLimit, Clone and refunds; charges take no lock
	disabled atomic.Bool                // written under mu
	_        [cacheLineSize - 36]byte
}

// NewRate returns a Rate limited to limitBps bytes per second with a full
// burst available. A limitBps <= 0 means unlimited; such a Rate takes no
// more memory than its own until something is charged to it.
func NewRate(limitBps int64) *Rate {
	limitBps = max(limitBps, 0)
	r := &Rate{debtRate: limitBps}
	r.rate.Store(limitBps)
	if limitBps > 0 {
		r.gen.Store(newGeneration(nowNs())) // the full burst starts now
	}
	return r
}

// generation returns r's generation, making a zero value's first one, with a
// full burst, at now.
func (r *Rate) generation(now int64) *generation {
	if g := r.gen.Load(); g != nil {
		return g
	}
	if g := newGeneration(now); r.gen.CompareAndSwap(nil, g) {
		return g
	}
	return r.gen.Load() // another charge's, or Enable's
}

// Clone returns an independent copy of r's limit, debt, enabled or disabled
// state and meter. Callers blocked on r are not affected by the copy.
func (r *Rate) Clone() *Rate {
	if r == nil {
		return nil
	}
	// Under r.mu, so that the enabled or disabled state is not torn by a
	// concurrent Enable or Disable: a clone with a closed stop channel that is
	// enabled would charge without ever waiting.
	r.mu.Lock()
	defer r.mu.Unlock()
	c := &Rate{debtRate: r.debtRate}
	c.rate.Store(r.rate.Load())
	if g := r.gen.Load(); g != nil {
		cg := &generation{}
		cg.tat.Store(g.tat.Load())
		cg.bytesAcc.Store(g.bytesAcc.Load())
		cg.lastSampleNs.Store(g.lastSampleNs.Load())
		cg.nowBps.Store(g.nowBps.Load())
		if r.disabled.Load() {
			cg.closeStop()
		}
		c.gen.Store(cg)
	}
	c.disabled.Store(r.disabled.Load())
	return c
}

// SetLimit sets the limit in bytes per second; limitBps <= 0 means unlimited.
// It applies from the next charge, and debt is kept as bytes: what callers
// owe at the old limit is repaid at the new one, also after a time without a
// limit. Callers blocked in Wait are not woken and keep their wait; Reset
// wakes them, drops the debt, restores the full burst and clears the meter.
func (r *Rate) SetLimit(limitBps int64) {
	if r == nil {
		return
	}
	limitBps = max(limitBps, 0)
	r.mu.Lock()
	defer r.mu.Unlock()
	from := r.debtRate
	if limitBps == 0 || from == 0 || limitBps == from {
		// Nothing to convert: a Rate that never had a limit has no debt,
		// and none is charged while unlimited, so the debt stays reckoned
		// at the last limit until there is one again.
		r.rate.Store(limitBps)
		if limitBps > 0 {
			r.debtRate = limitBps
		}
		return
	}
	r.debtRate = limitBps
	// Debt is kept as time, so it is converted to the new limit: debt at the
	// old one would hold back a raised limit, or let a lowered one through
	// early, and a refund converted at the new limit would not cancel the
	// charge it refunds. Charges load tat before the limit and the
	// conversion always changes tat, so a charge that read tat before it
	// fails its CAS and retries. One that lands before it with the new
	// limit has its cost converted too: raising shrinks the debt, so the
	// limit is stored first and that charge is only undercharged; lowering
	// grows it, so the limit is stored after.
	g := r.gen.Load()
	if limitBps > from {
		r.rate.Store(limitBps)
		g.convertDebt(from, limitBps)
	} else {
		g.convertDebt(from, limitBps)
		r.rate.Store(limitBps)
	}
}

// convertDebt converts the debt in g's tat from time at limit from to time
// at limit to, and always changes tat. The Rate's mu must be held.
func (g *generation) convertDebt(from, to int64) {
	if g == nil {
		return
	}
	for {
		prev := g.tat.Load()
		now := nowNs()
		next := prev
		if prev > now {
			next = clampAdd(now, mulDiv(subSat(prev, now), from, to))
		}
		next--
		if next == prev {
			next-- // the conversion left tat as it was
		}
		if g.tat.CompareAndSwap(prev, next) {
			return
		}
	}
}

// Reset is Disable, SetLimit and Enable: it wakes blocked callers, clears the
// debt and the meter, and starts over with a full burst at the new limit.
func (r *Rate) Reset(limitBps int64) {
	if r == nil {
		return
	}
	r.Disable()
	r.SetLimit(limitBps)
	r.Enable()
}

// Limit returns the limit in bytes per second, or 0 if unlimited.
func (r *Rate) Limit() int64 {
	if r == nil {
		return 0
	}
	return r.rate.Load()
}

// Throughput returns the measured throughput in bytes per second over the
// last sampling window of at least one second, closing the current window if
// it is due. It reports 0 while r is disabled.
func (r *Rate) Throughput() int64 {
	if r == nil || r.disabled.Load() {
		return 0
	}
	g := r.gen.Load()
	if g == nil {
		return 0
	}
	g.roll(nowNs())
	return g.nowBps.Load()
}

// Enable undoes Disable, starting over with a full burst and a cleared
// meter. It is a no-op on an enabled Rate.
func (r *Rate) Enable() {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.disabled.Load() {
		// Only a zero value's first generation may be missing; a charge
		// that makes it at the same time keeps its own.
		r.gen.CompareAndSwap(nil, newGeneration(nowNs()))
		return
	}
	// A charge that loaded the old generation lands there, where it no
	// longer limits anyone, or sees the new one and charges it instead.
	r.gen.Store(newGeneration(nowNs()))
	r.disabled.Store(false)
}

// Disable turns r off until Enable: it stops limiting, so traffic charged to
// it passes at once and callers blocked in Wait are woken, and stops
// metering. It does not block traffic; a Hierarchy keeps its other Rates.
func (r *Rate) Disable() {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.disabled.Load() {
		// Disabled before the generation is loaded: a charge that made a zero
		// value's first generation and then saw r enabled made it before this
		// load, so it waits on channels that are closed here.
		r.disabled.Store(true)
		if g := r.gen.Load(); g != nil {
			g.closeStop() // wake sleepers immediately
		}
	}
}

// Refund gives back n bytes charged by Wait or Charge that were not
// transferred, such as the unwritten part of a short write. A refund never
// raises the available burst above two seconds' worth of bytes. The bytes also
// come off the meter: a refund of bytes metered in an earlier sampling window
// is taken off the following windows. While r is unlimited, the refund still
// comes off the debt left from the last limit, which SetLimit keeps as bytes.
// Bytes charged before the last Enable or Reset, which cleared the debt and the
// meter, are refunded all the same; the connection wrappers of this package
// leave them out.
//
// This method takes no receipt: callers must match refunds to limited charges
// when limits change, and avoid refunding charges that Enable or Reset cleared.
// The connection wrappers track both conditions automatically, including
// meter-only refunds for writes charged while unlimited.
func (r *Rate) Refund(n int64) {
	if r == nil || n <= 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if g := r.gen.Load(); g != nil && !r.disabled.Load() {
		r.refund(g, n, true)
	}
}

// refund takes size bytes off g's meter and, with debt, off its debt. r.mu
// must be held, which serializes it with Enable, Disable and SetLimit: the
// caller's check that g is current and the conversion at debtRate then see
// the same generation and limit as the tat they change. Charges still move
// tat concurrently, hence the CAS.
func (r *Rate) refund(g *generation, size int64, debt bool) {
	g.bytesAcc.Add(-size)

	// While temporarily unlimited, old debt is still denominated at the
	// last positive limit. Canceling an old write must remove that debt too.
	rate := r.debtRate
	if !debt || rate <= 0 {
		return
	}

	refund := bytesToNsCeil(size, rate)
	minTat := nowNs() - burstWindowNs
	for {
		prev := g.tat.Load()
		next := max(clampSub(prev, refund), minTat)
		if g.tat.CompareAndSwap(prev, next) {
			return
		}
	}
}

// Wait charges n bytes and blocks until the limit allows them, or until
// ctx is done, when it returns ctx.Err() and keeps the charge; refund bytes
// that were not transferred with Refund. It returns at once if r is nil,
// disabled or unlimited; Disable and Reset wake a blocked Wait early.
func (r *Rate) Wait(ctx context.Context, n int64) error {
	wait, g := r.reserveAt(n, nowNs())
	if wait <= coalesceWaitNs {
		return nil
	}
	if sleepNs(wait, g, ctx.Done()) == canceled {
		return ctx.Err()
	}
	return nil
}

// Ready blocks until r has no debt worth a wait, or until ctx is done, and
// returns how many of n bytes may pass at once: r's credit or, without
// enough, a quantum of about 20 ms of traffic, at least 1 byte. It charges
// nothing; Charge records the bytes that passed.
func (r *Rate) Ready(ctx context.Context, n int64) (int64, error) {
	if n <= 0 {
		return n, nil
	}
	v := levels{one: level{rate: r}}
	return v.ready(ctx, nil, nil, n, false)
}

// Charge records n transferred bytes without waiting, as debt that the next
// Ready or Wait waits for. It returns nil.
func (r *Rate) Charge(n int64) error {
	r.reserveAt(n, nowNs())
	return nil
}

func (r *Rate) reserve(size int64) int64 {
	wait, _ := r.reserveAt(size, nowNs())
	return wait
}

// credit returns r's credit at now, the nanoseconds of traffic it passes at
// once, and its limit; a limit of 0 when r does not limit.
func (r *Rate) credit(now int64) (creditNs, limit int64) {
	if r == nil || r.disabled.Load() {
		return 0, 0
	}
	limit = r.rate.Load()
	if limit <= 0 {
		return 0, 0
	}
	g := r.gen.Load()
	if g == nil {
		return burstWindowNs, limit // a zero value's first charge gets a full burst
	}
	return max(subSat(now, max(g.tat.Load(), now-burstWindowNs)), 0), limit
}

// covers reports whether creditNs of traffic at limit, as credit returns
// them, cover size bytes. It multiplies instead of dividing, as most charges
// are covered.
func covers(size, creditNs, limit int64) bool {
	if limit <= 0 {
		return true
	}
	hi, lo := bits.Mul64(uint64(creditNs), uint64(limit))
	shi, slo := bits.Mul64(uint64(size), uint64(time.Second))
	return hi > shi || hi == shi && lo >= slo
}

// grant returns how much of size creditNs of traffic at limit let through
// in one charge: the bytes they cover or, when fewer, a quantum.
func grant(size, creditNs, limit int64) int64 {
	if covers(size, creditNs, limit) {
		return size
	}
	return min(size, max(mulDiv(creditNs, limit, int64(time.Second)), quantumAt(limit)))
}

// quantumAt returns the grant without credit at limit: about quantumNs of
// traffic, within [minQuantum, maxQuantum], but never more than the burst,
// which a credit at its cap must cover.
func quantumAt(limit int64) int64 {
	q := min(max(mulDiv(limit, quantumNs, int64(time.Second)), minQuantum), maxQuantum)
	return max(min(q, mulDiv(limit, burstWindowNs, int64(time.Second))), 1)
}

// quantum returns r's grant without credit, or maxI64 if r does not limit.
func (r *Rate) quantum() int64 {
	if r == nil || r.disabled.Load() {
		return maxI64
	}
	if limit := r.rate.Load(); limit > 0 {
		return quantumAt(limit)
	}
	return maxI64
}

// allowance returns how much of size r lets through at now in one charge,
// as grant does. It charges nothing; a charge of the allowance that loses a
// race to other chargers waits as any charge does.
func (r *Rate) allowance(size, now int64) int64 {
	creditNs, limit := r.credit(now)
	return grant(size, creditNs, limit)
}

// debtEnd returns when r's debt is repaid, as nowNs, and its generation,
// whose Disable ends a wait for it; 0 when r has no debt at now worth a wait.
func (r *Rate) debtEnd(now int64) (int64, *generation) {
	if r == nil || r.disabled.Load() || r.rate.Load() <= 0 {
		return 0, nil
	}
	g := r.gen.Load()
	if g == nil {
		return 0, nil
	}
	if tat := g.tat.Load(); subSat(tat, now) > coalesceWaitNs {
		return tat, g
	}
	return 0, nil
}

// reserveAt charges size at time now, as returned by nowNs, and returns how
// long the caller must wait and the generation charged, whose Disable ends the
// wait. A Hierarchy passes one now to all its Rates, reading the
// clock once.
func (r *Rate) reserveAt(size, now int64) (int64, *generation) {
	wait, g, _, _ := r.reserveCore(size, now)
	r.notifyReserved(wait)
	return wait, g
}

// reserveCore is reserveAt without the test hook. It returns the generation
// charged, and reports whether the charge was metered there and whether it
// added debt there, for write receipts.
func (r *Rate) reserveCore(size, now int64) (wait int64, g *generation, metered, debt bool) {
	if r == nil || size <= 0 {
		return 0, nil, false, false
	}
	for {
		// The generation before the disabled check, so that a Disable after
		// this charge closes the channel the caller waits on.
		g = r.generation(now)
		if r.disabled.Load() {
			return 0, g, false, false
		}
		if hook := testHooks.charging.Load(); hook != nil {
			(*hook)(r)
		}
		wait, debt = g.charge(r, size, now)
		// g was current throughout the charge, as generations never return.
		// Otherwise an Enable retired g, which no longer limits anyone, and
		// the bytes are charged to the new generation instead.
		if r.gen.Load() == g {
			return wait, g, true, debt
		}
	}
}

// charge adds size to g's meter and, while r has a limit, to g's debt. It
// returns how long the caller must wait and whether it added debt.
func (g *generation) charge(r *Rate, size, now int64) (int64, bool) {
	g.bytesAcc.Add(size)
	g.roll(now)

	rate := r.rate.Load()
	if rate <= 0 {
		return 0, false
	}
	cost := bytesToNsCeil(size, rate)
	minTat := now - burstWindowNs
	for {
		prev := g.tat.Load()
		// The limit again, after tat: see SetLimit.
		if cur := r.rate.Load(); cur != rate {
			if cur <= 0 {
				return 0, false
			}
			rate, cost = cur, bytesToNsCeil(size, cur)
		}
		next := clampAdd(max(prev, minTat), cost)
		if g.tat.CompareAndSwap(prev, next) {
			return max(subSat(next, now), 0), true
		}
		// Retry at once with the same now: reading the clock again would
		// widen the window in which other cores win the race.
		if r.disabled.Load() {
			return 0, false
		}
	}
}

func (r *Rate) notifyReserved(wait int64) {
	if wait > 0 {
		if hook := testHooks.reserved.Load(); hook != nil {
			(*hook)(r)
		}
	}
}

// testHooks are read by every charge, so they keep a cache line of their own.
var testHooks struct {
	_ cacheLinePad
	// charging, if set, is called with a Rate at the start of a charge,
	// after the charger has loaded its generation, so that tests can Reset
	// the Rate there.
	charging atomic.Pointer[func(*Rate)]
	// reserved, if set, is called with a Rate whose charge must wait,
	// between the charge and the wait, so that tests can Disable the Rate
	// there.
	reserved atomic.Pointer[func(*Rate)]
	_        cacheLinePad
}

// stopCh returns a channel that Disable closes.
func (r *Rate) stopCh() <-chan struct{} {
	if r == nil {
		return nil
	}
	return r.gen.Load().waitCh(0)
}

// roll closes g's sampling window once it is at least a second old.
func (g *generation) roll(now int64) {
	last := g.lastSampleNs.Load()
	if now-last < sampleIntervalNs {
		return
	}
	if !g.lastSampleNs.CompareAndSwap(last, now) {
		return
	}

	bytes := g.bytesAcc.Swap(0)
	if bytes < 0 {
		// Refunds of bytes metered in an earlier window, such as a canceled
		// write, outweigh this window's charges: carry the rest forward.
		// Charges and refunds share one counter, so a refund never lands
		// in an earlier window than its charge.
		g.bytesAcc.Add(bytes)
		bytes = 0
	}
	dt := now - last
	if dt <= 0 {
		return
	}
	g.nowBps.Store(bytesPerSec(bytes, dt))
}
