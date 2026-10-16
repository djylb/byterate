package byterate

import (
	"context"
	"math/rand/v2"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// burstWindowNs is the burst allowance: after an idle period a Rate lets up
// to two seconds' worth of bytes through without waiting.
const burstWindowNs = int64(2 * time.Second)

// generation is the state of a Rate from one Start that clears it to the
// next: its debt, its meter and the channels that Stop closes. Start
// replaces the generation instead of clearing it in place, and a new one is
// complete before it is published, so a charge, a refund and a sampling
// window each act on the one generation they loaded, and one that a Start
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

// stopSet holds the channels of a generation that Stop closes. Waiters
// spread over them, as a wait locks the channel it selects on, and one
// channel for every waiter of a shared Rate would serialize them. A
// generation makes its set for its first waiter, sized for the processors,
// and each channel for the first waiter keyed to it, so that a Rate that
// never makes callers wait pays for none.
type stopSet struct {
	shards []atomic.Pointer[stopChan] // a power of two of them
}

func newStopSet() *stopSet {
	n := 8
	for n < 8*runtime.GOMAXPROCS(0) && n < 256 {
		n *= 2
	}
	return &stopSet{shards: make([]atomic.Pointer[stopChan], n)}
}

// stopChan is one of the channels of a generation that Stop closes.
type stopChan struct{ ch chan struct{} }

var (
	// stoppedSet is left by Stop in a generation that no waiter had given a
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

// waitCh returns the channel of g that Stop closes for the waiter keyed by
// key, or nil for no generation.
func (g *generation) waitCh(key uint32) <-chan struct{} {
	if g == nil {
		return nil
	}
	set := g.stop.Load()
	if set == nil {
		set = newStopSet()
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
			c = s.Load() // another waiter's, or closedStop
		}
	}
	return c.ch
}

// closeStop closes g's channels, waking its waiters, and leaves stoppedSet
// and closedStop for waiters to come. It runs once, for Stop.
func (g *generation) closeStop() {
	set := g.stop.Swap(stoppedSet)
	if set == nil {
		return
	}
	for i := range set.shards {
		if c := set.shards[i].Swap(closedStop); c != nil {
			close(c.ch)
		}
	}
}

// Rate is a bytes-per-second limiter (GCRA, a token-bucket variant) with a
// built-in throughput meter. A limit <= 0 means unlimited, and after an idle
// period up to two seconds' worth of bytes pass without waiting. The meter
// counts up to 2^63-1 bytes in a sampling window.
//
// All methods are safe for concurrent use and on a nil *Rate, which never
// limits. The zero value is an unlimited Rate that is not started; call
// ResetLimit, or SetLimit and Start, before use. A Rate must not be copied
// after first use; use Clone.
//
// It fills its size class's cache line exactly, so that the fields every
// charge reads share it with nothing that others write.
type Rate struct {
	gen      atomic.Pointer[generation] // nil until the first Start of a zero value
	rate     atomic.Int64               // bytes/s, <=0 => unlimited
	debtRate int64                      // protected by mu: the limit tat's debt is reckoned at, the last one above 0
	mu       sync.Mutex                 // serializes Start, Stop, SetLimit, Clone and refunds; charges take no lock
	enabled  atomic.Bool
	stopped  bool // protected by mu
	_        [cacheLineSize - 37]byte
}

// NewRate returns a started Rate limited to limitBps bytes per second with a
// full burst available. A limitBps <= 0 means unlimited.
func NewRate(limitBps int64) *Rate {
	limitBps = max(limitBps, 0)
	r := &Rate{debtRate: limitBps}
	r.rate.Store(limitBps)
	r.gen.Store(newGeneration(nowNs())) // a full burst, also for a limit set later
	r.enabled.Store(true)
	return r
}

// Clone returns an independent copy of r's limit, debt, started or stopped
// state and meter. Callers blocked on r are not affected by the copy.
func (r *Rate) Clone() *Rate {
	if r == nil {
		return nil
	}
	// Under r.mu, so that the started or stopped state is not torn by a
	// concurrent Start or Stop: a clone with a closed stop channel that is
	// enabled would charge without ever waiting.
	r.mu.Lock()
	defer r.mu.Unlock()
	c := &Rate{debtRate: r.debtRate, stopped: r.stopped}
	c.rate.Store(r.rate.Load())
	if g := r.gen.Load(); g != nil {
		cg := &generation{}
		cg.tat.Store(g.tat.Load())
		cg.bytesAcc.Store(g.bytesAcc.Load())
		cg.lastSampleNs.Store(g.lastSampleNs.Load())
		cg.nowBps.Store(g.nowBps.Load())
		if r.stopped {
			cg.closeStop()
		}
		c.gen.Store(cg)
	}
	c.enabled.Store(r.enabled.Load())
	return c
}

// SetLimit sets the limit in bytes per second; limitBps <= 0 means unlimited.
// It applies from the next charge, and debt is kept as bytes: what callers
// owe at the old limit is repaid at the new one, also after a time without a
// limit. Callers blocked in Get are not woken and keep their wait; ResetLimit
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

// ResetLimit is Stop, SetLimit and Start: it wakes blocked callers, clears the
// debt and the meter, and restarts with a full burst at the new limit.
func (r *Rate) ResetLimit(limitBps int64) {
	if r == nil {
		return
	}
	r.Stop()
	r.SetLimit(limitBps)
	r.Start()
}

// Limit returns the limit in bytes per second, or 0 if unlimited.
func (r *Rate) Limit() int64 {
	if r == nil {
		return 0
	}
	return r.rate.Load()
}

// Now returns the measured throughput in bytes per second over the last
// sampling window of at least one second, closing the current window if it is
// due. It reports 0 while r is stopped.
func (r *Rate) Now() int64 {
	if r == nil || !r.enabled.Load() {
		return 0
	}
	g := r.gen.Load()
	if g == nil {
		return 0
	}
	g.roll(nowNs())
	return g.nowBps.Load()
}

// Start enables r. A stopped or never-started Rate also gets a full burst and
// a cleared meter; Start is a no-op on a running Rate.
func (r *Rate) Start() {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped || !r.enabled.Load() || r.gen.Load() == nil {
		// A charge that loaded the old generation lands there, where it no
		// longer limits anyone, or sees the new one and charges it instead.
		r.gen.Store(newGeneration(nowNs()))
		r.stopped = false
	}
	r.enabled.Store(true)
}

// Stop disables limiting and metering until Start: Get returns immediately
// and callers blocked in Get are woken.
func (r *Rate) Stop() {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.enabled.Store(false)
	if !r.stopped {
		if g := r.gen.Load(); g != nil {
			g.closeStop() // wake sleepers immediately
		}
		r.stopped = true
	}
}

// ReturnBucket refunds size bytes previously charged by Get, for example the
// unwritten part of a short write. A refund never raises the available burst
// above two seconds' worth of bytes. The bytes also come off the meter: a
// refund of bytes metered in an earlier sampling window is taken off the
// following windows. While r is unlimited, the refund still comes off the
// debt left from the last limit, which SetLimit keeps as bytes. Bytes
// charged before the last Start, which cleared the debt and the meter, are
// refunded all the same; the connection wrappers of this package leave them
// out.
//
// This method takes no receipt: callers must match refunds to limited
// charges when limits change, and avoid refunding charges cleared by Start.
// The connection wrappers track both conditions automatically, including
// meter-only refunds for writes charged while unlimited.
func (r *Rate) ReturnBucket(size int64) {
	if r == nil || size <= 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.enabled.Load() {
		r.refund(r.gen.Load(), size, true)
	}
}

// refund takes size bytes off g's meter and, with debt, off its debt. r.mu
// must be held, which serializes it with Start, Stop and SetLimit: the
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

// Get charges size bytes and blocks until the limit allows them. It returns
// immediately if r is nil, stopped or unlimited; Stop and ResetLimit wake a
// blocked Get early. Use GetContext to abandon a wait.
func (r *Rate) Get(size int64) {
	wait, g := r.reserveAt(size, nowNs())
	if wait <= coalesceWaitNs {
		return
	}
	sleepNs(wait, g.waitCh(rand.Uint32()), nil)
}

// GetContext is like Get but returns ctx.Err() if ctx is done before the wait
// ends. The charge is kept either way; refund unused bytes with ReturnBucket.
func (r *Rate) GetContext(ctx context.Context, size int64) error {
	wait, g := r.reserveAt(size, nowNs())
	if wait <= coalesceWaitNs {
		return nil
	}
	if !sleepNs(wait, g.waitCh(rand.Uint32()), ctx.Done()) {
		return ctx.Err()
	}
	return nil
}

// MarshalJSON encodes r as {"NowRate":bps,"Limit":bps}, where NowRate is Now
// and Limit is Limit. Like Now, it closes the current sampling window if it is
// due, so an idle Rate reports 0.
func (r *Rate) MarshalJSON() ([]byte, error) {
	if r == nil {
		return []byte("null"), nil
	}
	// Encoded by hand so that importing the package does not link
	// encoding/json, which adds about 150 KB to a binary.
	b := append(make([]byte, 0, 48), `{"NowRate":`...)
	b = strconv.AppendInt(b, r.Now(), 10)
	b = append(b, `,"Limit":`...)
	b = strconv.AppendInt(b, r.Limit(), 10)
	return append(b, '}'), nil
}

func (r *Rate) reserve(size int64) int64 {
	wait, _ := r.reserveAt(size, nowNs())
	return wait
}

// reserveAt charges size at time now, as returned by nowNs, and returns how
// long the caller must wait and the generation charged, whose Stop ends the
// wait. A HierarchicalLimiter passes one now to all its Rates, reading the
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
		// The generation before enabled, so that a Stop after this charge
		// closes the channel the caller waits on.
		g = r.gen.Load()
		if g == nil || !r.enabled.Load() {
			return 0, g, false, false
		}
		if hook := testHooks.charging.Load(); hook != nil {
			(*hook)(r)
		}
		wait, debt = g.charge(r, size, now)
		// g was current throughout the charge, as generations never return.
		// Otherwise a Start retired g, which no longer limits anyone, and
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
		if !r.enabled.Load() {
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
	// after the charger has loaded its generation, so that tests can restart
	// the Rate there.
	charging atomic.Pointer[func(*Rate)]
	// reserved, if set, is called with a Rate whose charge must wait,
	// between the charge and the wait, so that tests can stop the Rate there.
	reserved atomic.Pointer[func(*Rate)]
	_        cacheLinePad
}

// stopCh returns a channel that Stop closes.
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
