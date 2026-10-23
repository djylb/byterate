package byterate

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// burstWindowNs is the default burst allowance: after an idle period a Rate
// lets up to two seconds' worth of bytes through without waiting.
const burstWindowNs = int64(2 * time.Second)

// Rate is a bytes-per-second limiter (GCRA, a token-bucket variant) with a
// built-in throughput meter. A limit <= 0 means unlimited, and after an idle
// period a burst of bytes passes without waiting, two seconds' worth unless
// SetBurst sets another. Disable turns the limit off for a while, waking
// the callers waiting for it; unlimited and disabled alike, a Rate passes
// everything and still meters it. The meter counts up to 2^63-1 bytes in a
// sampling window.
//
// All methods are safe for concurrent use and on a nil *Rate, which never
// limits. The zero value is an enabled, unlimited Rate, ready for SetLimit. A
// Rate must not be copied after first use; use Clone.
type Rate struct {
	// The fields fill the size class's cache line exactly, so that those
	// every charge reads share it with nothing that others write.
	gen      atomic.Pointer[generation] // nil until a zero value's first charge or Enable
	rate     atomic.Int64               // bytes/s, <=0 => unlimited
	burst    atomic.Int64               // ns of traffic passed at once after an idle period, 0 for burstWindowNs
	debtRate int64                      // protected by mu: the limit tat's debt is reckoned at, the last one above 0
	mu       sync.Mutex                 // serializes Enable, Disable, SetLimit, Clone and refunds; charges take no lock
	disabled atomic.Bool                // written under mu
	_        [cacheLineSize - 44]byte
}

// NewRate returns a Rate limited to limitBps bytes per second with a full
// burst available. A limitBps <= 0 means unlimited; such a Rate takes no
// more memory than its own until something is charged to it.
func NewRate(limitBps int64) *Rate {
	limitBps = max(limitBps, 0)
	r := &Rate{debtRate: limitBps}
	r.rate.Store(limitBps)
	if limitBps > 0 {
		r.gen.Store(newGeneration(nowNs(), burstWindowNs)) // the full burst starts now
	}
	return r
}

// generation returns r's generation, making a zero value's first one, with a
// full burst, at now.
func (r *Rate) generation(now int64) *generation {
	if g := r.gen.Load(); g != nil {
		return g
	}
	if g := newGeneration(now, r.burstNs()); r.gen.CompareAndSwap(nil, g) {
		return g
	}
	return r.gen.Load() // another charge's, or Enable's
}

// Clone returns an independent copy of r's limit, burst, debt, enabled or
// disabled state and meter. Callers blocked on r are not affected by the copy.
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
	c.burst.Store(r.burst.Load())
	if g := r.gen.Load(); g != nil {
		cg := &generation{}
		cg.tat.Store(g.tat.Load())
		cg.total.Store(g.total.Load())
		cg.meter.copyFrom(&g.meter)
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
			next = addSat(now, mulDiv(subSat(prev, now), from, to))
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

// Throughput returns the bytes charged to r, refunds taken off, in the last
// whole second: the sampling windows are one-second slices of a clock that
// every Rate and Counter shares, and Throughput reports the one before the
// current one, so it lags by one to two seconds. What it reports depends only on
// when the bytes were charged, not on how often or by how many callers it is
// called. A refund of bytes charged in an earlier window comes off the
// following ones. r meters whether it is limited, unlimited or disabled;
// Enable and Reset clear the meter.
func (r *Rate) Throughput() int64 {
	if r == nil {
		return 0
	}
	g := r.gen.Load()
	if g == nil {
		return 0
	}
	return g.throughput(nowNs())
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
		r.gen.CompareAndSwap(nil, newGeneration(nowNs(), r.burstNs()))
		return
	}
	// A charge that loaded the old generation lands there, where it no
	// longer limits anyone, or sees the new one and charges it instead.
	r.gen.Store(newGeneration(nowNs(), r.burstNs()))
	r.disabled.Store(false)
}

// Disable turns r's limit off until Enable: traffic charged to it passes at
// once, as with no limit, and callers blocked in Wait are woken. It still
// meters, and keeps the limit for Enable. It does not block traffic; a
// hierarchy keeps its other Rates.
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

// Refund gives back n bytes charged by Wait, TryWait or Charge that were not
// transferred, such as the unwritten part of a short write. A refund never
// raises the available burst above r's burst. The bytes also come off the
// meter: a refund of bytes metered in an earlier sampling window is taken off
// the following windows. While r is unlimited, the refund still comes off the
// debt left from the last limit, which SetLimit keeps as bytes; while it is
// disabled, only off the meter.
//
// Refund takes no receipt, so it cannot tell which charge it gives back:
// bytes charged before the last Enable or Reset, which cleared the debt and
// the meter, are refunded all the same, and so is debt for bytes charged
// while r was unlimited or disabled, which added none. Refund only bytes of
// a charge that r took since the last Enable or Reset and while it had the
// same kind of limit. The connection wrappers, hierarchies, Switches,
// Budgets and Groups keep a receipt of each charge instead and refund
// exactly what it took, in the generation it took it.
func (r *Rate) Refund(n int64) {
	if r == nil || n <= 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if g := r.gen.Load(); g != nil {
		r.refund(g, n, !r.disabled.Load())
	}
}

// refund takes size bytes off g's meter and, with debt, off its debt. r.mu
// must be held, which serializes it with Enable, Disable and SetLimit: the
// caller's check that g is current and the conversion at debtRate then see
// the same generation and limit as the tat they change. Charges still move
// tat concurrently, hence the CAS.
func (r *Rate) refund(g *generation, size int64, debt bool) {
	now := nowNs()
	g.count(-size, now)

	// While temporarily unlimited, old debt is still denominated at the
	// last positive limit. Canceling an old write must remove that debt too.
	rate := r.debtRate
	if !debt || rate <= 0 {
		return
	}

	refund := bytesToNsCeil(size, rate)
	minTat := subSat(now, r.burstNs())
	for {
		prev := g.tat.Load()
		next := max(subSat(prev, refund), minTat)
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
	// A whole charge, as grant makes it, without its cuts and receipt.
	wait, g := r.reserveAt(n, nowNs())
	if wait > coalesceWaitNs && (*waitTimer)(nil).sleep(wait, g, ctx.Done(), nil) == canceled {
		return ctx.Err()
	}
	return nil
}

// TryWait charges n bytes if r lets them through without waiting, as Wait
// would, and otherwise charges nothing and returns ErrWouldWait. A nil,
// disabled or unlimited r lets everything through; a limited one never
// lets through more than its burst at once, so set the burst to at least
// the largest datagram.
func (r *Rate) TryWait(n int64) error {
	if n <= 0 || r.tryTake(n, nowNs(), nil) {
		return nil
	}
	return ErrWouldWait
}

// Ready blocks until r has no debt worth a wait, or until ctx is done, and
// returns how many of n bytes may pass at once: r's credit or, without
// enough, a quantum of about 20 ms of traffic, at least 1 byte. It charges
// nothing; Charge records the bytes that passed.
func (r *Rate) Ready(ctx context.Context, n int64) (int64, error) {
	if n <= 0 {
		return n, nil
	}
	return r.ready(ctx, nil, n, false)
}

// Charge records n transferred bytes without waiting, as debt that the next
// Ready or Wait waits for. It returns nil.
func (r *Rate) Charge(n int64) error {
	r.reserveAt(n, nowNs())
	return nil
}

// grant is levels.wait for r alone, without a waitSet: it charges r for a
// transfer of size bytes, or unless whole for r's grant of them, records the
// charge in rc, and waits in t until r allows it or ctx is done. It returns
// the bytes charged.
func (r *Rate) grant(ctx context.Context, t *waitTimer, size int64, whole bool, rc *receipt) (int64, error) {
	now := nowNs()
	if !whole {
		size = r.allowance(size, now)
	}
	wait, g := r.take(size, now, rc)
	if !whole && wait > coalesceWaitNs {
		if q, ok := requeue(size, r.quantum()); ok {
			rc.refund(size)
			size = q
			wait, g = r.take(size, nowNs(), rc)
		}
	}
	if wait > coalesceWaitNs && t.sleep(wait, g, ctx.Done(), nil) == canceled {
		return size, ctx.Err()
	}
	return size, nil
}

// ready is levels.ready for r alone, without a waitSet: it waits in t until
// r has no debt worth a wait and returns how much of size a transfer may
// take, all of it when whole.
func (r *Rate) ready(ctx context.Context, t *waitTimer, size int64, whole bool) (int64, error) {
	now := nowNs()
	if end, g := r.debtEnd(now); end != 0 {
		if t.sleep(subSat(end, now), g, ctx.Done(), nil) == canceled {
			return 0, ctx.Err()
		}
		now = nowNs()
	}
	if whole {
		return size, nil
	}
	return r.allowance(size, now), nil
}

// take charges size to r at now, records the charge in rc unless rc is nil,
// and returns how long the caller must wait and the generation charged,
// whose Disable ends the wait. It takes no lock.
func (r *Rate) take(size, now int64, rc *receipt) (int64, *generation) {
	wait, g, metered, debt := r.reserveCore(size, now)
	if metered && rc != nil {
		rc.addRate(r, g, debt)
	}
	r.notifyReserved(wait)
	return wait, g
}

// wouldWait reports whether a charge of size bytes at now would wait, as
// tryTake would refuse it. It charges nothing.
func (r *Rate) wouldWait(size, now int64) bool {
	if r == nil || r.disabled.Load() {
		return false
	}
	limit := r.rate.Load()
	if limit <= 0 {
		return false
	}
	tat := subSat(now, r.burstNs()) // a zero value's first charge gets a full burst
	if g := r.gen.Load(); g != nil {
		tat = max(tat, g.tat.Load())
	}
	return subSat(addSat(tat, bytesToNsCeil(size, limit)), now) > coalesceWaitNs
}

// tryTake charges size to r at now, recording the charge in rc unless rc is
// nil, unless the charge would wait, and reports whether it charged. It
// takes no lock.
func (r *Rate) tryTake(size, now int64, rc *receipt) bool {
	if r == nil || size <= 0 {
		return true
	}
	for {
		g := r.generation(now)
		debt := false
		if r.disabled.Load() {
			g.count(size, now)
		} else {
			var ok bool
			if ok, debt = g.tryCharge(r, size, now); !ok {
				return false
			}
		}
		// As in reserveCore: a charge to a generation that an Enable
		// retired meanwhile is made again on the new one.
		if r.gen.Load() == g {
			if rc != nil {
				rc.addRate(r, g, debt)
			}
			return true
		}
	}
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
	burst := r.burstNs()
	g := r.gen.Load()
	if g == nil {
		return burst, limit // a zero value's first charge gets a full burst
	}
	return max(subSat(now, max(g.tat.Load(), subSat(now, burst))), 0), limit
}

// grant returns how much of size creditNs of traffic at limit, with a burst
// of burstNs, let through in one charge: the bytes they cover or, when fewer,
// a quantum.
func grant(size, creditNs, limit, burstNs int64) int64 {
	if covers(size, creditNs, limit) {
		return size
	}
	return min(size, max(mulDiv(creditNs, limit, int64(time.Second)), quantumAt(limit, burstNs)))
}

// quantumAt returns the grant without credit at limit: about quantumNs of
// traffic, within [minQuantum, maxQuantum], but never more than the burst of
// burstNs, which a credit at its cap must cover.
func quantumAt(limit, burstNs int64) int64 {
	q := min(max(mulDiv(limit, quantumNs, int64(time.Second)), minQuantum), maxQuantum)
	return max(min(q, mulDiv(limit, burstNs, int64(time.Second))), 1)
}

// quantum returns r's grant without credit, or maxI64 if r does not limit.
func (r *Rate) quantum() int64 {
	if r == nil || r.disabled.Load() {
		return maxI64
	}
	if limit := r.rate.Load(); limit > 0 {
		return quantumAt(limit, r.burstNs())
	}
	return maxI64
}

// allowance returns how much of size r lets through at now in one charge,
// as grant does. It charges nothing; a charge of the allowance that loses a
// race to other chargers waits as any charge does.
func (r *Rate) allowance(size, now int64) int64 {
	creditNs, limit := r.credit(now)
	return grant(size, creditNs, limit, r.burstNs())
}

// SetBurst sets how much traffic r lets through at once after an idle
// period: d's worth at the limit, two seconds' worth by default and for a
// d <= 0. A smaller burst smooths traffic out, a larger one lets bursts
// through; it also bounds the bytes a connection passes per grant. It
// applies from the next charge.
func (r *Rate) SetBurst(d time.Duration) {
	if r == nil {
		return
	}
	r.burst.Store(max(int64(d), 0))
}

// Burst returns how much traffic r lets through at once after an idle
// period.
func (r *Rate) Burst() time.Duration {
	return time.Duration(r.burstNs())
}

// burstNs returns r's burst in nanoseconds of traffic.
func (r *Rate) burstNs() int64 {
	if r != nil {
		if b := r.burst.Load(); b > 0 {
			return b
		}
	}
	return burstWindowNs
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
// wait. A hierarchy passes one now to all its Rates, reading the
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
			g.count(size, now)
			if r.gen.Load() == g {
				return 0, g, true, false
			}
			continue
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
	g.count(size, now)

	rate := r.rate.Load()
	if rate <= 0 {
		return 0, false
	}
	cost := bytesToNsCeil(size, rate)
	minTat := subSat(now, r.burstNs())
	for {
		prev := g.tat.Load()
		// The limit again, after tat: see SetLimit.
		if cur := r.rate.Load(); cur != rate {
			if cur <= 0 {
				return 0, false
			}
			rate, cost = cur, bytesToNsCeil(size, cur)
		}
		next := addSat(max(prev, minTat), cost)
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

// tryCharge is charge for a charge that may not wait: it charges size
// unless that would make the caller wait, and reports whether it charged and
// whether it added debt.
func (g *generation) tryCharge(r *Rate, size, now int64) (ok, debt bool) {
	if rate := r.rate.Load(); rate > 0 {
		cost := bytesToNsCeil(size, rate)
		minTat := subSat(now, r.burstNs())
		for !debt {
			prev := g.tat.Load()
			if cur := r.rate.Load(); cur != rate { // after tat: see SetLimit
				if cur <= 0 {
					break
				}
				rate, cost = cur, bytesToNsCeil(size, cur)
			}
			next := addSat(max(prev, minTat), cost)
			if subSat(next, now) > coalesceWaitNs {
				return false, false
			}
			debt = g.tat.CompareAndSwap(prev, next)
			if !debt && r.disabled.Load() {
				break
			}
		}
	}
	g.count(size, now)
	return true, debt
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
