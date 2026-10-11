package byterate

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// burstWindowNs is the burst allowance: after an idle period a Rate lets up
// to two seconds' worth of bytes through without waiting.
const burstWindowNs = int64(2 * time.Second)

// stopSignal is one generation of a Rate: Start makes a new one when it
// clears the Rate, and Stop closes its channel. A charge waits on the
// channel of the generation it charged, and write receipts refer to it, so
// a refund skips a Rate restarted since without the charge taking a lock.
type stopSignal struct {
	ch    chan struct{}
	ready atomic.Bool // set once Start has reset tat for this generation
}

func newStopSignal() *stopSignal {
	s := &stopSignal{ch: make(chan struct{})}
	s.ready.Store(true)
	return s
}

// stopCh returns s's channel, or nil for no generation.
func (s *stopSignal) stopCh() <-chan struct{} {
	if s == nil {
		return nil
	}
	return s.ch
}

// Rate is a bytes-per-second limiter (GCRA, a token-bucket variant) with a
// built-in throughput meter. A limit <= 0 means unlimited, and after an idle
// period up to two seconds' worth of bytes pass without waiting.
//
// All methods are safe for concurrent use and on a nil *Rate, which never
// limits. The zero value is an unlimited Rate that is not started; call
// ResetLimit, or SetLimit and Start, before use. A Rate must not be copied
// after first use; use Clone.
type Rate struct {
	// Keep the frequently read configuration off the counters' cache line.
	stop         atomic.Pointer[stopSignal]
	rate         atomic.Int64 // bytes/s, <=0 => unlimited
	lastSampleNs atomic.Int64 // start of the sampling window
	enabled      atomic.Bool
	stopped      bool // protected by mu; fills otherwise unused alignment space
	reset        bool // protected by mu: Start has reset tat, to resetTat
	_            cacheLinePad
	// Written by every charge, isolated from configuration and lifecycle state.
	tat      atomic.Int64 // theoretical arrival time (ns since epoch), can be negative
	bytesAcc atomic.Int64 // bytes charged in the current sampling window, net of refunds
	_        cacheLinePad
	nowBps   atomic.Int64 // throughput of the last sampling window, bytes/s
	debtRate int64        // the limit tat's debt is reckoned at: the last one above 0
	resetTat int64        // protected by mu: the tat Start last reset to
	mu       sync.Mutex   // serializes Start, Stop, SetLimit and refunds; charges take no lock
}

// NewRate returns a started Rate limited to limitBps bytes per second with a
// full burst available. A limitBps <= 0 means unlimited.
func NewRate(limitBps int64) *Rate {
	if limitBps <= 0 {
		limitBps = 0
	}
	r := &Rate{debtRate: limitBps}
	r.enabled.Store(true)
	r.rate.Store(limitBps)
	r.stop.Store(newStopSignal())

	now := nowNs()
	r.tat.Store(now - burstWindowNs) // full burst, also for a limit set later

	r.lastSampleNs.Store(now)
	r.nowBps.Store(0)
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
	stopped := r.stopped

	cloned := &Rate{debtRate: r.debtRate, stopped: stopped, reset: r.reset, resetTat: r.resetTat}
	cloned.rate.Store(r.rate.Load())
	cloned.tat.Store(r.tat.Load())
	cloned.enabled.Store(r.enabled.Load())
	cloned.bytesAcc.Store(r.bytesAcc.Load())
	cloned.lastSampleNs.Store(r.lastSampleNs.Load())
	cloned.nowBps.Store(r.nowBps.Load())

	signal := newStopSignal()
	if stopped {
		close(signal.ch)
	}
	cloned.stop.Store(signal)
	return cloned
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
	if limitBps > from {
		r.rate.Store(limitBps)
		r.convertDebt(from, limitBps)
	} else {
		r.convertDebt(from, limitBps)
		r.rate.Store(limitBps)
	}
}

// convertDebt converts the debt in tat from time at limit from to time at
// limit to, and always changes tat. r.mu must be held.
func (r *Rate) convertDebt(from, to int64) {
	for {
		prev := r.tat.Load()
		now := nowNs()
		next := prev
		if prev > now {
			next = clampAdd(now, mulDiv(prev-now, from, to))
		}
		if r.tat.CompareAndSwap(prev, next-1) {
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
		return 0 // also when a charge that raced with Stop is still metered
	}
	r.updateRateWithNow(nowNs())
	return r.nowBps.Load()
}

// Start enables r. A stopped or never-started Rate also gets a full burst and
// a cleared meter; Start is a no-op on a running Rate.
func (r *Rate) Start() {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	prevEnabled := r.enabled.Load()
	needReset := r.stopped || !prevEnabled || r.stop.Load() == nil

	if needReset {
		// The new generation is stored before tat is reset, and is ready
		// after: a charge that sees the old one after loading tat lands
		// before the reset, which clears it, and one that sees a ready one
		// lands after. A Rate that was not stopped has no waiters on the old
		// channel, since it was not enabled.
		gen := &stopSignal{ch: make(chan struct{})}
		r.stop.Store(gen)
		r.stopped = false
		now := nowNs()
		// Every reset stores a new tat, also within one tick of a coarse
		// clock, so that a charge that loaded tat before it fails its CAS.
		tat := now - burstWindowNs // full burst on (re)enable
		if r.reset && tat <= r.resetTat {
			tat = r.resetTat + 1
		}
		r.reset, r.resetTat = true, tat
		r.tat.Store(tat)
		r.bytesAcc.Store(0)
		r.lastSampleNs.Store(now)
		r.nowBps.Store(0)
		gen.ready.Store(true)
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
		if s := r.stop.Load(); s != nil && s.ch != nil {
			close(s.ch) // wake sleepers immediately
		}
		r.stopped = true
	}
	r.bytesAcc.Store(0)
	r.nowBps.Store(0)
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
	r.refund(size)
}

// refund refunds size bytes. r.mu must be held, which serializes it with
// Start, Stop and SetLimit: a receipt's generation check and the
// conversion at debtRate then see the same start and the same limit as the
// tat they change. Charges still move tat concurrently, hence the CAS.
func (r *Rate) refund(size int64) {
	if !r.enabled.Load() {
		return
	}

	r.bytesAcc.Add(-size)

	// While temporarily unlimited, old debt is still denominated at the
	// last positive limit. Canceling an old write must remove that debt too.
	rate := r.debtRate
	if rate <= 0 {
		return
	}

	refund := bytesToNsCeil(size, rate)
	now := nowNs()
	minTat := now - burstWindowNs

	for {
		prev := r.tat.Load()
		next := max(clampSub(prev, refund), minTat)
		if r.tat.CompareAndSwap(prev, next) {
			return
		}
	}
}

// Get charges size bytes and blocks until the limit allows them. It returns
// immediately if r is nil, stopped or unlimited; Stop and ResetLimit wake a
// blocked Get early. Use GetContext to abandon a wait.
func (r *Rate) Get(size int64) {
	wait, stopCh := r.reserveAt(size, nowNs())
	if wait <= coalesceWaitNs {
		return
	}
	sleepNs(wait, stopCh, nil)
}

// GetContext is like Get but returns ctx.Err() if ctx is done before the wait
// ends. The charge is kept either way; refund unused bytes with ReturnBucket.
func (r *Rate) GetContext(ctx context.Context, size int64) error {
	wait, stopCh := r.reserveAt(size, nowNs())
	if wait <= coalesceWaitNs {
		return nil
	}
	if !sleepNs(wait, stopCh, ctx.Done()) {
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
// long the caller must wait and the channel that ends the wait: that of the
// generation charged, which Stop closes. A HierarchicalLimiter passes one now
// to all its Rates, reading the clock once.
func (r *Rate) reserveAt(size, now int64) (int64, <-chan struct{}) {
	wait, gen, _, _ := r.reserveCore(size, now)
	r.notifyReserved(wait)
	return wait, gen.stopCh()
}

// reserveCore is reserveAt without the test hook. It returns the generation
// charged, and reports whether the charge was metered and whether it added
// debt, for write receipts. Debt is charged in exactly that generation; the
// meter of a charge racing with Start may count it on either side.
func (r *Rate) reserveCore(size, now int64) (wait int64, gen *stopSignal, metered, debt bool) {
	if r == nil || size <= 0 {
		return 0, nil, false, false
	}
	// The generation before enabled, so that a Stop after this charge
	// closes the channel the caller waits on.
	gen = r.stop.Load()
	if !r.enabled.Load() {
		return 0, gen, false, false
	}
	if hook := testHookCharging.Load(); hook != nil {
		(*hook)(r)
	}

	r.bytesAcc.Add(size)
	r.updateRateWithNow(now)

	rate := r.rate.Load()
	if rate <= 0 {
		return 0, gen, true, false
	}
	cost := bytesToNsCeil(size, rate)
	minTat := now - burstWindowNs
	for {
		// A charge must wait in, and be refunded to, the generation whose
		// tat it lands on, so it reads the generation before tat and checks
		// it after: a ready generation has had its reset, and an unchanged
		// one has not had the next. While a Start is resetting tat, wait for
		// it on mu.
		gen = r.stop.Load()
		if gen == nil || !gen.ready.Load() {
			r.mu.Lock()
			r.mu.Unlock()
			if !r.enabled.Load() {
				return 0, r.stop.Load(), true, false
			}
			continue
		}
		prev := r.tat.Load()
		if r.stop.Load() != gen {
			continue
		}
		// The limit again, after tat: see SetLimit.
		if cur := r.rate.Load(); cur != rate {
			if cur <= 0 {
				return 0, gen, true, false
			}
			rate, cost = cur, bytesToNsCeil(size, cur)
		}
		next := clampAdd(max(prev, minTat), cost)
		if r.tat.CompareAndSwap(prev, next) {
			return max(next-now, 0), gen, true, true
		}
		// Retry at once with the same now: reading the clock again would
		// widen the window in which other cores win the race.
		if !r.enabled.Load() {
			return 0, gen, true, false
		}
	}
}

func (r *Rate) notifyReserved(wait int64) {
	if wait > 0 {
		if hook := testHookReserved.Load(); hook != nil {
			(*hook)(r)
		}
	}
}

// testHookCharging, if set, is called with a Rate at the start of a charge,
// so that tests can restart the Rate there.
var testHookCharging atomic.Pointer[func(*Rate)]

// testHookReserved, if set, is called with a Rate whose charge must wait,
// between the charge and the wait, so that tests can stop the Rate there.
var testHookReserved atomic.Pointer[func(*Rate)]

// stopCh returns the channel that Stop closes.
func (r *Rate) stopCh() <-chan struct{} {
	if r == nil {
		return nil
	}
	return r.stop.Load().stopCh()
}

func (r *Rate) updateRateWithNow(now int64) {
	last := r.lastSampleNs.Load()
	if now-last < sampleIntervalNs {
		return
	}
	if !r.lastSampleNs.CompareAndSwap(last, now) {
		return
	}

	bytes := r.bytesAcc.Swap(0)
	if bytes < 0 {
		// Refunds of bytes metered in an earlier window, such as a canceled
		// write, outweigh this window's charges: carry the rest forward.
		// Charges and refunds share one counter, so a refund never lands
		// in an earlier window than its charge.
		r.bytesAcc.Add(bytes)
		bytes = 0
	}
	dt := now - last
	if dt <= 0 {
		return
	}
	r.nowBps.Store(bytesPerSec(bytes, dt))
}
