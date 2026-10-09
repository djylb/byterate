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

type stopSignal struct {
	ch chan struct{}
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
	// Read by every charge and written rarely, so they stay cached on every
	// core.
	rate         atomic.Int64 // bytes/s, <=0 => unlimited
	lastSampleNs atomic.Int64 // start of the sampling window
	stop         atomic.Pointer[stopSignal]
	enabled      atomic.Bool

	_ cacheLinePad
	// Written by every charge, on a cache line of their own.
	tat      atomic.Int64 // theoretical arrival time (ns since epoch), can be negative
	bytesAcc atomic.Int64 // bytes charged in the current sampling window
	_        cacheLinePad

	nowBps  atomic.Int64 // throughput of the last sampling window, bytes/s
	mu      sync.Mutex
	stopped bool
}

// NewRate returns a started Rate limited to limitBps bytes per second with a
// full burst available. A limitBps <= 0 means unlimited.
func NewRate(limitBps int64) *Rate {
	if limitBps <= 0 {
		limitBps = 0
	}
	r := &Rate{}
	r.enabled.Store(true)
	r.rate.Store(limitBps)
	r.stop.Store(&stopSignal{ch: make(chan struct{})})

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
	r.mu.Lock()
	stopped := r.stopped
	r.mu.Unlock()

	cloned := &Rate{stopped: stopped}
	cloned.rate.Store(r.rate.Load())
	cloned.tat.Store(r.tat.Load())
	cloned.enabled.Store(r.enabled.Load())
	cloned.bytesAcc.Store(r.bytesAcc.Load())
	cloned.lastSampleNs.Store(r.lastSampleNs.Load())
	cloned.nowBps.Store(r.nowBps.Load())

	signal := &stopSignal{ch: make(chan struct{})}
	if stopped {
		close(signal.ch)
	}
	cloned.stop.Store(signal)
	return cloned
}

// SetLimit sets the limit in bytes per second; limitBps <= 0 means unlimited.
// It applies to later reservations only: debt accrued at the previous limit is
// kept and callers blocked in Get are not woken, so a raised limit takes full
// effect once that debt is repaid. Use ResetLimit to apply a limit at once.
func (r *Rate) SetLimit(limitBps int64) {
	if r == nil {
		return
	}
	if limitBps <= 0 {
		limitBps = 0
	}
	r.rate.Store(limitBps)
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
	if r == nil {
		return 0
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

	if r.stopped || r.stop.Load() == nil {
		r.stop.Store(&stopSignal{ch: make(chan struct{})})
		r.stopped = false
	}

	if needReset {
		now := nowNs()
		r.tat.Store(now - burstWindowNs) // full burst on (re)enable
		r.bytesAcc.Store(0)
		r.lastSampleNs.Store(now)
		r.nowBps.Store(0)
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
// above two seconds' worth of bytes.
func (r *Rate) ReturnBucket(size int64) {
	if r == nil || size <= 0 || !r.enabled.Load() {
		return
	}

	r.bytesAcc.Add(-size)

	rate := r.rate.Load()
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
		if !r.enabled.Load() {
			return
		}
	}
}

// Get charges size bytes and blocks until the limit allows them. It returns
// immediately if r is nil, stopped or unlimited; Stop and ResetLimit wake a
// blocked Get early. Use GetContext to abandon a wait.
func (r *Rate) Get(size int64) {
	wait := r.reserve(size)
	if wait <= coalesceWaitNs {
		return
	}
	sleepNs(wait, r.stopCh(), nil)
}

// GetContext is like Get but returns ctx.Err() if ctx is done before the wait
// ends. The charge is kept either way; refund unused bytes with ReturnBucket.
func (r *Rate) GetContext(ctx context.Context, size int64) error {
	wait := r.reserve(size)
	if wait <= coalesceWaitNs {
		return nil
	}
	if !sleepNs(wait, r.stopCh(), ctx.Done()) {
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
	return r.reserveAt(size, nowNs())
}

// reserveAt charges size at time now, as returned by nowNs, and returns how
// long the caller must wait. A HierarchicalLimiter passes one now to all its
// Rates, reading the clock once.
func (r *Rate) reserveAt(size, now int64) int64 {
	if r == nil || size <= 0 || !r.enabled.Load() {
		return 0
	}

	r.bytesAcc.Add(size)
	r.updateRateWithNow(now)

	currentRate := r.rate.Load()
	if currentRate <= 0 {
		return 0
	}

	cost := bytesToNsCeil(size, currentRate)
	minTat := now - burstWindowNs
	for {
		prev := r.tat.Load()
		next := clampAdd(max(prev, minTat), cost)
		if r.tat.CompareAndSwap(prev, next) {
			return max(next-now, 0)
		}
		// Retry at once with the same now: reading the clock again would
		// widen the window in which other cores win the race.
		if !r.enabled.Load() {
			return 0
		}
	}
}

func (r *Rate) stopCh() <-chan struct{} {
	if r == nil {
		return nil
	}
	if s := r.stop.Load(); s != nil {
		return s.ch
	}
	return nil
}

func (r *Rate) updateRateWithNow(now int64) {
	last := r.lastSampleNs.Load()
	if now-last < sampleIntervalNs {
		return
	}
	if !r.lastSampleNs.CompareAndSwap(last, now) {
		return
	}

	bytes := max(r.bytesAcc.Swap(0), 0)
	dt := now - last
	if dt <= 0 {
		return
	}
	r.nowBps.Store(bytesPerSec(bytes, dt))
}
