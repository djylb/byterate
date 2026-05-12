package byterate

import (
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"
)

const burstWindowNs = int64(2 * time.Second)

type stopSignal struct {
	ch chan struct{}
}

type Rate struct {
	rate    int64
	burstNs int64
	tat     int64

	enabled int32
	t0      time.Time

	mu      sync.Mutex
	stopped bool
	stop    atomic.Pointer[stopSignal]

	bytesAcc     int64
	lastSampleNs int64
	nowBps       int64
}

type rateJSON struct {
	NowRate int64 `json:"NowRate"`
	Limit   int64 `json:"Limit"`
}

func NewRate(limitBps int64) *Rate {
	if limitBps <= 0 {
		limitBps = 0
	}
	r := &Rate{
		enabled: 1,
		t0:      time.Now(),
		burstNs: burstWindowNs,
	}
	atomic.StoreInt64(&r.rate, limitBps)
	r.stop.Store(&stopSignal{ch: make(chan struct{})})

	if limitBps > 0 {
		atomic.StoreInt64(&r.tat, -r.burstNs)
	} else {
		atomic.StoreInt64(&r.tat, 0)
	}

	now := r.nowNs()
	atomic.StoreInt64(&r.lastSampleNs, now)
	atomic.StoreInt64(&r.nowBps, 0)
	return r
}

func (r *Rate) Clone() *Rate {
	if r == nil {
		return nil
	}
	enabled := atomic.LoadInt32(&r.enabled)
	if r.t0.IsZero() {
		cloned := NewRate(atomic.LoadInt64(&r.rate))
		if enabled == 0 {
			cloned.Stop()
		}
		return cloned
	}

	r.mu.Lock()
	stopped := r.stopped
	burstNs := r.burstNs
	t0 := r.t0
	r.mu.Unlock()

	cloned := &Rate{
		burstNs: burstNs,
		t0:      t0,
		stopped: stopped,
	}
	atomic.StoreInt64(&cloned.rate, atomic.LoadInt64(&r.rate))
	atomic.StoreInt64(&cloned.tat, atomic.LoadInt64(&r.tat))
	atomic.StoreInt32(&cloned.enabled, enabled)
	atomic.StoreInt64(&cloned.bytesAcc, atomic.LoadInt64(&r.bytesAcc))
	atomic.StoreInt64(&cloned.lastSampleNs, atomic.LoadInt64(&r.lastSampleNs))
	atomic.StoreInt64(&cloned.nowBps, atomic.LoadInt64(&r.nowBps))

	signal := &stopSignal{ch: make(chan struct{})}
	if stopped {
		close(signal.ch)
	}
	cloned.stop.Store(signal)
	return cloned
}

func (r *Rate) SetLimit(limitBps int64) {
	if r == nil {
		return
	}
	if limitBps <= 0 {
		limitBps = 0
	}
	atomic.StoreInt64(&r.rate, limitBps)
}

func (r *Rate) ResetLimit(limitBps int64) {
	if r == nil {
		return
	}
	r.Stop()
	r.SetLimit(limitBps)
	r.Start()
}

func (r *Rate) Limit() int64 {
	if r == nil {
		return 0
	}
	return atomic.LoadInt64(&r.rate)
}

func (r *Rate) Now() int64 {
	if r == nil {
		return 0
	}
	r.updateRateWithNow(r.nowNs())
	return atomic.LoadInt64(&r.nowBps)
}

func (r *Rate) Start() {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	prevEnabled := atomic.LoadInt32(&r.enabled) != 0
	needReset := r.stopped || !prevEnabled || r.stop.Load() == nil

	if r.stopped || r.stop.Load() == nil {
		r.stop.Store(&stopSignal{ch: make(chan struct{})})
		r.stopped = false
	}

	if needReset {
		rate := atomic.LoadInt64(&r.rate)
		if rate > 0 {
			now := r.nowNs()
			atomic.StoreInt64(&r.tat, now-r.burstNs)
		} else {
			atomic.StoreInt64(&r.tat, 0)
		}
		atomic.StoreInt64(&r.bytesAcc, 0)
		now := r.nowNs()
		atomic.StoreInt64(&r.lastSampleNs, now)
		atomic.StoreInt64(&r.nowBps, 0)
	}

	atomic.StoreInt32(&r.enabled, 1)
}

func (r *Rate) Stop() {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	atomic.StoreInt32(&r.enabled, 0)
	if !r.stopped {
		if s := r.stop.Load(); s != nil && s.ch != nil {
			close(s.ch)
		}
		r.stopped = true
	}
	atomic.StoreInt64(&r.bytesAcc, 0)
	atomic.StoreInt64(&r.nowBps, 0)
}

func (r *Rate) ReturnBucket(size int64) {
	if r == nil || size <= 0 || atomic.LoadInt32(&r.enabled) == 0 {
		return
	}

	atomic.AddInt64(&r.bytesAcc, -size)

	rate := atomic.LoadInt64(&r.rate)
	if rate <= 0 {
		return
	}

	refund := bytesToNsCeil(size, rate)
	now := r.nowNs()
	minTat := now - r.burstNs

	for {
		prev := atomic.LoadInt64(&r.tat)
		next := clampSub(prev, refund)
		if next < minTat {
			next = minTat
		}
		if atomic.CompareAndSwapInt64(&r.tat, prev, next) {
			return
		}
		if atomic.LoadInt32(&r.enabled) == 0 {
			return
		}
	}
}

func (r *Rate) Get(size int64) {
	wait := r.reserve(size)
	if wait <= coalesceWaitNs {
		return
	}
	sleepNs(wait, r.stopCh())
}

func (r *Rate) MarshalJSON() ([]byte, error) {
	if r == nil {
		return []byte("null"), nil
	}
	return json.Marshal(rateJSON{
		NowRate: r.Now(),
		Limit:   r.Limit(),
	})
}

func (r *Rate) reserve(size int64) int64 {
	if r == nil || size <= 0 || atomic.LoadInt32(&r.enabled) == 0 {
		return 0
	}

	atomic.AddInt64(&r.bytesAcc, size)

	now := r.nowNs()
	r.updateRateWithNow(now)

	currentRate := atomic.LoadInt64(&r.rate)
	if currentRate <= 0 {
		return 0
	}

	cost := bytesToNsCeil(size, currentRate)

	for {
		minTat := now - r.burstNs

		prev := atomic.LoadInt64(&r.tat)
		base := prev
		if base < minTat {
			base = minTat
		}
		next := clampAdd(base, cost)

		if atomic.CompareAndSwapInt64(&r.tat, prev, next) {
			wait := next - now
			if wait < 0 {
				return 0
			}
			return wait
		}

		if atomic.LoadInt32(&r.enabled) == 0 {
			return 0
		}
		now = r.nowNs()
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
	last := atomic.LoadInt64(&r.lastSampleNs)
	if now-last < sampleIntervalNs {
		return
	}
	if !atomic.CompareAndSwapInt64(&r.lastSampleNs, last, now) {
		return
	}

	bytes := atomic.SwapInt64(&r.bytesAcc, 0)
	if bytes < 0 {
		bytes = 0
	}
	dt := now - last
	if dt <= 0 {
		return
	}
	atomic.StoreInt64(&r.nowBps, bytesPerSec(bytes, dt))
}

func (r *Rate) nowNs() int64 {
	return int64(time.Since(r.t0))
}
