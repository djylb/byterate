package byterate

import (
	"sync"
	"time"
	"weak"
)

// The Counters of SampleEverySecond, held weakly, and whether the ticker
// that samples them runs.
var sampler struct {
	mu       sync.Mutex
	counters []weak.Pointer[Counter]
	running  bool
}

// SampleEverySecond samples c's totals now and then once a second, from a
// ticker that all the Counters given to it share, so that Throughput
// reports each second however rarely it is read. The ticker holds c
// weakly: it drops c once c is no longer referenced elsewhere, and stops
// while it has no Counter. A Counter given more than once is sampled once.
func (c *Counter) SampleEverySecond() {
	if c == nil {
		return
	}
	c.Sample()
	sampler.mu.Lock()
	defer sampler.mu.Unlock()
	p := weak.Make(c)
	for _, q := range sampler.counters {
		if q == p {
			return
		}
	}
	sampler.counters = append(sampler.counters, p)
	if !sampler.running {
		sampler.running = true
		go sampleLoop()
	}
}

// sampleLoop samples the Counters of SampleEverySecond once a second until
// none is left.
func sampleLoop() {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for range t.C {
		sampler.mu.Lock()
		live := sampler.counters[:0]
		for _, p := range sampler.counters {
			if c := p.Value(); c != nil {
				live = append(live, p)
			}
		}
		clear(sampler.counters[len(live):])
		sampler.counters = live
		if len(live) == 0 {
			sampler.running = false
			sampler.mu.Unlock()
			return
		}
		cs := make([]*Counter, 0, len(live))
		for _, p := range live {
			if c := p.Value(); c != nil {
				cs = append(cs, c)
			}
		}
		sampler.mu.Unlock()
		for _, c := range cs {
			c.Sample()
		}
	}
}
