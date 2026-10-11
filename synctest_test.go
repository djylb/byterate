package byterate

import (
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

// TestWaitsInAndOutOfSynctestBubbles waits in a Rate outside a
// testing/synctest bubble, inside one and outside again. A timer belongs to
// the bubble that made it, so timers shared between these waits would stop
// the process with a fatal error or hang the wait inside the bubble.
func TestWaitsInAndOutOfSynctestBubbles(t *testing.T) {
	waitOutside := func() {
		r := NewRate(1000)
		r.Get(2000) // the burst
		r.Get(10)   // waits 10ms
	}
	waitOutside()
	for range 3 {
		synctest.Test(t, func(t *testing.T) {
			r := NewRate(1000)
			r.Get(2000)
			start := time.Now()
			r.Get(1500)
			if elapsed := time.Since(start); elapsed < 1500*time.Millisecond {
				t.Fatalf("Get(1500) at 1000 B/s returned after %v of bubble time, want 1.5s", elapsed)
			}
		})
	}
	waitOutside()
}

// TestRateConnReusesWaitTimer throttles writes on a connection: the waits
// after the first must reuse its timer, and so must not allocate.
func TestRateConnReusesWaitTimer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := NewRate(1 << 20)
		c := NewRateConn(&nopNetConn{}, r)
		r.Get(2 << 20) // the burst
		buf := make([]byte, 16<<10)
		_, _ = c.Write(buf) // makes the timer and the generation's Done channel
		allocs := testing.AllocsPerRun(100, func() {
			_, _ = c.Write(buf)
		})
		if allocs != 0 {
			t.Fatalf("throttled Write allocated %v times, want 0", allocs)
		}
		_, _ = c.Read(buf)
		if allocs := testing.AllocsPerRun(100, func() { _, _ = c.Read(buf) }); allocs != 0 {
			t.Fatalf("throttled Read allocated %v times, want 0", allocs)
		}
	})
}

// TestWaitTimerConcurrentWaits waits on one waitTimer from several
// goroutines: each wait must last its own time, whichever timer it gets.
func TestWaitTimerConcurrentWaits(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var w waitTimer
		done := make(chan struct{})
		var wg sync.WaitGroup
		for i := range 8 {
			wg.Go(func() {
				for j := range 20 {
					wait := time.Duration(i+j+1) * time.Millisecond
					start := time.Now()
					if !w.sleep(int64(wait), nil, done) {
						t.Error("sleep reported a cancellation")
					}
					if got := time.Since(start); got != wait {
						t.Errorf("sleep(%v) took %v", wait, got)
					}
				}
			})
		}
		wg.Wait()
		close(done)
		if w.sleep(int64(time.Second), nil, done) {
			t.Fatal("sleep with done closed reported a full wait")
		}
	})
}
