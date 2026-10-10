package byterate

import (
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
