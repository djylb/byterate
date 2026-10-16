package byterate

import (
	"os"
	"os/exec"
	"runtime"
	"strings"
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
		_ = r.Wait(bg, 2000) // the burst
		_ = r.Wait(bg, 10)   // waits 10ms
	}
	waitOutside()
	for range 3 {
		synctest.Test(t, func(t *testing.T) {
			r := NewRate(1000)
			_ = r.Wait(bg, 2000)
			start := time.Now()
			_ = r.Wait(bg, 1500)
			if elapsed := time.Since(start); elapsed < 1500*time.Millisecond {
				t.Fatalf("Wait(1500) at 1000 B/s returned after %v of bubble time, want 1.5s", elapsed)
			}
		})
	}
	waitOutside()
}

// TestConnReusesWaitTimer throttles writes on a connection: the waits
// after the first must reuse its timer, and so must not allocate, for a Rate
// and for a hierarchy whose levels both wait.
func TestConnReusesWaitTimer(t *testing.T) {
	for _, hierarchical := range []bool{false, true} {
		t.Run(map[bool]string{false: "rate", true: "hierarchical"}[hierarchical], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				r, other := NewRate(1<<20), NewRate(1<<20)
				_ = r.Wait(bg, 2<<20) // the burst
				_ = other.Wait(bg, 2<<20)
				var l Limiter = r
				if hierarchical {
					l = NewHierarchy(r, other, NewRate(0))
				}
				c := NewConn(&nopNetConn{}, l)
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
		})
	}
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
					if w.sleep(int64(wait), nil, done, nil) != slept {
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
		if w.sleep(int64(time.Second), nil, done, nil) != canceled {
			t.Fatal("sleep with done closed reported a full wait")
		}
	})
}

// TestWaitTimerDrainsFiredTimer keeps a timer that fired while its wait
// ended another way. With asynchronous timer channels, which a program built
// with Go 1.26 can still choose with GODEBUG asynctimerchan=1, the fired
// value stays in the channel and would end the next wait at once, so the test
// runs itself again with that setting where the toolchain has it.
func TestWaitTimerDrainsFiredTimer(t *testing.T) {
	if os.Getenv("BYTERATE_ASYNC_TIMER_CHILD") == "" && runtime.GOOS != "js" && runtime.GOOS != "wasip1" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestWaitTimerDrainsFiredTimer$", "-test.count=1")
		cmd.Env = append(os.Environ(), "GODEBUG=asynctimerchan=1", "BYTERATE_ASYNC_TIMER_CHILD=1")
		out, err := cmd.CombinedOutput()
		switch {
		case err == nil:
		case strings.Contains(string(out), `removed GODEBUG "asynctimerchan"`):
			t.Log("this toolchain has synchronous timer channels only")
		default:
			t.Fatalf("with asynchronous timer channels: %v\n%s", err, out)
		}
	}
	var w waitTimer
	fired := time.NewTimer(0)
	time.Sleep(10 * time.Millisecond) // fires, as during a wait that Disable ended
	w.put(fired)
	next := w.take(time.Hour)
	defer w.put(next)
	select {
	case <-next.C:
		t.Fatal("the next wait saw the value of a timer that fired before")
	case <-time.After(20 * time.Millisecond):
	}
}
