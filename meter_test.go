package byterate

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// nextWindow sleeps, in a testing/synctest bubble, to the start of the next
// sampling window.
func nextWindow() {
	m := nowNs() % sampleIntervalNs
	if m < 0 {
		m += sampleIntervalNs
	}
	time.Sleep(time.Duration(sampleIntervalNs - m))
}

// Sampled each second, Throughput reports the bytes of the last whole
// second, the same all through the next one, and 0 for an idle one.
func TestThroughputWindows(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var c Counter
		nextWindow()
		c.Sample()
		time.Sleep(300 * time.Millisecond)
		c.Add(1000, 100)
		if in, out := c.Throughput(); in != 0 || out != 0 {
			t.Fatalf("Throughput() in the window of the bytes = %d, %d; want 0 until it ends", in, out)
		}
		time.Sleep(500 * time.Millisecond)
		c.Add(500, 50)
		nextWindow()
		for range 3 {
			if in, out := c.Throughput(); in != 1500 || out != 150 {
				t.Fatalf("Throughput() = %d, %d; want 1500, 150", in, out)
			}
			time.Sleep(300 * time.Millisecond)
		}
		nextWindow()
		if in, out := c.Throughput(); in != 0 || out != 0 {
			t.Fatalf("Throughput() after an idle second = %d, %d; want 0", in, out)
		}
	})
}

// A reader that samples every N seconds sees the average of those seconds.
func TestThroughputAveragesSparseSamples(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var c Counter
		nextWindow()
		c.Sample()
		c.Add(3000, 0)
		for range 3 {
			nextWindow()
		}
		if in, _ := c.Throughput(); in != 1000 {
			t.Fatalf("Throughput() over 3 s = %d, want their average 1000", in)
		}
	})
}

// Readers at any time within a window read the same throughput, so many of
// them change nothing.
func TestThroughputIgnoresReaders(t *testing.T) {
	run := func(readers int) []int64 {
		var got []int64
		synctest.Test(t, func(t *testing.T) {
			c := NewSharedCounter()
			nextWindow()
			c.Sample()
			for i := range 50 {
				c.Add(int64(100*(i+1)), 0)
				for range readers {
					_, _ = c.Throughput()
				}
				time.Sleep(100 * time.Millisecond)
				if i%10 == 9 {
					c.Sample()
					in, _ := c.Throughput()
					got = append(got, in)
				}
			}
		})
		return got
	}
	want := run(0)
	for _, readers := range []int{1, 3} {
		got := run(readers)
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("with %d readers: %v, want %v", readers, got, want)
			}
		}
	}
}

// Refunds that outweigh a window report 0 and come off the next ones.
func TestThroughputCarriesRefunds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var c Counter
		nextWindow()
		c.Sample()
		c.Add(1000, 0)
		nextWindow()
		c.Sample()
		c.Add(-1500, 0)
		nextWindow()
		if in, _ := c.Throughput(); in != 0 {
			t.Fatalf("Throughput() of a window of refunds = %d, want 0", in)
		}
		c.Add(2000, 0)
		nextWindow()
		if in, _ := c.Throughput(); in != 500 {
			t.Fatalf("Throughput() = %d, want 2000 less the 1500 carried", in)
		}
	})
}

// Restore and Set are not traffic.
func TestCounterThroughputOfAdds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var c Counter
		if in, out := c.Throughput(); in != 0 || out != 0 {
			t.Fatalf("Throughput() before any count = %d, %d; want 0, 0", in, out)
		}
		nextWindow()
		c.Sample()
		c.Restore(1<<30, 1<<20)
		c.Add(0, 500)
		c.Set(5, 5+500)
		nextWindow()
		if in, out := c.Throughput(); in != 0 || out != 500 {
			t.Fatalf("Throughput() = %d, %d; want 0, 500: Restore and Set not counted", in, out)
		}
	})
}

// fakeClock is a Clock that tests step by hand.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// A Counter samples on the Clock it is given.
func TestCounterClock(t *testing.T) {
	clk := &fakeClock{now: time.Unix(1000, 0)}
	var c Counter
	c.SetClock(clk)
	c.Sample()
	c.Add(700, 70)
	clk.advance(time.Second)
	if in, out := c.Throughput(); in != 700 || out != 70 {
		t.Fatalf("Throughput() on a fake clock = %d, %d; want 700, 70", in, out)
	}
	clk.advance(time.Second)
	if in, _ := c.Throughput(); in != 0 {
		t.Fatalf("Throughput() after an idle fake second = %d, want 0", in)
	}
}

// Set racing with Throughput moves the totals and the window's start
// together: the windows report the bytes added and nothing of what Set
// replaced.
func TestCounterSetRacesThroughput(t *testing.T) {
	c := NewSharedCounter()
	at := nowNs() + int64(time.Hour)
	const adds = 1 << 15
	var stop atomic.Bool
	var wg sync.WaitGroup
	wg.Go(func() {
		for range adds {
			c.Add(1, 0)
		}
	})
	wg.Go(func() {
		for !stop.Load() {
			c.Set(1<<40, 0)
			c.Set(0, 0)
			runtime.Gosched() // js/wasm does not preempt this loop
		}
	})
	var reported int64
	for range 2000 {
		at += sampleIntervalNs
		reported += c.throughput(at)[0]
		runtime.Gosched()
	}
	stop.Store(true)
	wg.Wait()
	if reported > adds {
		t.Fatalf("windows reported %d bytes, more than the %d added", reported, adds)
	}
}
