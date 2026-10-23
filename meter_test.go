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

// Throughput reports the bytes of the last whole second of the clock, for a
// Rate as for a Counter counted through its views, and 0 for an idle one.
func TestThroughputWindows(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := NewRate(0)
		var c Counter
		l := NewHierarchy(r, c.Reads(), c.Writes())
		nextWindow()
		time.Sleep(300 * time.Millisecond)
		_ = l.Wait(bg, 1000)
		if got := r.Throughput(); got != 0 {
			t.Fatalf("Throughput() in the window of the charge = %d, want 0 until it ends", got)
		}
		time.Sleep(500 * time.Millisecond)
		_ = l.Wait(bg, 500)
		nextWindow()
		for range 3 { // the same all through the next window
			if got := r.Throughput(); got != 1500 {
				t.Fatalf("Rate.Throughput() = %d, want the 1500 bytes of the last second", got)
			}
			if in, out := c.Throughput(); in != 1500 || out != 1500 {
				t.Fatalf("Counter.Throughput() = %d, %d; want 1500, 1500", in, out)
			}
			time.Sleep(300 * time.Millisecond)
		}
		nextWindow()
		if got := r.Throughput(); got != 0 {
			t.Fatalf("Throughput() after an idle second = %d, want 0", got)
		}
		if in, out := c.Throughput(); in != 0 || out != 0 {
			t.Fatalf("Counter.Throughput() after an idle second = %d, %d; want 0", in, out)
		}
	})
}

// What Throughput reports depends on when the bytes were charged, not on
// who reads it or how often: readers that read at different times read the
// same windows.
func TestThroughputIgnoresReaders(t *testing.T) {
	type reading struct{ rate, read, written int64 }
	run := func(readEvery int) []reading {
		var got []reading
		synctest.Test(t, func(t *testing.T) {
			r := NewRate(0)
			c := NewSharedCounter()
			reads, writes := NewHierarchy(r, c.Reads()), c.Writes()
			nextWindow()
			for i := range 50 {
				_ = reads.Wait(bg, int64(100*(i+1)))
				_ = writes.Charge(int64(10 * (i + 1)))
				if readEvery > 0 && i%readEvery == 0 {
					_ = r.Throughput()
					_, _ = c.Throughput()
				}
				time.Sleep(70 * time.Millisecond)
				if i%10 == 9 {
					rb := r.Throughput()
					in, out := c.Throughput()
					got = append(got, reading{rb, in, out})
				}
			}
		})
		return got
	}
	want := run(0)
	for _, every := range []int{1, 3, 7} {
		if got := run(every); len(got) != len(want) {
			t.Fatalf("%d readings, want %d", len(got), len(want))
		} else {
			for i := range got {
				if got[i] != want[i] {
					t.Fatalf("reading every %d charges: reading %d = %+v, want %+v as without readers", every, i, got[i], want[i])
				}
			}
		}
	}
	for _, w := range want[1:] { // the first is read within the first window
		if w.rate == 0 || w.rate != w.read || w.written != w.read/10 {
			t.Fatalf("readings %+v: want the bytes of the last whole second", want)
		}
	}
}

// A Rate meters whether it is limited, unlimited or disabled; Enable starts
// its meter over.
func TestDisabledAndUnlimitedRatesMeter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		limited, unlimited, disabled := NewRate(1<<20), NewRate(0), NewRate(1<<20)
		disabled.Disable()
		nextWindow()
		for _, r := range []*Rate{limited, unlimited, disabled} {
			_ = r.Wait(bg, 4000)
		}
		disabled.Refund(1000) // off its meter alone
		nextWindow()
		for _, c := range []struct {
			r    *Rate
			want int64
		}{{limited, 4000}, {unlimited, 4000}, {disabled, 3000}} {
			if got := c.r.Throughput(); got != c.want {
				t.Fatalf("Throughput() = %d, want %d", got, c.want)
			}
		}
		disabled.Enable()
		if got := disabled.Throughput(); got != 0 {
			t.Fatalf("Throughput() after Enable = %d, want 0", got)
		}
	})
}

// A refund of bytes charged in an earlier window, such as a canceled write,
// comes off the following windows, through idle ones and ones that other
// refunds netted to 0, rather than being dropped.
func TestMeterCarriesRefunds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := NewRate(0)
		window := func() int64 { nextWindow(); return r.Throughput() }
		nextWindow()
		_ = r.Wait(bg, 1000)
		if got := window(); got != 1000 {
			t.Fatalf("window 1 = %d, want 1000", got)
		}
		r.Refund(1000) // never sent
		_ = r.Wait(bg, 600)
		if got := window(); got != 0 {
			t.Fatalf("window 2 = %d, want 0", got)
		}
		_ = r.Wait(bg, 1000)
		if got := window(); got != 600 {
			t.Fatalf("window 3 = %d, want 600 after the rest of the refund", got)
		}
		window() // idle
		r.Refund(500)
		window() // idle, the refund carried
		_ = r.Wait(bg, 100)
		if got := window(); got != 0 {
			t.Fatalf("window after an idle one = %d, want 0 with the refund carried", got)
		}
		_ = r.Wait(bg, 1000)
		if got := window(); got != 600 {
			t.Fatalf("next window = %d, want 600", got)
		}
	})
}

// Charges and refunds of the same bytes, racing with windows that close:
// the windows together report what the refunds still carried take off.
func TestMeterChargeRefundRace(t *testing.T) {
	r := NewRate(0)
	g := genOf(r)
	// Windows far ahead of the charges' clock, so that only roll closes them.
	at := nowNs() + int64(time.Hour)
	roll := func() int64 {
		at += sampleIntervalNs
		return g.throughput(at)
	}
	roll()
	var stop atomic.Bool
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for !stop.Load() {
				r.reserve(1000)
				r.Refund(1000)
				runtime.Gosched() // js/wasm does not preempt this loop
			}
		})
	}
	var reported int64
	for range 10000 {
		reported += roll()
	}
	stop.Store(true)
	wg.Wait()
	reported += roll()
	if carried := g.meter.start[0] - g.total.Load(); reported != carried {
		t.Fatalf("windows reported %d bytes of charges that were all refunded, with %d bytes of refunds carried", reported, carried)
	}
}

// Add does not read the clock: its bytes count in the window that the next
// timed count, or Throughput, closes. Restore and Set are not traffic.
func TestCounterThroughputOfAdds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var c Counter
		if in, out := c.Throughput(); in != 0 || out != 0 {
			t.Fatalf("Throughput() before any count = %d, %d; want 0, 0", in, out)
		}
		c.Restore(1<<30, 1<<20)
		c.Add(3000, 1000)
		nextWindow()
		_ = c.Writes().Charge(500) // closes the window of the Adds
		c.Set(5, 5)
		nextWindow()
		if in, out := c.Throughput(); in != 0 || out != 500 {
			t.Fatalf("Throughput() = %d, %d; want 0, 500: the Adds before, Set not counted", in, out)
		}
		c.Add(10<<30, -1)
		nextWindow()
		if in, out := c.Throughput(); in != 10<<30 || out != 0 {
			t.Fatalf("Throughput() of 10 GiB read and a negative count written = %d, %d", in, out)
		}
		if in, out := c.Total(); in != 5+10<<30 || out != 5-1 {
			t.Fatalf("Total() = %d, %d", in, out)
		}
	})
}

// Set racing with Throughput moves the totals and the window's start
// together: the windows report the bytes added and nothing of what Set
// replaced.
func TestCounterSetRacesThroughput(t *testing.T) {
	c := NewSharedCounter()
	at := nowNs() + int64(time.Hour) // windows that only throughput closes
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
