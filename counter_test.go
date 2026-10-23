package byterate

import (
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
)

func TestCounterAddsAndSwaps(t *testing.T) {
	for name, c := range map[string]*Counter{"zero": {}, "shared": NewSharedCounter()} {
		t.Run(name, func(t *testing.T) {
			c.Add(10, 20)
			_ = c.Reads().Charge(5)
			_ = c.Writes().Charge(7)
			if in, out := c.Load(); in != 15 || out != 27 {
				t.Fatalf("Load() = %d, %d; want 15, 27", in, out)
			}
			if in, out := c.Swap(); in != 15 || out != 27 {
				t.Fatalf("Swap() = %d, %d; want 15, 27", in, out)
			}
			if in, out := c.Load(); in != 0 || out != 0 {
				t.Fatalf("Load() after Swap = %d, %d; want 0, 0", in, out)
			}
		})
	}
	var none *Counter
	none.Add(1, 1)
	if in, out := none.Swap(); in != 0 || out != 0 {
		t.Fatal("a nil Counter counted")
	}
}

// Swaps and Sets racing with Adds count every byte exactly once.
func TestCounterSwapRacesAdds(t *testing.T) {
	for name, c := range map[string]*Counter{"zero": {}, "shared": NewSharedCounter()} {
		t.Run(name, func(t *testing.T) {
			const adders, adds = 8, 1 << 12
			var stop atomic.Bool
			var swappedIn, swappedOut atomic.Int64
			var swapper sync.WaitGroup
			swapper.Go(func() {
				for !stop.Load() {
					in, out := c.Swap()
					swappedIn.Add(in)
					swappedOut.Add(out)
					runtime.Gosched() // js/wasm does not preempt this loop
				}
			})
			// Set replaces the totals, which Swap does not report.
			swapper.Go(func() {
				for !stop.Load() {
					c.Set(0, 0)
					runtime.Gosched()
				}
			})
			var wg sync.WaitGroup
			for range adders {
				wg.Go(func() {
					for range adds {
						c.Add(3, 5)
					}
				})
			}
			wg.Wait()
			stop.Store(true)
			swapper.Wait()
			in, out := c.Swap()
			if in+swappedIn.Load() != 3*adders*adds || out+swappedOut.Load() != 5*adders*adds {
				t.Fatalf("swapped %d/%d bytes in total, want %d/%d", in+swappedIn.Load(), out+swappedOut.Load(), 3*adders*adds, 5*adders*adds)
			}
			// Each Add came before the last Set or after it, whole.
			if in, out := c.Total(); in < 0 || in > 3*adders*adds || in%3 != 0 || out < 0 || out%5 != 0 {
				t.Fatalf("Total() after Sets = %d, %d; want whole Adds after the last Set", in, out)
			}
		})
	}
}

func BenchmarkCounterAdd(b *testing.B) {
	for name, c := range map[string]*Counter{"zero": {}, "shared": NewSharedCounter()} {
		b.Run(name, func(b *testing.B) {
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					c.Add(1500, 0)
				}
			})
		})
	}
}

// A Counter's views count what a connection's reads and writes take, refunds
// taken off, and follow a Switch to another Counter.
func TestCounterViewsInHierarchy(t *testing.T) {
	var c Counter
	failed := errors.New("write failed")
	r := NewRate(1 << 20)
	inner := &scriptedConn{readBuf: make([]byte, 3000), writeN: 1000, writeErr: failed}
	conn := NewDuplexReadWriteCloser(inner, NewHierarchy(r, c.Reads()), NewHierarchy(r, c.Writes()))
	if n, _ := conn.Read(make([]byte, 5000)); n != 3000 {
		t.Fatalf("Read = %d, want 3000", n)
	}
	if n, err := conn.Write(make([]byte, 4000)); n != 1000 || !errors.Is(err, failed) {
		t.Fatalf("Write = %d, %v; want 1000 and the failure", n, err)
	}
	if in, out := c.Load(); in != 3000 || out != 1000 {
		t.Fatalf("counted %d in, %d out; want the 3000 read and the 1000 written", in, out)
	}

	var a, b Counter
	s := NewSwitch(a.Writes())
	sc := NewConn(&dataConn{}, s)
	_, _ = sc.Write(make([]byte, 100))
	s.Set(b.Writes())
	_, _ = sc.Write(make([]byte, 200))
	if _, out := a.Load(); out != 100 {
		t.Fatalf("first counter got %d out, want 100", out)
	}
	if _, out := b.Load(); out != 200 {
		t.Fatalf("second counter got %d out after Set, want 200", out)
	}
}

// Total keeps the running totals across Swaps, and Restore takes them back
// without reporting them as new traffic.
func TestCounterTotal(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var c Counter
		c.Add(100, 50)
		c.Swap()
		c.Add(10, 5)
		if in, out := c.Total(); in != 110 || out != 55 {
			t.Fatalf("Total() = %d, %d; want 110, 55", in, out)
		}
		var resumed Counter
		resumed.Restore(1<<30, 1<<20)
		nextWindow()
		_ = resumed.Reads().Charge(1000)
		nextWindow()
		if in, out := resumed.Total(); in != 1<<30+1000 || out != 1<<20 {
			t.Fatalf("resumed Total() = %d, %d", in, out)
		}
		if in, out := resumed.Swap(); in != 1000 || out != 0 {
			t.Fatalf("resumed Swap() = %d, %d; want only the 1000 new bytes", in, out)
		}
		if in, _ := resumed.Throughput(); in != 1000 {
			t.Fatalf("resumed Throughput() = %d, want only the new 1000 B/s", in)
		}
	})
}

// The views, as every Limiter here, ignore counts of zero or less.
func TestCounterViewsIgnoreNonPositive(t *testing.T) {
	var c Counter
	for _, l := range []Limiter{c.Reads(), c.Writes()} {
		_ = l.Wait(bg, -5)
		_ = l.Charge(-5)
		l.Refund(-5)
	}
	if in, out := c.Total(); in != 0 || out != 0 {
		t.Fatalf("Total() after non-positive counts = %d, %d; want 0, 0", in, out)
	}
}
