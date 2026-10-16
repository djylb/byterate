package byterate

import (
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestCounterAddsAndSwaps(t *testing.T) {
	for name, c := range map[string]*Counter{"zero": {}, "shared": NewSharedCounter()} {
		t.Run(name, func(t *testing.T) {
			c.Add(10, 20)
			_ = c.ChargeIn(5)
			_ = c.ChargeOut(7)
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

// Swaps racing with Adds count every byte exactly once.
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

// Throughput reports the bytes per second of the last window of at least a
// second, from the first Add of a zero value, and 0 for an idle window.
func TestCounterThroughput(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var c Counter
		if in, out := c.Throughput(); in != 0 || out != 0 {
			t.Fatalf("Throughput() before any Add = %d, %d; want 0, 0", in, out)
		}
		time.Sleep(time.Hour) // the window starts at the first Add, not before
		c.Add(3000, 1000)
		time.Sleep(500 * time.Millisecond)
		if in, out := c.Throughput(); in != 0 || out != 0 {
			t.Fatalf("Throughput() within the first second = %d, %d; want 0, 0", in, out)
		}
		time.Sleep(1500 * time.Millisecond)
		c.Swap() // takes the totals, not the throughput
		if in, out := c.Throughput(); in != 1500 || out != 500 {
			t.Fatalf("Throughput() over 2s = %d, %d; want 1500, 500", in, out)
		}
		time.Sleep(500 * time.Millisecond)
		if in, out := c.Throughput(); in != 1500 || out != 500 {
			t.Fatalf("Throughput() within the next window = %d, %d; want the last one's", in, out)
		}
		time.Sleep(time.Second)
		if in, out := c.Throughput(); in != 0 || out != 0 {
			t.Fatalf("Throughput() of an idle window = %d, %d; want 0, 0", in, out)
		}
		c.Add(10<<30, -1)
		time.Sleep(time.Second)
		if in, out := c.Throughput(); in != 10<<30 || out != 0 {
			t.Fatalf("Throughput() of 10 GiB in and a negative count out = %d, %d", in, out)
		}

		shared := NewSharedCounter()
		shared.Add(2000, 0)
		time.Sleep(time.Second)
		if in, _ := shared.Throughput(); in != 2000 {
			t.Fatalf("shared Throughput() = %d, want 2000", in)
		}
	})
}

// A Counter's views count what a connection's reads and writes take, refunds
// taken off, and follow a Switch to another Counter.
func TestCounterViewsInHierarchy(t *testing.T) {
	var c Counter
	failed := errors.New("write failed")
	r := NewRate(1 << 20)
	inner := &scriptedConn{readBuf: make([]byte, 3000), writeN: 1000, writeErr: failed}
	conn := NewDuplexReadWriteCloser(inner, NewHierarchy(r, c.Inbound()), NewHierarchy(r, c.Outbound()))
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
	s := NewSwitch(a.Outbound())
	sc := NewConn(&dataConn{}, s)
	_, _ = sc.Write(make([]byte, 100))
	s.Set(b.Outbound())
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
		resumed.Add(1000, 0)
		time.Sleep(time.Second)
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
	for _, l := range []Limiter{c.Inbound(), c.Outbound()} {
		_ = l.Wait(bg, -5)
		_ = l.Charge(-5)
		l.Refund(-5)
	}
	if in, out := c.Total(); in != 0 || out != 0 {
		t.Fatalf("Total() after non-positive counts = %d, %d; want 0, 0", in, out)
	}
}
