package byterate

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"runtime"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

func TestShortWaitCancellation(t *testing.T) {
	for _, hierarchical := range []bool{false, true} {
		t.Run(map[bool]string{false: "rate", true: "hierarchical"}[hierarchical], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				r := NewRate(1000)
				_ = r.Wait(bg, 2000)
				var l Limiter = r
				if hierarchical {
					l = NewHierarchy(r, NewRate(0))
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				done := make(chan error, 1)
				go func() { done <- l.Wait(ctx, 1) }() // 1ms of debt
				synctest.Wait()
				time.Sleep(100 * time.Microsecond)
				start := time.Now()
				cancel()
				if err := <-done; !errors.Is(err, context.Canceled) {
					t.Fatalf("short wait returned %v, want context.Canceled", err)
				}
				if elapsed := time.Since(start); elapsed != 0 {
					t.Fatalf("cancellation took %v of virtual time", elapsed)
				}
			})
		})
	}
}

func TestShortReadWaitDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := NewRate(1000)
		_ = r.Wait(bg, 2000)
		r.reserve(1) // 1ms of debt, which a Read waits for before reading
		inner := &deadlineReadConn{scriptedConn: scriptedConn{readBuf: []byte("x")}}
		c := NewConn(inner, r)
		defer func() { _ = c.Close() }()
		if err := c.SetReadDeadline(time.Now().Add(100 * time.Microsecond)); err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		n, err := c.Read(make([]byte, 1))
		if n != 0 || !errors.Is(err, os.ErrDeadlineExceeded) || len(inner.readBuf) != 1 {
			t.Fatalf("Read = %d, %v; want no data read and a deadline error", n, err)
		}
		if elapsed := time.Since(start); elapsed != 100*time.Microsecond {
			t.Fatalf("Read took %v, want 100us", elapsed)
		}
	})
}

type deadlineReadConn struct{ scriptedConn }

func (*deadlineReadConn) LocalAddr() net.Addr              { return nil }
func (*deadlineReadConn) RemoteAddr() net.Addr             { return nil }
func (*deadlineReadConn) SetDeadline(time.Time) error      { return nil }
func (*deadlineReadConn) SetReadDeadline(time.Time) error  { return nil }
func (*deadlineReadConn) SetWriteDeadline(time.Time) error { return nil }
func (*deadlineReadConn) CloseRead() error                 { return nil }
func (*deadlineReadConn) CloseWrite() error                { return nil }

func TestHalfCloseWakesLimiter(t *testing.T) {
	for _, read := range []bool{false, true} {
		t.Run(map[bool]string{false: "write", true: "read"}[read], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				r := NewRate(1)
				_ = r.Wait(bg, 2)
				r.reserve(1) // a second of debt, which both directions wait for
				inner := &deadlineReadConn{scriptedConn: scriptedConn{readBuf: []byte("x"), writeN: 1}}
				var c io.ReadWriteCloser
				if read {
					c = NewDuplexReadWriteCloser(inner, r, nil)
				} else {
					c = NewDuplexReadWriteCloser(inner, nil, r)
				}
				defer func() { _ = c.Close() }()
				done := make(chan ioResult, 1)
				go func() {
					var n int
					var err error
					if read {
						n, err = c.Read(make([]byte, 1))
					} else {
						n, err = c.Write([]byte("x"))
					}
					done <- ioResult{n, err}
				}()
				synctest.Wait()
				var err error
				if read {
					err = c.(interface{ CloseRead() error }).CloseRead()
				} else {
					err = c.(interface{ CloseWrite() error }).CloseWrite()
				}
				if err != nil {
					t.Fatal(err)
				}
				synctest.Wait()
				select {
				case result := <-done:
					// A Read waits before reading, and a Write's grant is
					// refunded, so neither moved or kept a byte.
					if result.n != 0 || !errors.Is(result.err, net.ErrClosed) {
						t.Fatalf("half-closed operation = %+v, want 0 and net.ErrClosed", result)
					}
					if metered(r) != 3 {
						t.Fatalf("meter = %d, want the 3 charged before", metered(r))
					}
				default:
					r.Disable()
					<-done
					t.Fatal("half-close left the operation blocked in the limiter")
				}
				// Half-closing one direction must leave the other usable.
				if read {
					_, err = c.Write([]byte("x"))
				} else {
					_, err = c.Read(make([]byte, 1))
				}
				if err != nil {
					t.Fatalf("opposite direction failed: %v", err)
				}
			})
		})
	}
}

func TestRefundWhileTemporarilyUnlimited(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := NewRate(1000)
		_ = r.Wait(bg, 2000)
		r.reserve(10000)
		r.SetLimit(0)
		r.Refund(10000)
		r.SetLimit(2000)
		if wait := time.Duration(r.reserve(2)); wait > time.Millisecond {
			t.Fatalf("refunded bytes still impose %v of debt", wait)
		}
	})
}

// Delay the first underlying setter after it has applied its deadline. A
// second setter must not get between that update and the wrapper's update.
type interleavedDeadlineConn struct {
	deadlineReadConn
	mu      sync.Mutex
	last    time.Time
	calls   int
	release chan struct{}
	entered chan struct{}
}

func (c *interleavedDeadlineConn) SetReadDeadline(at time.Time) error {
	c.mu.Lock()
	c.last = at
	c.calls++
	first := c.calls == 1
	c.mu.Unlock()
	if first {
		close(c.entered)
		<-c.release
	}
	return nil
}

func (c *interleavedDeadlineConn) SetDeadline(at time.Time) error {
	return c.SetReadDeadline(at)
}

func TestConcurrentDeadlineSettersStayConsistent(t *testing.T) {
	for _, both := range []bool{false, true} {
		t.Run(map[bool]string{false: "read", true: "both"}[both], func(t *testing.T) {
			r := NewRate(1000)
			_ = r.Wait(bg, 2000)
			inner := &interleavedDeadlineConn{release: make(chan struct{}), entered: make(chan struct{})}
			inner.readBuf = []byte("x")
			c := NewConn(inner, r)
			defer func() { _ = c.Close() }()
			done := make(chan error, 1)
			second := make(chan error, 1)
			past := time.Now().Add(-time.Second)
			future := time.Now().Add(time.Minute)
			go func() {
				if both {
					done <- c.SetDeadline(past)
				} else {
					done <- c.SetReadDeadline(past)
				}
			}()
			<-inner.entered
			go func() { second <- c.SetReadDeadline(future) }()
			// A serialized setter waits for the first setter to finish. A broken
			// wrapper lets it finish early; retain its result before releasing the first.
			select {
			case err := <-second:
				second <- err
			case <-time.After(20 * time.Millisecond):
			}
			close(inner.release)
			for _, result := range []<-chan error{done, second} {
				if err := <-result; err != nil {
					t.Fatal(err)
				}
			}
			inner.mu.Lock()
			last := inner.last
			inner.mu.Unlock()
			if last != future {
				t.Fatal("underlying deadline is not the future deadline")
			}
			// Create debt after the interleaving, since real time elapsed during
			// it. Keep it above shortWaitNs so the wait watches the deadline
			// even without the short-wait fix.
			genOf(r).tat.Store(nowNs() + int64(5*time.Millisecond))
			if n, err := c.Read(make([]byte, 1)); n != 1 || err != nil {
				t.Fatalf("Read = %d, %v; limiter still uses the superseded deadline", n, err)
			}
		})
	}
}

// A Read deadline setter blocked in the wrapped connection must not hold up
// a Write deadline setter: only setters of the same direction are ordered.
func TestDeadlineSettersOfOtherDirectionDoNotWait(t *testing.T) {
	inner := &interleavedDeadlineConn{release: make(chan struct{}), entered: make(chan struct{})}
	c := NewConn(inner, NewRate(1000))
	defer func() { _ = c.Close() }()
	first := make(chan error, 1)
	go func() { first <- c.SetReadDeadline(time.Now().Add(time.Minute)) }()
	<-inner.entered
	write := make(chan error, 1)
	go func() { write <- c.SetWriteDeadline(time.Now().Add(time.Minute)) }()
	select {
	case err := <-write:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		close(inner.release)
		t.Fatal("SetWriteDeadline waited for a blocked SetReadDeadline")
	}
	close(inner.release)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
}

// A plain Wait has nothing but Disable to watch, so a short wait keeps sleeping
// without a timer, as documented in the README. It allocates no more than
// time.Sleep does, which is nothing on most platforms but not on js/wasm.
func TestShortWaitAllocatesNothing(t *testing.T) {
	if runtime.GOOS == "js" {
		// Sleeping allocates there, by a varying count that hides the
		// charge's own; TestShortWaitMakesNoStopChannels still runs.
		t.Skip("time.Sleep allocates on js")
	}
	r := NewRate(1 << 20)
	g := genOf(r)
	short := func() {
		// Spend the burst before each charge, so that each 1 KiB charge
		// waits about 1 ms however late the previous sleep woke.
		if now := nowNs(); g.tat.Load() < now {
			g.tat.Store(now)
		}
		_ = r.Wait(bg, 1<<10)
	}
	// Totals, as testing.AllocsPerRun rounds an average below one down to 0,
	// with a little slack for the runtime's own.
	const runs = 200
	sleep := mallocs(runs, func() { time.Sleep(time.Millisecond) })
	if allocs := mallocs(runs, short); allocs > sleep+runs/20 {
		t.Fatalf("%d short Gets allocated %d times, want no more than time.Sleep's %d and %d of slack", runs, allocs, sleep, runs/20)
	}
}

// mallocs returns the number of heap allocations made by n calls of f.
func mallocs(n int, f func()) uint64 {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for range n {
		f()
	}
	runtime.ReadMemStats(&after)
	return after.Mallocs - before.Mallocs
}

// A wait short enough to sleep without a timer cannot watch Disable, so it must
// not make the generation's stop channels either, also on the first wait.
func TestShortWaitMakesNoStopChannels(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		get  func(r *Rate)
	}{
		{"Wait", func(r *Rate) { _ = r.Wait(ctx, 1<<10) }},
		{"hierarchical Wait", func(r *Rate) {
			_ = NewHierarchy(r, NewRate(0)).Wait(ctx, 1<<10)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := NewRate(1 << 20)
			_ = r.Wait(bg, 2<<20) // spend the burst, so a 1 KiB charge waits about 1 ms
			tc.get(r)
			if set := genOf(r).stop.Load(); set != nil {
				t.Fatalf("short wait made a stop set of %d channels", len(set.shards))
			}
		})
	}
}
