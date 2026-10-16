package byterate

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
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
				r.Get(2000)
				var l ContextLimiter = r
				if hierarchical {
					l = NewHierarchicalLimiter(r, NewRate(0)).(ContextLimiter)
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				done := make(chan error, 1)
				go func() { done <- l.GetContext(ctx, 1) }() // 1ms of debt
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
		r.Get(2000)
		inner := &deadlineReadConn{scriptedConn: scriptedConn{readBuf: []byte("x")}}
		c := NewRateConn(inner, r)
		defer func() { _ = c.Close() }()
		if err := c.SetReadDeadline(time.Now().Add(100 * time.Microsecond)); err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		n, err := c.Read(make([]byte, 1))
		if n != 1 || !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("Read = %d, %v; want data with deadline error", n, err)
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
				r.Get(2)
				inner := &deadlineReadConn{scriptedConn: scriptedConn{readBuf: []byte("x"), writeN: 1}}
				var c io.ReadWriteCloser
				if read {
					c = NewDuplexRateReadWriteCloser(inner, r, nil)
				} else {
					c = NewDuplexRateReadWriteCloser(inner, nil, r)
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
					wantN := 0
					if read {
						wantN = 1
					}
					if result.n != wantN || !errors.Is(result.err, net.ErrClosed) {
						t.Fatalf("half-closed operation = %+v, want %d and net.ErrClosed", result, wantN)
					}
					if metered(r) != int64(2+wantN) {
						t.Fatalf("meter = %d, want %d", metered(r), 2+wantN)
					}
				default:
					r.Stop()
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
		r.Get(2000)
		r.reserve(10000)
		r.SetLimit(0)
		r.ReturnBucket(10000)
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
			r.Get(2000)
			inner := &interleavedDeadlineConn{release: make(chan struct{}), entered: make(chan struct{})}
			inner.readBuf = []byte("x")
			c := NewRateConn(inner, r)
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
			r.gen.Load().tat.Store(nowNs() + int64(5*time.Millisecond))
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
	c := NewRateConn(inner, NewRate(1000))
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

// A plain Get has nothing but Stop to watch, so a short wait keeps sleeping
// without a timer, as documented in the README. It allocates no more than
// time.Sleep does, which is nothing on most platforms but not on js/wasm.
func TestShortGetAllocatesNothing(t *testing.T) {
	r := NewRate(1 << 20)
	r.Get(2 << 20) // spend the burst, so each 1 KiB charge waits about 1 ms
	sleep := testing.AllocsPerRun(20, func() { time.Sleep(time.Millisecond) })
	if allocs := testing.AllocsPerRun(20, func() { r.Get(1 << 10) }); allocs > sleep {
		t.Fatalf("short Get allocated %v times, want no more than time.Sleep's %v", allocs, sleep)
	}
}
