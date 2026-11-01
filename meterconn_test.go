package byterate

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

// A connection without a limit counts every byte on the fast path, also
// through ReadFrom and WriteTo, and a Counter attached to it adds them up.
func TestConnMeterCounts(t *testing.T) {
	var traffic Counter
	inner := &copyConn{}
	c := NewConn(inner, WithCounter(&traffic))
	if _, err := c.Write(make([]byte, 700)); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Read(make([]byte, 300)); err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte{1}, 3*copyChunk+5)
	if n, err := c.ReadFrom(zeroCopyFile{bytes.NewReader(data)}); n != int64(len(data)) || err != nil {
		t.Fatalf("ReadFrom = %d, %v", n, err)
	}
	for _, ch := range inner.chunks {
		if ch > copyChunk {
			t.Fatalf("chunk of %d B, more than %d between checks", ch, copyChunk)
		}
	}
	want := int64(700 + len(data))
	if r, w := c.Meter().Total(); r != 300 || w != want {
		t.Fatalf("Meter = %d, %d; want 300, %d", r, w, want)
	}
	if r, w := traffic.Total(); r != 300 || w != want {
		t.Fatalf("Counter = %d, %d; want 300, %d", r, w, want)
	}
	_ = c.Close()
	_ = c.Close() // a second Close moves nothing twice
	_, _ = c.Write(make([]byte, 50))
	if r, w := traffic.Total(); r != 300 || w != want {
		t.Fatalf("Counter after Close = %d, %d; want the bytes until Close", r, w)
	}
}

// SetLimiters applies to a connection that is open, also to a write that
// waits: the wait ends and the write goes on at the new limit.
func TestConnSetLimitersHotSwap(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		inner := &recordingConn{}
		c := NewConn(inner)
		if c.write.state.Load() != 0 {
			t.Fatal("a connection without a limit is not on the fast path")
		}
		slow := NewRate(1000)
		slow.SetBurst(time.Millisecond)
		c.SetLimiters(nil, slow)
		done := make(chan error, 1)
		start := time.Now()
		go func() {
			_, err := c.Write(make([]byte, 100_000)) // 100 s at the slow limit
			done <- err
		}()
		time.Sleep(2 * time.Second)
		synctest.Wait()
		c.SetLimiters(nil, nil)
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if d := time.Since(start); d > 3*time.Second {
			t.Fatalf("write took %v after its limit was lifted", d)
		}
		if _, w := c.Meter().Total(); w != 100_000 {
			t.Fatalf("Meter = %d written, want 100000", w)
		}
		if c.write.state.Load() != 0 {
			t.Fatal("not back on the fast path")
		}
		_ = c.Close()
	})
}

// Connections that share a Budget together pass exactly what it has, and
// Used counts what passed, not what is leased.
func TestConnBudgetLeasesShared(t *testing.T) {
	b := stopAfter(1 << 20)
	var conns []*Conn
	for range 4 {
		conns = append(conns, NewConn(&recordingConn{}, WithLimiter(b)))
	}
	if _, err := conns[0].Write(make([]byte, 100)); err != nil {
		t.Fatal(err)
	}
	if used := b.Used(); used != 100 {
		t.Fatalf("Used() = %d with leases out, want the 100 B written", used)
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	var sent int64 = 100
	for _, c := range conns {
		wg.Go(func() {
			for {
				n, err := c.Write(make([]byte, 1000))
				mu.Lock()
				sent += int64(n)
				mu.Unlock()
				if err != nil {
					if !errors.Is(err, ErrBudgetExhausted) {
						t.Error(err)
					}
					return
				}
			}
		})
	}
	wg.Wait()
	if sent != 1<<20 || b.Used() != 1<<20 || b.Remaining() != 0 {
		t.Fatalf("sent %d, used %d, remaining %d; want %d, %d, 0", sent, b.Used(), b.Remaining(), 1<<20, 1<<20)
	}
	for _, c := range conns {
		_ = c.Close()
	}
	if b.Used() != 1<<20 {
		t.Fatalf("Used() after Close = %d", b.Used())
	}
}

// Closing gives back what a lease holds unused.
func TestConnBudgetCloseReturnsLease(t *testing.T) {
	b := stopAfter(10 << 20)
	c := NewConn(&recordingConn{}, WithLimiter(b))
	_, _ = c.Write(make([]byte, 10))
	if raw := b.per().used.Load(); raw <= 10 {
		t.Fatalf("raw usage %d, want a lease taken ahead", raw)
	}
	_ = c.Close()
	if raw := b.per().used.Load(); raw != 10 {
		t.Fatalf("raw usage after Close = %d, want 10", raw)
	}
}

// Once a step's Limit applies, the Budget is charged at every write and
// its Limit slows the writes down.
func TestConnBudgetStepLimit(t *testing.T) {
	limit := NewRate(1 << 40)
	b := NewBudget(Step{After: 5000, Limit: limit})
	var steps []int
	b.OnStep(func(step int, _ int64) { steps = append(steps, step) })
	c := NewConn(&recordingConn{}, WithLimiter(b))
	if _, err := c.Write(make([]byte, 4000)); err != nil || c.write.state.Load() != dirLeased {
		t.Fatalf("Write before the step = %v, state %d; want leases alone", err, c.write.state.Load())
	}
	if _, err := c.Write(make([]byte, 4000)); err != nil {
		t.Fatal(err)
	}
	if c.write.state.Load()&dirLimited == 0 {
		t.Fatal("the step's Limit does not apply")
	}
	if got := metered(limit); got != 3000 {
		t.Fatalf("step Limit charged %d, want the 3000 B past its After", got)
	}
	_ = c.Close()
	if b.Used() != 8000 || len(steps) != 1 {
		t.Fatalf("used %d, steps %v; want 8000, [0]", b.Used(), steps)
	}
}

// A Group's limits apply to its open connections when they change, and its
// Traffic adds up their Meters.
func TestGroupOpenFollowsChanges(t *testing.T) {
	root, user, _, tunnel := tree()
	c, err := tunnel.Open(&recordingConn{})
	if err != nil {
		t.Fatal(err)
	}
	if c.write.state.Load() != 0 {
		t.Fatal("a connection in a Group without limits is not on the fast path")
	}
	_, _ = c.Write(make([]byte, 100))
	setWrite(user, NewRate(1<<40))
	if c.write.state.Load() != dirLimited {
		t.Fatal("the user's limit does not apply to the open connection")
	}
	_, _ = c.Write(make([]byte, 50))
	other := NewGroup(root)
	if err := tunnel.Parent().SetParent(other); err != nil {
		t.Fatal(err)
	}
	if c.write.state.Load() != 0 {
		t.Fatal("the connection moved out from under the limit is still limited")
	}
	_, _ = c.Write(make([]byte, 25))
	for _, x := range []struct {
		g    *Group
		want int64
	}{{root, 175}, {user, 150}, {other, 25}, {tunnel, 175}} {
		if _, w := x.g.Traffic().Total(); w != x.want {
			t.Errorf("Traffic written = %d, want %d", w, x.want)
		}
	}
	_ = c.Close()
	if root.Conns() != 0 {
		t.Fatalf("Conns() = %d after Close", root.Conns())
	}
	if _, w := root.Traffic().Total(); w != 175 {
		t.Fatalf("root Traffic after Close = %d, want 175", w)
	}
}

// A PacketConn counts its datagrams, leases whole ones and drops those
// that its limit refuses at once.
func TestPacketConnMeterAndLease(t *testing.T) {
	pc := &scriptedPacketConn{}
	b := stopAfter(2500)
	c := NewPacketConn(pc, WithLimiter(b))
	for range 2 {
		if n, err := c.WriteTo(make([]byte, 1000), nil); n != 1000 || err != nil {
			t.Fatalf("WriteTo = %d, %v", n, err)
		}
	}
	if _, err := c.WriteTo(make([]byte, 1000), nil); !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("WriteTo past the Budget = %v, want ErrBudgetExhausted", err)
	}
	if _, w := c.Meter().Total(); w != 2000 || pc.written != 2000 {
		t.Fatalf("Meter = %d, sent %d; want 2000 whole datagrams", w, pc.written)
	}
	_ = c.Close()

	r := NewRate(1000)
	r.SetBurst(time.Second)
	d := NewPacketConn(&scriptedPacketConn{}, WithLimiter(r), WithDrop())
	var passed int
	for range 5 {
		if n, err := d.WriteTo(make([]byte, 600), nil); n != 600 || err != nil {
			t.Fatalf("WriteTo with drop = %d, %v", n, err)
		}
	}
	_, passed64 := d.Meter().Total()
	passed = int(passed64)
	if passed == 0 || passed >= 3000 {
		t.Fatalf("%d B passed of 3000, want some dropped", passed)
	}
	_ = d.Close()
}

// scriptedPacketConn takes every datagram and reads 100 B ones.
type scriptedPacketConn struct {
	net.PacketConn
	written int
}

func (p *scriptedPacketConn) WriteTo(b []byte, _ net.Addr) (int, error) {
	p.written += len(b)
	return len(b), nil
}
func (*scriptedPacketConn) ReadFrom(b []byte) (int, net.Addr, error) {
	return min(len(b), 100), nil, nil
}
func (*scriptedPacketConn) Close() error { return nil }

// The fast and the limited paths, alone and together, give the same
// results: what arrives, what each Meter, Counter, Group and Budget counts,
// also when the limit changes mid-transfer, when the Budget takes its
// leases back and lends them to another connection meanwhile, and through
// the zero-copy paths between sockets.
func TestConnFeatureCombinations(t *testing.T) {
	const size = 1<<20 + 12345
	for _, limited := range []bool{false, true} {
		for _, budget := range []bool{false, true} {
			for _, group := range []bool{false, true} {
				for _, swap := range []bool{false, true} {
					for _, revoke := range []bool{false, true} {
						if revoke && !budget {
							continue
						}
						for _, how := range []string{"Write", "ReadFrom", "WriteTo"} {
							name := fmt.Sprintf("limited=%v/budget=%v/group=%v/swap=%v/revoke=%v/%s",
								limited, budget, group, swap, revoke, how)
							t.Run(name, func(t *testing.T) {
								combination(t, size, limited, budget, group, swap, revoke, how)
							})
						}
					}
				}
			}
		}
	}
}

func combination(t *testing.T, size int, limited, budget, group, swap, revoke bool, how string) {
	client, server := tcpPair(t)
	var levels []Limiter
	if limited {
		levels = append(levels, NewRate(1<<40))
	}
	var b *Budget
	if budget {
		b = stopAfter(1 << 40)
		levels = append(levels, b)
	}
	var traffic Counter
	opts := []Option{WithCounter(&traffic), WithLimiter(NewHierarchy(levels...))}
	var c *Conn
	var g *Group
	if group {
		g = NewGroup(nil)
		var err error
		if c, err = g.Open(client, opts...); err != nil {
			t.Fatal(err)
		}
	} else {
		c = NewConn(client, opts...)
	}
	data := bytes.Repeat([]byte("0123456789abcdef"), size/16+1)[:size]
	got := make(chan []byte, 1)
	go func() {
		b, _ := io.ReadAll(server)
		got <- b
	}()
	var swapper sync.WaitGroup
	other := int64(0) // what another connection of the Budget wrote
	if revoke {
		// Another connection takes leases of the Budget, which takes them
		// back, and those of c, at every change of its steps and expiry.
		o := NewConn(&recordingConn{}, WithLimiter(b))
		defer func() { _ = o.Close() }()
		swapper.Go(func() {
			steps := b.Steps()
			for i := range 200 {
				switch i % 3 {
				case 0:
					b.SetSteps(steps...)
				case 1:
					b.SetExpiry(time.Now().Add(time.Hour))
				default:
					b.SetExpiry(time.Time{})
				}
				m, _ := o.Write(make([]byte, 100))
				other += int64(m)
			}
		})
	}
	if swap {
		swapper.Go(func() {
			for i := range 20 {
				var l Limiter
				if i%2 == 0 {
					l = NewHierarchy(NewRate(1<<40), b)
				} else {
					l = NewHierarchy(levels...)
				}
				c.SetLimiters(l, l)
				time.Sleep(100 * time.Microsecond)
			}
		})
	}
	var n int64
	var err error
	switch how {
	case "Write":
		for off := 0; off < size && err == nil; {
			m := 0
			m, err = c.Write(data[off:min(off+50_000, size)])
			off += m
			n += int64(m)
		}
	case "ReadFrom":
		src, dst := tcpPair(t)
		go func() {
			_, _ = src.Write(data)
			_ = src.Close()
		}()
		n, err = c.ReadFrom(dst)
	case "WriteTo":
		// Read through a wrapper of the receiving side, copying on to the
		// sender's raw socket.
		src, dst := tcpPair(t)
		go func() {
			_, _ = src.Write(data)
			_ = src.Close()
		}()
		rc := NewConn(dst, opts...)
		n, err = rc.WriteTo(client)
		defer func() { _ = rc.Close() }()
		if r, _ := rc.Meter().Total(); r != int64(size) {
			t.Errorf("reader Meter = %d, want %d", r, size)
		}
	}
	swapper.Wait()
	if err != nil || n != int64(size) {
		t.Fatalf("copied %d, %v; want %d", n, err, size)
	}
	_ = c.CloseWrite()
	if b := <-got; !bytes.Equal(b, data) {
		t.Fatalf("peer got %d B, not the data sent", len(b))
	}
	_, w := c.Meter().Total()
	if how == "WriteTo" {
		w = 0 // the bytes went to the raw socket
	}
	want := int64(size)
	if how == "WriteTo" {
		want = 0
	}
	if w != want {
		t.Fatalf("Meter written = %d, want %d", w, want)
	}
	cr, cw := traffic.Total()
	if how == "WriteTo" {
		if cr != int64(size) {
			t.Fatalf("Counter read = %d, want %d", cr, size)
		}
	} else if cw != int64(size) {
		t.Fatalf("Counter written = %d, want %d", cw, size)
	}
	if g != nil {
		if _, gw := g.Traffic().Total(); gw != want {
			t.Fatalf("Group written = %d, want %d", gw, want)
		}
	}
	_ = c.Close()
	if b != nil && b.Used() != int64(size)+other {
		t.Fatalf("Budget used %d, want %d", b.Used(), int64(size)+other)
	}
}

// Lease takes up to a step or the stop, refuses at the stop and while a
// step's Limit applies, and Return and Settle give back what was not used.
func TestBudgetLeaseAPI(t *testing.T) {
	b := NewBudget(Step{After: 1000}, Step{After: 3000, Limit: NewRate(1)}, Step{After: 5000, Stop: true})
	l, err := b.Lease(5000)
	if err != nil || l.Bytes != 1000 {
		t.Fatalf("Lease(5000) = %d, %v; want 1000 up to the first step", l.Bytes, err)
	}
	b.Settle(l, 400)
	if b.Used() != 400 {
		t.Fatalf("Used() after Settle(400) = %d", b.Used())
	}
	l, _ = b.Lease(10_000)
	if l.Bytes != 600 {
		t.Fatalf("Lease = %d, want the 600 B to the step", l.Bytes)
	}
	l, _ = b.Lease(10_000)
	if l.Bytes != 2000 {
		t.Fatalf("Lease = %d, want the 2000 B of the step", l.Bytes)
	}
	if _, err := b.Lease(1); !errors.Is(err, ErrWouldWait) {
		t.Fatalf("Lease in a step with a Limit = %v, want ErrWouldWait", err)
	}
	b.Return(l, 500)
	if b.Used() != 2500 {
		t.Fatalf("Used() after Return(500) = %d, want 2500", b.Used())
	}
	b.SetUsed(5000)
	if _, err := b.Lease(1); !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("Lease at the stop = %v", err)
	}
	b.Reset()
	b.Return(l, 1000) // of the last period: nothing
	if b.Used() != 0 {
		t.Fatalf("Return across a Reset changed Used to %d", b.Used())
	}
	if l, err := (*Budget)(nil).Lease(7); l.Bytes != 7 || err != nil {
		t.Fatalf("nil Budget Lease = %d, %v", l.Bytes, err)
	}
}

// A Budget reads its expiry on the Clock it is given.
func TestBudgetClock(t *testing.T) {
	clk := &fakeClock{now: time.Unix(1000, 0)}
	b := NewBudget()
	b.SetClock(clk)
	b.SetExpiry(clk.now.Add(time.Hour))
	if _, err := b.Lease(10); err != nil {
		t.Fatal(err)
	}
	clk.advance(time.Hour)
	if _, err := b.Lease(10); !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("Lease after the fake expiry = %v", err)
	}
}

// Closing a connection takes it out of every registry it joined, so that
// nothing keeps it, and leaves no goroutine behind.
func TestConnCloseLeavesNothing(t *testing.T) {
	before := runtime.NumGoroutine()
	root, user, _, tunnel := tree()
	b := stopAfter(1 << 30)
	setWrite(user, NewHierarchy(NewRate(1<<40), b))
	var traffic Counter
	for range 50 {
		c, err := tunnel.Open(&recordingConn{}, WithCounter(&traffic))
		if err != nil {
			t.Fatal(err)
		}
		_, _ = c.Write(make([]byte, 1000))
		_ = c.SetDeadline(time.Now().Add(time.Hour))
		c.SetLimiters(NewRate(1<<40), nil)
		_ = c.Close()
	}
	for _, g := range []*Group{root, user, tunnel} {
		if n := g.Traffic().nsrc.Load(); n != 0 {
			t.Fatalf("%d Meters left attached to a Group", n)
		}
	}
	if traffic.nsrc.Load() != 0 || b.lessees.n.Load() != 0 {
		t.Fatalf("%d Meters attached, %d lessees left", traffic.nsrc.Load(), b.lessees.n.Load())
	}
	if b.per().used.Load() != 50_000 {
		t.Fatalf("raw usage %d, want the 50000 B written and no lease kept", b.per().used.Load())
	}
	deadline := time.Now().Add(5 * time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := runtime.NumGoroutine(); n > before {
		t.Fatalf("%d goroutines, %d before", n, before)
	}
}
