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

// writeUntil writes chunks of size through c until it fails, and returns
// the bytes written and the error.
func writeUntil(c *Stream, size int) (int64, error) {
	var n int64
	for {
		m, err := c.Write(make([]byte, size))
		n += int64(m)
		if err != nil {
			return n, err
		}
	}
}

// An idle connection's lease does not keep the others from the Budget:
// before refusing, it takes back what the leases hold unused.
func TestBudgetReclaimsIdleLeases(t *testing.T) {
	const stop = 100 << 10
	b := stopAfter(stop)
	idle := NewWriter(&recordingConn{}, WithLimiter(b))
	if _, err := idle.Write(make([]byte, 10)); err != nil {
		t.Fatal(err)
	}
	busy := NewWriter(&recordingConn{}, WithLimiter(b))
	n, err := writeUntil(busy, 1024)
	if !errors.Is(err, ErrBudgetExhausted) || n != stop-10 {
		t.Fatalf("wrote %d, %v with an idle lease out; want %d, ErrBudgetExhausted", n, err, stop-10)
	}
	if b.Used() != stop || b.Remaining() != 0 {
		t.Fatalf("Used %d, Remaining %d; want %d, 0", b.Used(), b.Remaining(), stop)
	}
	// A charge of the Budget as a Limiter takes leases back too.
	c := stopAfter(stop)
	holder := NewWriter(&recordingConn{}, WithLimiter(c))
	if _, err := holder.Write(make([]byte, 10)); err != nil {
		t.Fatal(err)
	}
	if err := c.Wait(bg, stop-10); err != nil {
		t.Fatalf("Wait for what the lease holds unused = %v", err)
	}
	if _, err := holder.Write(make([]byte, 1)); !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("Write after the rest was charged = %v", err)
	}
	_ = idle.Close()
	_ = busy.Close()
	_ = holder.Close()
	if b.Used() != stop || c.Used() != stop {
		t.Fatalf("Used after Close = %d, %d; want %d", b.Used(), c.Used(), stop)
	}
}

// SetUsed, Reset, SetSteps and the expiry take the leases back at once.
func TestBudgetChangesRevokeLeases(t *testing.T) {
	const stop = 1 << 20
	b := stopAfter(stop)
	c := NewWriter(&recordingConn{}, WithLimiter(b))
	if _, err := c.Write(make([]byte, 10)); err != nil {
		t.Fatal(err)
	}
	b.SetUsed(stop) // cut off
	if n, err := c.Write(make([]byte, 1024)); n != 0 || !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("Write after SetUsed at the stop = %d, %v", n, err)
	}
	b.Reset()
	if _, err := c.Write(make([]byte, 1000)); err != nil {
		t.Fatal(err)
	}
	if b.Used() != 1000 {
		t.Fatalf("Used after Reset and 1000 B = %d", b.Used())
	}
	b.SetSteps(Step{After: 1500, Stop: true}) // a lower stop
	n, err := writeUntil(c, 100)
	if n != 500 || !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("wrote %d, %v after a lower stop; want 500", n, err)
	}
	b.SetSteps()
	if _, err := c.Write(make([]byte, 100)); err != nil {
		t.Fatal(err)
	}
	_ = c.Close()
	if b.Used() != 1600 {
		t.Fatalf("Used after Close = %d, want 1600", b.Used())
	}
}

// The expiry takes leases back when it passes, by a timer, and with a
// Clock the wrappers check it at every write.
func TestBudgetExpiryRevokesLeases(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := NewBudget()
		c := NewWriter(&recordingConn{}, WithLimiter(b))
		defer func() { _ = c.Close() }()
		b.SetExpiry(time.Now().Add(time.Second))
		if _, err := c.Write(make([]byte, 10)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second)
		synctest.Wait()
		if n, err := c.Write(make([]byte, 10)); n != 0 || !errors.Is(err, ErrBudgetExhausted) {
			t.Fatalf("Write after the expiry = %d, %v", n, err)
		}
		b.SetExpiry(time.Time{})
		if _, err := c.Write(make([]byte, 10)); err != nil {
			t.Fatalf("Write without expiry = %v", err)
		}
	})
	clk := &fakeClock{now: time.Unix(1000, 0)}
	b := NewBudget()
	b.SetClock(clk)
	c := NewWriter(&recordingConn{}, WithLimiter(b))
	defer func() { _ = c.Close() }()
	b.SetExpiry(clk.now.Add(time.Hour))
	if _, err := c.Write(make([]byte, 10)); err != nil {
		t.Fatal(err)
	}
	clk.advance(time.Hour)
	if n, err := c.Write(make([]byte, 10)); n != 0 || !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("Write after the fake expiry = %d, %v", n, err)
	}
	b.SetClock(nil)
	b.SetExpiry(time.Time{})
	if _, err := c.Write(make([]byte, 10)); err != nil {
		t.Fatal(err)
	}
	if c.write.state.Load() != dirLeased {
		t.Fatalf("state %b without expiry, want the leases alone", c.write.state.Load())
	}
}

// A Budget leaves the charged path once a Reset leaves the step whose Limit
// applied.
func TestBudgetBackToLeases(t *testing.T) {
	b := NewBudget(Step{After: 1000, Limit: NewRate(1 << 40)})
	c := NewWriter(&recordingConn{}, WithLimiter(b))
	defer func() { _ = c.Close() }()
	for range 3 {
		if _, err := c.Write(make([]byte, 1000)); err != nil {
			t.Fatal(err)
		}
	}
	if c.write.state.Load()&dirLimited == 0 {
		t.Fatal("not charged in the step with a Limit")
	}
	b.Reset()
	if _, err := c.Write(make([]byte, 10)); err != nil {
		t.Fatal(err)
	}
	if c.write.state.Load() != dirLeased {
		t.Fatalf("state %b after Reset, want the leases alone", c.write.state.Load())
	}
	if b.Used() != 10 {
		t.Fatalf("Used = %d, want 10", b.Used())
	}
}

// A small credit left leases more before the write, rather than splitting
// it.
func TestBudgetSmallCreditPrefetches(t *testing.T) {
	b := stopAfter(32 << 10)
	rec := &recordingConn{}
	c := NewWriter(rec, WithLimiter(b))
	defer func() { _ = c.Close() }()
	if _, err := c.Write(make([]byte, 1)); err != nil { // leases 16 KiB
		t.Fatal(err)
	}
	if _, err := c.Write(make([]byte, 16<<10-6)); err != nil { // leaves 5
		t.Fatal(err)
	}
	if _, err := c.Write(make([]byte, 100)); err != nil {
		t.Fatal(err)
	}
	if last := rec.writes[len(rec.writes)-1]; last != 100 || len(rec.writes) != 3 {
		t.Fatalf("writes %v, want the 100 B whole", rec.writes)
	}
}

// Leases, their reclaim and revocation, and periods ending at any time
// keep the accounts exact: concurrent writers of a shared Budget never pass
// its stop together, and what passes after a Reset counts in the new
// period.
func TestBudgetLeasesUnderChanges(t *testing.T) {
	const writers = 8
	b := stopAfter(1 << 40)
	conns := make([]*Stream, writers)
	for i := range conns {
		conns[i] = NewWriter(&recordingConn{}, WithLimiter(b))
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i, c := range conns {
		wg.Go(func() {
			for j := 0; ; j++ {
				select {
				case <-stop:
					return
				default:
				}
				_, _ = c.Write(make([]byte, 1+(i*977+j*131)%5000))
			}
		})
	}
	wg.Go(func() {
		for i := range 300 {
			switch i % 4 {
			case 0:
				b.Reset()
			case 1:
				b.SetUsed(int64(i) << 10)
			case 2:
				b.SetSteps(Step{After: 1 << 40, Stop: true})
			default:
				_ = b.Wait(bg, 100)
			}
			runtime.Gosched()
		}
		close(stop)
	})
	wg.Wait()
	// Exact from a Reset on: writers race for the last bytes.
	const limit = 3<<20 + 17
	b.SetSteps(Step{After: limit, Stop: true})
	b.Reset()
	sent := make([]int64, writers)
	for i, c := range conns {
		wg.Go(func() {
			n, err := writeUntil(c, 1+i*700)
			sent[i] = n
			if !errors.Is(err, ErrBudgetExhausted) {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	var total int64
	for _, n := range sent {
		total += n
	}
	if total > limit || b.Used() != total {
		t.Fatalf("writers sent %d, Used %d; want both the same and at most %d", total, b.Used(), limit)
	}
	for _, c := range conns {
		_ = c.Close()
	}
	if b.Used() != total || b.lessees.n.Load() != 0 {
		t.Fatalf("Used after Close %d, want %d; %d lessees left", b.Used(), total, b.lessees.n.Load())
	}
}

// A wrapper dropped without Close leaves its Counters, Budgets and Group
// once collected, keeping what it counted.
func TestDroppedWrapperIsSettled(t *testing.T) {
	var traffic Counter
	b := stopAfter(1 << 30)
	g := NewGroup(nil)
	func() {
		c, err := g.OpenStream(nopRWC{}, WithCounter(&traffic), WithLimiter(b))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.Write(make([]byte, 100)); err != nil {
			t.Fatal(err)
		}
	}()
	deadline := time.Now().Add(10 * time.Second)
	for traffic.nsrc.Load() != 0 || b.lessees.n.Load() != 0 || g.Conns() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("not settled: %d Meters, %d lessees, %d conns", traffic.nsrc.Load(), b.lessees.n.Load(), g.Conns())
		}
		runtime.GC()
		time.Sleep(time.Millisecond)
	}
	if _, w := traffic.Total(); w != 100 || b.Used() != 100 {
		t.Fatalf("Counter %d, Budget %d; want 100", w, b.Used())
	}
	if _, w := g.Traffic().Total(); w != 100 || g.Traffic().nsrc.Load() != 0 {
		t.Fatalf("Group Traffic %d, %d Meters", w, g.Traffic().nsrc.Load())
	}
}

type nopRWC struct{}

func (nopRWC) Read(b []byte) (int, error)  { return len(b), nil }
func (nopRWC) Write(b []byte) (int, error) { return len(b), nil }
func (nopRWC) Close() error                { return nil }

// A Counter shared by Groups, or given to a connection as well, counts
// each connection where it is: moving one Group moves its connections
// alone.
func TestSharedTrafficCounter(t *testing.T) {
	var shared Counter
	p, q := NewGroup(nil), NewGroup(nil)
	g1, g2 := NewGroup(p), NewGroup(nil)
	g1.SetTraffic(&shared)
	g2.SetTraffic(&shared)
	c1, _ := g1.OpenStream(nopRWC{})
	c2, _ := g2.OpenStream(nopRWC{}, WithCounter(&shared))
	defer func() { _ = c1.Close() }()
	defer func() { _ = c2.Close() }()
	write := func() {
		_, _ = c1.Write(make([]byte, 100))
		_, _ = c2.Write(make([]byte, 10))
	}
	write()
	if err := g1.SetParent(q); err != nil {
		t.Fatal(err)
	}
	write()
	if _, w := q.Traffic().Total(); w != 100 {
		t.Fatalf("new parent counted %d, want the 100 B of its own connection", w)
	}
	if _, w := p.Traffic().Total(); w != 100 {
		t.Fatalf("old parent counted %d, want 100", w)
	}
	var moved Counter
	g1.SetTraffic(&moved)
	write()
	if _, w := moved.Total(); w != 100 {
		t.Fatalf("moved Counter counted %d, want 100", w)
	}
	if _, w := shared.Total(); w != 230 {
		t.Fatalf("shared Counter counted %d, want 230", w)
	}
	// g2's Traffic and the connection's WithCounter are one Counter: it
	// counts the connection once, and still after g2 moves.
	if err := g2.SetParent(p); err != nil {
		t.Fatal(err)
	}
	g2.SetTraffic(&Counter{})
	write()
	if _, w := shared.Total(); w != 240 {
		t.Fatalf("shared Counter counted %d after g2 left it, want 240", w)
	}
}

// A Write that starts after SetUsed at the stop returned passes nothing,
// also while other writes of the connection race the cut-off.
func TestBudgetCutoffConcurrentWrites(t *testing.T) {
	for r := range 50 {
		const stop = 64 << 20
		b := stopAfter(stop)
		w := NewWriter(&recordingConn{}, WithLimiter(b))
		var cut atomic.Bool
		var after atomic.Int64
		var wg sync.WaitGroup
		for range 4 {
			wg.Go(func() {
				for range 3000 {
					c := cut.Load()
					n, err := w.Write(make([]byte, 100))
					if c {
						after.Add(int64(n))
					}
					if err != nil {
						return
					}
				}
			})
		}
		time.Sleep(time.Millisecond)
		b.SetUsed(stop)
		cut.Store(true)
		wg.Wait()
		_ = w.Close()
		// A write admitted before the cut-off, or one that failed as it
		// came, may still count; one at most per writer.
		if after.Load() != 0 || b.Used() > stop+4*100 {
			t.Fatalf("round %d: %d B written after the cut-off, Used %d", r, after.Load(), b.Used())
		}
	}
}
