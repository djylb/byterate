package byterate

import (
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// fairRun runs bulk busy writers and, after they fill the limit, small
// writes of one interactive wrapper every 100 ms, or reads if reads is set,
// in a bubble. It returns the worst latency of a small transfer and the
// bytes all passed in the measured second.
func fairRun(t *testing.T, fair, group, reads bool, bulk int) (worst time.Duration, total int64) {
	t.Helper()
	synctest.Test(t, func(t *testing.T) {
		const limit = 1 << 20
		r := NewRate(limit)
		r.SetFair(fair)
		_ = r.Wait(bg, 2*limit) // the burst
		var g *Group
		if group {
			g = NewGroup(nil)
			if reads {
				g.SetLimiters(r, nil)
			} else {
				g.SetLimiters(nil, r)
			}
		}
		open := func() *Conn {
			if g != nil {
				c, err := g.Open(&nopNetConn{})
				if err != nil {
					t.Fatal(err)
				}
				return c
			}
			if reads {
				return NewConn(&nopNetConn{}, WithLimiters(r, nil))
			}
			return NewConn(&nopNetConn{}, WithLimiters(nil, r))
		}
		var stop atomic.Bool
		var passed atomic.Int64
		var wg sync.WaitGroup
		for range bulk {
			c := open()
			wg.Go(func() {
				buf := make([]byte, 32<<10)
				for !stop.Load() {
					var n int
					if reads {
						n, _ = c.Read(buf)
					} else {
						n, _ = c.Write(buf)
					}
					passed.Add(int64(n))
				}
				_ = c.Close()
			})
		}
		time.Sleep(500 * time.Millisecond) // the bulk flows fill the limit
		passed.Store(0)
		start := time.Now()
		small := open()
		buf := make([]byte, 100)
		for range 10 {
			t0 := time.Now()
			var n int
			if reads {
				n, _ = small.Read(buf)
			} else {
				n, _ = small.Write(buf)
			}
			passed.Add(int64(n))
			worst = max(worst, time.Since(t0))
			time.Sleep(100*time.Millisecond - time.Since(t0))
		}
		total = passed.Load() * int64(time.Second) / int64(time.Since(start))
		stop.Store(true)
		_ = r.Wait(bg, 0)
		r.Disable() // ends the bulk waits
		wg.Wait()
		_ = small.Close()
	})
	return worst, total
}

// TestFairSparseFlowLatency checks that on a fair Rate a sparse flow's
// small transfers pass at once while busy flows keep the limit, for
// writes and reads, through a wrapper's own Limiter and a Group, and that
// without SetFair they queue behind the busy flows' grants.
func TestFairSparseFlowLatency(t *testing.T) {
	const limit = 1 << 20
	for _, group := range []bool{false, true} {
		for _, reads := range []bool{false, true} {
			name := map[bool]string{false: "own", true: "group"}[group] + "/" + map[bool]string{false: "writes", true: "reads"}[reads]
			t.Run(name, func(t *testing.T) {
				fifo, _ := fairRun(t, false, group, reads, 8)
				worst, total := fairRun(t, true, group, reads, 8)
				t.Logf("worst small-transfer latency: FIFO %v, fair %v; fair throughput %d B/s", fifo, worst, total)
				if fifo < 50*time.Millisecond {
					t.Fatalf("FIFO latency %v, want the busy flows' grants (>= 50ms)", fifo)
				}
				if worst > 5*time.Millisecond {
					t.Fatalf("fair latency %v, want at once (<= 5ms; FIFO %v)", worst, fifo)
				}
				// The limit holds: the measured second passes the limit,
				// plus at most the sparse lane's quantum.
				if total > limit*11/10 || total < limit*8/10 {
					t.Fatalf("%d B/s passed, want about %d", total, limit)
				}
			})
		}
	}
}

// TestFairBusyFlowGetsNoLane checks that a flow that keeps a fair Rate busy
// does not keep the sparse lane: after its first quantum it waits like the
// others, so it cannot pass more than its share.
func TestFairBusyFlowGetsNoLane(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const limit = 1 << 20
		r := NewRate(limit)
		r.SetFair(true)
		_ = r.Wait(bg, 2*limit)
		var stop atomic.Bool
		var counts [2]atomic.Int64
		var wg sync.WaitGroup
		for i := range counts {
			c := NewConn(&nopNetConn{}, WithLimiters(nil, r))
			wg.Go(func() {
				buf := make([]byte, 32<<10)
				for !stop.Load() {
					n, _ := c.Write(buf)
					counts[i].Add(int64(n))
				}
				_ = c.Close()
			})
		}
		time.Sleep(2 * time.Second)
		stop.Store(true)
		r.Disable()
		wg.Wait()
		a, b := counts[0].Load(), counts[1].Load()
		if a+b > 2*limit*12/10 {
			t.Fatalf("%d bytes in 2s, want about %d", a+b, 2*limit)
		}
		if a > 2*b || b > 2*a {
			t.Fatalf("busy flows got %d and %d bytes, want about equal", a, b)
		}
	})
}

func TestFairSetting(t *testing.T) {
	var r *Rate
	r.SetFair(true)
	if r.Fair() {
		t.Fatal("a nil Rate is fair")
	}
	r = NewRate(10)
	r.SetFair(true)
	if !r.Fair() || !r.Clone().Fair() {
		t.Fatal("SetFair or Clone lost the setting")
	}
}

// BenchmarkFairWrite measures a throttled write of a busy wrapper on a
// Rate with and without SetFair.
func BenchmarkFairWrite(b *testing.B) {
	for _, fair := range []bool{false, true} {
		b.Run(map[bool]string{false: "fifo", true: "fair"}[fair], func(b *testing.B) {
			r := NewRate(1 << 40)
			r.SetFair(fair)
			c := NewConn(&nopNetConn{}, WithLimiter(r))
			defer func() { _ = c.Close() }()
			buf := make([]byte, 1<<10)
			b.ReportAllocs()
			b.SetBytes(int64(len(buf)))
			for b.Loop() {
				_, _ = c.Write(buf)
			}
		})
	}
}
