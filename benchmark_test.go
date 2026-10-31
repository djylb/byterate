package byterate

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func BenchmarkConnWrite(b *testing.B) {
	for _, levels := range []int{1, 3, 5} {
		b.Run(fmt.Sprint(levels), func(b *testing.B) {
			rates := make([]*Rate, levels)
			for i := range rates {
				rates[i] = NewRate(benchLimit)
			}
			c := NewConn(&nopNetConn{}, WithLimiter(NewHierarchy(limiters(rates...)...)))
			defer func() { _ = c.Close() }()
			buf := make([]byte, 32<<10)
			b.ReportAllocs()
			for b.Loop() {
				_, _ = c.Write(buf)
			}
		})
	}
}

func BenchmarkConnRead(b *testing.B) {
	for _, levels := range []int{1, 3, 5} {
		b.Run(fmt.Sprint(levels), func(b *testing.B) {
			rates := make([]*Rate, levels)
			for i := range rates {
				rates[i] = NewRate(benchLimit)
			}
			c := NewConn(&recordingConn{}, WithLimiter(NewHierarchy(limiters(rates...)...)))
			defer func() { _ = c.Close() }()
			buf := make([]byte, 32<<10)
			b.ReportAllocs()
			for b.Loop() {
				_, _ = c.Read(buf)
			}
		})
	}
}

func BenchmarkCanceledWait(b *testing.B) {
	r := NewRate(1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	b.ReportAllocs()
	for b.Loop() {
		_ = r.Wait(ctx, 3)
	}
}

func BenchmarkRateRefund(b *testing.B) {
	r := NewRate(benchLimit)
	b.ReportAllocs()
	for b.Loop() {
		_ = r.Wait(bg, 32<<10)
		r.Refund(32 << 10)
	}
}

// BenchmarkRateRefundShared charges and refunds one Rate from every
// goroutine, the worst case for the lock Refund takes.
func BenchmarkRateRefundShared(b *testing.B) {
	r := NewRate(benchLimit)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = r.Wait(bg, 32<<10)
			r.Refund(32 << 10)
		}
	})
}

// benchLimit is a limit that never makes a 32 KiB charge wait.
const benchLimit = 1 << 50

func BenchmarkRateWait(b *testing.B) {
	for _, limit := range []int64{0, benchLimit} {
		name := "unlimited"
		if limit > 0 {
			name = "limited"
		}
		b.Run(name, func(b *testing.B) {
			r := NewRate(limit)
			for b.Loop() {
				_ = r.Wait(bg, 32<<10)
			}
		})
	}
}

// BenchmarkRateWaitShared charges one Rate from every goroutine, as a global
// limit does.
func BenchmarkRateWaitShared(b *testing.B) {
	r := NewRate(benchLimit)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = r.Wait(bg, 32<<10)
		}
	})
}

// BenchmarkHierarchyWait charges a connection Rate, one of four user Rates
// and a global Rate.
func BenchmarkHierarchyWait(b *testing.B) {
	b.Run("single", func(b *testing.B) {
		l := NewHierarchy(NewRate(benchLimit), NewRate(benchLimit), NewRate(benchLimit))
		for b.Loop() {
			_ = l.Wait(bg, 32<<10)
		}
	})
	b.Run("parallel", func(b *testing.B) {
		global := NewRate(benchLimit)
		users := []*Rate{NewRate(benchLimit), NewRate(benchLimit), NewRate(benchLimit), NewRate(benchLimit)}
		var next atomic.Int64
		b.RunParallel(func(pb *testing.PB) {
			user := users[next.Add(1)%int64(len(users))]
			l := NewHierarchy(NewRate(benchLimit), user, global)
			for pb.Next() {
				_ = l.Wait(bg, 32<<10)
			}
		})
	})
}

func BenchmarkCounterThroughput(b *testing.B) {
	var c Counter
	for b.Loop() {
		_, _ = c.Throughput()
	}
}

func BenchmarkNewConn(b *testing.B) {
	c, r := &nopNetConn{}, NewRate(benchLimit)
	b.ReportAllocs()
	for b.Loop() {
		_ = NewConn(c, WithLimiter(r))
	}
}

// BenchmarkConnSetDeadline sets a deadline before every operation, as
// servers with idle timeouts do.
func BenchmarkConnSetDeadline(b *testing.B) {
	c := NewConn(&nopNetConn{}, WithLimiter(NewRate(benchLimit)))
	b.ReportAllocs()
	for b.Loop() {
		_ = c.SetReadDeadline(time.Now().Add(time.Minute))
	}
}

// BenchmarkConnWriteShared writes small packets on one connection per
// goroutine, each charged to its own Rate, one of four user Rates and a
// global Rate, as a proxy does.
func BenchmarkConnWriteShared(b *testing.B) {
	for _, limit := range []int64{0, benchLimit} {
		name := "unlimited"
		if limit > 0 {
			name = "limited"
		}
		b.Run(name, func(b *testing.B) {
			global := NewRate(limit)
			users := []*Rate{NewRate(limit), NewRate(limit), NewRate(limit), NewRate(limit)}
			var next atomic.Int64
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				user := users[next.Add(1)%int64(len(users))]
				c := NewConn(&nopNetConn{}, WithLimiter(NewHierarchy(NewRate(limit), user, global)))
				defer func() { _ = c.Close() }()
				buf := make([]byte, 1500)
				for pb.Next() {
					_, _ = c.Write(buf)
				}
			})
		})
	}
}

// BenchmarkGroupWrite writes through a connection opened in the leaf of a
// tree of four Groups, each with a Rate and its traffic counted.
func BenchmarkGroupWrite(b *testing.B) {
	_, user, client, tunnel := tree()
	root := user.Parent()
	for _, g := range []*Group{root, user, client, tunnel} {
		setRead(g, NewRate(benchLimit))
		setWrite(g, NewRate(benchLimit))
	}
	c, err := tunnel.Open(&nopNetConn{})
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	buf := make([]byte, 32<<10)
	b.ReportAllocs()
	for b.Loop() {
		_, _ = c.Write(buf)
	}
}

// BenchmarkConnWriteNested writes through a Hierarchy of a Rate, a Budget
// whose first step has a Limit, and a Switch, so that the receipts of the
// Budget and the Switch are on the path.
func BenchmarkConnWriteNested(b *testing.B) {
	budget := NewBudget(Step{After: 0, Limit: NewRate(benchLimit)})
	sw := NewSwitch(NewRate(benchLimit))
	c := NewConn(&nopNetConn{}, WithLimiter(NewHierarchy(NewRate(benchLimit), budget, sw)))
	defer func() { _ = c.Close() }()
	buf := make([]byte, 32<<10)
	b.ReportAllocs()
	for b.Loop() {
		_, _ = c.Write(buf)
	}
}

// BenchmarkGroupOpenWrite writes through connections of users under one
// root, which only counts, while another goroutine takes and gives back a
// connection slot under the root without pause, so that the root's slot
// count changes under charges that read the root's chain. Only the writes
// are timed; more such goroutines would take processors from the writers.
func BenchmarkGroupOpenWrite(b *testing.B) {
	root := NewGroup(nil)
	root.SetTraffic(NewSharedCounter())
	stop := make(chan struct{})
	var churn sync.WaitGroup
	churner := NewGroup(root)
	churn.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			if churner.Acquire() == nil {
				churner.Release()
			}
		}
	})
	defer churn.Wait()
	defer close(stop)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		user := NewGroup(root)
		setWrite(user, NewRate(benchLimit))
		c, err := user.Open(&nopNetConn{})
		if err != nil {
			b.Error(err)
			return
		}
		defer func() { _ = c.Close() }()
		buf := make([]byte, 1500)
		for pb.Next() {
			_, _ = c.Write(buf)
		}
	})
}

// sinkConn takes every grant that a wrapper's ReadFrom hands it at once,
// as sendfile of a cached file nearly does.
type sinkConn struct{ nopNetConn }

func (*sinkConn) ReadFrom(r io.Reader) (int64, error) {
	lr := r.(*io.LimitedReader)
	n := lr.N
	lr.N = 0
	return n, nil
}

// BenchmarkConnReadFrom measures what a wrapper adds to each grant of a
// zero-copy io.Copy, 64 grants per ReadFrom.
func BenchmarkConnReadFrom(b *testing.B) {
	rf := any(NewConn(&sinkConn{}, WithLimiter(NewRate(benchLimit)))).(io.ReaderFrom)
	src := &io.LimitedReader{R: zeroCopyFile{bytes.NewReader(nil)}}
	b.ReportAllocs()
	for b.Loop() {
		src.N = 64 * copyChunk
		_, _ = rf.ReadFrom(src)
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(64*b.N), "ns/grant")
}

// BenchmarkTryWait admits datagrams without waiting, through a Rate alone
// and through a connection's, a user's and a global Rate.
func BenchmarkTryWait(b *testing.B) {
	b.Run("rate", func(b *testing.B) {
		r := NewRate(benchLimit)
		b.ReportAllocs()
		for b.Loop() {
			_ = r.TryWait(1500)
		}
	})
	b.Run("hierarchy", func(b *testing.B) {
		l := NewHierarchy(NewRate(benchLimit), NewRate(benchLimit), NewRate(benchLimit))
		b.ReportAllocs()
		for b.Loop() {
			_ = l.TryWait(1500)
		}
	})
}

// BenchmarkMeterWrite writes 512 B through a connection without a limit,
// as most connections are, so that only the Meter counts: raw is the
// wrapped connection alone, for comparison.
func BenchmarkMeterWrite(b *testing.B) {
	buf := make([]byte, 512)
	b.Run("raw", func(b *testing.B) {
		var c net.Conn = &nopNetConn{}
		b.ReportAllocs()
		for b.Loop() {
			_, _ = c.Write(buf)
		}
	})
	b.Run("meter", func(b *testing.B) {
		var c net.Conn = NewConn(&nopNetConn{})
		b.ReportAllocs()
		for b.Loop() {
			_, _ = c.Write(buf)
		}
	})
	b.Run("group", func(b *testing.B) {
		_, _, _, tunnel := tree()
		c, err := tunnel.Open(&nopNetConn{})
		if err != nil {
			b.Fatal(err)
		}
		defer func() { _ = c.Close() }()
		b.ReportAllocs()
		for b.Loop() {
			_, _ = c.Write(buf)
		}
	})
	b.Run("budget", func(b *testing.B) {
		c := NewConn(&nopNetConn{}, WithLimiter(NewBudget(Step{After: 1 << 62, Stop: true})))
		defer func() { _ = c.Close() }()
		b.ReportAllocs()
		for b.Loop() {
			_, _ = c.Write(buf)
		}
	})
	b.Run("limited", func(b *testing.B) {
		c := NewConn(&nopNetConn{}, WithLimiter(NewRate(1<<50)))
		defer func() { _ = c.Close() }()
		b.ReportAllocs()
		for b.Loop() {
			_, _ = c.Write(buf)
		}
	})
}

// BenchmarkMeterWriteParallel writes through a connection per goroutine,
// all in one Group tree and one shared Budget, as the users of a server do:
// counting must not slow down with the processors.
func BenchmarkMeterWriteParallel(b *testing.B) {
	_, _, _, tunnel := tree()
	budget := NewBudget(Step{After: 1 << 62, Stop: true})
	setWrite(tunnel, budget)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		c, err := tunnel.Open(&nopNetConn{})
		if err != nil {
			b.Error(err)
			return
		}
		defer func() { _ = c.Close() }()
		buf := make([]byte, 512)
		for pb.Next() {
			_, _ = c.Write(buf)
		}
	})
}

// BenchmarkMeterWriteParallelUnlimited is BenchmarkMeterWriteParallel
// without the Budget.
func BenchmarkMeterWriteParallelUnlimited(b *testing.B) {
	_, _, _, tunnel := tree()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		c, err := tunnel.Open(&nopNetConn{})
		if err != nil {
			b.Error(err)
			return
		}
		defer func() { _ = c.Close() }()
		buf := make([]byte, 512)
		for pb.Next() {
			_, _ = c.Write(buf)
		}
	})
}
