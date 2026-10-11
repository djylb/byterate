package byterate

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func BenchmarkRateConnWrite(b *testing.B) {
	for _, levels := range []int{1, 3, 5} {
		b.Run(fmt.Sprint(levels), func(b *testing.B) {
			rates := make([]*Rate, levels)
			for i := range rates {
				rates[i] = NewRate(benchLimit)
			}
			c := NewRateConn(&nopNetConn{}, NewHierarchicalLimiter(rates...))
			defer func() { _ = c.Close() }()
			buf := make([]byte, 32<<10)
			b.ReportAllocs()
			for b.Loop() {
				_, _ = c.Write(buf)
			}
		})
	}
}

func BenchmarkCanceledGetContext(b *testing.B) {
	r := NewRate(1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	b.ReportAllocs()
	for b.Loop() {
		_ = r.GetContext(ctx, 3)
	}
}

func BenchmarkRateRefund(b *testing.B) {
	r := NewRate(benchLimit)
	b.ReportAllocs()
	for b.Loop() {
		r.Get(32 << 10)
		r.ReturnBucket(32 << 10)
	}
}

// BenchmarkRateRefundShared charges and refunds one Rate from every
// goroutine, the worst case for the lock ReturnBucket takes.
func BenchmarkRateRefundShared(b *testing.B) {
	r := NewRate(benchLimit)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			r.Get(32 << 10)
			r.ReturnBucket(32 << 10)
		}
	})
}

// benchLimit is a limit that never makes a 32 KiB charge wait.
const benchLimit = 1 << 50

func BenchmarkRateGet(b *testing.B) {
	for _, limit := range []int64{0, benchLimit} {
		name := "unlimited"
		if limit > 0 {
			name = "limited"
		}
		b.Run(name, func(b *testing.B) {
			r := NewRate(limit)
			for b.Loop() {
				r.Get(32 << 10)
			}
		})
	}
}

// BenchmarkRateGetShared charges one Rate from every goroutine, as a global
// limit does.
func BenchmarkRateGetShared(b *testing.B) {
	r := NewRate(benchLimit)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			r.Get(32 << 10)
		}
	})
}

// BenchmarkHierarchicalGet charges a connection Rate, one of four user Rates
// and a global Rate.
func BenchmarkHierarchicalGet(b *testing.B) {
	b.Run("single", func(b *testing.B) {
		l := NewHierarchicalLimiter(NewRate(benchLimit), NewRate(benchLimit), NewRate(benchLimit))
		for b.Loop() {
			l.Get(32 << 10)
		}
	})
	b.Run("parallel", func(b *testing.B) {
		global := NewRate(benchLimit)
		users := []*Rate{NewRate(benchLimit), NewRate(benchLimit), NewRate(benchLimit), NewRate(benchLimit)}
		var next atomic.Int64
		b.RunParallel(func(pb *testing.PB) {
			user := users[next.Add(1)%int64(len(users))]
			l := NewHierarchicalLimiter(NewRate(benchLimit), user, global)
			for pb.Next() {
				l.Get(32 << 10)
			}
		})
	})
}

func BenchmarkRateNow(b *testing.B) {
	r := NewRate(benchLimit)
	for b.Loop() {
		_ = r.Now()
	}
}

func BenchmarkMeterAdd(b *testing.B) {
	b.Run("single", func(b *testing.B) {
		m := NewMeter()
		for b.Loop() {
			m.Add(32<<10, 0)
		}
	})
	b.Run("parallel", func(b *testing.B) {
		m := NewMeter()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				m.Add(32<<10, 0)
			}
		})
	})
}

func BenchmarkNewRateConn(b *testing.B) {
	c, r := &nopNetConn{}, NewRate(benchLimit)
	b.ReportAllocs()
	for b.Loop() {
		_ = NewRateConn(c, r)
	}
}

// BenchmarkRateConnSetDeadline sets a deadline before every operation, as
// servers with idle timeouts do.
func BenchmarkRateConnSetDeadline(b *testing.B) {
	c := NewRateConn(&nopNetConn{}, NewRate(benchLimit))
	b.ReportAllocs()
	for b.Loop() {
		_ = c.SetReadDeadline(time.Now().Add(time.Minute))
	}
}

// BenchmarkRateConnWriteShared writes small packets on one connection per
// goroutine, each charged to its own Rate, one of four user Rates and a
// global Rate, as a proxy does.
func BenchmarkRateConnWriteShared(b *testing.B) {
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
				c := NewRateConn(&nopNetConn{}, NewHierarchicalLimiter(NewRate(limit), user, global))
				defer func() { _ = c.Close() }()
				buf := make([]byte, 1500)
				for pb.Next() {
					_, _ = c.Write(buf)
				}
			})
		})
	}
}
