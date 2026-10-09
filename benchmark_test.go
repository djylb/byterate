package byterate

import (
	"sync/atomic"
	"testing"
)

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
