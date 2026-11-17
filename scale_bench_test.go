package byterate

import (
	"runtime"
	"strconv"
	"testing"
)

// BenchmarkScaleOpenClose wraps and closes connections from every
// processor at once: alone, in a Group of a shared tree, and with a
// Counter. Run with -cpu 1,8 to see how it scales.
func BenchmarkScaleOpenClose(b *testing.B) {
	b.Run("plain", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_ = NewConn(&nopNetConn{}).Close()
			}
		})
	})
	b.Run("group", func(b *testing.B) {
		root := NewGroup(nil)
		setWrite(root, NewRate(benchLimit))
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			user := NewGroup(root)
			for pb.Next() {
				c, err := user.Open(&nopNetConn{})
				if err != nil {
					b.Error(err)
					return
				}
				_ = c.Close()
			}
		})
	})
	b.Run("counter", func(b *testing.B) {
		ctr := &Counter{}
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_ = NewConn(&nopNetConn{}, WithCounter(ctr)).Close()
			}
		})
	})
}

// BenchmarkScaleAcquire takes and gives back slots of a Group whose tree
// is shared by every processor.
func BenchmarkScaleAcquire(b *testing.B) {
	root := NewGroup(nil)
	b.RunParallel(func(pb *testing.PB) {
		g := NewGroup(NewGroup(root))
		for pb.Next() {
			if g.Acquire() == nil {
				g.Release()
			}
		}
	})
}

// BenchmarkScaleMemory reports the heap that n Groups, each with a
// connection's worth of traffic counted, or n shared Counters take, per
// item, at both ends of the scale.
func BenchmarkScaleMemory(b *testing.B) {
	for _, n := range []int{10, 1 << 20} {
		b.Run("groups-"+strconv.Itoa(n), func(b *testing.B) {
			for b.Loop() {
				root := NewGroup(nil)
				memPerItem(b, n, func(int) any {
					g := NewGroup(root)
					g.Traffic().Add(1, 1)
					return g
				})
			}
		})
		b.Run("counters-"+strconv.Itoa(n), func(b *testing.B) {
			for b.Loop() {
				memPerItem(b, n, func(int) any {
					c := new(Counter)
					c.Add(1, 1)
					return c
				})
			}
		})
	}
}

// BenchmarkScaleConnMemory reports the heap that each connection open in a
// tree of Groups three deep takes, at both ends of the scale.
func BenchmarkScaleConnMemory(b *testing.B) {
	for _, n := range []int{10, 100_000} {
		b.Run("conns-"+strconv.Itoa(n), func(b *testing.B) {
			for b.Loop() {
				root := NewGroup(nil)
				users := make([]*Group, 100)
				for i := range users {
					users[i] = NewGroup(NewGroup(root))
				}
				var conns []*Conn
				memPerItem(b, n, func(i int) any {
					c, err := users[i%len(users)].Open(&nopNetConn{})
					if err != nil {
						b.Fatal(err)
					}
					_, _ = c.Write(make([]byte, 1))
					conns = append(conns, c)
					return c
				})
				for _, c := range conns {
					_ = c.Close()
				}
			}
		})
	}
}

func memPerItem(b *testing.B, n int, mk func(int) any) {
	b.Helper()
	keep := make([]any, n)
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	for i := range keep {
		keep[i] = mk(i)
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	b.ReportMetric(float64(after.HeapAlloc-before.HeapAlloc)/float64(n), "B/item")
	runtime.KeepAlive(keep)
}
