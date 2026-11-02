package byterate

import (
	"runtime"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestBudgetAfterExpiry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var b Budget
		var ran atomic.Int32
		b.AfterExpiry(func() { ran.Add(1) })
		stopped := b.AfterExpiry(func() { ran.Add(100) })
		if !stopped() || stopped() {
			t.Fatal("stop before the expiry did not report the unregistration once")
		}
		b.SetExpiry(time.Now().Add(time.Minute))
		b.SetExpiry(time.Now().Add(2 * time.Minute)) // moved: still waits
		time.Sleep(90 * time.Second)
		synctest.Wait()
		if ran.Load() != 0 {
			t.Fatal("ran before the moved expiry")
		}
		time.Sleep(time.Minute)
		synctest.Wait()
		if ran.Load() != 1 {
			t.Fatalf("ran %d times at the expiry, want 1", ran.Load())
		}
		late := b.AfterExpiry(func() { ran.Add(1) })
		synctest.Wait()
		if ran.Load() != 2 || late() {
			t.Fatal("a func registered after the expiry did not run at once")
		}
		b.SetExpiry(time.Time{})
	})
}

func TestBudgetAfterExpiryClock(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clk := &fakeNow{t: time.Now()}
		var b Budget
		b.SetClock(clk)
		b.SetExpiry(clk.t.Add(time.Minute))
		var ran atomic.Int32
		b.AfterExpiry(func() { ran.Add(1) })
		clk.t = clk.t.Add(time.Minute)
		_ = b.Remaining()
		synctest.Wait()
		if ran.Load() != 1 {
			t.Fatal("a Clock-driven expiry did not run the func")
		}
	})
}

type fakeNow struct{ t time.Time }

func (c *fakeNow) Now() time.Time { return c.t }

func TestCounterSampleEverySecond(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := new(Counter)
		c.SampleEverySecond()
		c.SampleEverySecond()
		c.Add(1000, 2000)
		time.Sleep(1500 * time.Millisecond)
		c.Add(1000, 2000)
		time.Sleep(time.Second)
		// Read rarely: the ticker sampled each second.
		time.Sleep(3 * time.Second)
		if r, w := c.Throughput(); r != 0 || w != 0 {
			t.Fatalf("Throughput after idle seconds = %d, %d; want 0, 0", r, w)
		}
		sampler.mu.Lock()
		n := len(sampler.counters)
		sampler.mu.Unlock()
		if n != 1 {
			t.Fatalf("%d Counters sampled, want 1", n)
		}
		runtime.KeepAlive(c)
		c = nil
		runtime.GC()
		time.Sleep(2 * time.Second)
		synctest.Wait()
		sampler.mu.Lock()
		running := sampler.running
		sampler.mu.Unlock()
		if running {
			t.Fatal("the sampler kept running without Counters")
		}
	})
}
