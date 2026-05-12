package byterate

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewHierarchicalLimiterCollapsesSingleEnabledRate(t *testing.T) {
	enabled := NewRate(64 * 1024)
	enabled.Start()
	disabled := NewRate(0)
	disabled.Start()

	limiter := NewHierarchicalLimiter(nil, disabled, enabled)
	if limiter != enabled {
		t.Fatalf("NewHierarchicalLimiter() = %#v, want enabled rate pointer", limiter)
	}
}

func TestHierarchicalLimiterRefundsAllChildrenOnShortWrite(t *testing.T) {
	first := NewRate(1 << 20)
	first.Start()
	second := NewRate(2 << 20)
	second.Start()

	limiter := NewHierarchicalLimiter(first, second)
	if _, ok := limiter.(*HierarchicalLimiter); !ok {
		t.Fatalf("NewHierarchicalLimiter() type = %T, want *HierarchicalLimiter", limiter)
	}

	wantErr := errors.New("short write")
	conn := NewRateConn(&scriptedConn{writeN: 2, writeErr: wantErr}, limiter)
	n, err := conn.Write([]byte("hello"))
	if !errors.Is(err, wantErr) {
		t.Fatalf("Write() error = %v, want %v", err, wantErr)
	}
	if n != 2 {
		t.Fatalf("Write() n = %d, want 2", n)
	}
	if got := atomic.LoadInt64(&first.bytesAcc); got != 2 {
		t.Fatalf("first bytesAcc after Write() = %d, want 2", got)
	}
	if got := atomic.LoadInt64(&second.bytesAcc); got != 2 {
		t.Fatalf("second bytesAcc after Write() = %d, want 2", got)
	}
}

func TestHierarchicalLimiterWakeOnPrimaryRateStop(t *testing.T) {
	first := NewRate(1)
	first.Start()
	second := NewRate(1)
	second.Start()

	limiter := NewHierarchicalLimiter(first, second)
	done := make(chan struct{})
	start := time.Now()
	go func() {
		limiter.Get(10)
		close(done)
	}()

	time.Sleep(20 * time.Millisecond)
	first.Stop()

	select {
	case <-done:
		if elapsed := time.Since(start); elapsed > 250*time.Millisecond {
			t.Fatalf("HierarchicalLimiter.Get() returned after %s, want fast wake on stop", elapsed)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("HierarchicalLimiter.Get() did not wake after primary rate stop")
	}
}

func TestHierarchicalLimiterWakeOnFirstEnabledRateStop(t *testing.T) {
	disabled := NewRate(0)
	disabled.Start()
	enabled := NewRate(1)
	enabled.Start()

	limiter := NewHierarchicalLimiter2(disabled, enabled)
	done := make(chan struct{})
	start := time.Now()
	go func() {
		limiter.Get(10)
		close(done)
	}()

	time.Sleep(20 * time.Millisecond)
	enabled.Stop()

	select {
	case <-done:
		if elapsed := time.Since(start); elapsed > 250*time.Millisecond {
			t.Fatalf("HierarchicalLimiter.Get() returned after %s, want fast wake on enabled stop", elapsed)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("HierarchicalLimiter.Get() did not wake after enabled rate stop")
	}
}

func TestHierarchicalLimiterWakeOnMaxWaitLimiterStop(t *testing.T) {
	fast := NewRate(1 << 20)
	fast.Start()
	slow := NewRate(1)
	slow.Start()

	limiter := NewHierarchicalLimiter2(fast, slow)
	done := make(chan struct{})
	start := time.Now()
	go func() {
		limiter.Get(10)
		close(done)
	}()

	time.Sleep(20 * time.Millisecond)
	slow.Stop()

	select {
	case <-done:
		if elapsed := time.Since(start); elapsed > 250*time.Millisecond {
			t.Fatalf("HierarchicalLimiter.Get() returned after %s, want fast wake on max-wait limiter stop", elapsed)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("HierarchicalLimiter.Get() did not wake after max-wait limiter stop")
	}
}
