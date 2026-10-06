package byterate

import (
	"context"
	"errors"
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

func TestNewHierarchicalLimiterReturnsUntypedNilWhenNoRateEnabled(t *testing.T) {
	disabled := NewRate(0)
	cases := map[string]Limiter{
		"none":     NewHierarchicalLimiter(),
		"nil":      NewHierarchicalLimiter(nil),
		"disabled": NewHierarchicalLimiter(disabled),
		"two":      NewHierarchicalLimiter2(nil, disabled),
		"three":    NewHierarchicalLimiter3(nil, disabled, nil),
		"four":     NewHierarchicalLimiter(nil, disabled, nil, disabled),
	}
	for name, l := range cases {
		if l != nil {
			t.Fatalf("%s: got %T(%v), want untyped nil", name, l, l)
		}
	}

	enabled := NewRate(1024)
	if l := NewHierarchicalLimiter(enabled); l != Limiter(enabled) {
		t.Fatalf("single enabled: got %#v, want the rate itself", l)
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
	if got := first.bytesAcc.Load(); got != 2 {
		t.Fatalf("first bytesAcc after Write() = %d, want 2", got)
	}
	if got := second.bytesAcc.Load(); got != 2 {
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

func TestHierarchicalLimiterGetContextCancel(t *testing.T) {
	first := NewRate(1024)
	second := NewRate(2048)
	limiter := NewHierarchicalLimiter2(first, second).(*HierarchicalLimiter)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- limiter.GetContext(ctx, 64<<10)
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("GetContext() error = %v, want %v", err, context.Canceled)
		}
	case <-time.After(500 * time.Millisecond):
		first.Stop()
		second.Stop()
		t.Fatal("HierarchicalLimiter.GetContext() did not return promptly after cancel")
	}
}
