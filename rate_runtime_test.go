package byterate

import (
	"testing"
	"time"
)

func TestRateDisableUnblocksBlockedWait(t *testing.T) {
	r := NewRate(1)
	done := make(chan struct{})

	go func() {
		_ = r.Wait(bg, 10)
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)

	select {
	case <-done:
		t.Fatal("Wait() returned before Disable() despite a long rate-limited wait")
	default:
	}

	r.Disable()

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Wait() did not unblock promptly after Disable()")
	}
}
