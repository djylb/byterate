package byterate

import (
	"testing"
	"time"
)

func TestRateStopUnblocksBlockedGet(t *testing.T) {
	r := NewRate(1)
	done := make(chan struct{})

	go func() {
		r.Get(10)
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)

	select {
	case <-done:
		t.Fatal("Get() returned before Stop() despite a long rate-limited wait")
	default:
	}

	r.Stop()

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Get() did not unblock promptly after Stop()")
	}
}
