package byterate

import (
	"context"
	"errors"
	"io"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// waitBlocked reports whether ch stays empty for a moment.
func waitBlocked(ch <-chan error) bool {
	select {
	case err := <-ch:
		_ = err
		return false
	case <-time.After(50 * time.Millisecond):
		return true
	}
}

// recv returns what ch delivers, failing after a while.
func recv(t *testing.T, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("wait not woken")
		return nil
	}
}

// A paused step neither passes nor refuses: Wait and Ready block without a
// charge, TryWait would wait, and SetSteps lets them through.
func TestPauseHoldsUntilSetSteps(t *testing.T) {
	b := NewBudget(Step{After: 10, Pause: true})
	if err := b.Wait(bg, 10); err != nil {
		t.Fatal(err)
	}
	if err := b.TryWait(1); !errors.Is(err, ErrWouldWait) {
		t.Fatalf("TryWait = %v, want ErrWouldWait", err)
	}
	if _, err := b.Lease(1); !errors.Is(err, ErrWouldWait) {
		t.Fatalf("Lease = %v, want ErrWouldWait", err)
	}
	waited := make(chan error, 2)
	go func() { waited <- b.Wait(bg, 5) }()
	go func() { _, err := b.Ready(bg, 5); waited <- err }()
	if !waitBlocked(waited) {
		t.Fatal("a wait passed a pause")
	}
	if got := b.Used(); got != 10 {
		t.Fatalf("Used = %d while paused, want 10", got)
	}
	b.SetSteps()
	for range 2 {
		if err := recv(t, waited); err != nil {
			t.Fatal(err)
		}
	}
	if got := b.Used(); got != 15 {
		t.Fatalf("Used = %d, want 15", got)
	}
}

// The waits of a pause end with ctx, with its error as it is and nothing
// charged, and with a Stop, refused.
func TestPauseEndsWithCtxAndStop(t *testing.T) {
	b := NewBudget(Step{Pause: true})
	ctx, cancel := context.WithTimeout(bg, 20*time.Millisecond)
	defer cancel()
	if err := b.Wait(ctx, 1); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Wait = %v, want ctx's error", err)
	}
	h := NewHierarchy(NewRate(1<<20), b)
	if _, err := h.Ready(ctx, 1); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("hierarchy Ready = %v, want ctx's error", err)
	}
	if got := b.Used(); got != 0 {
		t.Fatalf("Used = %d, want 0", got)
	}
	waited := make(chan error, 1)
	go func() { waited <- h.Wait(bg, 1) }()
	if !waitBlocked(waited) {
		t.Fatal("a hierarchy wait passed a pause")
	}
	b.SetSteps(Step{Stop: true})
	if err := recv(t, waited); !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("Wait = %v, want ErrBudgetExhausted", err)
	}
}

// Reset, SetUsed, SetExpiry and the expiry itself wake the waits too.
func TestPauseWokenByChanges(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(b *Budget)
		want   error
	}{
		{"Reset", func(b *Budget) { b.Reset() }, nil},
		{"SetUsed", func(b *Budget) { b.SetUsed(1) }, nil},
		{"SetExpiry", func(b *Budget) { b.SetExpiry(time.Now().Add(-time.Second)) }, ErrBudgetExhausted},
		{"expiry", func(b *Budget) { b.SetExpiry(time.Now().Add(30 * time.Millisecond)) }, ErrBudgetExhausted},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := NewBudget(Step{After: 5, Pause: true})
			b.SetUsed(5)
			waited := make(chan error, 1)
			go func() { waited <- b.Wait(bg, 1) }()
			if !waitBlocked(waited) {
				t.Fatal("a wait passed a pause")
			}
			tc.change(b)
			if err := recv(t, waited); !errors.Is(err, tc.want) {
				t.Fatalf("Wait = %v, want %v", err, tc.want)
			}
		})
	}
}

// Pause wins over a Limit of its step, and Stop over Pause.
func TestPauseStepPrecedence(t *testing.T) {
	r := NewRate(1)
	b := NewBudget(Step{Pause: true, Limit: r})
	if err := b.Charge(100); err != nil {
		t.Fatalf("Charge = %v", err)
	}
	if err := b.TryWait(1); !errors.Is(err, ErrWouldWait) {
		t.Fatalf("TryWait = %v, want ErrWouldWait", err)
	}
	if r.reserve(0) != 0 {
		t.Fatal("the Limit of a paused step was charged")
	}
	b.SetSteps(Step{Pause: true, Stop: true})
	if err := b.Wait(bg, 1); !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("Wait = %v, want ErrBudgetExhausted", err)
	}
}

// A wrapper's Write and Read wait on a pause, as on a Limit step: the leases
// go back, nothing passes, and Close or SetSteps ends the wait. The bytes
// used are the bytes that passed.
func TestPauseConn(t *testing.T) {
	b := NewBudget()
	a, peer := net.Pipe()
	defer func() { _ = peer.Close() }()
	c := NewConn(a, WithLimiter(b))
	go func() { _, _ = io.Copy(io.Discard, peer) }()
	if _, err := c.Write(make([]byte, 1000)); err != nil {
		t.Fatal(err)
	}
	b.SetSteps(Step{After: 1000, Pause: true})
	wrote := make(chan error, 1)
	go func() { _, err := c.Write(make([]byte, 10)); wrote <- err }()
	if !waitBlocked(wrote) {
		t.Fatal("a Write passed a pause")
	}
	if got := b.Used(); got != 1000 {
		t.Fatalf("Used = %d while paused, want 1000", got)
	}
	b.SetSteps()
	if err := recv(t, wrote); err != nil {
		t.Fatal(err)
	}
	if got := b.Used(); got != 1010 {
		t.Fatalf("Used = %d, want 1010", got)
	}
	b.SetSteps(Step{Pause: true})
	go func() { _, err := c.Write(make([]byte, 10)); wrote <- err }()
	if !waitBlocked(wrote) {
		t.Fatal("a Write passed a pause")
	}
	_ = c.Close()
	if err := recv(t, wrote); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("Write = %v, want net.ErrClosed", err)
	}
	if got := b.Used(); got != 1010 {
		t.Fatalf("Used = %d after Close, want 1010", got)
	}
}

// SetLimiters ends a wrapper's wait on a pause, which starts over on the new
// limiter.
func TestPauseSetLimiters(t *testing.T) {
	b := NewBudget(Step{Pause: true})
	a, peer := net.Pipe()
	defer func() { _ = peer.Close() }()
	c := NewConn(a, WithLimiter(b))
	defer func() { _ = c.Close() }()
	go func() { _, _ = io.Copy(io.Discard, peer) }()
	wrote := make(chan error, 1)
	go func() { _, err := c.Write(make([]byte, 10)); wrote <- err }()
	if !waitBlocked(wrote) {
		t.Fatal("a Write passed a pause")
	}
	c.SetLimiters(nil, nil)
	if err := recv(t, wrote); err != nil {
		t.Fatal(err)
	}
}

// pausedNotifies returns how many notifies b's gate holds.
func pausedNotifies(b *Budget) int {
	g := b.pause.Load()
	if g == nil {
		return 0
	}
	n := 0
	for set := g.notes.Load(); set != nil && set != openNotes; set = set.prev {
		for i := range set.shards {
			l := &set.shards[i].noteList
			l.mu.Lock()
			for p := l.head; p != nil; p = p.gnext {
				n++
			}
			l.mu.Unlock()
		}
	}
	return n
}

// The NotifyReadable of many readers paused by a Budget holds no goroutine;
// SetSteps calls each once it may read, Close calls its own, and stop takes
// the call off the Budget.
func TestPauseNotifyNoGoroutines(t *testing.T) {
	const conns = 2000
	b := NewBudget(Step{Pause: true})
	streams := make([]*notifyStream, conns)
	wrapped := make([]*NotifyStream, conns)
	var called atomic.Int64
	stops := make([]func() bool, conns)
	before := runtime.NumGoroutine()
	for i := range conns {
		streams[i] = &notifyStream{buf: []byte("x")}
		wrapped[i] = NewStream(streams[i], WithLimiter(b)).Notifying().(*NotifyStream)
		var buf [1]byte
		if _, err := wrapped[i].TryRead(buf[:]); !errors.Is(err, ErrWouldBlock) {
			t.Fatalf("TryRead = %v, want ErrWouldBlock", err)
		}
		stops[i] = wrapped[i].NotifyReadable(func() { called.Add(1) })
	}
	if got := runtime.NumGoroutine() - before; got > 10 {
		t.Fatalf("%d goroutines for %d paused readers", got, conns)
	}
	if called.Load() != 0 {
		t.Fatal("a paused reader was called")
	}
	if got := pausedNotifies(b); got != conns {
		t.Fatalf("gate holds %d notifies, want %d", got, conns)
	}
	// Stop one, close another: the Close calls its reader for the error.
	if !stops[0]() {
		t.Fatal("stop of a parked notify = false")
	}
	_ = wrapped[1].Close()
	if called.Load() != 1 {
		t.Fatalf("Close called %d readers, want 1", called.Load())
	}
	var buf [1]byte
	if _, err := wrapped[1].TryRead(buf[:]); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("TryRead after Close = %v, want net.ErrClosed", err)
	}
	if got := pausedNotifies(b); got != conns-2 {
		t.Fatalf("gate holds %d notifies, want %d", got, conns-2)
	}
	b.SetSteps()
	for deadline := time.Now().Add(5 * time.Second); called.Load() != conns-1; {
		if time.Now().After(deadline) {
			t.Fatalf("SetSteps called %d readers, want %d", called.Load(), conns-1)
		}
		time.Sleep(time.Millisecond)
	}
	for i := 2; i < conns; i++ {
		if n, err := wrapped[i].TryRead(buf[:]); n != 1 || err != nil {
			t.Fatalf("TryRead = %d, %v after the pause", n, err)
		}
		_ = wrapped[i].Close()
	}
	if got := b.Used(); got != conns-2 {
		t.Fatalf("Used = %d, want %d", got, conns-2)
	}
}

// A pause set while a notify waits out a Rate's debt, and one replaced by
// another pause, keep the call parked until the pause ends; the read
// deadline and SetLimiters advance it too.
func TestPauseNotifyAdvances(t *testing.T) {
	b := NewBudget(Step{Pause: true})
	s := &notifyStream{buf: []byte("xy")}
	ns := NewStream(s, WithLimiter(b)).Notifying().(*NotifyStream)
	called := make(chan error, 4)
	ns.NotifyReadable(func() { called <- nil })
	b.SetSteps(Step{Pause: true}) // another pause: parked again
	if !waitBlocked(called) {
		t.Fatal("called while still paused")
	}
	if got := pausedNotifies(b); got != 1 {
		t.Fatalf("gate holds %d notifies, want 1", got)
	}
	ns.SetLimiters(nil, nil)
	if err := recv(t, called); err != nil {
		t.Fatal(err)
	}
	if got := pausedNotifies(b); got != 0 {
		t.Fatalf("gate holds %d notifies after the call, want 0", got)
	}

	a, peer := net.Pipe()
	defer func() { _ = peer.Close() }()
	src := &notifyStream{buf: []byte("xy")}
	c := NewConn(notifyNetConn{Conn: a, notifyStream: src}, WithLimiter(b)).Notifying().(*NotifyConn)
	defer func() { _ = c.Close() }()
	c.NotifyReadable(func() { called <- nil })
	if !waitBlocked(called) {
		t.Fatal("called while paused")
	}
	if err := c.SetReadDeadline(time.Now().Add(20 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if err := recv(t, called); err != nil {
		t.Fatal(err)
	}
	var buf [1]byte
	if _, err := c.TryRead(buf[:]); !errors.Is(err, errDeadline()) {
		t.Fatalf("TryRead = %v, want the deadline", err)
	}
}

// errDeadline is the error of a wrapper's I/O past its deadline.
func errDeadline() error { return (&waitGen{}).cause() }

// SetSteps wakes many paused writers at once, and each passes.
func TestPauseWakesManyWaiters(t *testing.T) {
	const waiters = 500
	b := NewBudget(Step{Pause: true})
	var wg sync.WaitGroup
	errs := make(chan error, waiters)
	for range waiters {
		wg.Go(func() { errs <- b.Wait(bg, 1) })
	}
	time.Sleep(20 * time.Millisecond)
	b.SetSteps()
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := b.Used(); got != waiters {
		t.Fatalf("Used = %d, want %d", got, waiters)
	}
}
