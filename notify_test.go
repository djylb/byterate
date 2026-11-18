package byterate

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// srcWouldBlockError is the would-block error of notifyStream, of its own type.
type srcWouldBlockError struct{}

func (srcWouldBlockError) Error() string    { return "source would block" }
func (srcWouldBlockError) WouldBlock() bool { return true }

// notifyStream is an in-memory source with TryRead and NotifyReadable, as a
// stream of a multiplexed connection would be. Notifications run on the
// feeding goroutine.
type notifyStream struct {
	f      func()
	buf    []byte
	mu     sync.Mutex
	closed bool
	reads  int // blocking Reads, under mu
}

func (s *notifyStream) feed(b []byte) {
	s.mu.Lock()
	s.buf = append(s.buf, b...)
	f := s.f
	s.f = nil
	s.mu.Unlock()
	if f != nil {
		f()
	}
}

func (s *notifyStream) TryRead(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, net.ErrClosed
	}
	if len(s.buf) == 0 {
		return 0, srcWouldBlockError{}
	}
	n := copy(p, s.buf)
	s.buf = s.buf[n:]
	return n, nil
}

func (s *notifyStream) NotifyReadable(f func()) func() bool {
	s.mu.Lock()
	if len(s.buf) > 0 || s.closed {
		s.mu.Unlock()
		f()
		return func() bool { return false }
	}
	s.f = f
	s.mu.Unlock()
	return func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		stopped := s.f != nil
		s.f = nil
		return stopped
	}
}

func (s *notifyStream) Read(p []byte) (int, error) {
	s.mu.Lock()
	s.reads++
	s.mu.Unlock()
	return s.TryRead(p)
}

func (s *notifyStream) Write(p []byte) (int, error) { return len(p), nil }

func (s *notifyStream) Close() error {
	s.mu.Lock()
	s.closed = true
	f := s.f
	s.f = nil
	s.mu.Unlock()
	if f != nil {
		f()
	}
	return nil
}

// notifyNetConn is a notifyStream as a net.Conn.
type notifyNetConn struct {
	net.Conn
	*notifyStream
}

func (c notifyNetConn) Read(p []byte) (int, error)  { return c.notifyStream.Read(p) }
func (c notifyNetConn) Write(p []byte) (int, error) { return c.notifyStream.Write(p) }
func (c notifyNetConn) Close() error                { return c.notifyStream.Close() }

// Only a wrapper of a source with TryRead and NotifyReadable has them.
func TestNotifyingOnlyWhenSupported(t *testing.T) {
	a, b := net.Pipe()
	defer func() { _ = a.Close(); _ = b.Close() }()
	if _, ok := NewConn(a).Notifying().(readNotifier); ok {
		t.Fatal("Notifying of a plain net.Conn has TryRead")
	}
	if _, ok := NewStream(&scriptedConn{}).Notifying().(readNotifier); ok {
		t.Fatal("Notifying of a plain stream has TryRead")
	}
	if _, ok := any(NewConn(a)).(readNotifier); ok {
		t.Fatal("*Conn has TryRead")
	}
	nc := NewConn(notifyNetConn{Conn: a, notifyStream: &notifyStream{}}).Notifying()
	if _, ok := nc.(*NotifyConn); !ok {
		t.Fatalf("Notifying = %T, want *NotifyConn", nc)
	}
	if _, ok := NewStream(&notifyStream{}).Notifying().(*NotifyStream); !ok {
		t.Fatal("Notifying of a notifying stream is no *NotifyStream")
	}
}

// Without a limit, TryRead and NotifyReadable pass through and count.
func TestNotifyUnlimitedCounts(t *testing.T) {
	src := &notifyStream{}
	s := NewStream(src)
	n := s.Notifying().(readNotifier)
	buf := make([]byte, 16)
	if _, err := n.TryRead(buf); !errors.Is(err, srcWouldBlockError{}) {
		t.Fatalf("TryRead of an idle source: %v, want its would-block error", err)
	}
	var called atomic.Bool
	n.NotifyReadable(func() { called.Store(true) })
	src.feed([]byte("hello"))
	if !called.Load() {
		t.Fatal("not notified when data arrived")
	}
	if m, err := n.TryRead(buf); m != 5 || err != nil {
		t.Fatalf("TryRead = %d, %v", m, err)
	}
	if r, _ := s.Meter().Total(); r != 5 {
		t.Fatalf("Meter read %d, want 5", r)
	}
}

// Over a Rate, TryRead charges what it reads as Read does, returns
// ErrWouldBlock while the debt lasts though data is there, and
// NotifyReadable calls back once the debt is repaid, not before.
func TestNotifyRateLimited(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const limit = 1000
		r := NewRate(limit)
		src := &notifyStream{}
		src.feed(make([]byte, 64<<10))
		s := NewStream(src, WithLimiter(r))
		n := s.Notifying().(readNotifier)
		buf := make([]byte, 32<<10)
		var total int64
		for {
			m, err := n.TryRead(buf)
			total += int64(m)
			if errors.Is(err, ErrWouldBlock) {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			if total > 8*limit {
				t.Fatalf("read %d bytes without would-block at %d B/s", total, limit)
			}
		}
		if got := metered(r); got != total {
			t.Fatalf("charged %d, read %d", got, total)
		}
		if rd, _ := s.Meter().Total(); rd != total {
			t.Fatalf("Meter read %d, read %d", rd, total)
		}
		start := time.Now()
		var called atomic.Bool
		n.NotifyReadable(func() { called.Store(true) })
		synctest.Wait()
		if called.Load() {
			t.Fatal("notified while the limit has debt")
		}
		for !called.Load() {
			time.Sleep(time.Millisecond)
			synctest.Wait()
		}
		if waited := time.Since(start); waited < 100*time.Millisecond {
			t.Fatalf("notified after %v, before the debt was repaid", waited)
		}
		if m, err := n.TryRead(buf); m == 0 || err != nil {
			t.Fatalf("TryRead after the notification = %d, %v", m, err)
		}
		if src.reads != 0 {
			t.Fatalf("%d blocking Reads", src.reads)
		}
	})
}

// A stop during the wait for the debt keeps f from being called; Close
// during it calls f, and TryRead then fails.
func TestNotifyStopAndClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		indebt := func() (*Stream, readNotifier) {
			r := NewRate(1000)
			r.reserve(2000)
			r.reserve(5000)
			src := &notifyStream{}
			src.feed([]byte("data"))
			s := NewStream(src, WithLimiter(r))
			return s, s.Notifying().(readNotifier)
		}
		_, n := indebt()
		var called atomic.Bool
		stop := n.NotifyReadable(func() { called.Store(true) })
		synctest.Wait()
		if !stop() {
			t.Fatal("stop during the wait = false")
		}
		time.Sleep(10 * time.Second)
		synctest.Wait()
		if called.Load() {
			t.Fatal("called after stop")
		}

		s, n := indebt()
		called.Store(false)
		n.NotifyReadable(func() { called.Store(true) })
		synctest.Wait()
		_ = s.Close()
		synctest.Wait()
		if !called.Load() {
			t.Fatal("Close did not call f")
		}
		if _, err := n.TryRead(make([]byte, 4)); !errors.Is(err, net.ErrClosed) {
			t.Fatalf("TryRead after Close: %v", err)
		}
	})
}

// An exhausted Budget refuses TryRead and makes the source readable at
// once, for TryRead to return the refusal.
func TestNotifyBudgetExhausted(t *testing.T) {
	b := NewBudget(Step{After: 10, Stop: true})
	src := &notifyStream{}
	src.feed(make([]byte, 100))
	s := NewStream(src, WithLimiter(b))
	n := s.Notifying().(readNotifier)
	buf := make([]byte, 100)
	var total int
	var err error
	for err == nil {
		var m int
		m, err = n.TryRead(buf)
		total += m
	}
	if !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("TryRead: %v, want ErrBudgetExhausted", err)
	}
	if total > 10 {
		t.Fatalf("read %d bytes past a Stop at 10", total)
	}
	var called atomic.Bool
	n.NotifyReadable(func() { called.Store(true) })
	if !called.Load() {
		t.Fatal("not notified at once with the Budget exhausted")
	}
	if _, err := io.ReadFull(s, buf[:1]); !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("Read: %v, want ErrBudgetExhausted", err)
	}
}

// ctxFirstLimiter is a Limiter whose Ready fails on an ended context
// however ready it is.
type ctxFirstLimiter struct{ Limiter }

func (ctxFirstLimiter) Ready(ctx context.Context, n int64) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return n, nil
}

func (ctxFirstLimiter) Charge(int64) error { return nil }

// A Limiter whose Ready cannot tell readiness without waiting still lets
// the read that its NotifyReadable announced through.
func TestNotifyLimiterWithoutProbe(t *testing.T) {
	src := &notifyStream{}
	src.feed([]byte("data"))
	s := NewStream(src, WithLimiter(ctxFirstLimiter{NewRate(0)}))
	n := s.Notifying().(readNotifier)
	buf := make([]byte, 8)
	if _, err := n.TryRead(buf); !errors.Is(err, ErrWouldBlock) {
		t.Fatalf("TryRead: %v, want ErrWouldBlock", err)
	}
	ready := make(chan struct{})
	n.NotifyReadable(func() { close(ready) })
	<-ready
	if m, err := n.TryRead(buf); m != 4 || err != nil {
		t.Fatalf("TryRead after the notification = %d, %v", m, err)
	}
}
