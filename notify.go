package byterate

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
)

// ErrWouldBlock is returned by the TryRead of a wrapper while its read
// limit has debt to wait out: the read would block on the limit, as it
// would on a source without data. Its WouldBlock method reports true, the
// way a source of such reads tells that a read found nothing yet.
var ErrWouldBlock error = wouldBlockError{}

type wouldBlockError struct{}

func (wouldBlockError) Error() string    { return "byterate: read would block" }
func (wouldBlockError) WouldBlock() bool { return true }

// readNotifier is a source that reads without blocking and tells when it
// becomes readable, such as a stream of a multiplexed connection: TryRead
// reads as Read does, returning an error whose WouldBlock method reports
// true while it holds nothing, and NotifyReadable calls f once when it is
// readable, unless the function it returns stops the call first.
type readNotifier interface {
	TryRead(p []byte) (int, error)
	NotifyReadable(f func()) (stop func() bool)
}

// probeCtx is an ended context, with which a Limiter's Ready reports at
// once whether it would wait.
var probeCtx = func() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}()

// probeRead returns, without waiting, how many of size bytes d's limiter
// lets a read take now, as awaitRead does once its wait is over, or
// ErrWouldBlock while it would make the read wait. A wait of
// NotifyReadable that found the limiter ready grants the read that
// follows, so that a Limiter whose Ready fails on an ended context
// however ready it is does not keep the reader from reading.
func (d *direction) probeRead(size int64, whole bool) (int64, error) {
	if g := d.cancel.current(); g.ended() {
		return 0, g.cause()
	}
	for {
		st := d.sw.state()
		var n int64
		var err error
		if r := st.t.one[0].rate; r != nil {
			if n, err = r.ready(probeCtx, &d.timer, size, whole, st.changed); errors.Is(err, errSwitched) {
				continue
			}
		} else {
			n, err = d.lim.ready(probeCtx, &d.timer, size, whole)
		}
		switch {
		case err == nil:
			d.granted.Store(false)
			return n, nil
		case !errors.Is(err, context.Canceled):
			return 0, err // a refusal, such as an exhausted Budget
		case d.granted.Swap(false):
			return size, nil
		default:
			return 0, ErrWouldBlock
		}
	}
}

// tryRead is the TryRead of a wrapper over u: Read without waiting for
// data or for the limiter, charged and counted as Read is.
func (s *rateConn) tryRead(u readNotifier, b []byte) (int, error) {
	if u == nil {
		return 0, ErrNilConn
	}
	d := &s.read
	if st := d.load(); st == 0 || len(b) == 0 ||
		st == dirLeased && d.side.end.Load()-d.side.total.Load() >= int64(len(b)) {
		n, err := u.TryRead(b)
		d.count(int64(n))
		return n, err
	}
	return s.readSlow(b, u)
}

// notifyReadable is the NotifyReadable of a wrapper over u. While the read
// limit has no debt, it passes the call to u. Otherwise a goroutine waits
// out the debt, as Read would, and then passes it to u, so that the reader
// is called once both the limit and u let it read; Close and the read
// deadline end the wait and call f, for TryRead to return their error.
// The goroutine lives only while the debt lasts: a reader that is idle
// within its limit holds none.
func (s *rateConn) notifyReadable(u readNotifier, f func()) (stop func() bool) {
	d := &s.read
	if u == nil || d.load() == 0 {
		if u == nil {
			f()
			return func() bool { return false }
		}
		return u.NotifyReadable(f)
	}
	if _, err := d.probeRead(1, false); !errors.Is(err, ErrWouldBlock) {
		if err != nil {
			f() // readable: TryRead returns the error
			return func() bool { return false }
		}
		return u.NotifyReadable(f)
	}
	n := newPendingNotify(f)
	go n.wait(d, u)
	return n.stop
}

// pendingNotify is a NotifyReadable waiting out the debt of the read limit
// before it passes the call on.
type pendingNotify struct {
	f      func()
	inner  func() bool   // u's stop, set before passed is closed
	passed chan struct{} // closed once the call is passed on to u
	mu     sync.Mutex
	state  uint8 // under mu
	called atomic.Bool
}

// The states of a pendingNotify.
const (
	notifyWaiting uint8 = iota // waiting out the debt
	notifyPassing              // the call is being passed on, or f called
	notifyStopped              // stopped before it was passed on
)

func newPendingNotify(f func()) *pendingNotify {
	return &pendingNotify{f: f, passed: make(chan struct{})}
}

// call calls f once.
func (n *pendingNotify) call() {
	if !n.called.Swap(true) {
		n.f()
	}
}

func (n *pendingNotify) wait(d *direction, u readNotifier) {
	_, err := d.awaitRead(1, false)
	n.mu.Lock()
	if n.state == notifyStopped {
		n.mu.Unlock()
		return
	}
	n.state = notifyPassing
	n.mu.Unlock()
	if err != nil {
		close(n.passed)
		n.call() // readable: TryRead returns the error
		return
	}
	d.granted.Store(true)
	// f may run before NotifyReadable returns and call stop, which then
	// finds it called and does not wait for passed.
	n.inner = u.NotifyReadable(n.call)
	close(n.passed)
}

// stop stops the call as the stop of NotifyReadable does. A goroutine
// still waiting out the debt ends with the wait.
func (n *pendingNotify) stop() bool {
	n.mu.Lock()
	if n.state == notifyWaiting {
		n.state = notifyStopped
		n.mu.Unlock()
		return true
	}
	stopped := n.state == notifyStopped
	n.mu.Unlock()
	if stopped || n.called.Load() {
		return false
	}
	<-n.passed // NotifyReadable does not block
	return n.inner != nil && n.inner()
}

// NotifyConn is a Conn over a connection that reads without blocking and
// tells when it becomes readable, made by Conn.Notifying. Its TryRead and
// NotifyReadable pass those of the wrapped connection on, charged,
// counted and limited as Read is.
type NotifyConn struct {
	*Conn
	u readNotifier
}

// TryRead reads as Read does without blocking: where Read would wait for
// data, it returns what the wrapped connection's TryRead returns, an error
// whose WouldBlock method reports true; where it would wait for the read
// limit, it returns ErrWouldBlock. What it reads is charged and counted as
// Read charges and counts it.
func (c *NotifyConn) TryRead(b []byte) (int, error) { return c.tryRead(c.u, b) }

// NotifyReadable calls f once TryRead would not block: once the read limit
// has no debt and the wrapped connection is readable, or on Close or when
// the read deadline passes. It returns a function that stops the call, as
// the wrapped connection's NotifyReadable does.
func (c *NotifyConn) NotifyReadable(f func()) (stop func() bool) {
	return c.notifyReadable(c.u, f)
}

// NotifyStream is a Stream over a stream that reads without blocking and
// tells when it becomes readable, made by Stream.Notifying; see NotifyConn.
type NotifyStream struct {
	*Stream
	u readNotifier
}

// TryRead reads without blocking, as NotifyConn's TryRead does.
func (s *NotifyStream) TryRead(b []byte) (int, error) { return s.tryRead(s.u, b) }

// NotifyReadable calls f once TryRead would not block, as NotifyConn's
// NotifyReadable does.
func (s *NotifyStream) NotifyReadable(f func()) (stop func() bool) {
	return s.notifyReadable(s.u, f)
}

// Notifying returns c as a *NotifyConn when the wrapped connection has
// methods TryRead(p []byte) (int, error) and NotifyReadable(f func())
// (stop func() bool), such as a stream of a multiplexed connection, so
// that a relay can copy from it without a goroutine waiting while it is
// idle; otherwise it returns c itself, without those methods.
func (c *Conn) Notifying() net.Conn {
	if u, ok := c.conn.(readNotifier); ok {
		return &NotifyConn{Conn: c, u: u}
	}
	return c
}

// Notifying returns s as a *NotifyStream when what it reads has the
// methods TryRead and NotifyReadable, as Conn's Notifying does; otherwise
// it returns s itself.
func (s *Stream) Notifying() io.ReadWriteCloser {
	if u, ok := s.r.(readNotifier); ok {
		return &NotifyStream{Stream: s, u: u}
	}
	return s
}
