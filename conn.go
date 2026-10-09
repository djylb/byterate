package byterate

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// ErrNilConn is returned when a wrapper has no underlying connection.
var ErrNilConn = errors.New("byterate: nil connection")

var (
	errCloseReadUnsupported  = fmt.Errorf("byterate: connection does not support CloseRead: %w", errors.ErrUnsupported)
	errCloseWriteUnsupported = fmt.Errorf("byterate: connection does not support CloseWrite: %w", errors.ErrUnsupported)
)

type rateConn struct {
	conn        io.ReadWriteCloser
	read, write direction
}

// direction holds the limiter charged for one direction of a rateConn and
// the signal that ends a wait in it.
type direction struct {
	rate    Limiter
	ctxRate ContextLimiter // rate, if it supports cancellation
	cancel  waitCancel     // used only with ctxRate
}

// NewRateReadWriteCloser wraps rwc so that its traffic in both directions is
// charged to rate. Read charges the bytes received before returning them;
// Write charges len(b) before writing and refunds any unwritten part. If rate
// is a ContextLimiter, Close also ends a wait in progress, which then returns
// net.ErrClosed. CloseRead and CloseWrite are passed on to rwc when it has
// them, so relays can half-close through the wrapper. A nil rate, including a
// nil *Rate or *HierarchicalLimiter, returns rwc unchanged. Methods return
// ErrNilConn if rwc is nil.
func NewRateReadWriteCloser(rwc io.ReadWriteCloser, rate Limiter) io.ReadWriteCloser {
	return NewDuplexRateReadWriteCloser(rwc, rate, rate)
}

// NewDuplexRateReadWriteCloser is NewRateReadWriteCloser with separate
// limiters for reads and writes, so that each direction is limited and
// metered on its own. A nil limiter leaves its direction unlimited; with both
// nil, rwc is returned unchanged.
func NewDuplexRateReadWriteCloser(rwc io.ReadWriteCloser, read, write Limiter) io.ReadWriteCloser {
	read, write = nilLimiter(read), nilLimiter(write)
	if read == nil && write == nil {
		return rwc
	}
	return newRateConn(rwc, read, write)
}

// NewRateConn is NewRateReadWriteCloser for a net.Conn. The result reports
// c's addresses, sets c's deadlines and returns c from its RawConn method,
// which wrapper-aware helpers use to unwrap it.
//
// If rate is a ContextLimiter, deadlines also bound the time a Read or Write
// spends waiting in the limiter. A Write whose deadline passes during the wait
// refunds its charge and returns 0 and os.ErrDeadlineExceeded, a net.Error
// whose Timeout method reports true. A Read has already received its bytes
// when it waits, so it keeps the charge and returns them together with
// os.ErrDeadlineExceeded, as it returns them with net.ErrClosed when Close
// ends the wait. Moving a deadline later does not end a wait.
func NewRateConn(c net.Conn, rate Limiter) net.Conn {
	return NewDuplexRateConn(c, rate, rate)
}

// NewDuplexRateConn is NewRateConn with separate limiters for reads and
// writes, as NewDuplexRateReadWriteCloser.
func NewDuplexRateConn(c net.Conn, read, write Limiter) net.Conn {
	read, write = nilLimiter(read), nilLimiter(write)
	if read == nil && write == nil {
		return c
	}
	return &rateNetConn{rateConn: newRateConn(c, read, write), conn: c}
}

func newRateConn(rwc io.ReadWriteCloser, read, write Limiter) *rateConn {
	c := &rateConn{conn: rwc}
	c.read.init(read)
	c.write.init(write)
	return c
}

// nilLimiter returns nil for a nil *Rate or *HierarchicalLimiter, which
// limit nothing, so that they do not hide the wrapped connection.
func nilLimiter(l Limiter) Limiter {
	switch v := l.(type) {
	case *Rate:
		if v == nil {
			return nil
		}
	case *HierarchicalLimiter:
		if v == nil {
			return nil
		}
	}
	return l
}

func (d *direction) setDeadline(t time.Time) {
	if d.ctxRate != nil {
		d.cancel.setDeadline(t)
	}
}

// init sets d's limiter, which the constructors have passed through
// nilLimiter.
func (d *direction) init(rate Limiter) {
	d.rate = rate
	d.ctxRate, _ = rate.(ContextLimiter)
	if d.ctxRate != nil {
		d.cancel.init()
	}
}

func (s *rateConn) Read(b []byte) (n int, err error) {
	if s == nil || s.conn == nil {
		return 0, ErrNilConn
	}
	n, err = s.conn.Read(b)
	if n > 0 {
		// The bytes were received, so the charge stands even if Close or a
		// deadline cut the wait short.
		if werr := s.wait(&s.read, int64(n)); werr != nil && err == nil {
			err = werr
		}
	}
	return
}

func (s *rateConn) Write(b []byte) (n int, err error) {
	if s == nil || s.conn == nil {
		return 0, ErrNilConn
	}
	limited := len(b) > 0 && s.write.rate != nil
	if limited {
		if err = s.wait(&s.write, int64(len(b))); err != nil {
			s.write.rate.ReturnBucket(int64(len(b)))
			return 0, err
		}
	}
	n, err = s.conn.Write(b)
	if limited && n < len(b) {
		s.write.rate.ReturnBucket(int64(len(b) - n))
	}
	return
}

// CloseRead shuts down the reading side of the wrapped connection, as
// *net.TCPConn does, or returns an error matching errors.ErrUnsupported when
// it cannot.
func (s *rateConn) CloseRead() error {
	if s == nil || s.conn == nil {
		return ErrNilConn
	}
	if cr, ok := s.conn.(interface{ CloseRead() error }); ok {
		return cr.CloseRead()
	}
	return errCloseReadUnsupported
}

// CloseWrite shuts down the writing side of the wrapped connection, as
// *net.TCPConn and *tls.Conn do, or returns an error matching
// errors.ErrUnsupported when it cannot.
func (s *rateConn) CloseWrite() error {
	if s == nil || s.conn == nil {
		return ErrNilConn
	}
	if cw, ok := s.conn.(interface{ CloseWrite() error }); ok {
		return cw.CloseWrite()
	}
	return errCloseWriteUnsupported
}

func (s *rateConn) Close() error {
	if s == nil || s.conn == nil {
		return ErrNilConn
	}
	s.read.cancel.close()
	s.write.cancel.close()
	return s.conn.Close()
}

// wait charges size to d's limiter, if any. It returns net.ErrClosed if Close
// ended the wait and os.ErrDeadlineExceeded if d's deadline did.
func (s *rateConn) wait(d *direction, size int64) error {
	switch {
	case d.ctxRate != nil:
		g := d.cancel.current() // the whole wait uses one generation
		if d.ctxRate.GetContext(g, size) != nil {
			return g.cause()
		}
	case d.rate != nil:
		d.rate.Get(size)
	}
	return nil
}

// waitGen is one generation of a direction's wait signal and the context a
// wait passes to GetContext. Its Done channel never changes, and Err is
// non-nil once the channel is closed, as context.Context requires.
type waitGen struct {
	ch     chan struct{}
	closed atomic.Bool // ch was closed by Close rather than a deadline; set before ch is closed
}

var _ context.Context = (*waitGen)(nil)

// closedGen is the generation of every closed connection whose own
// generation a deadline had already ended.
var closedGen = func() *waitGen {
	g := &waitGen{ch: make(chan struct{})}
	g.closed.Store(true)
	close(g.ch)
	return g
}()

func (g *waitGen) Deadline() (time.Time, bool) { return time.Time{}, false }

func (g *waitGen) Done() <-chan struct{} { return g.ch }

func (g *waitGen) Err() error {
	if !isClosed(g.ch) {
		return nil
	}
	if g.closed.Load() {
		return context.Canceled
	}
	return context.DeadlineExceeded
}

func (g *waitGen) Value(any) any { return nil }

// cause returns the error for a wait that g ended.
func (g *waitGen) cause() error {
	if g.closed.Load() {
		return net.ErrClosed
	}
	return os.ErrDeadlineExceeded
}

// waitCancel is a direction's wait signal. Its current generation is closed
// when the connection is closed or the direction's deadline passes. A
// deadline that has passed and is moved later or cleared starts a new
// generation, as net.Pipe does, and waits on the old one stay ended.
type waitCancel struct {
	gen atomic.Pointer[waitGen] // nil until init

	mu     sync.Mutex
	closed bool        // the connection is closed
	timer  *time.Timer // closes the current generation at the deadline
}

func (w *waitCancel) init() { w.gen.Store(&waitGen{ch: make(chan struct{})}) }

// current returns the generation a new wait uses.
func (w *waitCancel) current() *waitGen { return w.gen.Load() }

// setDeadline sets the time at which the wait signal is closed; a zero t
// means never.
func (w *waitCancel) setDeadline(t time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return
	}
	w.stopTimer()

	g := w.gen.Load()
	expired := isClosed(g.ch)
	if t.IsZero() {
		if expired {
			w.init()
		}
		return
	}
	if d := time.Until(t); d > 0 {
		if expired {
			w.init()
			g = w.gen.Load()
		}
		w.timer = time.AfterFunc(d, func() { close(g.ch) })
		return
	}
	if !expired {
		close(g.ch)
	}
}

// close closes the wait signal for good, ending any wait.
func (w *waitCancel) close() {
	w.mu.Lock()
	defer w.mu.Unlock()
	g := w.gen.Load()
	if w.closed || g == nil {
		return
	}
	w.closed = true
	w.stopTimer()
	if isClosed(g.ch) {
		// Waits on g keep their deadline error; later ones see the close.
		w.gen.Store(closedGen)
		return
	}
	g.closed.Store(true)
	close(g.ch)
}

// stopTimer stops the deadline timer, waiting for it to close the current
// generation if it has already fired. w.mu must be held.
func (w *waitCancel) stopTimer() {
	if w.timer != nil && !w.timer.Stop() {
		<-w.gen.Load().ch
	}
	w.timer = nil
}

func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

type rateNetConn struct {
	*rateConn
	conn net.Conn
}

func (c *rateNetConn) LocalAddr() net.Addr {
	if c.conn == nil {
		return nil
	}
	return c.conn.LocalAddr()
}

func (c *rateNetConn) RemoteAddr() net.Addr {
	if c.conn == nil {
		return nil
	}
	return c.conn.RemoteAddr()
}

// SetDeadline sets the read and write deadlines of the wrapped connection
// and of the waits in the limiters.
func (c *rateNetConn) SetDeadline(t time.Time) error {
	if c.conn == nil {
		return ErrNilConn
	}
	c.read.setDeadline(t)
	c.write.setDeadline(t)
	return c.conn.SetDeadline(t)
}

// SetReadDeadline sets the read deadline of the wrapped connection and of
// the waits in the read limiter.
func (c *rateNetConn) SetReadDeadline(t time.Time) error {
	if c.conn == nil {
		return ErrNilConn
	}
	c.read.setDeadline(t)
	return c.conn.SetReadDeadline(t)
}

// SetWriteDeadline sets the write deadline of the wrapped connection and of
// the waits in the write limiter.
func (c *rateNetConn) SetWriteDeadline(t time.Time) error {
	if c.conn == nil {
		return ErrNilConn
	}
	c.write.setDeadline(t)
	return c.conn.SetWriteDeadline(t)
}

// RawConn returns the wrapped connection.
func (c *rateNetConn) RawConn() net.Conn {
	return c.conn
}
