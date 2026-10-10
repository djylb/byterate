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
// net.ErrClosed; any other error from GetContext is returned as it is, and a
// Write refunds its charge. CloseRead and CloseWrite are passed on to rwc when
// it has them, so relays can half-close through the wrapper. A nil rate,
// including a nil *Rate or *HierarchicalLimiter, returns rwc unchanged.
// Methods return ErrNilConn if rwc is nil.
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
	c := &rateConn{}
	c.init(rwc, read, write)
	return c
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
	nc := &rateNetConn{conn: c}
	nc.init(c, read, write)
	return nc
}

// init sets the wrapped connection and the limiters, which the constructors
// have passed through nilLimiter.
func (s *rateConn) init(rwc io.ReadWriteCloser, read, write Limiter) {
	s.conn = rwc
	s.read.init(read)
	s.write.init(write)
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
	var epoch uint64
	if limited {
		epoch = resetEpoch.Load() // before the charge: see Rate.returnSince
		if err = s.wait(&s.write, int64(len(b))); err != nil {
			s.write.refund(int64(len(b)), epoch)
			return 0, err
		}
	}
	n, err = s.conn.Write(b)
	if limited && n < len(b) {
		s.write.refund(int64(len(b)-n), epoch)
	}
	return
}

// refund returns size bytes, charged when resetEpoch read epoch, to d's
// limiter. *Rate and *HierarchicalLimiter drop the bytes for each Rate that
// was started since.
func (d *direction) refund(size int64, epoch uint64) {
	if r, ok := d.rate.(interface{ returnSince(int64, uint64) }); ok {
		r.returnSince(size, epoch)
		return
	}
	d.rate.ReturnBucket(size)
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
// ended the wait, os.ErrDeadlineExceeded if d's deadline did, and any other
// error from the limiter as it is.
func (s *rateConn) wait(d *direction, size int64) error {
	switch {
	case d.ctxRate != nil:
		g := d.cancel.current() // the whole wait uses one generation
		if err := d.ctxRate.GetContext(g, size); err != nil {
			if g.ended() {
				return g.cause()
			}
			return err
		}
	case d.rate != nil:
		d.rate.Get(size)
	}
	return nil
}

// Generation states. A generation ends once, by a deadline or by Close.
const (
	genOpen uint32 = iota
	genExpired
	genClosed
)

// waitGen is one generation of a direction's wait signal and the context a
// wait passes to GetContext. Its Done channel is made on first use, since
// most waits need none, and never changes; Err is non-nil once it is closed,
// as context.Context requires.
type waitGen struct {
	done  atomic.Value // chan struct{}, set by Done or end
	state atomic.Uint32
}

var _ context.Context = (*waitGen)(nil)

// closedChan is the Done channel of a generation that ended before any wait
// asked for one.
var closedChan = func() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}()

// closedGen is the generation of every closed connection whose own
// generation a deadline had already ended.
var closedGen = func() *waitGen {
	g := &waitGen{}
	g.end(genClosed)
	return g
}()

func (g *waitGen) Deadline() (time.Time, bool) { return time.Time{}, false }

func (g *waitGen) Done() <-chan struct{} {
	if ch, ok := g.done.Load().(chan struct{}); ok {
		return ch
	}
	g.done.CompareAndSwap(nil, make(chan struct{})) // loses only to end or another Done
	return g.done.Load().(chan struct{})
}

func (g *waitGen) Err() error {
	var err error
	switch g.state.Load() {
	case genOpen:
		return nil
	case genExpired:
		err = context.DeadlineExceeded
	default:
		err = context.Canceled
	}
	<-g.Done() // end sets the state before it closes the channel
	return err
}

func (g *waitGen) Value(any) any { return nil }

// end ends g with state genExpired or genClosed, unless it has ended.
func (g *waitGen) end(state uint32) {
	if !g.state.CompareAndSwap(genOpen, state) {
		return
	}
	if !g.done.CompareAndSwap(nil, closedChan) {
		close(g.done.Load().(chan struct{}))
	}
}

func (g *waitGen) ended() bool { return g.state.Load() != genOpen }

// cause returns the error for a wait that g ended.
func (g *waitGen) cause() error {
	if g.state.Load() == genClosed {
		return net.ErrClosed
	}
	return os.ErrDeadlineExceeded
}

// waitCancel is a direction's wait signal. Its current generation is ended
// when the connection is closed or the direction's deadline passes. A
// deadline that has passed and is moved later or cleared starts a new
// generation, as net.Pipe does, and waits on the old one stay ended.
type waitCancel struct {
	gen atomic.Pointer[waitGen] // nil until init

	mu     sync.Mutex
	closed bool           // the connection is closed
	timer  *deadlineTimer // made by the first deadline in the future, then reused
}

// deadlineTimer ends a generation at its deadline. It refers to the
// generation alone, so a pending deadline keeps no connection reachable. A
// deadline moved later, as servers do before every read, leaves the runtime
// timer alone: it fires at the earlier time and sets itself again.
type deadlineTimer struct {
	mu      sync.Mutex
	t       *time.Timer
	gen     *waitGen  // the generation to end
	at      time.Time // the deadline, zero for none
	firesAt time.Time // the deadline t is set for, zero once it has fired
}

func newDeadlineTimer(g *waitGen, at time.Time, d time.Duration) *deadlineTimer {
	dt := &deadlineTimer{gen: g, at: at, firesAt: at}
	dt.mu.Lock() // fire may run before t is set
	dt.t = time.AfterFunc(d, dt.fire)
	dt.mu.Unlock()
	return dt
}

func (dt *deadlineTimer) fire() {
	dt.mu.Lock()
	defer dt.mu.Unlock()
	dt.firesAt = time.Time{}
	if dt.at.IsZero() {
		return
	}
	if d := time.Until(dt.at); d > 0 {
		dt.schedule(d) // the deadline was moved later
		return
	}
	dt.gen.end(genExpired)
}

// set makes dt end g at at, which is d from now. dt.mu must be held.
func (dt *deadlineTimer) set(g *waitGen, at time.Time, d time.Duration) {
	dt.gen, dt.at = g, at
	if dt.firesAt.IsZero() || dt.firesAt.After(at) {
		dt.schedule(d)
	}
}

// schedule sets t to fire after d, at dt.at. dt.mu must be held.
func (dt *deadlineTimer) schedule(d time.Duration) {
	dt.firesAt = dt.at
	dt.t.Reset(d)
}

func (w *waitCancel) init() { w.gen.Store(&waitGen{}) }

// current returns the generation a new wait uses.
func (w *waitCancel) current() *waitGen { return w.gen.Load() }

// setDeadline sets the time at which the current generation ends; a zero t
// means never.
func (w *waitCancel) setDeadline(t time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return
	}
	dt := w.timer
	if dt != nil {
		dt.mu.Lock() // keeps fire from ending g while it is replaced
		defer dt.mu.Unlock()
		dt.at = time.Time{} // set again below for a deadline in the future
	}

	g := w.gen.Load()
	var d time.Duration
	if !t.IsZero() {
		now := time.Now()
		if d = t.Sub(now); d <= 0 {
			g.end(genExpired)
			return
		}
		// On the monotonic clock, as the wrapped connection keeps it: a
		// deadline without a monotonic reading would otherwise move with
		// wall-clock steps each time the timer checks it.
		t = now.Add(d)
	}
	if g.ended() {
		g = &waitGen{} // waits on the ended generation stay ended
		w.gen.Store(g)
	}
	switch {
	case t.IsZero():
	case dt == nil:
		w.timer = newDeadlineTimer(g, t, d)
	default:
		dt.set(g, t, d)
	}
}

// close ends the wait signal for good, ending any wait.
func (w *waitCancel) close() {
	w.mu.Lock()
	defer w.mu.Unlock()
	g := w.gen.Load()
	if w.closed || g == nil {
		return
	}
	w.closed = true
	if dt := w.timer; dt != nil {
		dt.mu.Lock()
		dt.at, dt.firesAt = time.Time{}, time.Time{}
		dt.t.Stop()
		dt.mu.Unlock()
	}
	if g.ended() {
		// Waits on g keep their deadline error; later ones see the close.
		w.gen.Store(closedGen)
		return
	}
	g.end(genClosed)
}

type rateNetConn struct {
	rateConn
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
// and, unless it rejects them, of the waits in the limiters.
func (c *rateNetConn) SetDeadline(t time.Time) error {
	if c.conn == nil {
		return ErrNilConn
	}
	err := c.conn.SetDeadline(t)
	if limitsWaits(err) {
		c.read.setDeadline(t)
		c.write.setDeadline(t)
	}
	return err
}

// SetReadDeadline sets the read deadline of the wrapped connection and,
// unless it rejects it, of the waits in the read limiter.
func (c *rateNetConn) SetReadDeadline(t time.Time) error {
	if c.conn == nil {
		return ErrNilConn
	}
	err := c.conn.SetReadDeadline(t)
	if limitsWaits(err) {
		c.read.setDeadline(t)
	}
	return err
}

// SetWriteDeadline sets the write deadline of the wrapped connection and,
// unless it rejects it, of the waits in the write limiter.
func (c *rateNetConn) SetWriteDeadline(t time.Time) error {
	if c.conn == nil {
		return ErrNilConn
	}
	err := c.conn.SetWriteDeadline(t)
	if limitsWaits(err) {
		c.write.setDeadline(t)
	}
	return err
}

// limitsWaits reports whether a deadline that the wrapped connection
// answered with err also bounds the waits in the limiters. A connection
// without deadline support, such as an SSH channel, rejects it, and I/O on
// it must not time out only when throttled; one that is closed, or whose peer
// closed a pipe, rejects it too, but a deadline in the past must still end a
// wait there.
func limitsWaits(err error) bool {
	return err == nil || errors.Is(err, net.ErrClosed) || errors.Is(err, io.ErrClosedPipe)
}

// RawConn returns the wrapped connection.
func (c *rateNetConn) RawConn() net.Conn {
	return c.conn
}
