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
	datagram    bool // every Read and Write carries one whole message
	read, write direction
}

// direction holds the limiter charged for one direction of a rateConn and
// the signal that ends a wait in it.
type direction struct {
	rate   Limiter
	cancel waitCancel // ended by Close and the deadlines
	timer  waitTimer  // reused by the waits of *Rate and *Hierarchy
	// writing holds a token while a Write sends its grants, so that the
	// grants of concurrent Writes do not interleave, as the writes of a
	// *net.TCPConn do not. Made for the writes of a limited stream alone.
	writing chan struct{}
}

// NewConn wraps c so that its traffic in both directions is charged to l and
// limited by it. The result is a net.Conn that reports c's addresses, sets
// c's deadlines and returns c from its RawConn method, which wrapper-aware
// helpers use to unwrap it. A nil l, including a nil *Rate or *Hierarchy,
// returns c unchanged.
//
// Data passes in grants of what the limit lets through at once: its credit,
// up to two seconds of traffic after an idle period, or without credit a
// quantum of about 20 ms of traffic, no more than the credit at its cap. A
// Write sends its bytes a grant at a time, each charged before it goes out,
// so a large Write neither bursts past the credit nor holds a shared limit
// while other connections wait; concurrent Writes do not interleave. A Read
// first waits until the limit has no debt, then reads at most a grant and
// charges what arrived, so a reader in debt leaves the data with the sender
// and one blocked waiting for data holds no share of the limit. Readers
// waiting for the debt of one shared limit read together when it is repaid,
// up to a grant each.
//
// Close ends a wait with net.ErrClosed, and CloseRead and CloseWrite do the
// same for their side; they are passed on to c when it has them, so relays
// can half-close through the wrapper, and return an error matching
// errors.ErrUnsupported when it has not. Deadlines set through the result
// bound the waits too; one set on c before wrapping it does not. A wait that
// a deadline ends returns os.ErrDeadlineExceeded, a net.Error whose Timeout
// method reports true. A Write that is ended or fails part way returns the
// bytes it sent and refunds the rest, and one that c reports short without
// an error returns io.ErrShortWrite. A Read ended in its wait returns no
// bytes. Moving a deadline later does not end a wait. Any other error from
// l's Wait is returned as it is.
//
// UDP, IP and Unix datagram sockets are never split: each Write is charged
// and sent whole, and a Read may take a whole datagram. NewDatagramConn does
// the same for other connections that carry one message per call. A Limiter
// other than *Rate and *Hierarchy cannot grant part of a request: a Write
// charges all of its bytes before sending them, and a Read charges what it
// read after reading it, returning those bytes with an error that ends the
// wait.
func NewConn(c net.Conn, l Limiter) net.Conn {
	return NewDuplexConn(c, l, l)
}

// NewDuplexConn is NewConn with separate limiters for reads and writes, so
// that each direction is limited and metered on its own. A nil limiter leaves
// its direction alone; with both nil, c is returned unchanged.
func NewDuplexConn(c net.Conn, read, write Limiter) net.Conn {
	return newRateNetConn(c, read, write, isDatagramConn(c))
}

// NewDatagramConn is NewDuplexConn for a connection whose every Read and
// Write carries one whole message, such as a message-oriented tunnel, which
// must not be split: each Write is charged and sent whole, and a Read may
// take a whole message. NewConn and NewDuplexConn treat UDP, IP and Unix
// datagram sockets so by themselves.
func NewDatagramConn(c net.Conn, read, write Limiter) net.Conn {
	return newRateNetConn(c, read, write, true)
}

// NewReadWriteCloser is NewConn for an io.ReadWriteCloser, which has no
// addresses or deadlines. Its methods return ErrNilConn if rwc is nil.
func NewReadWriteCloser(rwc io.ReadWriteCloser, l Limiter) io.ReadWriteCloser {
	return NewDuplexReadWriteCloser(rwc, l, l)
}

// NewDuplexReadWriteCloser is NewDuplexConn for an io.ReadWriteCloser.
func NewDuplexReadWriteCloser(rwc io.ReadWriteCloser, read, write Limiter) io.ReadWriteCloser {
	read, write = nilLimiter(read), nilLimiter(write)
	if read == nil && write == nil {
		return rwc
	}
	c := &rateConn{}
	c.init(rwc, read, write, isDatagramConn(rwc))
	return c
}

func newRateNetConn(c net.Conn, read, write Limiter, datagram bool) net.Conn {
	read, write = nilLimiter(read), nilLimiter(write)
	if read == nil && write == nil {
		return c
	}
	nc := &rateNetConn{conn: c}
	nc.init(c, read, write, datagram)
	return nc
}

// isDatagramConn reports whether c is a socket whose every Read and Write
// carries one datagram.
func isDatagramConn(c any) bool {
	switch v := c.(type) {
	case *net.UDPConn, *net.IPConn:
		return true
	case *net.UnixConn:
		if v == nil {
			return false
		}
		addr := v.LocalAddr()
		return addr == nil || addr.Network() != "unix"
	}
	return false
}

// init sets the wrapped connection and the limiters, which the constructors
// have passed through nilLimiter.
func (s *rateConn) init(rwc io.ReadWriteCloser, read, write Limiter, datagram bool) {
	s.conn = rwc
	s.datagram = datagram
	s.read.init(read)
	s.write.init(write)
	if write != nil && !datagram {
		s.write.writing = make(chan struct{}, 1)
	}
}

// lockWrite waits for d's turn to write, or until Close or the deadline
// ends d's waits, which also ends the wait of the Write holding the turn.
func (d *direction) lockWrite() error {
	select {
	case d.writing <- struct{}{}:
		return nil
	default:
	}
	g := d.cancel.current()
	select {
	case d.writing <- struct{}{}:
		return nil
	case <-g.Done():
		return g.cause()
	}
}

func (d *direction) unlockWrite() { <-d.writing }

// nilLimiter returns nil for a nil *Rate or *Hierarchy, which
// limit nothing, so that they do not hide the wrapped connection.
func nilLimiter(l Limiter) Limiter {
	switch v := l.(type) {
	case *Rate:
		if v == nil {
			return nil
		}
	case *Hierarchy:
		if v == nil {
			return nil
		}
	}
	return l
}

func (d *direction) setDeadline(t time.Time) {
	if d.rate != nil {
		d.cancel.setDeadline(t)
	}
}

func (d *direction) init(rate Limiter) {
	d.rate = rate
	if rate != nil {
		d.cancel.init()
	}
}

func (s *rateConn) Read(b []byte) (int, error) {
	if s == nil || s.conn == nil {
		return 0, ErrNilConn
	}
	d := &s.read
	if d.rate == nil || len(b) == 0 {
		return s.conn.Read(b)
	}
	size, err := d.awaitRead(int64(len(b)), s.datagram)
	if err != nil {
		return 0, err
	}
	n, err := s.conn.Read(b[:size])
	if n > 0 {
		// The bytes were received, so the charge stands even if Close or a
		// deadline cuts a wait short.
		if werr := d.chargeRead(int64(n)); werr != nil && err == nil {
			err = werr
		}
	}
	return n, err
}

func (s *rateConn) Write(b []byte) (n int, err error) {
	if s == nil || s.conn == nil {
		return 0, ErrNilConn
	}
	d := &s.write
	if d.rate == nil || len(b) == 0 {
		return s.conn.Write(b)
	}
	if d.writing != nil {
		if err := d.lockWrite(); err != nil {
			return 0, err
		}
		defer d.unlockWrite()
	}
	for n < len(b) {
		var charge writeCharge
		size, err := d.grantWrite(int64(len(b)-n), s.datagram, &charge)
		if err != nil {
			charge.refund(size)
			return n, err
		}
		m, err := s.conn.Write(b[n : n+int(size)])
		// No more than the charge, even for a writer that reports n < 0
		// or more than it was given.
		m = min(max(m, 0), int(size))
		if int64(m) < size {
			charge.refund(size - int64(m))
		}
		n += m
		if err != nil {
			return n, err
		}
		if int64(m) < size {
			return n, io.ErrShortWrite
		}
	}
	return n, nil
}

// CloseRead shuts down the reading side of the wrapped connection, as
// *net.TCPConn does, or returns an error matching errors.ErrUnsupported when
// it cannot. On success it also ends a read's limiter wait with net.ErrClosed.
func (s *rateConn) CloseRead() error {
	if s == nil || s.conn == nil {
		return ErrNilConn
	}
	if cr, ok := s.conn.(interface{ CloseRead() error }); ok {
		err := cr.CloseRead()
		if err == nil {
			s.read.cancel.close()
		}
		return err
	}
	return errCloseReadUnsupported
}

// CloseWrite shuts down the writing side of the wrapped connection, as
// *net.TCPConn and *tls.Conn do, or returns an error matching
// errors.ErrUnsupported when it cannot. On success it also ends a write's
// limiter wait with net.ErrClosed and refunds the unsent bytes.
func (s *rateConn) CloseWrite() error {
	if s == nil || s.conn == nil {
		return ErrNilConn
	}
	if cw, ok := s.conn.(interface{ CloseWrite() error }); ok {
		err := cw.CloseWrite()
		if err == nil {
			s.write.cancel.close()
		}
		return err
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

// sleep waits wait in d's timer until Disable on gen or g's end. It returns g's
// cause if g ended the wait.
func (d *direction) sleep(wait int64, gen *generation, g *waitGen) error {
	if wait > coalesceWaitNs && d.timer.sleep(wait, gen, g.Done()) == canceled {
		return g.cause()
	}
	return nil
}

// sleepSet waits for the levels in waits in d's timer until g's end. It
// returns g's cause if g ended the wait.
func (d *direction) sleepSet(waits *waitSet, g *waitGen) error {
	if waits.n > 0 && !waits.sleep(&d.timer, g.Done()) {
		return g.cause()
	}
	return nil
}

// Generation states. A generation ends once, by a deadline or by Close.
const (
	genOpen uint32 = iota
	genExpired
	genClosed
)

// waitGen is one generation of a direction's wait signal and the context a wait
// passes to its Limiter's Wait. Its Done channel is made on first use, since
// most waits need none, and never changes; Err is non-nil once it is closed, as
// context.Context requires.
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

// stop disarms dt for good.
func (dt *deadlineTimer) stop() {
	dt.mu.Lock()
	defer dt.mu.Unlock()
	dt.at, dt.firesAt = time.Time{}, time.Time{}
	dt.t.Stop()
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
		if dt != nil {
			dt.t.Stop()
			dt.firesAt = time.Time{}
		}
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
		dt.stop()
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
	// Held across setting the wrapped connection's deadline and the
	// limiter's, so that concurrent setters leave both at the same one:
	// otherwise an earlier setter could overwrite the limiter's deadline
	// after a later one had set both. SetDeadline takes readMu first.
	readMu, writeMu sync.Mutex
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
	c.readMu.Lock()
	defer c.readMu.Unlock()
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
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
	c.readMu.Lock()
	defer c.readMu.Unlock()
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
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
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
