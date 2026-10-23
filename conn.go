package byterate

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// spliceSockets reports whether the ReadFrom of a socket splices from
// another socket, which needs no copy through user space.
const spliceSockets = runtime.GOOS == "linux" || runtime.GOOS == "android"

// ErrNilConn is returned when a wrapper has no underlying connection.
var ErrNilConn = errors.New("byterate: nil connection")

var (
	errCloseReadUnsupported  = fmt.Errorf("byterate: connection does not support CloseRead: %w", errors.ErrUnsupported)
	errCloseWriteUnsupported = fmt.Errorf("byterate: connection does not support CloseWrite: %w", errors.ErrUnsupported)
)

// rateConn is the wrapper of a stream or of datagrams, read from r and
// written to w, and the core of every other wrapper.
type rateConn struct {
	r           io.Reader // nil for a wrapper of writes alone
	w           io.Writer // nil for a wrapper of reads alone
	c           io.Closer // what Close closes, if anything
	read, write direction
	group       *Group      // whose connection slot Close gives back, if set
	released    atomic.Bool // the slot was given back
	datagram    bool        // every Read and Write carries one whole message
}

// direction holds the limiter charged for one direction of a rateConn and
// the signal that ends a wait in it.
type direction struct {
	lim    target     // the limiter, of kind kindNone for none
	cancel waitCancel // ended by Close and the deadlines
	timer  waitTimer  // reused by the waits of every Rate it charges
	// writing holds a token while a Write sends its grants, so that the
	// grants of concurrent Writes do not interleave, as the writes of a
	// *net.TCPConn do not. Made for the writes of a limited stream alone.
	writing chan struct{}
}

// NewConn wraps c so that its traffic in both directions is charged to l and
// limited by it. The result is a net.Conn that reports c's addresses, sets
// c's deadlines and returns c from its RawConn method, which wrapper-aware
// helpers use to unwrap it. A nil l, including a nil *Rate, returns c
// unchanged.
//
// Data passes in grants of what the limit lets through at once: its credit,
// up to the limit's burst after an idle period, or without credit a
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
// The result keeps the zero-copy paths of c: io.Copy to it, through its
// ReadFrom, and from it, through its WriteTo, hands c, and the connection
// or file at the other end, a grant at a time, as an io.LimitedReader, so
// that sendfile and splice still move the data between files and sockets
// where the system has them, within the limit. Copies that cannot take
// such a path go through Read and Write.
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
// the same for other connections that carry one message per call. With a
// Limiter other than a *Rate, the grant is what its Ready allows; one that
// refuses bytes, such as an exhausted Budget, ends the Read or Write with its
// error, a Read keeping the bytes it already read. Through a Switch, a wait
// starts over on the new Limiter when Set replaces the old one.
func NewConn(c net.Conn, l Limiter) net.Conn {
	return NewDuplexConn(c, l, l)
}

// NewDuplexConn is NewConn with separate limiters for reads and writes, so
// that each direction is limited and metered on its own. A nil limiter leaves
// its direction alone; with both nil, c is returned unchanged.
func NewDuplexConn(c net.Conn, read, write Limiter) net.Conn {
	return wrapConn(c, read, write, isDatagramConn(c))
}

// NewDatagramConn is NewConn for a connection whose every Read and Write
// carries one whole message, such as a message-oriented tunnel, which must
// not be split: each Write is charged and sent whole, and a Read may take a
// whole message. NewConn treats UDP, IP and Unix datagram sockets so by
// itself.
func NewDatagramConn(c net.Conn, l Limiter) net.Conn {
	return wrapConn(c, l, l, true)
}

// NewDuplexDatagramConn is NewDatagramConn with separate limiters for reads
// and writes, as NewDuplexConn has.
func NewDuplexDatagramConn(c net.Conn, read, write Limiter) net.Conn {
	return wrapConn(c, read, write, true)
}

// NewPacketConn wraps c so that the datagrams it reads and writes are
// charged to l and limited by it, as NewConn does for a connection's
// datagrams: a WriteTo is charged and sent whole, and a ReadFrom waits until
// the limit has no debt, then reads a whole datagram and charges it. Close,
// the deadlines set through the result and errors end its waits as they end
// those of NewConn. A nil l, including a nil *Rate, returns c unchanged. A
// sender that would rather drop the datagrams that the limit does not let
// through at once than wait for them passes WithDrop.
func NewPacketConn(c net.PacketConn, l Limiter, opts ...OpenOption) net.PacketConn {
	return NewDuplexPacketConn(c, l, l, opts...)
}

// NewDuplexPacketConn is NewPacketConn with separate limiters for reads and
// writes. A nil limiter leaves its direction alone; with both nil, c is
// returned unchanged. Of the options, it takes WithDrop.
func NewDuplexPacketConn(c net.PacketConn, read, write Limiter, opts ...OpenOption) net.PacketConn {
	if classify(read).kind == kindNone && classify(write).kind == kindNone {
		return c
	}
	return newRatePacketConn(c, read, write, dropping(opts))
}

// NewReadWriteCloser is NewConn for an io.ReadWriteCloser, which has no
// addresses or deadlines. Its methods return ErrNilConn if rwc is nil.
func NewReadWriteCloser(rwc io.ReadWriteCloser, l Limiter) io.ReadWriteCloser {
	return NewDuplexReadWriteCloser(rwc, l, l)
}

// NewDuplexReadWriteCloser is NewDuplexConn for an io.ReadWriteCloser.
func NewDuplexReadWriteCloser(rwc io.ReadWriteCloser, read, write Limiter) io.ReadWriteCloser {
	if classify(read).kind == kindNone && classify(write).kind == kindNone {
		return rwc
	}
	c := &rateConn{}
	c.init(rwc, rwc, rwc, read, write, isDatagramConn(rwc))
	return c
}

// NewReader wraps r so that its reads are charged to l and limited by it, as
// the reads of NewConn are, a datagram socket's whole. Close ends a wait in
// progress, and later ones, with net.ErrClosed, and closes r if it is an
// io.Closer. A nil l returns r itself, with a Close that does nothing unless
// r has one. With a Limiter, the methods return ErrNilConn for a nil r.
func NewReader(r io.Reader, l Limiter) io.ReadCloser {
	if classify(l).kind == kindNone {
		if rc, ok := r.(io.ReadCloser); ok {
			return rc
		}
		return io.NopCloser(r)
	}
	c, _ := r.(io.Closer)
	rr := &rateReader{}
	rr.s.init(r, nil, c, l, nil, isDatagramConn(r))
	return rr
}

// NewWriter wraps w so that its writes are charged to l and limited by it, as
// the writes of NewConn are, a datagram socket's whole. Close ends a wait in
// progress, and later ones, with net.ErrClosed, and closes w if it is an
// io.Closer. A nil l returns w itself, with a Close that does nothing unless
// w has one. With a Limiter, the methods return ErrNilConn for a nil w.
func NewWriter(w io.Writer, l Limiter) io.WriteCloser {
	if classify(l).kind == kindNone {
		if wc, ok := w.(io.WriteCloser); ok {
			return wc
		}
		return nopWriteCloser{w}
	}
	c, _ := w.(io.Closer)
	rw := &rateWriter{}
	rw.s.init(nil, w, c, nil, l, isDatagramConn(w))
	return rw
}

// nopWriteCloser is an io.Writer with a Close that does nothing, and a
// ReadFrom that keeps the zero-copy paths of io.Copy to the writer.
type nopWriteCloser struct{ io.Writer }

func (nopWriteCloser) Close() error { return nil }

func (w nopWriteCloser) ReadFrom(r io.Reader) (int64, error) { return io.Copy(w.Writer, r) }

// wrapConn is NewDuplexConn for c, datagrams or not.
func wrapConn(c net.Conn, read, write Limiter, datagram bool) net.Conn {
	if classify(read).kind == kindNone && classify(write).kind == kindNone {
		return c
	}
	return newRateNetConn(c, read, write, datagram)
}

func newRateNetConn(c net.Conn, read, write Limiter, datagram bool) *rateNetConn {
	nc := &rateNetConn{conn: c}
	nc.init(c, c, c, read, write, datagram)
	return nc
}

func newRatePacketConn(c net.PacketConn, read, write Limiter, drop bool) *ratePacketConn {
	pc := &ratePacketConn{pc: c, drop: drop}
	pc.s.init(nil, nil, c, read, write, true)
	return pc
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

// init sets what the wrapper reads from, writes to and closes, which a nil
// interface value leaves out, and the limiters.
func (s *rateConn) init(r io.Reader, w io.Writer, c io.Closer, read, write Limiter, datagram bool) {
	s.r, s.w, s.c = r, w, c
	s.datagram = datagram
	s.read.init(read)
	s.write.init(write)
	if s.write.limited() && !datagram {
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

func (d *direction) setDeadline(t time.Time) {
	if d.limited() {
		d.cancel.setDeadline(t)
	}
}

func (d *direction) init(l Limiter) {
	d.lim.init(l)
	if d.limited() {
		d.cancel.init()
	}
}

// limited reports whether d has a limiter.
func (d *direction) limited() bool { return d.lim.one[0].kind != kindNone }

// grantWrite charges d's limiter for the next part of a write of size bytes
// and waits until it may be sent. It returns how many bytes it charged,
// which rc records for a refund on error: all of size when whole, as for a
// datagram, and otherwise the grant of every level.
func (d *direction) grantWrite(size int64, whole bool, rc *receipt) (int64, error) {
	// Capture the deadline generation before charging, just as reads do.
	g := d.cancel.current()
	rc.reset()
	if r := d.lim.one[0].rate; r != nil {
		n, err := r.grant(g, &d.timer, size, whole, rc)
		if err != nil {
			return n, g.cause()
		}
		return n, nil
	}
	n, err := d.lim.wait(g, &d.timer, size, whole, rc)
	return n, g.causeOf(err)
}

// sent settles a grant of size bytes that rc records, of which a write sent
// m with err: it refunds what was not sent and returns the bytes sent and
// the error that ends the write, io.ErrShortWrite for a short one that has
// none. A writer that reports m < 0, or more than it was given, is taken to
// have sent none, or the grant.
func (rc *receipt) sent(size int64, m int, err error) (int, error) {
	m = min(max(m, 0), int(size))
	if int64(m) < size {
		rc.refund(size - int64(m))
		if err == nil {
			err = io.ErrShortWrite
		}
	}
	return m, err
}

// awaitRead waits until d's limiter may let a read through and returns how
// many of size bytes the read may take: all of them when whole, as for a
// datagram, and otherwise the grant of every level. It charges nothing, so a
// reader blocked in the wrapped connection holds no share of a limit;
// received charges what arrives.
func (d *direction) awaitRead(size int64, whole bool) (int64, error) {
	g := d.cancel.current()
	if r := d.lim.one[0].rate; r != nil {
		n, err := r.ready(g, &d.timer, size, whole)
		if err != nil {
			return 0, g.cause()
		}
		return n, nil
	}
	n, err := d.lim.ready(g, &d.timer, size, whole)
	return n, g.causeOf(err)
}

// received charges n bytes that a read received with err to d's limiter, as
// debt that the next read waits for, and returns the error that ends the
// read: err, or that of a level that refuses the bytes, such as an
// exhausted Budget. The bytes were received, so the charge stands even if
// Close or a deadline cut a wait short.
func (d *direction) received(n int64, err error) error {
	if n <= 0 {
		return err
	}
	var cerr error
	if r := d.lim.one[0].rate; r != nil {
		r.reserveAt(n, nowNs())
	} else {
		cerr = d.lim.charge(n)
	}
	if err == nil {
		return cerr
	}
	return err
}

func (s *rateConn) Read(b []byte) (int, error) {
	if s == nil || s.r == nil {
		return 0, ErrNilConn
	}
	d := &s.read
	if !d.limited() || len(b) == 0 {
		return s.r.Read(b)
	}
	size, err := d.awaitRead(int64(len(b)), s.datagram)
	if err != nil {
		return 0, err
	}
	n, err := s.r.Read(b[:size])
	return n, d.received(int64(n), err)
}

func (s *rateConn) Write(b []byte) (n int, err error) {
	if s == nil || s.w == nil {
		return 0, ErrNilConn
	}
	d := &s.write
	if !d.limited() || len(b) == 0 {
		return s.w.Write(b)
	}
	if d.writing != nil {
		if err := d.lockWrite(); err != nil {
			return 0, err
		}
		defer d.unlockWrite()
	}
	var rc receipt
	for n < len(b) {
		size, err := d.grantWrite(int64(len(b)-n), s.datagram, &rc)
		if err != nil {
			rc.refund(size)
			return n, err
		}
		m, err := s.w.Write(b[n : n+int(size)])
		m, err = rc.sent(size, m, err)
		n += m
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

// copyChunk is the most that ReadFrom and WriteTo ask a grant for: it is
// charged before the copy, as a Write is, and what does not arrive is
// refunded, so it also bounds the bytes an unlimited level counts before
// they pass.
const copyChunk = 256 << 10

// ReadFrom copies r to the wrapped writer until EOF, as io.Copy does, a
// grant at a time. When the writer has a ReadFrom of its own and r is a
// file or, where the system splices them, a socket, each grant goes to that
// ReadFrom as an io.LimitedReader of r, so that the copy keeps a zero-copy
// path within the limit; otherwise it goes through Write.
func (s *rateConn) ReadFrom(r io.Reader) (int64, error) {
	if s == nil || s.w == nil {
		return 0, ErrNilConn
	}
	d := &s.write
	rf, forward := s.w.(io.ReaderFrom)
	if !d.limited() {
		if forward {
			return rf.ReadFrom(r)
		}
		return io.Copy(s.w, r)
	}
	lr, outer := r.(*io.LimitedReader)
	src := r
	if outer {
		src = lr.R // one level, as the zero-copy paths unwrap it
	}
	if !forward || s.datagram || !zeroCopySource(src) {
		return io.Copy(writerOnly{s}, r)
	}
	if d.writing != nil {
		if err := d.lockWrite(); err != nil {
			return 0, err
		}
		defer d.unlockWrite()
	}
	var rc receipt
	chunk := &io.LimitedReader{R: src}
	var n int64
	for {
		want := int64(copyChunk)
		if outer {
			if lr.N <= 0 {
				return n, nil
			}
			want = min(want, lr.N)
		}
		size, err := d.grantWrite(want, false, &rc)
		if err != nil {
			rc.refund(size)
			return n, err
		}
		chunk.N = size
		m, err := rf.ReadFrom(chunk)
		m = min(max(m, 0), size)
		if m < size {
			rc.refund(size - m)
		}
		n += m
		if outer {
			lr.N -= m
		}
		if err != nil || m < size {
			return n, err // short without an error at the end of r
		}
	}
}

// WriteTo copies the wrapped reader to w until EOF, as io.Copy does, a
// grant at a time. When w has a ReadFrom of its own and is a socket, a file
// or a writer of this package, and the reader is a socket that the system
// splices, each grant goes to that ReadFrom as an io.LimitedReader of the
// reader, so that the copy keeps a zero-copy path within the limit;
// otherwise it goes through Read.
func (s *rateConn) WriteTo(w io.Writer) (int64, error) {
	if s == nil || s.r == nil {
		return 0, ErrNilConn
	}
	d := &s.read
	if !d.limited() {
		return io.Copy(w, s.r)
	}
	rf, forward := w.(io.ReaderFrom)
	if !forward || s.datagram || !zeroCopySource(s.r) || !zeroCopySink(w) {
		return io.Copy(w, readerOnly{s})
	}
	chunk := &io.LimitedReader{R: s.r}
	var n int64
	for {
		size, err := d.awaitRead(copyChunk, false)
		if err != nil {
			return n, err
		}
		chunk.N = size
		m, err := rf.ReadFrom(chunk)
		m = max(m, 0)
		n += m
		err = d.received(m, err)
		if err != nil || m < size {
			return n, err
		}
	}
}

// writerOnly and readerOnly hide the ReadFrom and WriteTo of a wrapper from
// io.Copy, which would call them again.
type (
	writerOnly struct{ io.Writer }
	readerOnly struct{ io.Reader }
)

// zeroCopySource reports whether the ReadFrom of a socket or a file may copy
// from r without passing its bytes through user space: r has a file
// descriptor and is a file, which sendfile and copy_file_range copy from,
// or a socket where the system splices sockets.
func zeroCopySource(r io.Reader) bool {
	if _, ok := r.(syscall.Conn); !ok {
		return false
	}
	if _, ok := r.(interface{ Stat() (fs.FileInfo, error) }); ok {
		return true
	}
	return spliceSockets
}

// zeroCopySink reports whether w may copy without passing the bytes through
// user space: it has a file descriptor, or it is a stream writer of this
// package whose ReadFrom passes grants on to such a writer.
func zeroCopySink(w io.Writer) bool {
	var s *rateConn
	switch t := w.(type) {
	case *rateConn:
		s = t
	case *rateNetConn:
		s = &t.rateConn
	case *rateWriter:
		s = &t.s
	case syscall.Conn:
		return true
	default:
		return false
	}
	if _, ok := s.w.(io.ReaderFrom); !ok || s.datagram {
		return false
	}
	return zeroCopySink(s.w)
}

// CloseRead shuts down the reading side of the wrapped connection, as
// *net.TCPConn does, or returns an error matching errors.ErrUnsupported when
// it cannot. On success it also ends a read's limiter wait with net.ErrClosed.
func (s *rateConn) CloseRead() error {
	if s == nil || s.c == nil {
		return ErrNilConn
	}
	if cr, ok := s.c.(interface{ CloseRead() error }); ok {
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
	if s == nil || s.c == nil {
		return ErrNilConn
	}
	if cw, ok := s.c.(interface{ CloseWrite() error }); ok {
		err := cw.CloseWrite()
		if err == nil {
			s.write.cancel.close()
		}
		return err
	}
	return errCloseWriteUnsupported
}

func (s *rateConn) Close() error {
	if s == nil || s.r == nil && s.w == nil && s.c == nil {
		return ErrNilConn
	}
	s.read.cancel.close()
	s.write.cancel.close()
	if s.group != nil && s.released.CompareAndSwap(false, true) {
		s.group.Release()
	}
	if s.c == nil {
		return nil
	}
	return s.c.Close()
}

// rateReader and rateWriter are the wrappers of NewReader and NewWriter: a
// rateConn of one direction, with its methods for that direction alone.
type (
	rateReader struct{ s rateConn }
	rateWriter struct{ s rateConn }
)

func (r *rateReader) Read(b []byte) (int, error)            { return r.s.Read(b) }
func (r *rateReader) WriteTo(w io.Writer) (int64, error)    { return r.s.WriteTo(w) }
func (r *rateReader) Close() error                          { return r.s.Close() }
func (w *rateWriter) Write(b []byte) (int, error)           { return w.s.Write(b) }
func (w *rateWriter) ReadFrom(src io.Reader) (int64, error) { return w.s.ReadFrom(src) }
func (w *rateWriter) Close() error                          { return w.s.Close() }

type rateNetConn struct {
	rateConn
	conn net.Conn
	dl   deadlines
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
	return c.dl.set(c.conn, &c.rateConn, t, true, true)
}

// SetReadDeadline sets the read deadline of the wrapped connection and,
// unless it rejects it, of the waits in the read limiter.
func (c *rateNetConn) SetReadDeadline(t time.Time) error {
	return c.dl.set(c.conn, &c.rateConn, t, true, false)
}

// SetWriteDeadline sets the write deadline of the wrapped connection and,
// unless it rejects it, of the waits in the write limiter.
func (c *rateNetConn) SetWriteDeadline(t time.Time) error {
	return c.dl.set(c.conn, &c.rateConn, t, false, true)
}

// RawConn returns the wrapped connection.
func (c *rateNetConn) RawConn() net.Conn {
	return c.conn
}

// ratePacketConn is the wrapper of NewPacketConn.
type ratePacketConn struct {
	s    rateConn // reads and writes nothing itself: its directions and Close
	pc   net.PacketConn
	dl   deadlines
	drop bool // WithDrop: drop datagrams that would wait
}

// admit charges size bytes to d's limiter, recording the charge in rc, if
// it lets them through at once, as TryWait does, and otherwise charges
// nothing and returns ErrWouldWait or the refusal.
func (d *direction) admit(size int64, rc *receipt) error {
	rc.reset()
	now := nowNs()
	if r := d.lim.one[0].rate; r != nil {
		if r.tryTake(size, now, rc) {
			return nil
		}
		return ErrWouldWait
	}
	return d.lim.tryWait(size, now, rc)
}

func (c *ratePacketConn) ReadFrom(b []byte) (int, net.Addr, error) {
	if c.pc == nil {
		return 0, nil, ErrNilConn
	}
	d := &c.s.read
	if !d.limited() || len(b) == 0 {
		return c.pc.ReadFrom(b)
	}
	if c.drop {
		var rc receipt
		for {
			n, addr, err := c.pc.ReadFrom(b)
			if n <= 0 {
				return n, addr, err
			}
			aerr := d.admit(int64(n), &rc)
			if aerr == nil {
				return n, addr, err
			}
			if !errors.Is(aerr, ErrWouldWait) {
				return 0, addr, aerr
			}
			if err != nil {
				return 0, addr, err
			}
			// Dropped: read the next one, until c's deadline or Close.
		}
	}
	if _, err := d.awaitRead(int64(len(b)), true); err != nil {
		return 0, nil, err
	}
	n, addr, err := c.pc.ReadFrom(b)
	return n, addr, d.received(int64(n), err)
}

func (c *ratePacketConn) WriteTo(b []byte, addr net.Addr) (int, error) {
	if c.pc == nil {
		return 0, ErrNilConn
	}
	d := &c.s.write
	if !d.limited() || len(b) == 0 {
		return c.pc.WriteTo(b, addr)
	}
	var rc receipt
	size := int64(len(b))
	if c.drop {
		if g := d.cancel.current(); g.ended() {
			return 0, g.cause()
		}
		if err := d.admit(size, &rc); errors.Is(err, ErrWouldWait) {
			return len(b), nil // dropped, as a full socket buffer drops it
		} else if err != nil {
			return 0, err
		}
	} else {
		var err error
		if size, err = d.grantWrite(size, true, &rc); err != nil {
			rc.refund(size)
			return 0, err
		}
	}
	m, err := c.pc.WriteTo(b, addr)
	return rc.sent(size, m, err)
}

func (c *ratePacketConn) Close() error { return c.s.Close() }

func (c *ratePacketConn) LocalAddr() net.Addr {
	if c.pc == nil {
		return nil
	}
	return c.pc.LocalAddr()
}

func (c *ratePacketConn) SetDeadline(t time.Time) error {
	return c.dl.set(c.pc, &c.s, t, true, true)
}

func (c *ratePacketConn) SetReadDeadline(t time.Time) error {
	return c.dl.set(c.pc, &c.s, t, true, false)
}

func (c *ratePacketConn) SetWriteDeadline(t time.Time) error {
	return c.dl.set(c.pc, &c.s, t, false, true)
}

// deadliner is a connection with deadlines, a net.Conn or a net.PacketConn.
type deadliner interface {
	SetDeadline(t time.Time) error
	SetReadDeadline(t time.Time) error
	SetWriteDeadline(t time.Time) error
}

// deadlines are held across setting the wrapped connection's deadline and
// the limiter's, so that concurrent setters leave both at the same one:
// otherwise an earlier setter could overwrite the limiter's deadline after
// a later one had set both. SetDeadline takes readMu first.
type deadlines struct {
	readMu, writeMu sync.Mutex
}

// set sets the deadline t of c's reads, writes or both and, unless c rejects
// it, of the waits in s's limiters of them.
func (dl *deadlines) set(c deadliner, s *rateConn, t time.Time, read, write bool) error {
	if c == nil {
		return ErrNilConn
	}
	if read {
		dl.readMu.Lock()
		defer dl.readMu.Unlock()
	}
	if write {
		dl.writeMu.Lock()
		defer dl.writeMu.Unlock()
	}
	var err error
	switch {
	case read && write:
		err = c.SetDeadline(t)
	case read:
		err = c.SetReadDeadline(t)
	default:
		err = c.SetWriteDeadline(t)
	}
	if limitsWaits(err) {
		if read {
			s.read.setDeadline(t)
		}
		if write {
			s.write.setDeadline(t)
		}
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
