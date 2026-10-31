package byterate

import (
	"context"
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

// Option changes how a wrapper of this package, or a Group's Open, wraps a
// connection or stream.
type Option struct {
	read, write Limiter
	limiters    bool
	counters    []*Counter
	datagram    bool
	drop        bool
	ctx         context.Context
}

// WithLimiter charges both directions to l, the same Limiter, so that reads
// and writes share one limit. A Group's Open charges it on top of the
// Group's levels.
func WithLimiter(l Limiter) Option { return Option{read: l, write: l, limiters: true} }

// WithLimiters charges reads to read and writes to write, each limited and
// counted on its own; a nil Limiter leaves its direction unlimited.
func WithLimiters(read, write Limiter) Option {
	return Option{read: read, write: write, limiters: true}
}

// WithCounter adds the bytes of the wrapper to c, as Counter.Attach does, until
// it is closed. It may be given more than once.
func WithCounter(c *Counter) Option { return Option{counters: []*Counter{c}} }

// WithDatagram treats every Read and Write as one whole message, which is
// never split, as for a message-oriented tunnel. UDP, IP and Unix datagram
// sockets are recognized without it.
func WithDatagram() Option { return Option{datagram: true} }

// WithDrop makes a PacketConn drop the datagrams that its limit does not let
// through at once, as TryWait decides, instead of waiting for them: such a
// datagram is not charged, a WriteTo of it returns len(b) and nil, as a UDP
// socket whose buffer is full does, and a ReadFrom discards it and reads the
// next one. A limit that refuses a datagram, such as an exhausted Budget,
// still ends the WriteTo or ReadFrom with its error.
func WithDrop() Option { return Option{drop: true} }

// WithWait makes a Group's Open wait for a connection slot, as
// AcquireContext does, until ctx is done, instead of refusing it with a
// *TooManyConnsError.
func WithWait(ctx context.Context) Option { return Option{ctx: ctx} }

// options are the Options of a wrapper, merged.
type options struct {
	read, write Limiter
	counters    []*Counter
	datagram    bool
	drop        bool
	ctx         context.Context
}

func mergeOptions(opts []Option) options {
	var o options
	for _, x := range opts {
		if x.limiters {
			o.read, o.write = NewHierarchy(o.read, x.read), NewHierarchy(o.write, x.write)
		}
		o.counters = append(o.counters, x.counters...)
		o.datagram = o.datagram || x.datagram
		o.drop = o.drop || x.drop
		if x.ctx != nil {
			o.ctx = x.ctx
		}
	}
	return o
}

// Meter holds the bytes read and written through one wrapper: the
// wrapper's reads and writes add to it alone, each direction on a cache line
// of its own, so counting costs an uncontended atomic add. Counter.Attach
// adds a Meter's bytes to a Counter shared by many wrappers without any
// cost to their reads and writes.
type Meter struct {
	read    atomic.Int64
	_       [cacheLineSize - 8]byte
	written atomic.Int64
	_       [cacheLineSize - 8]byte
	owner   *rateConn // the wrapper, for its Group to update it
}

// Total returns the bytes read and written so far.
func (m *Meter) Total() (read, written int64) {
	if m == nil {
		return 0, 0
	}
	return m.read.Load(), m.written.Load()
}

// rateConn is the core of every wrapper: it counts the bytes of each
// direction in its Meter, and charges them to the direction's limiter and
// leases while it has any.
type rateConn struct {
	meter       Meter
	r           io.Reader // nil for a wrapper of writes alone
	w           io.Writer // nil for a wrapper of reads alone
	c           io.Closer // what Close closes, if anything
	read, write direction
	datagram    bool // every Read and Write carries one whole message
	closed      atomic.Bool

	// Under configMu: the Limiters of the wrapper's own, its Group and
	// the Counters it was attached to by WithCounter.
	own      [2]Limiter
	group    *Group
	counters []*Counter
}

// Direction states: with none set, a direction passes its I/O straight
// through and only counts it.
const (
	dirLimited uint32 = 1 << iota // a limiter other than leases applies
	dirLeased                     // Budgets lease bytes to the direction
)

// direction holds the limiting of one direction of a rateConn.
type direction struct {
	state  atomic.Uint32
	total  *atomic.Int64 // the Meter's count of the direction
	sw     Switch        // the limiter, replaced by apply
	lim    target        // sw, prepared for charging
	cancel waitCancel    // ended by Close and the deadlines
	timer  waitTimer     // reused by the waits of every Rate it charges
	// writing holds a token while a limited Write sends its grants, so
	// that the grants of concurrent Writes do not interleave, as the
	// writes of a *net.TCPConn do not. Made for the writes of a stream.
	writing chan struct{}
	leases  leaseSet
	// src is the Limiter apply was given; charged lists the Budgets taken
	// out of leasing because a step's Limit applies. Under configMu.
	src     Limiter
	charged []*Budget
}

func (d *direction) init(total *atomic.Int64) {
	d.total = total
	d.lim.init(&d.sw)
	d.cancel.init()
	d.leases.end.Store(maxI64)
}

// SetLimiters replaces the Limiters of the wrapper's reads and writes, as
// WithLimiters set them, from the next read or write; a wait in progress
// ends and starts over on the new ones, and nil leaves a direction
// unlimited. A Group's levels still apply on top.
func (s *rateConn) SetLimiters(read, write Limiter) {
	configMu.Lock()
	defer configMu.Unlock()
	s.own = [2]Limiter{read, write}
	s.applyLocked()
}

// Meter returns the Meter of the wrapper's bytes.
func (s *rateConn) Meter() *Meter { return &s.meter }

// applyLocked gives each direction its Limiter: its own, within those of
// its Group's levels. configMu must be held.
func (s *rateConn) applyLocked() {
	read, write := s.own[0], s.own[1]
	for a := s.group; a != nil; a = a.parent.Load() {
		read, write = NewHierarchy(read, a.readLimiter()), NewHierarchy(write, a.writeLimiter())
	}
	s.read.apply(read)
	s.write.apply(write)
}

// apply makes l the direction's Limiter. The Budgets among its levels lease
// bytes to the direction rather than being charged each read and write;
// the other levels, and the Budgets in charged, are charged as a
// hierarchy. configMu must be held.
func (d *direction) apply(l Limiter) {
	if l != d.src {
		d.charged = nil
	}
	d.src = l
	var lease []*Budget
	rest := &hierarchy{}
	for _, lv := range flatten(l) {
		if lv.kind == kindBudget && !containsBudget(d.charged, lv.l.(*Budget)) {
			lease = append(lease, lv.l.(*Budget))
			continue
		}
		rest.add(lv.l)
	}
	d.leases.set(lease, d.total)
	lim := Limiter(nil)
	switch len(rest.lv) + len(rest.views) {
	case 0:
	case 1:
		lim = NewHierarchy(rest.levelLimiters()...)
	default:
		lim = rest
	}
	var st uint32
	if len(lease) > 0 {
		st |= dirLeased
	}
	if lim != nil {
		// The limiter before the state, so that no I/O sees the state
		// without it; without one, the state first.
		d.sw.swap(lim)
		d.state.Store(st | dirLimited)
		return
	}
	d.state.Store(st)
	if d.sw.state().t.limiter() != nil {
		d.sw.swap(nil) // a wait in progress passes at once
	}
}

// charge takes b out of the direction's leases, to charge it at every read
// and write, as a Limiter, while a step's Limit applies.
func (d *direction) charge(b *Budget) {
	configMu.Lock()
	defer configMu.Unlock()
	if !containsBudget(d.charged, b) {
		d.charged = append(d.charged, b)
		d.apply(d.src)
	}
}

func containsBudget(bs []*Budget, b *Budget) bool {
	for _, x := range bs {
		if x == b {
			return true
		}
	}
	return false
}

// flatten returns the levels of l, those of a hierarchy one by one.
func flatten(l Limiter) []level {
	lv := classify(l)
	switch lv.kind {
	case kindNone:
		return nil
	case kindHierarchy:
		h := lv.l.(*hierarchy)
		out := make([]level, 0, len(h.lv)+len(h.views))
		out = append(out, h.lv...)
		return append(out, h.views...)
	default:
		return []level{lv}
	}
}

// levelLimiters returns the Limiters of h's levels.
func (h *hierarchy) levelLimiters() []Limiter {
	out := make([]Limiter, 0, len(h.lv)+len(h.views))
	for _, x := range h.lv {
		out = append(out, x.l)
	}
	for _, x := range h.views {
		out = append(out, x.l)
	}
	return out
}

// finish settles the leases, takes the Meter out of the Counters and gives
// back the Group's slot, once, when the wrapper is closed.
func (s *rateConn) finish() {
	configMu.Lock()
	g := s.group
	for _, c := range s.counters {
		c.Detach(&s.meter)
	}
	for a := g; a != nil; a = a.parent.Load() {
		a.Traffic().Detach(&s.meter)
	}
	s.read.leases.set(nil, s.read.total)
	s.write.leases.set(nil, s.write.total)
	configMu.Unlock()
	if g != nil {
		g.Release()
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

// grantWrite charges d's limiter for the next part of a write of size bytes
// and waits until it may be sent. It returns how many bytes it charged,
// which rc records for a refund on error: all of size when whole, as for a
// datagram, and otherwise the grant of every level.
func (d *direction) grantWrite(size int64, whole bool, rc *receipt) (int64, error) {
	// Capture the deadline generation before charging, just as reads do.
	g := d.cancel.current()
	rc.reset()
	for {
		// A single Rate is charged in place; a Set of the Switch ends its
		// wait, which starts over on the new limiter.
		st := d.sw.state()
		r := st.t.one[0].rate
		if r == nil {
			n, err := d.lim.wait(g, &d.timer, size, whole, rc)
			return n, g.causeOf(err)
		}
		n, err := r.grant(g, &d.timer, size, whole, rc, st.changed)
		if errors.Is(err, errSwitched) {
			rc.refund(n)
			rc.reset()
			continue
		}
		if err != nil {
			return n, g.cause()
		}
		return n, nil
	}
}

// sent settles a grant of size bytes that rc records, of which a write sent
// m with err: it refunds what was not sent and returns the bytes sent and
// the error that ends the write, io.ErrShortWrite for a short one that has
// none. A writer that reports m < 0, or more than it was given, is taken to
// have sent none, or the grant. A nil rc refunds nothing.
func (rc *receipt) sent(size int64, m int, err error) (int, error) {
	m = min(max(m, 0), int(size))
	if int64(m) < size {
		if rc != nil {
			rc.refund(size - int64(m))
		}
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
	for {
		st := d.sw.state()
		r := st.t.one[0].rate
		if r == nil {
			n, err := d.lim.ready(g, &d.timer, size, whole)
			return n, g.causeOf(err)
		}
		n, err := r.ready(g, &d.timer, size, whole, st.changed)
		if errors.Is(err, errSwitched) {
			continue
		}
		if err != nil {
			return 0, g.cause()
		}
		return n, nil
	}
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
	if r := d.sw.state().t.one[0].rate; r != nil {
		r.reserveAt(n, nowNs())
	} else {
		cerr = d.lim.charge(n)
	}
	if err == nil {
		return cerr
	}
	return err
}

// allow returns how many of size bytes d's leases let pass now, all of
// them when whole, leasing more where needed. With a step's Limit applying
// to a Budget, it moves the Budget to d's limiter and returns errRecheck.
func (d *direction) allow(size int64, whole bool) (int64, error) {
	need := int64(1)
	if whole {
		need = size
	}
	for {
		credit := d.leases.end.Load() - d.total.Load()
		if credit >= need {
			if whole {
				return size, nil
			}
			return min(size, credit), nil
		}
		b, err := d.leases.refill(d.total, need)
		if errors.Is(err, ErrWouldWait) {
			d.charge(b)
			return 0, errRecheck
		}
		if err != nil {
			return 0, err
		}
	}
}

// errRecheck makes an I/O load its direction's state again.
var errRecheck = errors.New("byterate: direction changed")

// count adds n bytes, if positive, to d's Meter.
func (d *direction) count(n int64) {
	if n > 0 {
		d.total.Add(n)
	}
}

func (s *rateConn) Read(b []byte) (int, error) {
	if s == nil || s.r == nil {
		return 0, ErrNilConn
	}
	d := &s.read
	if st := d.state.Load(); st == 0 || len(b) == 0 ||
		st == dirLeased && d.leases.end.Load()-d.total.Load() >= int64(len(b)) {
		n, err := s.r.Read(b)
		d.count(int64(n))
		return n, err
	}
	return s.readSlow(b)
}

// readSlow is Read through d's leases and limiter.
func (s *rateConn) readSlow(b []byte) (int, error) {
	d := &s.read
	for {
		st := d.state.Load()
		size := int64(len(b))
		if st&dirLeased != 0 {
			allowed, err := d.allow(size, false)
			if errors.Is(err, errRecheck) {
				continue
			}
			if err != nil {
				return 0, err
			}
			if !s.datagram {
				size = allowed
			}
		}
		if st&dirLimited != 0 {
			var err error
			if size, err = d.awaitRead(size, s.datagram); err != nil {
				return 0, err
			}
		}
		n, err := s.r.Read(b[:size])
		d.count(int64(n))
		if st&dirLimited != 0 {
			err = d.received(int64(n), err)
		}
		return n, err
	}
}

func (s *rateConn) Write(b []byte) (int, error) {
	if s == nil || s.w == nil {
		return 0, ErrNilConn
	}
	d := &s.write
	if st := d.state.Load(); st == 0 || len(b) == 0 ||
		st == dirLeased && d.leases.end.Load()-d.total.Load() >= int64(len(b)) {
		// No limit, or leases that cover the write.
		n, err := s.w.Write(b)
		d.count(int64(n))
		return n, err
	}
	return s.writeSlow(b)
}

// writeSlow is Write through d's leases and limiter.
func (s *rateConn) writeSlow(b []byte) (n int, err error) {
	d := &s.write
	locked := false
	defer func() {
		if locked {
			d.unlockWrite()
		}
	}()
	var rc receipt
	for n < len(b) {
		st := d.state.Load()
		size := int64(len(b) - n)
		if st&dirLeased != 0 {
			if size, err = d.allow(size, s.datagram); errors.Is(err, errRecheck) {
				continue
			} else if err != nil {
				return n, err
			}
		}
		var r *receipt
		if st&dirLimited != 0 {
			if !locked && d.writing != nil {
				if err := d.lockWrite(); err != nil {
					return n, err
				}
				locked = true
			}
			r = &rc
			if size, err = d.grantWrite(size, s.datagram, r); err != nil {
				r.refund(size)
				return n, err
			}
		}
		m, err := s.w.Write(b[n : n+int(size)])
		m, err = r.sent(size, m, err)
		d.count(int64(m))
		n += m
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

// copyChunk is the most that ReadFrom and WriteTo pass to the zero-copy
// path of the wrapped connection at once: a limit or lease is checked again
// after every chunk, so a change of limit applies within one, and a
// limited direction charges a chunk before the copy, refunding what does
// not arrive.
const copyChunk = 256 << 10

// ReadFrom copies r to the wrapped writer until EOF, as io.Copy does. When
// the writer has a ReadFrom of its own and r is a file or, where the system
// splices them, a socket, the copy goes to that ReadFrom in chunks, each an
// io.LimitedReader of r within the limit and leases, so that it keeps a
// zero-copy path; otherwise it goes through Write.
func (s *rateConn) ReadFrom(r io.Reader) (int64, error) {
	if s == nil || s.w == nil {
		return 0, ErrNilConn
	}
	d := &s.write
	rf, forward := s.w.(io.ReaderFrom)
	lr, outer := r.(*io.LimitedReader)
	src := r
	if outer {
		src = lr.R // one level, as the zero-copy paths unwrap it
	}
	if !forward || s.datagram || !zeroCopySource(src) {
		return io.Copy(writerOnly{s}, r)
	}
	locked := false
	defer func() {
		if locked {
			d.unlockWrite()
		}
	}()
	var rc receipt
	chunk := &io.LimitedReader{R: src}
	var n int64
	for {
		size := int64(copyChunk)
		if outer {
			if lr.N <= 0 {
				return n, nil
			}
			size = min(size, lr.N)
		}
		st := d.state.Load()
		var err error
		if st&dirLeased != 0 {
			if size, err = d.allow(size, false); errors.Is(err, errRecheck) {
				continue
			} else if err != nil {
				return n, err
			}
		}
		limited := st&dirLimited != 0
		if limited {
			if !locked {
				if err := d.lockWrite(); err != nil {
					return n, err
				}
				locked = true
			}
			if size, err = d.grantWrite(size, false, &rc); err != nil {
				rc.refund(size)
				return n, err
			}
		}
		chunk.N = size
		m, err := rf.ReadFrom(chunk)
		m = min(max(m, 0), size)
		if limited && m < size {
			rc.refund(size - m)
		}
		d.count(m)
		n += m
		if outer {
			lr.N -= m
		}
		if err != nil || m < size {
			return n, err // short without an error at the end of r
		}
	}
}

// WriteTo copies the wrapped reader to w until EOF, as io.Copy does. When w
// has a ReadFrom of its own and is a socket, a file or a writer of this
// package, and the reader is a socket that the system splices, the copy
// goes to that ReadFrom in chunks, each an io.LimitedReader of the reader
// within the limit and leases, so that it keeps a zero-copy path; otherwise
// it goes through Read.
func (s *rateConn) WriteTo(w io.Writer) (int64, error) {
	if s == nil || s.r == nil {
		return 0, ErrNilConn
	}
	d := &s.read
	rf, forward := w.(io.ReaderFrom)
	if !forward || s.datagram || !zeroCopySource(s.r) || !zeroCopySink(w) {
		return io.Copy(w, readerOnly{s})
	}
	chunk := &io.LimitedReader{R: s.r}
	var n int64
	for {
		size := int64(copyChunk)
		st := d.state.Load()
		var err error
		if st&dirLeased != 0 {
			if size, err = d.allow(size, false); errors.Is(err, errRecheck) {
				continue
			} else if err != nil {
				return n, err
			}
		}
		limited := st&dirLimited != 0
		if limited {
			if size, err = d.awaitRead(size, false); err != nil {
				return n, err
			}
		}
		chunk.N = size
		m, err := rf.ReadFrom(chunk)
		m = max(m, 0)
		d.count(m)
		n += m
		if limited {
			err = d.received(m, err)
		}
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
// package whose ReadFrom passes chunks on to such a writer.
func zeroCopySink(w io.Writer) bool {
	var s *rateConn
	switch t := w.(type) {
	case *Conn:
		s = &t.rateConn
	case *Stream:
		s = &t.rateConn
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

// Close ends the waits in progress with net.ErrClosed, gives back the
// unused bytes of the leases, takes the wrapper's bytes out of its Counters
// as counted, gives back its Group's slot and closes what it wraps. Only
// the first Close does all but the last.
func (s *rateConn) Close() error {
	if s == nil || s.r == nil && s.w == nil && s.c == nil {
		return ErrNilConn
	}
	s.read.cancel.close()
	s.write.cancel.close()
	if s.closed.CompareAndSwap(false, true) {
		s.finish()
	}
	if s.c == nil {
		return nil
	}
	return s.c.Close()
}

// Stream is the wrapper of an io.Reader, io.Writer or io.ReadWriteCloser,
// made by NewReader, NewWriter and NewStream: a Conn without addresses or
// deadlines.
type Stream struct{ rateConn }

// NewStream wraps rwc as NewConn wraps a connection. Its methods return
// ErrNilConn if rwc is nil.
func NewStream(rwc io.ReadWriteCloser, opts ...Option) *Stream {
	s := &Stream{}
	s.init(rwc, rwc, rwc, withDatagram(mergeOptions(opts), rwc), nil)
	return s
}

// NewReader wraps r for reading alone, as NewStream does; Close closes r if
// it is an io.Closer, and Write returns ErrNilConn.
func NewReader(r io.Reader, opts ...Option) *Stream {
	c, _ := r.(io.Closer)
	s := &Stream{}
	s.init(r, nil, c, withDatagram(mergeOptions(opts), r), nil)
	return s
}

// NewWriter wraps w for writing alone, as NewStream does; Close closes w if
// it is an io.Closer, and Read returns ErrNilConn.
func NewWriter(w io.Writer, opts ...Option) *Stream {
	c, _ := w.(io.Closer)
	s := &Stream{}
	s.init(nil, w, c, withDatagram(mergeOptions(opts), w), nil)
	return s
}

func withDatagram(o options, c any) options {
	o.datagram = o.datagram || isDatagramConn(c)
	return o
}

// Conn is the wrapper of a net.Conn made by NewConn and Group.Open. It
// reports the wrapped connection's addresses, sets its deadlines and
// returns it from RawConn, which wrapper-aware helpers use to unwrap it.
type Conn struct {
	rateConn
	conn net.Conn
	dl   deadlines
}

// NewConn wraps c so that its bytes are counted in the Meter, and, with
// WithLimiter or WithLimiters, limited. Without a limit, and while none
// applies, reads and writes pass straight through and only count, at the
// cost of an atomic add; SetLimiters adds or changes a limit at any time,
// also for a transfer in progress.
//
// Limited data passes in grants of what the limit lets through at once:
// its credit, up to the limit's burst after an idle period, or without
// credit a quantum of about 20 ms of traffic, no more than the credit at
// its cap. A Write sends its bytes a grant at a time, each charged before
// it goes out, so a large Write neither bursts past the credit nor holds a
// shared limit while other connections wait; concurrent limited Writes do
// not interleave. A Read first waits until the limit has no debt, then
// reads at most a grant and charges what arrived, so a reader in debt
// leaves the data with the sender and one blocked waiting for data holds
// no share of the limit. Readers waiting for the debt of one shared limit
// read together when it is repaid, up to a grant each.
//
// A Budget among the levels leases bytes to the connection instead of being
// charged at each read and write, as Budget.Lease describes: a Write sends
// what the leases cover and a Read reads no more, so a Budget is never
// passed, and while a step's Limit applies the Budget is charged at every
// read and write as a Limiter.
//
// The result keeps the zero-copy paths of c: io.Copy to it, through its
// ReadFrom, and from it, through its WriteTo, hands c, and the connection
// or file at the other end, a chunk at a time as an io.LimitedReader, so
// that sendfile and splice still move the data between files and sockets
// where the system has them, counted and within the limit. Copies that
// cannot take such a path go through Read and Write.
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
// a Limiter is returned as it is.
//
// UDP, IP and Unix datagram sockets, and connections given WithDatagram,
// are never split: each Write is charged and sent whole, and a Read may
// take a whole datagram. With a Limiter other than a *Rate, the grant is
// what its Ready allows; one that refuses bytes ends the Read or Write with
// its error, a Read keeping the bytes it already read.
func NewConn(c net.Conn, opts ...Option) *Conn {
	return newConn(c, mergeOptions(opts), nil)
}

func newConn(c net.Conn, o options, g *Group) *Conn {
	nc := &Conn{conn: c}
	o = withDatagram(o, c)
	nc.init(c, c, c, o, g)
	return nc
}

// init sets what the wrapper reads from, writes to and closes, which a nil
// interface value leaves out, and its options, within g, whose levels limit
// the wrapper and whose Traffic counts it, unless g is nil.
func (s *rateConn) init(r io.Reader, w io.Writer, c io.Closer, o options, g *Group) {
	s.r, s.w, s.c = r, w, c
	s.datagram = o.datagram
	s.meter.owner = s
	s.read.init(&s.meter.read)
	s.write.init(&s.meter.written)
	if !s.datagram {
		s.write.writing = make(chan struct{}, 1)
	}
	configMu.Lock()
	defer configMu.Unlock()
	s.own = [2]Limiter{o.read, o.write}
	s.counters = o.counters
	s.group = g
	for _, c := range s.counters {
		c.Attach(&s.meter)
	}
	for a := g; a != nil; a = a.parent.Load() {
		a.Traffic().Attach(&s.meter)
	}
	s.applyLocked()
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

func (c *Conn) LocalAddr() net.Addr {
	if c.conn == nil {
		return nil
	}
	return c.conn.LocalAddr()
}

func (c *Conn) RemoteAddr() net.Addr {
	if c.conn == nil {
		return nil
	}
	return c.conn.RemoteAddr()
}

// SetDeadline sets the read and write deadlines of the wrapped connection
// and, unless it rejects them, of the waits in the limiters.
func (c *Conn) SetDeadline(t time.Time) error {
	return c.dl.set(c.conn, &c.rateConn, t, true, true)
}

// SetReadDeadline sets the read deadline of the wrapped connection and,
// unless it rejects it, of the waits in the read limiter.
func (c *Conn) SetReadDeadline(t time.Time) error {
	return c.dl.set(c.conn, &c.rateConn, t, true, false)
}

// SetWriteDeadline sets the write deadline of the wrapped connection and,
// unless it rejects it, of the waits in the write limiter.
func (c *Conn) SetWriteDeadline(t time.Time) error {
	return c.dl.set(c.conn, &c.rateConn, t, false, true)
}

// RawConn returns the wrapped connection.
func (c *Conn) RawConn() net.Conn {
	return c.conn
}

// PacketConn is the wrapper of a net.PacketConn made by NewPacketConn and
// Group.OpenPacketConn.
type PacketConn struct {
	s    rateConn // reads and writes nothing itself: its directions and Close
	pc   net.PacketConn
	dl   deadlines
	drop bool // WithDrop: drop datagrams that would wait
}

// NewPacketConn wraps c so that the datagrams it reads and writes are
// counted in the Meter and, with WithLimiter or WithLimiters, limited, as
// NewConn does for a connection's datagrams: a WriteTo is charged and sent
// whole, and a ReadFrom waits until the limit has no debt, then reads a
// whole datagram and charges it. Without a limit, they pass straight
// through and only count. Close, the deadlines set through the result and
// errors end its waits as they end those of NewConn. A sender that would
// rather drop the datagrams that the limit does not let through at once
// than wait for them passes WithDrop.
func NewPacketConn(c net.PacketConn, opts ...Option) *PacketConn {
	return newPacketConn(c, mergeOptions(opts), nil)
}

func newPacketConn(c net.PacketConn, o options, g *Group) *PacketConn {
	pc := &PacketConn{pc: c, drop: o.drop}
	o.datagram = true
	pc.s.init(nil, nil, c, o, g)
	return pc
}

// SetLimiters replaces the Limiters of reads and writes, as Conn's does.
func (c *PacketConn) SetLimiters(read, write Limiter) { c.s.SetLimiters(read, write) }

// Meter returns the Meter of the datagrams' bytes.
func (c *PacketConn) Meter() *Meter { return &c.s.meter }

// admit charges size bytes to d's limiter, recording the charge in rc, if
// it lets them through at once, as TryWait does, and otherwise charges
// nothing and returns ErrWouldWait or the refusal.
func (d *direction) admit(size int64, rc *receipt) error {
	rc.reset()
	return d.lim.tryWait(size, nowNs(), rc)
}

func (c *PacketConn) ReadFrom(b []byte) (int, net.Addr, error) {
	if c.pc == nil {
		return 0, nil, ErrNilConn
	}
	d := &c.s.read
	if d.state.Load() == 0 || len(b) == 0 {
		n, addr, err := c.pc.ReadFrom(b)
		d.count(int64(n))
		return n, addr, err
	}
	for {
		st := d.state.Load()
		if st&dirLeased != 0 {
			if _, err := d.allow(1, false); errors.Is(err, errRecheck) {
				continue
			} else if err != nil {
				return 0, nil, err
			}
		}
		if st&dirLimited == 0 {
			n, addr, err := c.pc.ReadFrom(b)
			d.count(int64(n))
			return n, addr, err
		}
		if c.drop {
			return c.readDropping(d, b)
		}
		if _, err := d.awaitRead(int64(len(b)), true); err != nil {
			return 0, nil, err
		}
		n, addr, err := c.pc.ReadFrom(b)
		d.count(int64(n))
		return n, addr, d.received(int64(n), err)
	}
}

// readDropping reads the next datagram that d's limiter admits at once,
// dropping those it would make wait.
func (c *PacketConn) readDropping(d *direction, b []byte) (int, net.Addr, error) {
	var rc receipt
	for {
		n, addr, err := c.pc.ReadFrom(b)
		if n <= 0 {
			return n, addr, err
		}
		aerr := d.admit(int64(n), &rc)
		if aerr == nil {
			d.count(int64(n))
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

func (c *PacketConn) WriteTo(b []byte, addr net.Addr) (int, error) {
	if c.pc == nil {
		return 0, ErrNilConn
	}
	d := &c.s.write
	if d.state.Load() == 0 || len(b) == 0 {
		n, err := c.pc.WriteTo(b, addr)
		d.count(int64(n))
		return n, err
	}
	size := int64(len(b))
	for {
		st := d.state.Load()
		if st&dirLeased != 0 {
			if _, err := d.allow(size, true); errors.Is(err, errRecheck) {
				continue
			} else if err != nil {
				return 0, err
			}
		}
		var r *receipt
		if st&dirLimited != 0 {
			var rc receipt
			r = &rc
			if c.drop {
				if g := d.cancel.current(); g.ended() {
					return 0, g.cause()
				}
				if err := d.admit(size, r); errors.Is(err, ErrWouldWait) {
					return len(b), nil // dropped, as a full socket buffer drops it
				} else if err != nil {
					return 0, err
				}
			} else if _, err := d.grantWrite(size, true, r); err != nil {
				r.refund(size)
				return 0, err
			}
		}
		m, err := c.pc.WriteTo(b, addr)
		m, err = r.sent(size, m, err)
		d.count(int64(m))
		return m, err
	}
}

func (c *PacketConn) Close() error { return c.s.Close() }

func (c *PacketConn) LocalAddr() net.Addr {
	if c.pc == nil {
		return nil
	}
	return c.pc.LocalAddr()
}

func (c *PacketConn) SetDeadline(t time.Time) error {
	return c.dl.set(c.pc, &c.s, t, true, true)
}

func (c *PacketConn) SetReadDeadline(t time.Time) error {
	return c.dl.set(c.pc, &c.s, t, true, false)
}

func (c *PacketConn) SetWriteDeadline(t time.Time) error {
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
			s.read.cancel.setDeadline(t)
		}
		if write {
			s.write.cancel.setDeadline(t)
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
