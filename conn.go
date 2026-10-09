package byterate

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"time"
)

// ErrNilConn is returned when a wrapper has no underlying connection.
var ErrNilConn = errors.New("byterate: nil connection")

var errCloseWriteUnsupported = fmt.Errorf("byterate: connection does not support CloseWrite: %w", errors.ErrUnsupported)

type rateConn struct {
	conn        io.ReadWriteCloser
	read, write direction
	ctx         context.Context // canceled by Close to end a pending wait
	cancel      context.CancelFunc
}

// direction holds the limiter charged for one direction of a rateConn.
type direction struct {
	rate    Limiter
	ctxRate ContextLimiter // rate, if it supports cancellation
}

// NewRateReadWriteCloser wraps rwc so that its traffic in both directions is
// charged to rate. Read charges the bytes received before returning them;
// Write charges len(b) before writing and refunds any unwritten part. If rate
// is a ContextLimiter, Close also ends a wait in progress, which then returns
// net.ErrClosed. CloseWrite is passed on to rwc when it has one, so relays can
// half-close through the wrapper. A nil rate returns rwc unchanged. Methods
// return ErrNilConn if rwc is nil.
func NewRateReadWriteCloser(rwc io.ReadWriteCloser, rate Limiter) io.ReadWriteCloser {
	return NewDuplexRateReadWriteCloser(rwc, rate, rate)
}

// NewDuplexRateReadWriteCloser is NewRateReadWriteCloser with separate
// limiters for reads and writes, so that each direction is limited and
// metered on its own. A nil limiter leaves its direction unlimited; with both
// nil, rwc is returned unchanged.
func NewDuplexRateReadWriteCloser(rwc io.ReadWriteCloser, read, write Limiter) io.ReadWriteCloser {
	if read == nil && write == nil {
		return rwc
	}
	return newRateConn(rwc, read, write)
}

// NewRateConn is NewRateReadWriteCloser for a net.Conn. The result reports
// c's addresses, sets c's deadlines and returns c from its RawConn method,
// which wrapper-aware helpers use to unwrap it.
func NewRateConn(c net.Conn, rate Limiter) net.Conn {
	return NewDuplexRateConn(c, rate, rate)
}

// NewDuplexRateConn is NewRateConn with separate limiters for reads and
// writes, as NewDuplexRateReadWriteCloser.
func NewDuplexRateConn(c net.Conn, read, write Limiter) net.Conn {
	if read == nil && write == nil {
		return c
	}
	return &rateNetConn{rateConn: newRateConn(c, read, write), conn: c}
}

func newRateConn(rwc io.ReadWriteCloser, read, write Limiter) *rateConn {
	c := &rateConn{conn: rwc, read: newDirection(read), write: newDirection(write)}
	if c.read.ctxRate != nil || c.write.ctxRate != nil {
		c.ctx, c.cancel = context.WithCancel(context.Background())
	}
	return c
}

func newDirection(rate Limiter) direction {
	d := direction{rate: rate}
	d.ctxRate, _ = rate.(ContextLimiter)
	return d
}

func (s *rateConn) Read(b []byte) (n int, err error) {
	if s == nil || s.conn == nil {
		return 0, ErrNilConn
	}
	n, err = s.conn.Read(b)
	if n > 0 {
		// The bytes were received, so the charge stands even if Close cut the wait short.
		if !s.wait(&s.read, int64(n)) && err == nil {
			err = net.ErrClosed
		}
	}
	return
}

func (s *rateConn) Write(b []byte) (n int, err error) {
	if s == nil || s.conn == nil {
		return 0, ErrNilConn
	}
	limited := len(b) > 0 && s.write.rate != nil
	if limited && !s.wait(&s.write, int64(len(b))) {
		s.write.rate.ReturnBucket(int64(len(b)))
		return 0, net.ErrClosed
	}
	n, err = s.conn.Write(b)
	if limited && n < len(b) {
		s.write.rate.ReturnBucket(int64(len(b) - n))
	}
	return
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
	if s.cancel != nil {
		s.cancel()
	}
	return s.conn.Close()
}

// wait charges size to d's limiter, if any, and reports false if Close ended
// the wait.
func (s *rateConn) wait(d *direction, size int64) bool {
	switch {
	case d.ctxRate != nil:
		return d.ctxRate.GetContext(s.ctx, size) == nil
	case d.rate != nil:
		d.rate.Get(size)
	}
	return true
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

func (c *rateNetConn) SetDeadline(t time.Time) error {
	if c.conn == nil {
		return ErrNilConn
	}
	return c.conn.SetDeadline(t)
}

func (c *rateNetConn) SetReadDeadline(t time.Time) error {
	if c.conn == nil {
		return ErrNilConn
	}
	return c.conn.SetReadDeadline(t)
}

func (c *rateNetConn) SetWriteDeadline(t time.Time) error {
	if c.conn == nil {
		return ErrNilConn
	}
	return c.conn.SetWriteDeadline(t)
}

// RawConn returns the wrapped connection.
func (c *rateNetConn) RawConn() net.Conn {
	return c.conn
}
