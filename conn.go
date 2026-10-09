package byterate

import (
	"context"
	"errors"
	"io"
	"net"
	"time"
)

// ErrNilConn is returned when a wrapper has no underlying connection.
var ErrNilConn = errors.New("byterate: nil connection")

type rateConn struct {
	conn    io.ReadWriteCloser
	rate    Limiter
	ctxRate ContextLimiter  // rate, if it supports cancellation
	ctx     context.Context // canceled by Close to end a pending wait
	cancel  context.CancelFunc
}

// NewRateReadWriteCloser wraps rwc so that its traffic is charged to rate.
// Read charges the bytes received before returning them; Write charges len(b)
// before writing and refunds any unwritten part. If rate is a ContextLimiter,
// Close also ends a wait in progress, which then returns net.ErrClosed. A nil
// rate returns rwc unchanged. Methods return ErrNilConn if rwc is nil.
func NewRateReadWriteCloser(rwc io.ReadWriteCloser, rate Limiter) io.ReadWriteCloser {
	if rate == nil {
		return rwc
	}
	return newRateConn(rwc, rate)
}

// NewRateConn is NewRateReadWriteCloser for a net.Conn. The result reports
// c's addresses, sets c's deadlines and returns c from its RawConn method,
// which wrapper-aware helpers use to unwrap it.
func NewRateConn(c net.Conn, rate Limiter) net.Conn {
	if rate == nil {
		return c
	}
	return &rateNetConn{rateConn: newRateConn(c, rate), conn: c}
}

func newRateConn(rwc io.ReadWriteCloser, rate Limiter) *rateConn {
	c := &rateConn{
		conn: rwc,
		rate: rate,
	}
	if ctxRate, ok := rate.(ContextLimiter); ok {
		c.ctxRate = ctxRate
		c.ctx, c.cancel = context.WithCancel(context.Background())
	}
	return c
}

func (s *rateConn) Read(b []byte) (n int, err error) {
	if s == nil || s.conn == nil {
		return 0, ErrNilConn
	}
	n, err = s.conn.Read(b)
	if n > 0 {
		// The bytes were received, so the charge stands even if Close cut the wait short.
		if !s.wait(int64(n)) && err == nil {
			err = net.ErrClosed
		}
	}
	return
}

func (s *rateConn) Write(b []byte) (n int, err error) {
	if s == nil || s.conn == nil {
		return 0, ErrNilConn
	}
	if len(b) > 0 {
		if !s.wait(int64(len(b))) {
			s.rate.ReturnBucket(int64(len(b)))
			return 0, net.ErrClosed
		}
	}
	n, err = s.conn.Write(b)
	if len(b) > 0 && n < len(b) {
		s.rate.ReturnBucket(int64(len(b) - n))
	}
	return
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

// wait charges size to the limiter and reports false if Close ended the wait.
func (s *rateConn) wait(size int64) bool {
	if s.ctxRate != nil {
		return s.ctxRate.GetContext(s.ctx, size) == nil
	}
	s.rate.Get(size)
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
