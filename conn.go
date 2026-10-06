package byterate

import (
	"context"
	"errors"
	"io"
	"net"
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

// NewRateConn wraps conn so that its traffic is charged to rate. Read charges
// the bytes received before returning them; Write charges len(b) before
// writing and refunds any unwritten part. A nil rate disables limiting. If
// rate is a ContextLimiter, Close also ends a wait in progress, which then
// returns net.ErrClosed. Methods return ErrNilConn if conn is nil.
func NewRateConn(conn io.ReadWriteCloser, rate Limiter) io.ReadWriteCloser {
	c := &rateConn{
		conn: conn,
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
	if s.rate != nil && n > 0 {
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
	if s.rate != nil && len(b) > 0 {
		if !s.wait(int64(len(b))) {
			s.rate.ReturnBucket(int64(len(b)))
			return 0, net.ErrClosed
		}
	}
	n, err = s.conn.Write(b)
	if s.rate != nil && len(b) > 0 && n < len(b) {
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
