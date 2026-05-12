package byterate

import (
	"errors"
	"io"
)

// ErrNilConn is returned when a wrapper has no underlying connection.
var ErrNilConn = errors.New("byterate: nil connection")

type rateConn struct {
	conn io.ReadWriteCloser
	rate Limiter
}

func NewRateConn(conn io.ReadWriteCloser, rate Limiter) io.ReadWriteCloser {
	return &rateConn{
		conn: conn,
		rate: rate,
	}
}

func (s *rateConn) Read(b []byte) (n int, err error) {
	if s == nil || s.conn == nil {
		return 0, ErrNilConn
	}
	n, err = s.conn.Read(b)
	if s.rate != nil && n > 0 {
		s.rate.Get(int64(n))
	}
	return
}

func (s *rateConn) Write(b []byte) (n int, err error) {
	if s == nil || s.conn == nil {
		return 0, ErrNilConn
	}
	if s.rate != nil && len(b) > 0 {
		s.rate.Get(int64(len(b)))
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
	return s.conn.Close()
}
