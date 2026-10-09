package byterate

import (
	"errors"
	"io"
	"net"
	"os"
	"testing"
	"time"
)

type scriptedConn struct {
	readBuf  []byte
	writeN   int
	writeErr error
}

func (c *scriptedConn) Read(b []byte) (int, error) {
	if len(c.readBuf) == 0 {
		return 0, io.EOF
	}
	n := copy(b, c.readBuf)
	c.readBuf = c.readBuf[n:]
	return n, nil
}

func (c *scriptedConn) Write(b []byte) (int, error) {
	if c.writeN > len(b) {
		c.writeN = len(b)
	}
	return c.writeN, c.writeErr
}

func (c *scriptedConn) Close() error { return nil }

func TestRateConnReadChargesActualBytes(t *testing.T) {
	r := NewRate(1 << 20)
	conn := NewRateReadWriteCloser(&scriptedConn{readBuf: []byte("ok")}, r)

	buf := make([]byte, 4)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatalf("Read() error = %v, want nil", err)
	}
	if n != 2 {
		t.Fatalf("Read() n = %d, want 2", n)
	}
	if string(buf[:n]) != "ok" {
		t.Fatalf("Read() data = %q, want %q", string(buf[:n]), "ok")
	}
	if got := r.bytesAcc.Load(); got != 2 {
		t.Fatalf("bytesAcc after Read() = %d, want 2", got)
	}
}

func TestRateConnWriteRefundsShortWrite(t *testing.T) {
	r := NewRate(1 << 20)
	wantErr := errors.New("short write")
	conn := NewRateReadWriteCloser(&scriptedConn{writeN: 2, writeErr: wantErr}, r)

	n, err := conn.Write([]byte("hello"))
	if !errors.Is(err, wantErr) {
		t.Fatalf("Write() error = %v, want %v", err, wantErr)
	}
	if n != 2 {
		t.Fatalf("Write() n = %d, want 2", n)
	}
	if got := r.bytesAcc.Load(); got != 2 {
		t.Fatalf("bytesAcc after Write() = %d, want 2", got)
	}
}

func TestRateConnNilConnectionReturnsError(t *testing.T) {
	conn := NewRateReadWriteCloser(nil, NewRate(1024))
	if _, err := conn.Read(make([]byte, 1)); !errors.Is(err, ErrNilConn) {
		t.Fatalf("Read() error = %v, want %v", err, ErrNilConn)
	}
	if _, err := conn.Write([]byte("x")); !errors.Is(err, ErrNilConn) {
		t.Fatalf("Write() error = %v, want %v", err, ErrNilConn)
	}
	if err := conn.Close(); !errors.Is(err, ErrNilConn) {
		t.Fatalf("Close() error = %v, want %v", err, ErrNilConn)
	}
}

type plainLimiter struct {
	got, returned int64
}

func (l *plainLimiter) Get(size int64)          { l.got += size }
func (l *plainLimiter) ReturnBucket(size int64) { l.returned += size }

type ioResult struct {
	n   int
	err error
}

// closeWhileBlocked starts op, closes conn once op is blocked in the limiter
// and returns op's result. stop releases op if Close fails to wake it.
func closeWhileBlocked(t *testing.T, conn io.Closer, op func() (int, error), stop func()) ioResult {
	t.Helper()
	done := make(chan ioResult, 1)
	go func() {
		n, err := op()
		done <- ioResult{n, err}
	}()

	time.Sleep(50 * time.Millisecond)
	select {
	case res := <-done:
		t.Fatalf("operation returned before Close: n=%d err=%v", res.n, res.err)
	default:
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	select {
	case res := <-done:
		return res
	case <-time.After(500 * time.Millisecond):
		stop()
		t.Fatal("Close() did not wake the operation blocked in the limiter")
		return ioResult{}
	}
}

func TestRateConnCloseWakesBlockedWrite(t *testing.T) {
	r := NewRate(1024)
	conn := NewRateReadWriteCloser(&scriptedConn{writeN: 64 << 10}, r)

	res := closeWhileBlocked(t, conn, func() (int, error) {
		return conn.Write(make([]byte, 64<<10))
	}, r.Stop)
	if res.n != 0 || !errors.Is(res.err, net.ErrClosed) {
		t.Fatalf("Write() = %d, %v; want 0, %v", res.n, res.err, net.ErrClosed)
	}
	if got := r.bytesAcc.Load(); got != 0 {
		t.Fatalf("bytesAcc after canceled Write() = %d, want 0", got)
	}
	// The unsent write was refunded, so other users of r get the full burst.
	if wait := r.reserve(2048); wait != 0 {
		t.Fatalf("reserve(2048) after canceled Write() wait=%s, want 0", time.Duration(wait))
	}
}

func TestRateConnCloseWakesBlockedRead(t *testing.T) {
	r := NewRate(1024)
	conn := NewRateReadWriteCloser(&scriptedConn{readBuf: make([]byte, 64<<10)}, r)

	res := closeWhileBlocked(t, conn, func() (int, error) {
		return conn.Read(make([]byte, 64<<10))
	}, r.Stop)
	if res.n != 64<<10 || !errors.Is(res.err, net.ErrClosed) {
		t.Fatalf("Read() = %d, %v; want %d, %v", res.n, res.err, 64<<10, net.ErrClosed)
	}
	// The bytes were received, so they stay charged.
	if got := r.bytesAcc.Load(); got != 64<<10 {
		t.Fatalf("bytesAcc after canceled Read() = %d, want %d", got, 64<<10)
	}
}

func TestRateConnCloseWakesHierarchicalWrite(t *testing.T) {
	first := NewRate(1024)
	second := NewRate(1 << 20)
	conn := NewRateReadWriteCloser(&scriptedConn{writeN: 64 << 10}, NewHierarchicalLimiter2(first, second))

	res := closeWhileBlocked(t, conn, func() (int, error) {
		return conn.Write(make([]byte, 64<<10))
	}, first.Stop)
	if res.n != 0 || !errors.Is(res.err, net.ErrClosed) {
		t.Fatalf("Write() = %d, %v; want 0, %v", res.n, res.err, net.ErrClosed)
	}
	if got := first.bytesAcc.Load(); got != 0 {
		t.Fatalf("first bytesAcc after canceled Write() = %d, want 0", got)
	}
	if got := second.bytesAcc.Load(); got != 0 {
		t.Fatalf("second bytesAcc after canceled Write() = %d, want 0", got)
	}
}

func TestRateConnPlainLimiter(t *testing.T) {
	l := &plainLimiter{}
	conn := NewRateReadWriteCloser(&scriptedConn{writeN: 5}, l)
	if n, err := conn.Write([]byte("hello")); n != 5 || err != nil {
		t.Fatalf("Write() = %d, %v; want 5, nil", n, err)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if l.got != 5 || l.returned != 0 {
		t.Fatalf("limiter got/returned = %d/%d, want 5/0", l.got, l.returned)
	}
}

func TestRateConnKeepsNetConnBehavior(t *testing.T) {
	client, server := net.Pipe()
	defer func() { _ = server.Close() }()
	r := NewRate(1 << 20)
	conn := NewRateConn(client, r)
	if conn.LocalAddr() != client.LocalAddr() || conn.RemoteAddr() != client.RemoteAddr() {
		t.Fatalf("addresses = %v/%v, want %v/%v", conn.LocalAddr(), conn.RemoteAddr(), client.LocalAddr(), client.RemoteAddr())
	}
	if raw := conn.(interface{ RawConn() net.Conn }).RawConn(); raw != client {
		t.Fatalf("RawConn() = %v, want wrapped conn", raw)
	}
	if err := conn.SetReadDeadline(time.Now().Add(-time.Second)); err != nil {
		t.Fatalf("SetReadDeadline() error = %v", err)
	}
	if _, err := conn.Read(make([]byte, 1)); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("Read() after past deadline error = %v", err)
	}
	if err := conn.SetDeadline(time.Time{}); err != nil {
		t.Fatalf("SetDeadline() error = %v", err)
	}
	if err := conn.SetWriteDeadline(time.Time{}); err != nil {
		t.Fatalf("SetWriteDeadline() error = %v", err)
	}
	go func() { _, _ = conn.Write([]byte("abc")) }()
	buf := make([]byte, 3)
	if _, err := io.ReadFull(server, buf); err != nil || string(buf) != "abc" {
		t.Fatalf("server read = %q, %v", buf, err)
	}
	if got := r.bytesAcc.Load(); got != 3 {
		t.Fatalf("bytesAcc = %d, want 3", got)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if _, err := server.Read(buf); !errors.Is(err, io.EOF) {
		t.Fatalf("server Read() after Close error = %v, want EOF", err)
	}
}

func TestRateConnNilArguments(t *testing.T) {
	client, server := net.Pipe()
	defer func() { _ = client.Close() }()
	defer func() { _ = server.Close() }()
	if got := NewRateConn(client, nil); got != client {
		t.Fatalf("NewRateConn(c, nil) = %v, want c", got)
	}
	if got := NewRateReadWriteCloser(client, nil); got != io.ReadWriteCloser(client) {
		t.Fatalf("NewRateReadWriteCloser(c, nil) = %v, want c", got)
	}
	conn := NewRateConn(nil, NewRate(1024))
	if conn.LocalAddr() != nil || conn.RemoteAddr() != nil {
		t.Fatalf("nil conn addresses = %v/%v, want nil", conn.LocalAddr(), conn.RemoteAddr())
	}
	for name, err := range map[string]error{
		"Close":            conn.Close(),
		"SetDeadline":      conn.SetDeadline(time.Time{}),
		"SetReadDeadline":  conn.SetReadDeadline(time.Time{}),
		"SetWriteDeadline": conn.SetWriteDeadline(time.Time{}),
	} {
		if !errors.Is(err, ErrNilConn) {
			t.Fatalf("%s() error = %v, want %v", name, err, ErrNilConn)
		}
	}
}
