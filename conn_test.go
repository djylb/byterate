package byterate

import (
	"context"
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

func TestRateConnCloseWrite(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	client, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	server, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = server.Close() }()

	limited := NewRateConn(client, NewRate(1<<20))
	cw, ok := limited.(interface{ CloseWrite() error })
	if !ok {
		t.Fatal("rate conn has no CloseWrite")
	}
	if err := cw.CloseWrite(); err != nil {
		t.Fatalf("CloseWrite() error = %v", err)
	}
	_ = server.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := server.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("peer Read() error = %v, want EOF after CloseWrite", err)
	}

	plain := NewRateReadWriteCloser(&scriptedConn{}, NewRate(1<<20))
	if err := plain.(interface{ CloseWrite() error }).CloseWrite(); !errors.Is(err, errors.ErrUnsupported) {
		t.Fatalf("CloseWrite() without support error = %v, want %v", err, errors.ErrUnsupported)
	}
}

func TestDuplexRateConnChargesEachDirection(t *testing.T) {
	in, out := NewRate(1<<30), NewRate(1<<30)
	conn := NewDuplexRateReadWriteCloser(&scriptedConn{readBuf: []byte("abc"), writeN: 64}, in, out)
	if _, err := conn.Read(make([]byte, 8)); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	if got, want := in.bytesAcc.Load(), int64(3); got != want {
		t.Fatalf("read limiter charged %d, want %d", got, want)
	}
	if got, want := out.bytesAcc.Load(), int64(5); got != want {
		t.Fatalf("write limiter charged %d, want %d", got, want)
	}

	// One direction may be left unlimited.
	conn = NewDuplexRateReadWriteCloser(&scriptedConn{readBuf: []byte("abc"), writeN: 64}, nil, out)
	if _, err := conn.Read(make([]byte, 8)); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write([]byte("hi")); err != nil {
		t.Fatal(err)
	}
	if got, want := out.bytesAcc.Load(), int64(7); got != want {
		t.Fatalf("write limiter charged %d in total, want %d", got, want)
	}

	raw, peer := net.Pipe()
	defer func() { _ = raw.Close() }()
	defer func() { _ = peer.Close() }()
	if c := NewDuplexRateConn(raw, nil, nil); c != raw {
		t.Fatalf("NewDuplexRateConn(nil, nil) = %T, want the connection itself", c)
	}
}

// drain reads from c until it fails.
func drain(c net.Conn) {
	buf := make([]byte, 64<<10)
	for {
		if _, err := c.Read(buf); err != nil {
			return
		}
	}
}

func isTimeout(err error) bool {
	var ne net.Error
	return errors.Is(err, os.ErrDeadlineExceeded) && errors.As(err, &ne) && ne.Timeout()
}

func TestRateConnWriteDeadlineEndsLimiterWait(t *testing.T) {
	for name, wrap := range map[string]func(net.Conn, *Rate) net.Conn{
		"Rate": func(c net.Conn, r *Rate) net.Conn { return NewRateConn(c, r) },
		"HierarchicalLimiter": func(c net.Conn, r *Rate) net.Conn {
			return NewDuplexRateConn(c, nil, NewHierarchicalLimiter2(NewRate(1<<30), r))
		},
	} {
		t.Run(name, func(t *testing.T) {
			client, server := net.Pipe()
			defer func() { _ = server.Close() }()
			go drain(server)
			r := NewRate(1024)
			conn := wrap(client, r)
			defer func() { _ = conn.Close() }()

			if err := conn.SetWriteDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
				t.Fatal(err)
			}
			done := make(chan ioResult, 1)
			start := time.Now()
			go func() {
				n, err := conn.Write(make([]byte, 8<<10)) // waits 6s at 1 KiB/s
				done <- ioResult{n, err}
			}()
			select {
			case res := <-done:
				if res.n != 0 || !isTimeout(res.err) {
					t.Fatalf("Write() = %d, %v; want 0 and a timeout matching %v", res.n, res.err, os.ErrDeadlineExceeded)
				}
				if elapsed := time.Since(start); elapsed < 80*time.Millisecond {
					t.Fatalf("Write() returned after %v, before its deadline", elapsed)
				}
			case <-time.After(time.Second):
				r.Stop()
				<-done
				t.Fatal("Write() blocked in the limiter past its deadline")
			}
			// The unsent write was refunded.
			if wait := r.reserve(2048); wait != 0 {
				t.Fatalf("reserve(2048) after timed-out Write() wait=%s, want 0", time.Duration(wait))
			}
		})
	}
}

func TestRateConnReadDeadlineEndsLimiterWait(t *testing.T) {
	client, server := net.Pipe()
	defer func() { _ = server.Close() }()
	go func() { _, _ = server.Write(make([]byte, 8<<10)) }()
	r := NewRate(1024)
	conn := NewRateConn(client, r)
	defer func() { _ = conn.Close() }()

	done := make(chan ioResult, 1)
	go func() {
		n, err := conn.Read(make([]byte, 8<<10)) // waits 6s at 1 KiB/s
		done <- ioResult{n, err}
	}()
	time.Sleep(50 * time.Millisecond)
	// Moving the deadline later must not end the wait.
	if err := conn.SetReadDeadline(time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	select {
	case res := <-done:
		t.Fatalf("Read() = %d, %v after a later deadline was set; want it still waiting", res.n, res.err)
	default:
	}
	// A deadline in the past, as net/http sets to abort a read, ends it.
	if err := conn.SetReadDeadline(time.Now()); err != nil {
		t.Fatal(err)
	}
	select {
	case res := <-done:
		if res.n != 8<<10 || !isTimeout(res.err) {
			t.Fatalf("Read() = %d, %v; want %d and a timeout matching %v", res.n, res.err, 8<<10, os.ErrDeadlineExceeded)
		}
	case <-time.After(time.Second):
		r.Stop()
		<-done
		t.Fatal("SetReadDeadline(now) did not end a Read blocked in the limiter")
	}
	// The bytes were received, so they stay charged.
	if got := r.bytesAcc.Load(); got != 8<<10 {
		t.Fatalf("bytesAcc after timed-out Read() = %d, want %d", got, 8<<10)
	}

	// Clearing the expired deadline lets later waits run to completion again.
	if err := conn.SetDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}
	r.ResetLimit(1 << 20)
	go func() { _, _ = server.Write([]byte("abc")) }()
	if n, err := conn.Read(make([]byte, 8)); n != 3 || err != nil {
		t.Fatalf("Read() after clearing the deadline = %d, %v; want 3, nil", n, err)
	}
}

func TestRateConnDeadlineAfterClose(t *testing.T) {
	client, server := net.Pipe()
	defer func() { _ = server.Close() }()
	r := NewRate(1024)
	conn := NewRateConn(client, r)
	if err := conn.SetDeadline(time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	for _, d := range []*direction{&conn.(*rateNetConn).read, &conn.(*rateNetConn).write} {
		if d.cancel.timer != nil {
			t.Fatal("Close() left a deadline timer running")
		}
	}
	_ = conn.Close()
	_ = conn.SetDeadline(time.Time{}) // must not reopen the wait signal
	_ = conn.SetWriteDeadline(time.Now().Add(-time.Second))
	r.reserve(2048) // spend the burst
	if n, err := conn.Write(make([]byte, 1024)); n != 0 || !errors.Is(err, net.ErrClosed) {
		t.Fatalf("Write() after Close = %d, %v; want 0, %v", n, err, net.ErrClosed)
	}
}

func TestRateConnCloseAfterDeadline(t *testing.T) {
	client, server := net.Pipe()
	defer func() { _ = server.Close() }()
	r := NewRate(1024)
	conn := NewRateConn(client, r)
	if err := conn.SetWriteDeadline(time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	r.reserve(2048) // spend the burst
	if n, err := conn.Write(make([]byte, 1024)); n != 0 || !errors.Is(err, net.ErrClosed) {
		t.Fatalf("Write() after an expired deadline and Close = %d, %v; want 0, %v", n, err, net.ErrClosed)
	}
}

// gateLimiter is a ContextLimiter that hands each wait's context to the test,
// waits for it to be done and then for the test to let it return ctx.Err().
type gateLimiter struct {
	entered chan context.Context
	proceed chan struct{}
}

func (l *gateLimiter) Get(int64)          {}
func (l *gateLimiter) ReturnBucket(int64) {}
func (l *gateLimiter) GetContext(ctx context.Context, _ int64) error {
	l.entered <- ctx
	<-ctx.Done()
	<-l.proceed
	return ctx.Err()
}

// checkEnded fails unless ctx, whose Done channel was done, still has that
// channel and a non-nil Err, as context.Context requires.
func checkEnded(t *testing.T, ctx context.Context, done <-chan struct{}, want error) {
	t.Helper()
	if ctx.Done() != done {
		t.Fatal("Done() returned a different channel after the wait ended")
	}
	if err := ctx.Err(); !errors.Is(err, want) {
		t.Fatalf("Err() of an ended wait = %v, want %v", err, want)
	}
}

func TestRateConnWaitContextStaysEnded(t *testing.T) {
	client, server := net.Pipe()
	defer func() { _ = server.Close() }()
	go drain(server)
	l := &gateLimiter{entered: make(chan context.Context), proceed: make(chan struct{})}
	conn := NewRateConn(client, l)
	defer func() { _ = conn.Close() }()

	write := func() <-chan ioResult {
		res := make(chan ioResult, 1)
		go func() {
			n, err := conn.Write([]byte("x"))
			res <- ioResult{n, err}
		}()
		return res
	}

	// Abort the wait with a deadline in the past, then clear the deadline
	// before the limiter looks at the context again.
	res := write()
	ctx := <-l.entered
	done := ctx.Done()
	_ = conn.SetWriteDeadline(time.Now().Add(-time.Second))
	_ = conn.SetWriteDeadline(time.Time{})
	l.proceed <- struct{}{}
	if r := <-res; r.n != 0 || !isTimeout(r.err) {
		t.Fatalf("Write() aborted by a deadline = %d, %v; want 0 and a timeout matching %v", r.n, r.err, os.ErrDeadlineExceeded)
	}
	checkEnded(t, ctx, done, context.DeadlineExceeded)

	// The cleared deadline gives the next wait an open context, which Close
	// ends.
	res = write()
	ctx2 := <-l.entered
	if isClosed(ctx2.Done()) || ctx2.Err() != nil {
		t.Fatalf("wait after clearing the deadline started ended: Err() = %v", ctx2.Err())
	}
	done2 := ctx2.Done()
	_ = conn.Close()
	l.proceed <- struct{}{}
	if r := <-res; r.n != 0 || !errors.Is(r.err, net.ErrClosed) {
		t.Fatalf("Write() ended by Close = %d, %v; want 0, %v", r.n, r.err, net.ErrClosed)
	}
	checkEnded(t, ctx2, done2, context.Canceled)
	checkEnded(t, ctx, done, context.DeadlineExceeded)
}

// derivingLimiter is a ContextLimiter that derives a context from the one it
// is given, as limiters that cap or watch their own wait do.
type derivingLimiter struct{ entered chan struct{} }

func (derivingLimiter) Get(int64)          {}
func (derivingLimiter) ReturnBucket(int64) {}
func (l derivingLimiter) GetContext(ctx context.Context, _ int64) error {
	ctx, cancel := context.WithTimeout(ctx, time.Hour)
	defer cancel()
	l.entered <- struct{}{}
	<-ctx.Done()
	return ctx.Err()
}

// A deadline that aborts a wait and is cleared at once must leave a context
// that the context package can derive from: with an Err of nil after Done is
// closed, it panics in a goroutine of its own and takes the process down.
func TestRateConnDerivedContextAbortThenClear(t *testing.T) {
	client, server := net.Pipe()
	defer func() { _ = server.Close() }()
	go drain(server)
	l := derivingLimiter{entered: make(chan struct{})}
	conn := NewRateConn(client, l)
	defer func() { _ = conn.Close() }()

	for i := range 1000 {
		res := make(chan ioResult, 1)
		go func() {
			n, err := conn.Write([]byte("x"))
			res <- ioResult{n, err}
		}()
		<-l.entered
		_ = conn.SetWriteDeadline(time.Now().Add(-time.Second))
		_ = conn.SetWriteDeadline(time.Time{})
		select {
		case r := <-res:
			if r.n != 0 || !isTimeout(r.err) {
				t.Fatalf("Write() %d aborted by a deadline = %d, %v; want 0 and a timeout matching %v", i, r.n, r.err, os.ErrDeadlineExceeded)
			}
		case <-time.After(time.Second):
			_ = conn.Close()
			<-res
			t.Fatalf("Write() %d still blocked in the limiter after its deadline", i)
		}
	}
}

// With *Rate, a wait aborted by a deadline that is then cleared must still
// end with the deadline error and a refund, not as if it had waited.
func TestRateConnAbortThenClearDeadline(t *testing.T) {
	client, server := net.Pipe()
	defer func() { _ = server.Close() }()
	go drain(server)
	r := NewRate(1024)
	conn := NewRateConn(client, r)
	defer func() { _ = conn.Close() }()

	for i := range 50 {
		r.reserve(2048) // spend the burst, so that the Write waits about 1s
		res := make(chan ioResult, 1)
		start := time.Now()
		go func() {
			n, err := conn.Write(make([]byte, 1024))
			res <- ioResult{n, err}
		}()
		time.Sleep(2 * time.Millisecond) // let it start waiting
		_ = conn.SetWriteDeadline(time.Now().Add(-time.Second))
		_ = conn.SetWriteDeadline(time.Time{})
		res1 := <-res
		if res1.err == nil && time.Since(start) < 900*time.Millisecond {
			t.Fatalf("Write() %d aborted by a deadline = %d, nil after %v; want a timeout or a full wait", i, res1.n, time.Since(start))
		}
		if res1.err != nil && (res1.n != 0 || !isTimeout(res1.err)) {
			t.Fatalf("Write() %d aborted by a deadline = %d, %v; want 0 and a timeout matching %v", i, res1.n, res1.err, os.ErrDeadlineExceeded)
		}
		r.ResetLimit(1024) // drop the debt, refunded or not
	}
}

func TestRateConnTypedNilLimiter(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	client, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	var rate *Rate
	var hier *HierarchicalLimiter
	for name, got := range map[string]any{
		"NewRateConn(*Rate)":                      NewRateConn(client, rate),
		"NewRateConn(*HierarchicalLimiter)":       NewRateConn(client, hier),
		"NewDuplexRateConn":                       NewDuplexRateConn(client, rate, hier),
		"NewRateReadWriteCloser(*Rate)":           NewRateReadWriteCloser(client, rate),
		"NewDuplexRateReadWriteCloser(nil, hier)": NewDuplexRateReadWriteCloser(client, nil, hier),
	} {
		if got != any(client) {
			t.Errorf("%s = %T, want the *net.TCPConn itself", name, got)
		}
	}

	conn := NewDuplexRateConn(client, rate, NewRate(1024)).(*rateNetConn)
	if conn.read.rate != nil || conn.read.ctxRate != nil {
		t.Fatalf("read direction limiter = %v, want nil for a nil *Rate", conn.read.rate)
	}
}

type halfCloseConn struct {
	scriptedConn
	closedRead bool
}

func (c *halfCloseConn) CloseRead() error {
	c.closedRead = true
	return nil
}

func TestRateConnCloseRead(t *testing.T) {
	inner := &halfCloseConn{}
	for _, conn := range []io.ReadWriteCloser{
		NewRateReadWriteCloser(inner, NewRate(1<<20)),
		NewDuplexRateReadWriteCloser(inner, nil, NewRate(1<<20)),
	} {
		inner.closedRead = false
		cr, ok := conn.(interface{ CloseRead() error })
		if !ok {
			t.Fatalf("%T has no CloseRead", conn)
		}
		if err := cr.CloseRead(); err != nil || !inner.closedRead {
			t.Fatalf("CloseRead() = %v, passed on = %v; want nil, true", err, inner.closedRead)
		}
	}

	client, server := net.Pipe()
	defer func() { _ = client.Close() }()
	defer func() { _ = server.Close() }()
	conn := NewDuplexRateConn(client, NewRate(1<<20), nil)
	if err := conn.(interface{ CloseRead() error }).CloseRead(); !errors.Is(err, errors.ErrUnsupported) {
		t.Fatalf("CloseRead() without support error = %v, want %v", err, errors.ErrUnsupported)
	}
	if err := NewRateReadWriteCloser(nil, NewRate(1)).(interface{ CloseRead() error }).CloseRead(); !errors.Is(err, ErrNilConn) {
		t.Fatalf("CloseRead() on nil conn error = %v, want %v", err, ErrNilConn)
	}
}
