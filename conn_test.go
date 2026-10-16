package byterate

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
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

func TestConnReadChargesActualBytes(t *testing.T) {
	r := NewRate(1 << 20)
	conn := NewReadWriteCloser(&scriptedConn{readBuf: []byte("ok")}, r)

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
	if got := metered(r); got != 2 {
		t.Fatalf("metered after Read() = %d, want 2", got)
	}
}

func TestConnWriteRefundsShortWrite(t *testing.T) {
	r := NewRate(1 << 20)
	wantErr := errors.New("short write")
	conn := NewReadWriteCloser(&scriptedConn{writeN: 2, writeErr: wantErr}, r)

	n, err := conn.Write([]byte("hello"))
	if !errors.Is(err, wantErr) {
		t.Fatalf("Write() error = %v, want %v", err, wantErr)
	}
	if n != 2 {
		t.Fatalf("Write() n = %d, want 2", n)
	}
	if got := metered(r); got != 2 {
		t.Fatalf("metered after Write() = %d, want 2", got)
	}
}

// negativeWriteConn reports a negative count, as a broken writer may.
type negativeWriteConn struct{ scriptedConn }

func (*negativeWriteConn) Write([]byte) (int, error) { return -1, errors.New("broken writer") }

// A writer reporting n < 0 must not get more than the charge refunded.
func TestConnNegativeWriteRefundsTheChargeOnly(t *testing.T) {
	r := NewRate(1000)
	conn := NewReadWriteCloser(&negativeWriteConn{}, r)
	_, _ = conn.Write(make([]byte, 1000))
	if got := metered(r); got != 0 {
		t.Fatalf("metered after a write reporting -1 = %d, want 0", got)
	}
}

func TestConnNilConnectionReturnsError(t *testing.T) {
	conn := NewReadWriteCloser(nil, NewRate(1024))
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

func (l *plainLimiter) Wait(_ context.Context, size int64) error {
	l.got += size
	return nil
}
func (l *plainLimiter) Refund(size int64) { l.returned += size }
func (l *plainLimiter) Ready(_ context.Context, n int64) (int64, error) {
	return n, nil
}
func (l *plainLimiter) Charge(n int64) error {
	l.got += n
	return nil
}

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

func TestConnCloseWakesBlockedWrite(t *testing.T) {
	r := NewRate(1024)
	conn := NewReadWriteCloser(&scriptedConn{writeN: 64 << 10}, r)

	// The burst goes out at once; the next grant waits until Close.
	res := closeWhileBlocked(t, conn, func() (int, error) {
		return conn.Write(make([]byte, 64<<10))
	}, r.Disable)
	if res.n != 2048 || !errors.Is(res.err, net.ErrClosed) {
		t.Fatalf("Write() = %d, %v; want 2048 sent, %v", res.n, res.err, net.ErrClosed)
	}
	// The grant Close ended was refunded; the bytes sent stay charged.
	if got := metered(r); got != 2048 {
		t.Fatalf("metered after canceled Write() = %d, want 2048", got)
	}
	if wait := r.reserve(1024); wait > int64(time.Second) {
		t.Fatalf("reserve(1024) after canceled Write() wait=%s, want the unsent grant refunded", time.Duration(wait))
	}
}

func TestConnCloseWakesBlockedRead(t *testing.T) {
	r := NewRate(1024)
	r.reserve(4096) // two seconds of debt, which a Read waits for before reading
	src := &scriptedConn{readBuf: make([]byte, 64<<10)}
	conn := NewReadWriteCloser(src, r)

	res := closeWhileBlocked(t, conn, func() (int, error) {
		return conn.Read(make([]byte, 64<<10))
	}, r.Disable)
	if res.n != 0 || !errors.Is(res.err, net.ErrClosed) {
		t.Fatalf("Read() = %d, %v; want 0, %v", res.n, res.err, net.ErrClosed)
	}
	// Nothing was read, so the data stays with the sender.
	if len(src.readBuf) != 64<<10 || metered(r) != 4096 {
		t.Fatalf("canceled Read() took %d bytes and metered %d, want none", 64<<10-len(src.readBuf), metered(r)-4096)
	}
}

func TestConnCloseWakesHierarchicalWrite(t *testing.T) {
	first := NewRate(1024)
	second := NewRate(1 << 20)
	conn := NewReadWriteCloser(&scriptedConn{writeN: 64 << 10}, NewHierarchy(first, second))

	first.reserve(2048) // spend the burst, so that the first grant waits
	second.reserve(2048)
	res := closeWhileBlocked(t, conn, func() (int, error) {
		return conn.Write(make([]byte, 64<<10))
	}, first.Disable)
	if res.n != 0 || !errors.Is(res.err, net.ErrClosed) {
		t.Fatalf("Write() = %d, %v; want 0, %v", res.n, res.err, net.ErrClosed)
	}
	if got := metered(first); got != 2048 {
		t.Fatalf("first metered after canceled Write() = %d, want the grant refunded", got-2048)
	}
	if got := metered(second); got != 2048 {
		t.Fatalf("second metered after canceled Write() = %d, want the grant refunded", got-2048)
	}
}

func TestConnPlainLimiter(t *testing.T) {
	l := &plainLimiter{}
	conn := NewReadWriteCloser(&scriptedConn{writeN: 5}, l)
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

func TestConnKeepsNetConnBehavior(t *testing.T) {
	client, server := net.Pipe()
	defer func() { _ = server.Close() }()
	r := NewRate(1 << 20)
	conn := NewConn(client, r)
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
	if got := metered(r); got != 3 {
		t.Fatalf("metered = %d, want 3", got)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if _, err := server.Read(buf); !errors.Is(err, io.EOF) {
		t.Fatalf("server Read() after Close error = %v, want EOF", err)
	}
}

func TestConnNilArguments(t *testing.T) {
	client, server := net.Pipe()
	defer func() { _ = client.Close() }()
	defer func() { _ = server.Close() }()
	if got := NewConn(client, nil); got != client {
		t.Fatalf("NewConn(c, nil) = %v, want c", got)
	}
	if got := NewReadWriteCloser(client, nil); got != io.ReadWriteCloser(client) {
		t.Fatalf("NewRateReadWriteCloser(c, nil) = %v, want c", got)
	}
	conn := NewConn(nil, NewRate(1024))
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

// tcpPair returns the two ends of a loopback TCP connection, closed when the
// test ends.
func tcpPair(t *testing.T) (client, server net.Conn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	client, err = net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	server, err = ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })
	return client, server
}

func TestConnCloseWrite(t *testing.T) {
	client, server := tcpPair(t)

	limited := NewConn(client, NewRate(1<<20))
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

	plain := NewReadWriteCloser(&scriptedConn{}, NewRate(1<<20))
	if err := plain.(interface{ CloseWrite() error }).CloseWrite(); !errors.Is(err, errors.ErrUnsupported) {
		t.Fatalf("CloseWrite() without support error = %v, want %v", err, errors.ErrUnsupported)
	}
}

func TestDuplexConnChargesEachDirection(t *testing.T) {
	in, out := NewRate(1<<30), NewRate(1<<30)
	conn := NewDuplexReadWriteCloser(&scriptedConn{readBuf: []byte("abc"), writeN: 64}, in, out)
	if _, err := conn.Read(make([]byte, 8)); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	if got, want := metered(in), int64(3); got != want {
		t.Fatalf("read limiter charged %d, want %d", got, want)
	}
	if got, want := metered(out), int64(5); got != want {
		t.Fatalf("write limiter charged %d, want %d", got, want)
	}

	// One direction may be left unlimited.
	conn = NewDuplexReadWriteCloser(&scriptedConn{readBuf: []byte("abc"), writeN: 64}, nil, out)
	if _, err := conn.Read(make([]byte, 8)); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write([]byte("hi")); err != nil {
		t.Fatal(err)
	}
	if got, want := metered(out), int64(7); got != want {
		t.Fatalf("write limiter charged %d in total, want %d", got, want)
	}

	raw, peer := net.Pipe()
	defer func() { _ = raw.Close() }()
	defer func() { _ = peer.Close() }()
	if c := NewDuplexConn(raw, nil, nil); c != raw {
		t.Fatalf("NewDuplexConn(nil, nil) = %T, want the connection itself", c)
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

func TestConnWriteDeadlineEndsLimiterWait(t *testing.T) {
	for name, wrap := range map[string]func(net.Conn, *Rate) net.Conn{
		"Rate": func(c net.Conn, r *Rate) net.Conn { return NewConn(c, r) },
		"Hierarchy": func(c net.Conn, r *Rate) net.Conn {
			return NewDuplexConn(c, nil, NewHierarchy(NewRate(1<<30), r))
		},
	} {
		t.Run(name, func(t *testing.T) {
			client, server := net.Pipe()
			defer func() { _ = server.Close() }()
			go drain(server)
			r := NewRate(1024)
			conn := wrap(client, r)
			defer func() { _ = conn.Close() }()

			start := time.Now()
			if err := conn.SetWriteDeadline(start.Add(100 * time.Millisecond)); err != nil {
				t.Fatal(err)
			}
			done := make(chan ioResult, 1)
			go func() {
				n, err := conn.Write(make([]byte, 8<<10)) // its grants wait 6s at 1 KiB/s
				done <- ioResult{n, err}
			}()
			select {
			case res := <-done:
				// The burst went out at once; the deadline ended the next grant.
				if res.n != 2048 || !isTimeout(res.err) {
					t.Fatalf("Write() = %d, %v; want 2048 and a timeout matching %v", res.n, res.err, os.ErrDeadlineExceeded)
				}
				if elapsed := time.Since(start); elapsed < 100*time.Millisecond {
					t.Fatalf("Write() returned after %v, before its deadline", elapsed)
				}
			case <-time.After(time.Second):
				r.Disable()
				<-done
				t.Fatal("Write() blocked in the limiter past its deadline")
			}
			// The grant the deadline ended was refunded.
			if got := metered(r); got != 2048 {
				t.Fatalf("metered after timed-out Write() = %d, want the 2048 sent", got)
			}
		})
	}
}

func TestConnReadDeadlineEndsLimiterWait(t *testing.T) {
	client, server := net.Pipe()
	defer func() { _ = server.Close() }()
	r := NewRate(1024)
	r.reserve(8 << 10) // 6s of debt, which a Read waits for before reading
	conn := NewConn(client, r)
	defer func() { _ = conn.Close() }()

	done := make(chan ioResult, 1)
	go func() {
		n, err := conn.Read(make([]byte, 8<<10))
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
		if res.n != 0 || !isTimeout(res.err) {
			t.Fatalf("Read() = %d, %v; want 0 and a timeout matching %v", res.n, res.err, os.ErrDeadlineExceeded)
		}
	case <-time.After(time.Second):
		r.Disable()
		<-done
		t.Fatal("SetReadDeadline(now) did not end a Read blocked in the limiter")
	}
	// Nothing was read, so nothing more was charged.
	if got := metered(r); got != 8<<10 {
		t.Fatalf("metered after timed-out Read() = %d, want the %d of debt alone", got, 8<<10)
	}

	// Clearing the expired deadline lets later waits run to completion again.
	if err := conn.SetDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}
	r.Reset(1 << 20)
	go func() { _, _ = server.Write([]byte("abc")) }()
	if n, err := conn.Read(make([]byte, 8)); n != 3 || err != nil {
		t.Fatalf("Read() after clearing the deadline = %d, %v; want 3, nil", n, err)
	}
}

func TestConnDeadlineAfterClose(t *testing.T) {
	client, server := net.Pipe()
	defer func() { _ = server.Close() }()
	r := NewRate(1024)
	conn := NewConn(client, r)
	if err := conn.SetDeadline(time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	for _, d := range []*direction{&conn.(*rateNetConn).read, &conn.(*rateNetConn).write} {
		if d.cancel.timer.t.Stop() {
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

func TestConnCloseAfterDeadline(t *testing.T) {
	client, server := net.Pipe()
	defer func() { _ = server.Close() }()
	r := NewRate(1024)
	conn := NewConn(client, r)
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

// gateLimiter is a Limiter that hands each wait's context to the test,
// waits for it to be done and then for the test to let it return ctx.Err().
type gateLimiter struct {
	entered chan context.Context
	proceed chan struct{}
}

func (l *gateLimiter) Refund(int64) {}
func (l *gateLimiter) Ready(_ context.Context, n int64) (int64, error) {
	return n, nil
}
func (l *gateLimiter) Charge(int64) error { return nil }
func (l *gateLimiter) Wait(ctx context.Context, _ int64) error {
	l.entered <- ctx
	<-ctx.Done()
	<-l.proceed
	return ctx.Err()
}

func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
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

func TestConnWaitContextStaysEnded(t *testing.T) {
	client, server := net.Pipe()
	defer func() { _ = server.Close() }()
	go drain(server)
	l := &gateLimiter{entered: make(chan context.Context), proceed: make(chan struct{})}
	conn := NewConn(client, l)
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

// derivingLimiter is a Limiter that derives a context from the one it
// is given, as limiters that cap or watch their own wait do.
type derivingLimiter struct{ entered chan struct{} }

func (derivingLimiter) Refund(int64) {}
func (derivingLimiter) Ready(_ context.Context, n int64) (int64, error) {
	return n, nil
}
func (derivingLimiter) Charge(int64) error { return nil }
func (l derivingLimiter) Wait(ctx context.Context, _ int64) error {
	ctx, cancel := context.WithTimeout(ctx, time.Hour)
	defer cancel()
	l.entered <- struct{}{}
	<-ctx.Done()
	return ctx.Err()
}

// A deadline that aborts a wait and is cleared at once must leave a context
// that the context package can derive from: with an Err of nil after Done is
// closed, it panics in a goroutine of its own and takes the process down.
func TestConnDerivedContextAbortThenClear(t *testing.T) {
	client, server := net.Pipe()
	defer func() { _ = server.Close() }()
	go drain(server)
	l := derivingLimiter{entered: make(chan struct{})}
	conn := NewConn(client, l)
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
func TestConnAbortThenClearDeadline(t *testing.T) {
	client, server := net.Pipe()
	defer func() { _ = server.Close() }()
	go drain(server)
	r := NewRate(1024)
	conn := NewConn(client, r)
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
		r.Reset(1024) // drop the debt, refunded or not
	}
}

func TestConnTypedNilLimiter(t *testing.T) {
	client, _ := tcpPair(t)

	var rate *Rate
	var hier *Hierarchy
	for name, got := range map[string]any{
		"NewConn(*Rate)":                          NewConn(client, rate),
		"NewConn(*Hierarchy)":                     NewConn(client, hier),
		"NewDuplexConn":                           NewDuplexConn(client, rate, hier),
		"NewRateReadWriteCloser(*Rate)":           NewReadWriteCloser(client, rate),
		"NewDuplexRateReadWriteCloser(nil, hier)": NewDuplexReadWriteCloser(client, nil, hier),
	} {
		if got != any(client) {
			t.Errorf("%s = %T, want the *net.TCPConn itself", name, got)
		}
	}

	conn := NewDuplexConn(client, rate, NewRate(1024)).(*rateNetConn)
	if conn.read.rate != nil {
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

func TestConnCloseRead(t *testing.T) {
	inner := &halfCloseConn{}
	for _, conn := range []io.ReadWriteCloser{
		NewReadWriteCloser(inner, NewRate(1<<20)),
		NewDuplexReadWriteCloser(inner, nil, NewRate(1<<20)),
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
	conn := NewDuplexConn(client, NewRate(1<<20), nil)
	if err := conn.(interface{ CloseRead() error }).CloseRead(); !errors.Is(err, errors.ErrUnsupported) {
		t.Fatalf("CloseRead() without support error = %v, want %v", err, errors.ErrUnsupported)
	}
	if err := NewReadWriteCloser(nil, NewRate(1)).(interface{ CloseRead() error }).CloseRead(); !errors.Is(err, ErrNilConn) {
		t.Fatalf("CloseRead() on nil conn error = %v, want %v", err, ErrNilConn)
	}
}

// errLimiter is a Limiter that refuses every charge before a transfer with
// an error of its own, and reports it after taking a transfer's bytes, as an
// adapter refusing a charge larger than its burst does.
type errLimiter struct {
	err           error
	got, returned int64
}

func (l *errLimiter) Refund(size int64) { l.returned += size }
func (l *errLimiter) Wait(context.Context, int64) error {
	return l.err
}
func (l *errLimiter) Ready(_ context.Context, n int64) (int64, error) {
	return n, nil
}
func (l *errLimiter) Charge(n int64) error {
	l.got += n
	return l.err
}

// An error of the limiter's own must reach the caller as it is, not as a
// deadline or a close.
func TestConnPassesLimiterError(t *testing.T) {
	wantErr := errors.New("charge exceeds burst")
	l := &errLimiter{err: wantErr}
	conn := NewReadWriteCloser(&scriptedConn{readBuf: []byte("abc"), writeN: 64}, l)

	if n, err := conn.Write([]byte("hello")); n != 0 || !errors.Is(err, wantErr) {
		t.Fatalf("Write() = %d, %v; want 0, %v", n, err, wantErr)
	}
	if l.got != 0 || l.returned != 0 {
		t.Fatalf("limiter got/returned after a refused Write() = %d/%d, want 0/0", l.got, l.returned)
	}
	if n, err := conn.Read(make([]byte, 8)); n != 3 || !errors.Is(err, wantErr) || l.got != 3 {
		t.Fatalf("Read() = %d, %v with %d charged; want 3, %v and the 3 charged", n, err, l.got, wantErr)
	}
}

// TestConnDeadlineTimerReuse moves, clears and expires the deadline of
// one direction many times: each wait must end at the deadline in force
// when it started, and never at an earlier one.
func TestConnDeadlineTimerReuse(t *testing.T) {
	client, server := net.Pipe()
	defer func() { _ = server.Close() }()
	go drain(server)
	r := NewRate(1024)
	conn := NewConn(client, r)
	defer func() { _ = conn.Close() }()

	write := func() <-chan ioResult {
		res := make(chan ioResult, 1)
		go func() {
			n, err := conn.Write(make([]byte, 1024))
			res <- ioResult{n, err}
		}()
		return res
	}
	for i := range 5 {
		r.Reset(1024)
		r.reserve(2048) // spend the burst, so that the Write waits about 1s

		// A deadline moved later, many times, must not end the wait early.
		for range 100 {
			_ = conn.SetWriteDeadline(time.Now().Add(time.Millisecond))
			_ = conn.SetWriteDeadline(time.Now().Add(time.Hour))
		}
		start := time.Now()
		_ = conn.SetWriteDeadline(start.Add(80 * time.Millisecond))
		res := write()
		select {
		case got := <-res:
			if got.n != 0 || !isTimeout(got.err) {
				t.Fatalf("round %d: Write() = %d, %v; want 0 and a timeout", i, got.n, got.err)
			}
			if elapsed := time.Since(start); elapsed < 80*time.Millisecond {
				t.Fatalf("round %d: Write() timed out after %v, before its deadline", i, elapsed)
			}
		case <-time.After(time.Second):
			r.Disable()
			<-res
			t.Fatalf("round %d: Write() blocked in the limiter past its deadline", i)
		}

		// The expired deadline fails new waits until it is moved.
		r.reserve(2048)
		if n, err := conn.Write(make([]byte, 1024)); n != 0 || !isTimeout(err) {
			t.Fatalf("round %d: Write() after the deadline = %d, %v; want 0 and a timeout", i, n, err)
		}
		_ = conn.SetWriteDeadline(time.Time{})
	}
}

// A pending deadline must not keep a connection that was dropped without
// Close reachable until the deadline passes.
func TestConnPendingDeadlineDoesNotPinConn(t *testing.T) {
	collected := make(chan struct{})
	func() {
		conn := NewConn(&nopNetConn{}, NewRate(1024))
		_ = conn.SetDeadline(time.Now().Add(time.Hour))
		runtime.AddCleanup(conn.(*rateNetConn), func(ch chan struct{}) { close(ch) }, collected)
	}()
	for range 20 {
		runtime.GC()
		select {
		case <-collected:
			return
		case <-time.After(10 * time.Millisecond):
		}
	}
	t.Fatal("a connection with a pending deadline was not garbage collected")
}

// TestBubbleDeadlineOutlivesBubble sets a deadline on a connection inside a
// testing/synctest bubble and collects the connection outside it: nothing
// may touch the bubble's timer from outside, which is a fatal error.
func TestBubbleDeadlineOutlivesBubble(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for range 2 {
			c := NewConn(&nopNetConn{}, NewRate(1000))
			_ = c.SetDeadline(time.Now().Add(time.Hour))
		}
		c := NewConn(&nopNetConn{}, NewRate(1000))
		_ = c.SetDeadline(time.Now().Add(time.Hour))
		_ = c.Close()
	})
	for range 5 {
		runtime.GC()
		time.Sleep(10 * time.Millisecond)
	}
}

// nopNetConn is a net.Conn that does nothing.
type nopNetConn struct{ net.Conn }

func (*nopNetConn) Read(b []byte) (int, error)       { return len(b), nil }
func (*nopNetConn) Write(b []byte) (int, error)      { return len(b), nil }
func (*nopNetConn) Close() error                     { return nil }
func (*nopNetConn) SetDeadline(time.Time) error      { return nil }
func (*nopNetConn) SetReadDeadline(time.Time) error  { return nil }
func (*nopNetConn) SetWriteDeadline(time.Time) error { return nil }

// TestConnDeadlineMovedLater moves a pending deadline later: the wait
// must outlast the first deadline and end at the second.
func TestConnDeadlineMovedLater(t *testing.T) {
	client, server := net.Pipe()
	defer func() { _ = server.Close() }()
	go drain(server)
	r := NewRate(1024)
	conn := NewConn(client, r)
	defer func() { _ = conn.Close() }()

	r.reserve(2048) // spend the burst, so that the Write waits about 1s
	start := time.Now()
	_ = conn.SetWriteDeadline(start.Add(20 * time.Millisecond))
	_ = conn.SetWriteDeadline(start.Add(150 * time.Millisecond))
	res := make(chan ioResult, 1)
	go func() {
		n, err := conn.Write(make([]byte, 1024))
		res <- ioResult{n, err}
	}()
	select {
	case got := <-res:
		if got.n != 0 || !isTimeout(got.err) {
			t.Fatalf("Write() = %d, %v; want 0 and a timeout", got.n, got.err)
		}
		if elapsed := time.Since(start); elapsed < 150*time.Millisecond {
			t.Fatalf("Write() timed out after %v, at the deadline that was moved later", elapsed)
		}
	case <-time.After(time.Second):
		r.Disable()
		<-res
		t.Fatal("Write() blocked in the limiter past its deadline")
	}
}

// deadlineRejectingConn is a net.Conn without deadline support, as SSH
// channels are.
type deadlineRejectingConn struct{ nopNetConn }

var errNoDeadline = errors.New("deadline not supported")

func (*deadlineRejectingConn) SetDeadline(time.Time) error      { return errNoDeadline }
func (*deadlineRejectingConn) SetReadDeadline(time.Time) error  { return errNoDeadline }
func (*deadlineRejectingConn) SetWriteDeadline(time.Time) error { return errNoDeadline }

// A deadline the wrapped connection rejects must not bound the waits in the
// limiters either: I/O on it would otherwise time out only when throttled.
func TestConnRejectedDeadline(t *testing.T) {
	conn := NewConn(&deadlineRejectingConn{}, NewRate(1024))
	past := time.Now().Add(-time.Second)
	for name, set := range map[string]func(time.Time) error{
		"SetDeadline":      conn.SetDeadline,
		"SetReadDeadline":  conn.SetReadDeadline,
		"SetWriteDeadline": conn.SetWriteDeadline,
	} {
		if err := set(past); !errors.Is(err, errNoDeadline) {
			t.Fatalf("%s() error = %v, want %v", name, err, errNoDeadline)
		}
	}
	rc := conn.(*rateNetConn)
	for _, d := range []*direction{&rc.read, &rc.write} {
		if d.cancel.current().ended() {
			t.Fatal("a rejected deadline ended the waits in the limiter")
		}
	}
}

// TestConnDeadlineAfterPeerClose ends a Write blocked in the limiter
// with a deadline in the past after the peer closed the pipe: the wrapped
// connection rejects the deadline as closed, which must still end the wait.
func TestConnDeadlineAfterPeerClose(t *testing.T) {
	client, server := net.Pipe()
	r := NewRate(1024)
	conn := NewConn(client, r)
	defer func() { _ = conn.Close() }()

	r.reserve(2048) // spend the burst, so that the Write waits about 8s
	res := make(chan ioResult, 1)
	go func() {
		n, err := conn.Write(make([]byte, 8<<10))
		res <- ioResult{n, err}
	}()
	time.Sleep(20 * time.Millisecond)
	_ = server.Close()
	if err := conn.SetWriteDeadline(time.Now().Add(-time.Second)); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("SetWriteDeadline() after the peer closed error = %v, want %v", err, io.ErrClosedPipe)
	}
	select {
	case got := <-res:
		if got.n != 0 || !isTimeout(got.err) {
			t.Fatalf("Write() = %d, %v; want 0 and a timeout", got.n, got.err)
		}
	case <-time.After(time.Second):
		r.Disable()
		<-res
		t.Fatal("a deadline in the past did not end the Write blocked in the limiter")
	}
}

// TestConnWallClockDeadline sets a deadline without a monotonic reading,
// as time.Unix returns: it must end the wait at that time all the same.
func TestConnWallClockDeadline(t *testing.T) {
	client, server := net.Pipe()
	defer func() { _ = server.Close() }()
	go drain(server)
	r := NewRate(1024)
	conn := NewConn(client, r)
	defer func() { _ = conn.Close() }()

	r.reserve(2048) // spend the burst, so that the Write waits about 1s
	start := time.Now()
	deadline := start.Add(100 * time.Millisecond).Round(0) // wall clock only
	_ = conn.SetWriteDeadline(deadline)
	rc := conn.(*rateNetConn)
	rc.write.cancel.timer.mu.Lock()
	at := rc.write.cancel.timer.at
	rc.write.cancel.timer.mu.Unlock()
	if at.Round(0) == at { //nolint:staticcheck // == compares the monotonic reading, which Equal ignores
		t.Fatal("the limiter keeps a wall-clock deadline without a monotonic reading")
	}
	n, err := conn.Write(make([]byte, 1024))
	if n != 0 || !isTimeout(err) {
		t.Fatalf("Write() = %d, %v; want 0 and a timeout", n, err)
	}
	if elapsed := time.Since(start); elapsed < 90*time.Millisecond || elapsed > 900*time.Millisecond {
		t.Fatalf("Write() timed out after %v, want about 100ms", elapsed)
	}
}

// TestConnDropsRefundFromBeforeReset wakes a Write blocked in the
// limiter with Reset, then fails it: its charge was cleared by the
// reset, so its refund must not hold the cleared meter below 0.
func TestConnDropsRefundFromBeforeReset(t *testing.T) {
	r := NewRate(1024)
	r.reserve(2048) // spend the burst, so that the Write waits about 4s
	wantErr := errors.New("connection reset")
	conn := NewReadWriteCloser(&scriptedConn{writeErr: wantErr}, r)
	res := make(chan ioResult, 1)
	go func() {
		n, err := conn.Write(make([]byte, 4096))
		res <- ioResult{n, err}
	}()
	time.Sleep(20 * time.Millisecond)
	r.Reset(1024)
	select {
	case got := <-res:
		if got.n != 0 || !errors.Is(got.err, wantErr) {
			t.Fatalf("Write() = %d, %v; want 0, %v", got.n, got.err, wantErr)
		}
	case <-time.After(time.Second):
		r.Disable()
		<-res
		t.Fatal("Reset did not wake the Write")
	}
	if got := metered(r); got != 0 {
		t.Fatalf("metered after the failed Write = %d, want 0", got)
	}
}

// recordingConn is a net.Conn that records the size of each Write and
// serves Reads from an endless stream of zeros.
type recordingConn struct {
	nopNetConn
	mu     sync.Mutex
	writes []int
	reads  int64 // bytes handed to Reads
}

func (c *recordingConn) Write(b []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writes = append(c.writes, len(b))
	return len(b), nil
}

func (c *recordingConn) Read(b []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reads += int64(len(b))
	return len(b), nil
}

func (c *recordingConn) maxWrite() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Max(append([]int{0}, c.writes...))
}

// A Write larger than the limit allows before its deadline still makes
// progress: each attempt sends what was granted before the deadline and
// returns it with the timeout, instead of refunding everything forever.
func TestWriteProgressesUnderDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := NewRate(1000)
		conn := NewConn(&recordingConn{}, r)
		defer func() { _ = conn.Close() }()
		buf := make([]byte, 5000) // more than 1000 B/s × (1s + the 2s burst)
		sent := 0
		for range 20 {
			_ = conn.SetWriteDeadline(time.Now().Add(time.Second))
			n, err := conn.Write(buf)
			if n == 0 || err != nil && !isTimeout(err) {
				t.Fatalf("Write() = %d, %v; want progress, with a timeout if any", n, err)
			}
			sent += n
		}
		// About 20s at 1000 B/s and the 2000 B burst.
		if sent < 20000 {
			t.Fatalf("20 Writes of 5000 B with 1s deadlines sent %d B, want about 22000", sent)
		}
	})
}

// A large Write goes out in grants: the burst at once, then quanta, never
// all of it at the end of one long wait.
func TestWriteBurstsNoMoreThanItsCredit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		inner := &recordingConn{}
		conn := NewConn(inner, NewRate(1000))
		start := time.Now()
		if n, err := conn.Write(make([]byte, 10000)); n != 10000 || err != nil {
			t.Fatalf("Write() = %d, %v", n, err)
		}
		if got := time.Since(start); got != 8*time.Second {
			t.Fatalf("10000 B at 1000 B/s with a 2000 B burst took %v, want 8s", got)
		}
		if got := inner.maxWrite(); got > 2000 {
			t.Fatalf("largest write to the connection = %d B, want at most the 2000 B burst", got)
		}
	})
}

// Connections sharing a limit take turns by the quantum, so a small Write
// does not wait behind all of another connection's large one.
func TestSharedLimitInterleavesWrites(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		shared := NewRate(1000)
		shared.reserve(2000) // spend the burst
		a := NewConn(&recordingConn{}, shared)
		b := NewConn(&recordingConn{}, shared)
		go func() { _, _ = a.Write(make([]byte, 20000)) }()
		synctest.Wait()
		start := time.Now()
		if _, err := b.Write(make([]byte, 10)); err != nil {
			t.Fatal(err)
		}
		if got := time.Since(start); got > 2*time.Second {
			t.Fatalf("10 B Write waited %v behind a 20000 B one, want about a quantum", got)
		}
		shared.Disable()
	})
}

// A Read takes at most what the limit lets through, so that the rest stays
// with the sender instead of arriving in one burst.
func TestReadTakesAGrant(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		inner := &recordingConn{}
		conn := NewConn(inner, NewRate(1000))
		if n, err := conn.Read(make([]byte, 64<<10)); n != 2000 || err != nil {
			t.Fatalf("first Read() = %d, %v; want the 2000 B burst", n, err)
		}
		start := time.Now()
		if n, err := conn.Read(make([]byte, 64<<10)); n != 512 || err != nil {
			t.Fatalf("Read() without credit = %d, %v; want a 512 B quantum", n, err)
		}
		if got := time.Since(start); got != 0 {
			t.Fatalf("Read() without debt waited %v", got)
		}
		start = time.Now()
		if n, err := conn.Read(make([]byte, 64<<10)); n != 512 || err != nil {
			t.Fatalf("Read() after one in debt = %d, %v; want a 512 B quantum", n, err)
		}
		if got := time.Since(start); got != 512*time.Millisecond {
			t.Fatalf("Read() waited %v for the last one's debt, want 512ms", got)
		}
	})
}

// A datagram connection is never split: each Write is sent and each Read
// read whole, however long the limit makes it wait.
func TestDatagramConnKeepsMessagesWhole(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		inner := &recordingConn{}
		r := NewRate(1000)
		conn := NewDatagramConn(inner, r, r)
		if n, err := conn.Write(make([]byte, 10000)); n != 10000 || err != nil {
			t.Fatalf("Write() = %d, %v", n, err)
		}
		if len(inner.writes) != 1 || inner.writes[0] != 10000 {
			t.Fatalf("writes to the connection = %v, want one of 10000 B", inner.writes)
		}
		if n, err := conn.Read(make([]byte, 9000)); n != 9000 || err != nil {
			t.Fatalf("Read() = %d, %v; want the whole 9000 B message", n, err)
		}
	})
}

// NewConn treats UDP, IP and Unix datagram sockets as datagram
// connections by itself, and stream sockets as streams.
func TestDatagramSocketsDetected(t *testing.T) {
	udp, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Skip(err)
	}
	defer func() { _ = udp.Close() }()
	if c := NewConn(udp, NewRate(1000)).(*rateNetConn); !c.datagram {
		t.Fatal("a UDP socket was wrapped as a stream")
	}
	client, server := net.Pipe()
	defer func() { _ = client.Close(); _ = server.Close() }()
	if c := NewConn(client, NewRate(1000)).(*rateNetConn); c.datagram {
		t.Fatal("a stream was wrapped as a datagram connection")
	}

	dir := t.TempDir()
	if ln, err := net.ListenUnix("unix", &net.UnixAddr{Name: filepath.Join(dir, "s"), Net: "unix"}); err == nil {
		defer func() { _ = ln.Close() }()
		if uc, err := net.DialUnix("unix", nil, ln.Addr().(*net.UnixAddr)); err == nil {
			defer func() { _ = uc.Close() }()
			if c := NewConn(uc, NewRate(1000)).(*rateNetConn); c.datagram {
				t.Fatal("a Unix stream socket was wrapped as a datagram connection")
			}
		}
	}
	if gc, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: filepath.Join(dir, "g"), Net: "unixgram"}); err == nil {
		defer func() { _ = gc.Close() }()
		if c := NewConn(gc, NewRate(1000)).(*rateNetConn); !c.datagram {
			t.Fatal("a Unix datagram socket was wrapped as a stream")
		}
	}
}

// dataConn is a net.Conn that keeps the bytes written to it.
type dataConn struct {
	nopNetConn
	mu   sync.Mutex
	data []byte
	runs []int // the size of each Write
}

func (c *dataConn) Write(b []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.data = append(c.data, b...)
	c.runs = append(c.runs, len(b))
	return len(b), nil
}

// Concurrent Writes send their grants one Write after the other, as the
// writes of a *net.TCPConn do not interleave.
func TestConcurrentWritesDoNotInterleave(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		inner := &dataConn{}
		conn := NewConn(inner, NewRate(1000))
		var wg sync.WaitGroup
		for _, c := range []byte{'a', 'b'} {
			wg.Go(func() { _, _ = conn.Write(bytes.Repeat([]byte{c}, 5000)) })
		}
		wg.Wait()
		switches := 0
		for i := 1; i < len(inner.data); i++ {
			if inner.data[i] != inner.data[i-1] {
				switches++
			}
		}
		if switches != 1 {
			t.Fatalf("two 5000 B Writes reached the connection in %d runs, want 2", switches+1)
		}
	})
}

// A Write queued behind another ends with it when Close ends their waits.
func TestCloseEndsQueuedWrite(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := NewRate(1000)
		r.reserve(2000) // spend the burst
		conn := NewConn(&dataConn{}, r)
		results := make(chan ioResult, 2)
		for range 2 {
			go func() {
				n, err := conn.Write(make([]byte, 5000))
				results <- ioResult{n, err}
			}()
		}
		synctest.Wait()
		_ = conn.Close()
		for range 2 {
			if res := <-results; !errors.Is(res.err, net.ErrClosed) {
				t.Fatalf("Write() after Close = %d, %v; want net.ErrClosed", res.n, res.err)
			}
		}
	})
}

// A writer whose allowance saw credit that other writers then took queues a
// quantum behind them, not all of that credit.
func TestLostCreditQueuesAQuantum(t *testing.T) {
	for name, wrap := range map[string]func(*Rate) Limiter{
		"Rate":         func(r *Rate) Limiter { return r },
		"Hierarchical": func(r *Rate) Limiter { return NewHierarchy(r, NewRate(0)) },
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				r := NewRate(100 << 10) // a 200 KiB burst and a 2000 B quantum
				var taken atomic.Bool
				hook := func(c *Rate) {
					if c == r && taken.CompareAndSwap(false, true) {
						r.reserve(200 << 10) // another writer takes the credit
					}
				}
				testHooks.charging.Store(&hook)
				defer testHooks.charging.Store(nil)
				inner := &dataConn{}
				conn := NewConn(inner, wrap(r))
				if _, err := conn.Write(make([]byte, 64<<10)); err != nil {
					t.Fatal(err)
				}
				if first := inner.runs[0]; first > 2048 {
					t.Fatalf("first grant after losing the credit = %d B, want a quantum", first)
				}
			})
		})
	}
}

// Below 256 B/s the quantum is the burst, so that Writes with short
// deadlines still make progress.
func TestSlowLimitWriteProgresses(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		conn := NewConn(&dataConn{}, NewRate(100))
		sent := 0
		for range 60 {
			_ = conn.SetWriteDeadline(time.Now().Add(time.Second))
			n, _ := conn.Write(make([]byte, 4096))
			sent += n
		}
		if sent < 5000 {
			t.Fatalf("60 Writes with 1s deadlines at 100 B/s sent %d B, want about 6000", sent)
		}
	})
}

func TestTypedNilUnixConn(t *testing.T) {
	if c := NewConn((*net.UnixConn)(nil), NewRate(1000)); c == nil {
		t.Fatal("NewConn returned nil")
	}
}
