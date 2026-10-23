package byterate

import (
	"bytes"
	"errors"
	"io"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// zeroCopyFile is a source that the zero-copy paths take for a file. Like
// the reader an *os.File hands io.Copy, it has no WriteTo.
type zeroCopyFile struct{ r *bytes.Reader }

func (f zeroCopyFile) Read(b []byte) (int, error) { return f.r.Read(b) }

func (zeroCopyFile) SyscallConn() (syscall.RawConn, error) { return nil, errors.ErrUnsupported }
func (zeroCopyFile) Stat() (fs.FileInfo, error)            { return nil, errors.ErrUnsupported }

// copyConn records the copies that a wrapper hands its ReadFrom, which a
// socket would make with sendfile or splice.
type copyConn struct {
	nopNetConn
	got    bytes.Buffer
	chunks []int64
	writes int
}

func (c *copyConn) Write(b []byte) (int, error) { c.writes++; return c.got.Write(b) }

func (c *copyConn) ReadFrom(r io.Reader) (int64, error) {
	lr, ok := r.(*io.LimitedReader)
	if !ok {
		return 0, errors.New("ReadFrom without a limit: it would bypass the limiter")
	}
	if _, ok := lr.R.(zeroCopyFile); !ok {
		return 0, errors.New("ReadFrom of a reader that is not the file")
	}
	c.chunks = append(c.chunks, lr.N)
	return io.Copy(&c.got, lr)
}

func (*copyConn) SyscallConn() (syscall.RawConn, error) { return nil, errors.ErrUnsupported }

// io.Copy to a wrapper hands the wrapped connection's ReadFrom a grant at a
// time, as a limited reader of the file itself, so that sendfile still
// works, and never past a Budget's stop; other sources go through Write.
func TestReadFromForwardsGrants(t *testing.T) {
	data := bytes.Repeat([]byte("0123456789"), 100_000) // 1 MB
	budget := stopAfter(700_000)
	rate := NewRate(1 << 40)
	inner := &copyConn{}
	c := NewConn(inner, NewHierarchy(rate, budget))
	n, err := io.Copy(c, zeroCopyFile{bytes.NewReader(data)})
	if n != 700_000 || !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("io.Copy = %d, %v; want 700000 and ErrBudgetExhausted", n, err)
	}
	if !bytes.Equal(inner.got.Bytes(), data[:n]) || inner.writes != 0 {
		t.Fatalf("copied %d bytes in %d writes, want the file's first %d through ReadFrom", inner.got.Len(), inner.writes, n)
	}
	for _, chunk := range inner.chunks {
		if chunk > copyChunk {
			t.Fatalf("a grant of %d bytes, want at most %d", chunk, copyChunk)
		}
	}
	if budget.Used() != n || metered(rate) != n {
		t.Fatalf("charged %d and %d, want the %d copied", budget.Used(), metered(rate), n)
	}

	// A LimitedReader of the file is unwrapped and kept in step.
	inner = &copyConn{}
	lr := &io.LimitedReader{R: zeroCopyFile{bytes.NewReader(data)}, N: 300_000}
	if n, err := NewConn(inner, NewRate(1<<40)).(io.ReaderFrom).ReadFrom(lr); n != 300_000 || err != nil || lr.N != 0 {
		t.Fatalf("ReadFrom(LimitedReader) = %d, %v with %d left; want 300000, nil, 0", n, err, lr.N)
	}

	// Any other source goes through Write.
	inner = &copyConn{}
	if n, err := io.Copy(NewConn(inner, NewRate(1<<40)), strings.NewReader("hello")); n != 5 || err != nil || inner.writes == 0 || len(inner.chunks) != 0 {
		t.Fatalf("io.Copy of a strings.Reader = %d, %v in %d writes and %d chunks", n, err, inner.writes, len(inner.chunks))
	}
}

// io.Copy from a wrapper hands the destination's ReadFrom a grant at a time
// of the wrapped reader, and charges what arrived.
func TestWriteToForwardsGrants(t *testing.T) {
	data := bytes.Repeat([]byte("abcdefghij"), 100_000)
	rate := NewRate(1 << 40)
	src := NewReader(zeroCopyFile{bytes.NewReader(data)}, rate)
	dst := &copyConn{}
	if n, err := io.Copy(dst, src); n != int64(len(data)) || err != nil {
		t.Fatalf("io.Copy = %d, %v; want %d, nil", n, err, len(data))
	}
	if !bytes.Equal(dst.got.Bytes(), data) || len(dst.chunks) < 2 {
		t.Fatalf("copied %d bytes in %d grants, want all of them in grants", dst.got.Len(), len(dst.chunks))
	}
	if got := metered(rate); got != int64(len(data)) {
		t.Fatalf("metered %d, want %d", got, len(data))
	}
}

// Copies between a file and real sockets through the wrappers arrive
// whole, at the limit.
func TestZeroCopyOverTCP(t *testing.T) {
	data := bytes.Repeat([]byte("zero-copy "), 300_000) // 3 MB
	path := filepath.Join(t.TempDir(), "data")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	client, server := tcpPair(t)
	relayIn, relayOut := tcpPair(t)
	write, read := NewRate(1<<40), NewRate(1<<40)
	done := make(chan error, 1)
	go func() {
		// file -> limited -> client ... server -> limited -> relayIn ... relayOut
		_, err := io.Copy(NewConn(client, write), f)
		_ = client.(*net.TCPConn).CloseWrite()
		done <- err
	}()
	go func() {
		_, err := io.Copy(relayIn, NewConn(server, read))
		_ = relayIn.(*net.TCPConn).CloseWrite()
		done <- err
	}()
	got, err := io.ReadAll(relayOut)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("got %d bytes, want the %d of the file", len(got), len(data))
	}
	if metered(write) != int64(len(data)) || metered(read) != int64(len(data)) {
		t.Fatalf("metered %d written and %d read, want %d", metered(write), metered(read), len(data))
	}
}

func udpPair(t *testing.T) (a, b *net.UDPConn) {
	t.Helper()
	listen := func() *net.UDPConn {
		c, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
		if err != nil {
			t.Skip("no UDP:", err)
		}
		t.Cleanup(func() { _ = c.Close() })
		return c
	}
	return listen(), listen()
}

// A PacketConn's datagrams are charged whole: a write waits for the limit
// and a deadline or Close ends the wait, refunding the datagram, and a read
// charges what arrived.
func TestPacketConn(t *testing.T) {
	a, b := udpPair(t)
	rate := NewRate(1000) // a burst of 2000 bytes
	pc := NewPacketConn(a, rate)
	msg := make([]byte, 1000)
	for range 2 {
		if n, err := pc.WriteTo(msg, b.LocalAddr()); n != 1000 || err != nil {
			t.Fatalf("WriteTo within the burst = %d, %v", n, err)
		}
	}
	_ = pc.SetWriteDeadline(time.Now().Add(50 * time.Millisecond))
	if n, err := pc.WriteTo(msg, b.LocalAddr()); n != 0 || !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("WriteTo past the burst = %d, %v; want 0 and the deadline", n, err)
	}
	if got := metered(rate); got != 2000 {
		t.Fatalf("metered %d, want the 2000 bytes sent, the third datagram refunded", got)
	}
	_ = pc.SetWriteDeadline(time.Time{})

	reader := NewPacketConn(b, NewRate(1<<30))
	buf := make([]byte, 1500)
	for range 2 {
		if n, addr, err := reader.ReadFrom(buf); n != 1000 || err != nil || addr.String() != a.LocalAddr().String() {
			t.Fatalf("ReadFrom = %d, %v, %v", n, addr, err)
		}
	}

	// A datagram socket wrapped as a Reader is read whole too, whatever the
	// grant.
	tiny := NewRate(1000)
	tiny.SetBurst(10 * time.Millisecond) // grants of 10 bytes
	if _, err := a.WriteTo(msg, b.LocalAddr()); err != nil {
		t.Fatal(err)
	}
	if n, err := NewReader(b, tiny).Read(buf); n != 1000 || err != nil {
		t.Fatalf("Read of a datagram through NewReader = %d, %v; want it whole", n, err)
	}

	res := closeWhileBlocked(t, pc, func() (int, error) { return pc.WriteTo(msg, b.LocalAddr()) }, rate.Disable)
	if res.n != 0 || !errors.Is(res.err, net.ErrClosed) {
		t.Fatalf("WriteTo ended by Close = %d, %v; want 0, net.ErrClosed", res.n, res.err)
	}
}

// WithDrop drops the datagrams that would wait, uncharged: WriteTo reports
// them sent, ReadFrom reads on to the next one until its deadline, a
// refusal still ends either with its error, and a dropped write allocates
// nothing.
func TestPacketConnDrop(t *testing.T) {
	a, b := udpPair(t)
	rate := NewRate(1000) // a burst of 2000 bytes
	pc := NewPacketConn(a, rate, WithDrop())
	msg := make([]byte, 1000)
	for i := range 3 {
		if n, err := pc.WriteTo(msg, b.LocalAddr()); n != 1000 || err != nil {
			t.Fatalf("WriteTo %d = %d, %v; want 1000, nil, sent or dropped", i, n, err)
		}
	}
	if got := metered(rate); got != 2000 {
		t.Fatalf("metered %d, want the 2000 bytes sent, the dropped datagram uncharged", got)
	}
	if allocs := testing.AllocsPerRun(100, func() { _, _ = pc.WriteTo(msg, b.LocalAddr()) }); allocs != 0 {
		t.Fatalf("a dropped WriteTo allocates %v times, want 0", allocs)
	}

	readRate := NewRate(1000)
	reader := NewPacketConn(b, readRate, WithDrop())
	buf := make([]byte, 1500)
	for range 2 {
		if n, _, err := reader.ReadFrom(buf); n != 1000 || err != nil {
			t.Fatalf("ReadFrom within the burst = %d, %v", n, err)
		}
	}
	if _, err := a.WriteTo(msg, b.LocalAddr()); err != nil { // dropped on arrival
		t.Fatal(err)
	}
	_ = reader.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	if n, _, err := reader.ReadFrom(buf); n != 0 || !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("ReadFrom past the burst = %d, %v; want the datagram dropped and the deadline", n, err)
	}
	if got := metered(readRate); got != 2000 {
		t.Fatalf("read metered %d, want 2000", got)
	}

	g := NewGroup(nil)
	g.SetWriteLimiter(stopAfter(1500))
	gpc, err := g.OpenPacketConn(a, WithDrop())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = gpc.Close() }()
	if _, err := gpc.WriteTo(msg, b.LocalAddr()); err != nil {
		t.Fatal(err)
	}
	if n, err := gpc.WriteTo(msg, b.LocalAddr()); n != 0 || !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("WriteTo past the budget = %d, %v; want 0 and ErrBudgetExhausted", n, err)
	}
}

// A Group's PacketConn takes a slot until Close and counts its datagrams.
func TestGroupOpenPacketConn(t *testing.T) {
	a, b := udpPair(t)
	g := NewGroup(nil)
	pc, err := g.OpenPacketConn(a)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pc.WriteTo([]byte("hello"), b.LocalAddr()); err != nil {
		t.Fatal(err)
	}
	if _, out := g.Traffic().Total(); out != 5 || g.Conns() != 1 {
		t.Fatalf("written %d with %d conns, want 5 and 1", out, g.Conns())
	}
	_ = pc.Close()
	_ = pc.Close()
	if g.Conns() != 0 {
		t.Fatalf("Conns() after Close = %d, want 0", g.Conns())
	}
}

// NewReader and NewWriter limit plain streams, and their Close ends a wait
// and closes what they wrap.
func TestReaderAndWriter(t *testing.T) {
	rate := NewRate(1 << 30)
	r := NewReader(io.NopCloser(strings.NewReader("hello, world")), rate)
	if got, err := io.ReadAll(r); string(got) != "hello, world" || err != nil {
		t.Fatalf("ReadAll = %q, %v", got, err)
	}
	if metered(rate) != 12 {
		t.Fatalf("metered %d, want 12", metered(rate))
	}

	slow := NewRate(1000)
	var buf bytes.Buffer
	w := NewWriter(&buf, slow)
	res := closeWhileBlocked(t, w, func() (int, error) { return w.Write(make([]byte, 5000)) }, slow.Disable)
	if res.n == 0 || res.n >= 5000 || !errors.Is(res.err, net.ErrClosed) {
		t.Fatalf("Write ended by Close = %d, %v; want part of it and net.ErrClosed", res.n, res.err)
	}
	if got := metered(slow); got != int64(res.n) || buf.Len() != res.n {
		t.Fatalf("metered %d and wrote %d, want the %d sent", got, buf.Len(), res.n)
	}

	pr, pw := io.Pipe()
	r = NewReader(pr, NewRate(1))
	_ = r.Close()
	if _, err := pw.Write([]byte("x")); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("write to the pipe after the Reader's Close = %v, want io.ErrClosedPipe", err)
	}
	if n, err := NewReader(nil, NewRate(1)).Read(make([]byte, 1)); n != 0 || !errors.Is(err, ErrNilConn) {
		t.Fatalf("Read of a nil reader = %d, %v; want ErrNilConn", n, err)
	}
}
