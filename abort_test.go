package byterate

import (
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

// netConnWrapper hides a connection behind a NetConn method, as the
// wrappers of other packages do.
type netConnWrapper struct{ net.Conn }

func (w netConnWrapper) NetConn() net.Conn { return w.Conn }

// Abort resets a TCP connection, also beneath a NetConn wrapper and the
// Notifying form, so that the peer reads a reset, not EOF.
func TestConnAbortResetsTCP(t *testing.T) {
	for name, wrap := range map[string]func(net.Conn) net.Conn{
		"direct":  func(c net.Conn) net.Conn { return NewConn(c, WithLimiter(NewRate(1<<20))) },
		"beneath": func(c net.Conn) net.Conn { return NewConn(netConnWrapper{c}).Notifying() },
	} {
		t.Run(name, func(t *testing.T) {
			client, server := tcpPair(t)
			c := wrap(client)
			a, ok := c.(interface{ Abort(cause error) })
			if !ok {
				t.Fatalf("%T has no Abort", c)
			}
			a.Abort(errors.New("cut"))
			_ = server.SetReadDeadline(time.Now().Add(5 * time.Second))
			_, err := io.ReadAll(server)
			if err == nil {
				t.Fatal("peer read after Abort ended at EOF, want a reset")
			}
			if _, err := c.Write([]byte("x")); err == nil {
				t.Fatal("Write after Abort = nil error")
			}
		})
	}
}

// abortStream records Abort and Close.
type abortStream struct {
	cause   error
	aborted bool
	closed  bool
}

func (*abortStream) Read([]byte) (int, error)    { return 0, io.EOF }
func (*abortStream) Write(p []byte) (int, error) { return len(p), nil }
func (s *abortStream) Close() error              { s.closed = true; return nil }
func (s *abortStream) Abort(cause error)         { s.aborted, s.cause = true, cause }

// Abort passes the cause to the wrapped stream's Abort, then closes the
// wrapper and the stream, as Close does.
func TestStreamAbortForwards(t *testing.T) {
	inner := &abortStream{}
	s := NewStream(inner, WithLimiter(NewRate(1<<20)))
	cause := errors.New("cut")
	s.Abort(cause)
	if !inner.aborted || !errors.Is(inner.cause, cause) || !inner.closed {
		t.Fatalf("inner aborted=%v cause=%v closed=%v, want true, %v, true", inner.aborted, inner.cause, inner.closed, cause)
	}
}
