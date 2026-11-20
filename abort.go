package byterate

import "net"

// Abort ends the connection abnormally, so that its peer sees a cut instead
// of an end of data, and then closes the wrapper as Close does: a wrapped
// connection with a method Abort(cause error), such as a stream of a
// multiplexed connection, is aborted with cause; otherwise a wrapped
// connection with a method SetLinger(sec int) error, a *net.TCPConn, also
// beneath wrappers that return what they wrap from a NetConn method, gets
// SO_LINGER 0, so that its close sends RST instead of FIN. A connection
// with neither is just closed. Relays that end a transfer on an error,
// such as netx.Relay, call it through the same method set.
func (s *rateConn) Abort(cause error) {
	if s != nil && s.c != nil {
		abortBeneath(s.c, cause)
	}
	_ = s.Close()
}

// maxAbortDepth bounds the NetConn chain that abortBeneath follows, so that
// a wrapper that returns itself cannot loop.
const maxAbortDepth = 16

// abortBeneath aborts v, or what it wraps, without closing it.
func abortBeneath(v any, cause error) {
	for range maxAbortDepth {
		switch x := v.(type) {
		case nil:
			return
		case interface{ Abort(cause error) }:
			x.Abort(cause)
			return
		case interface{ SetLinger(sec int) error }:
			_ = x.SetLinger(0)
			return
		case interface{ NetConn() net.Conn }:
			next := x.NetConn()
			if next == nil || any(next) == v {
				return
			}
			v = next
		default:
			return
		}
	}
}

// Abort aborts the wrapped connection, as Conn's Abort does, and closes
// the wrapper; a datagram socket has no reset, so for one it only closes.
func (c *PacketConn) Abort(cause error) { c.s.Abort(cause) }
