// Package byterate limits, counts and meters the bytes of Go connections and
// streams.
//
// NewConn wraps a net.Conn so that its bytes count in its Meter and, with
// WithLimiter or WithLimiters, are limited. Without a limit the reads and
// writes pass straight through, counted by an uncontended atomic add, and
// SetLimiters adds, changes or lifts a limit at any time, also for a
// transfer in progress:
//
//	limited := byterate.NewConn(conn, byterate.WithLimiter(byterate.NewRate(1<<20))) // 1 MiB/s
//
// A Rate is a limit in bytes per second; its zero value is ready to use.
// NewHierarchy nests limiters, such as a connection's Rate within its
// user's Rate and a global one, and waits for the strictest. A Budget counts
// the bytes used in a period and, step by step as they grow, slows them down
// or stops them; the wrappers lease its bytes ahead, so that a quota costs
// no more than counting. A Switch passes charges on to a limiter that can
// be replaced while many connections use it. A Counter adds up bytes, from
// its own Adds or from the Meters attached to it, and measures their
// throughput from samples, never on the path of a read or write.
//
// A Group is a level of a tree, such as a server, a user and a tunnel: the
// connections opened in it are limited and counted at every level, with
// their number capped where needed, and follow later changes of the levels.
//
// NewPacketConn does the same for the datagrams of a net.PacketConn,
// NewStream, NewReader and NewWriter for plain streams. A Limiter's TryWait
// charges bytes only if they may pass at once, for senders that drop
// datagrams rather than wait, and its Ready and Charge methods serve relays
// that call a hook before each read and after each write.
package byterate
