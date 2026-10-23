// Package byterate limits and meters the bytes of Go connections and streams.
//
// A Rate is a limit in bytes per second that also measures the throughput
// charged to it; its zero value is ready to use. NewConn and the other
// wrappers charge a connection's reads and writes to a Limiter, such as a
// Rate, passing data in grants so that a large transfer neither bursts past
// the limit nor holds a shared one:
//
//	limited := byterate.NewConn(conn, byterate.NewRate(1<<20)) // 1 MiB/s
//
// NewHierarchy nests limiters, such as a connection's Rate within its user's
// Rate and a global one, and waits for the strictest. A Budget counts the
// bytes used in a period and, step by step as they grow, slows them down or
// stops them. A Switch passes charges on to a limiter that can be replaced
// while connections use it. A Counter adds up the bytes transferred and
// measures their throughput, without limiting them.
//
// A Group is a level of a tree, such as a server, a user and a tunnel: the
// connections opened in it are limited and counted at every level, with
// their number capped where needed. It suits servers that account for many
// users, where the other types suit a connection or a few.
//
// NewPacketConn does the same for the datagrams of a net.PacketConn, and
// NewReader and NewWriter for plain streams. A Limiter's TryWait charges
// bytes only if they may pass at once, for senders that drop datagrams
// rather than wait, and its Ready and Charge methods serve relays that call
// a hook before each read and after each write, in place of the wrappers.
package byterate
