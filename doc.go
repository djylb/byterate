// Package byterate limits and meters the bytes of Go connections and streams.
//
// A Rate is a limit in bytes per second that also measures the throughput
// charged to it; its zero value is ready to use. A Budget is a total number
// of bytes, which refuses transfers once spent. A Hierarchy nests limiters,
// such as a connection's Rate within its user's Rate and Budget, and waits
// for the strictest. A Switch passes charges on to a limiter that can be
// replaced while connections use it. NewConn and the other wrappers charge a
// connection's reads and writes to a Limiter, passing data in grants so that
// a large transfer neither bursts past the limit nor holds a shared one; a
// Limiter's Ready and Charge methods serve the same purpose for relays that
// call a hook before each read and after each write. A Counter adds up the
// bytes transferred and measures their throughput, without limiting them.
package byterate
