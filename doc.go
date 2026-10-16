// Package byterate limits and meters the bytes of Go connections and streams.
//
// A Rate is a limit in bytes per second that also measures the throughput
// charged to it; its zero value is ready to use. A Budget counts the bytes
// used in a period and, step by step as they grow, slows them down or stops
// them. A Hierarchy nests limiters, such as a connection's Rate within its
// user's Rate and Budget, and waits for the strictest. A Switch passes charges on to a limiter that can be
// replaced while connections use it. NewConn and the other wrappers charge a
// connection's reads and writes to a Limiter, passing data in grants so that
// a large transfer neither bursts past the limit nor holds a shared one; a
// Limiter's Ready and Charge methods serve the same purpose for relays that
// call a hook before each read and after each write. A Counter adds up the
// bytes transferred and measures their throughput, without limiting them. A
// Group is a level of a tree, such as a server, a user and a tunnel, whose
// connections are limited and counted at every level, with their number
// capped where needed.
package byterate
