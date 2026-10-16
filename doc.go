// Package byterate limits and meters the bytes per second of Go connections
// and streams.
//
// A Rate is a limit in bytes per second that also measures the throughput
// charged to it; its zero value is ready to use. A Hierarchy nests Rates,
// such as a connection's within its user's within a global one, and waits
// for the strictest. NewConn and the other wrappers charge a connection's
// reads and writes to a Limiter, passing data in grants so that a large
// transfer neither bursts past the limit nor holds a shared one. A Meter
// measures throughput without limiting it.
package byterate
