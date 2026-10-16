# byterate

Byte rate limiting and throughput metering for Go connections and streams.

## Install

```bash
go get github.com/djylb/byterate
```

## Usage

Limit a connection; the result is still a `net.Conn`:

```go
limited := byterate.NewConn(conn, byterate.NewRate(1<<20)) // 1 MiB/s both ways
```

Nest limits, such as a connection's within its user's within a global one.
Every level meters the traffic, and a limit changed later applies to open
connections too:

```go
global := byterate.NewRate(0)      // unlimited, kept for its throughput
user := byterate.NewRate(10 << 20) // 10 MiB/s across the user's connections

c := byterate.NewConn(conn, byterate.NewHierarchy(byterate.NewRate(2<<20), user, global))

fmt.Println(global.Throughput(), user.Throughput()) // bytes per second
global.SetLimit(100 << 20)
```

Limit downloads and uploads apart:

```go
c := byterate.NewDuplexConn(conn, byterate.NewRate(8<<20), byterate.NewRate(1<<20))
```

Other wrappers and types:

- `NewReadWriteCloser` and `NewDuplexReadWriteCloser` wrap an
  `io.ReadWriteCloser`.
- `NewDatagramConn` wraps a connection that carries one message per `Read` and
  `Write`. UDP, IP and Unix datagram sockets are recognized by `NewConn` itself.
- A `Rate` can be used directly: `Wait(ctx, n)` charges `n` bytes and blocks
  until the limit allows them, and `Refund(n)` gives back bytes that were not
  transferred.
- `Meter` measures inbound and outbound throughput without limiting.
- The zero value of `Rate` is ready to use, unlimited until `SetLimit`.

## How it behaves

**Limits.** Limits are in bytes per second; `0` or less means unlimited. After
an idle period a `Rate` lets two seconds' worth of bytes through at once.
`SetLimit` takes effect from the next charge and keeps what is owed: bytes
charged at the old limit are repaid at the new one. `Reset` instead wakes
blocked callers, drops the debt and starts over with a full burst.

**Disable is not a block.** `Disable` turns a `Rate` off: traffic charged to it
passes without limit, blocked callers are woken, and metering stops until
`Enable`. To cut traffic, close the connections.

**Connections pass data in grants.** A grant is what the limit lets through at
once: its credit, or without credit about 20 ms of traffic.

- A `Write` sends its bytes a grant at a time, so a large `Write` neither
  bursts past the limit nor holds a shared limit while other connections wait.
  Concurrent `Write`s on one connection do not interleave.
- A `Read` first waits until the limit has no debt, then reads at most a grant.
  A reader in debt leaves the data with the sender, and a reader waiting for
  data holds no share of the limit. Readers waiting on one shared limit read
  together when its debt is repaid, a grant each.
- Datagrams are never split: each one is charged and sent whole.

**Ending waits.** `Close` ends a wait with `net.ErrClosed`, and `CloseRead` or
`CloseWrite` do the same for their side. Deadlines set through the wrapper end
waits with `os.ErrDeadlineExceeded`; a deadline set on the connection before
wrapping it does not. A `Write` that is ended part way returns the bytes it
sent and refunds the rest; a `Read` ended in its wait returns no bytes.

**Half-close.** The wrapper always has `CloseRead` and `CloseWrite`, which
return an error matching `errors.ErrUnsupported` when the wrapped connection
lacks them, so check the error rather than the method.

**Nested limits.** A `Hierarchy` charges every level and waits for the
strictest. `Disable` on one level ends the wait it imposes; the others still
apply. Every level is charged when the bytes are, so a looser level counts
bytes still waiting on a stricter one: it keeps its long-run limit, but when
many waits on stricter levels end together, it can pass more than its burst at
once. Pass `nil` for levels that should neither limit nor meter.

**Throughput.** `Rate.Throughput` and `Meter.Throughput` report bytes per second
over the last sampling window of at least one second, so they lag by up to
about two seconds. Refunds are taken off the throughput.

**Other limiters.** The wrappers accept any `Limiter`, but only `*Rate` and
`*Hierarchy` can grant part of a request: with another `Limiter`, a `Write` is
charged whole before it is sent and a `Read` is charged after it returns.

## Performance

Charges are lock-free; a refund takes the `Rate`'s lock briefly. A throttled
connection reuses one timer per direction and does not allocate per `Read` or
`Write` for hierarchies of up to eight levels. Waits of up to 200 µs are
accumulated as debt rather than slept. Callers waiting on a shared `Rate` are
spread over several channels, so that many throttled connections do not
contend on one.

Wrapping a connection hides the zero-copy `ReadFrom` and `WriteTo` of
`*net.TCPConn`, so wrap only connections that need a limit or a meter.

## Testing

`Rate` and `Meter` work inside `testing/synctest` bubbles, on the bubble's
clock. Create them inside the bubble that uses them.
