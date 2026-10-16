# byterate

Byte rate limiting and traffic metering for Go I/O.

## Install

```bash
go get github.com/djylb/byterate
```

## Usage

```go
import "github.com/djylb/byterate"

limiter := byterate.NewRate(64 * 1024)
limitedConn := byterate.NewRateConn(conn, limiter)           // net.Conn
limitedStream := byterate.NewRateReadWriteCloser(rwc, limiter) // io.ReadWriteCloser

meter := byterate.NewMeter()
meter.Add(readBytes, writtenBytes)
inBps, outBps, totalBps := meter.Snapshot()
```

Nested limits, such as a connection's within its user's within a global one,
charge every level, and each level reports its own throughput:

```go
global := byterate.NewRate(0)      // unlimited, kept for its throughput
user := byterate.NewRate(10 << 20) // 10 MiB/s for all of a user's connections
conn := byterate.NewRateConn(c, byterate.NewHierarchicalLimiter(byterate.NewRate(2<<20), user, global))
fmt.Println(global.Now(), user.Now()) // bytes/s of each level
global.SetLimit(100 << 20)            // applies to open connections too

// Separate download and upload limits for one connection.
duplex := byterate.NewDuplexRateConn(c, byterate.NewRate(8<<20), byterate.NewRate(1<<20))
```

## Semantics

- Limits are in bytes per second; a limit `<= 0` means unlimited.
- After an idle period a `Rate` lets two seconds' worth of bytes through
  without waiting. Its meter counts up to 2^63-1 bytes in a sampling window.
- `NewRate` returns a started `Rate`. The zero value is unlimited until
  `ResetLimit`, or `SetLimit` followed by `Start`.
- `SetLimit` applies from the next charge and keeps debt as bytes: what is
  owed at the old limit is repaid at the new one, also after a time without a
  limit. It does not wake blocked callers, which keep their wait; `ResetLimit`
  (`Stop`, `SetLimit`, `Start`) wakes them, drops the debt, restores the full
  burst and clears the meter.
- `Stop` disables limiting and wakes callers blocked in `Get`.
  `GetContext` also returns early when its context is done; the charge is
  kept, so refund bytes that were not transferred with `ReturnBucket`.
  Waits of at most 200 µs are accumulated as debt without sleeping. A plain
  `Get` sleep of at most 2 ms finishes normally even if `Stop` is called;
  cancelable `GetContext` waits watch cancellation even below 2 ms.
- `NewHierarchicalLimiter` charges every started `Rate` and waits until every
  one allows the bytes. `Stop` on one `Rate` ends the wait that `Rate`
  imposes, and the others still apply. Nil rates and repeats are left out,
  so a `Rate` given twice is charged once, and it returns nil when none
  remain, or the `Rate` itself when one does. Unlimited rates are kept, so
  they meter the traffic and a limit set on them later applies to existing
  connections. Pass nil for levels that should neither limit nor meter:
  wrapping a connection costs a charge per call and hides `*net.TCPConn`'s
  zero-copy `ReadFrom`/`WriteTo`.
- `NewRateConn` and `NewRateReadWriteCloser` charge reads after the data
  arrives and writes before sending, refunding short writes. When the limiter
  implements `ContextLimiter` (`*Rate` and `*HierarchicalLimiter` do), `Close`
  wakes a `Read` or `Write` blocked in the limiter, which then returns
  `net.ErrClosed`; any other error from `GetContext` is returned as it is,
  and a `Write` refunds its charge. A nil limiter, including a nil `*Rate` or
  `*HierarchicalLimiter`, returns the connection unchanged.
- `NewRateConn` keeps the `net.Conn` addresses and deadlines, and its
  `RawConn` method returns the wrapped connection, so wrapper-aware helpers
  can unwrap it. `CloseRead` and `CloseWrite` are passed on when the wrapped
  connection has them, so relays can half-close through the wrapper; when
  they succeed, they also end a wait in that direction's limiter with
  `net.ErrClosed`, as `Close` does for both.
- With a `ContextLimiter`, the read and write deadlines of `NewRateConn` also
  bound the wait in the limiter, and a deadline set in the past ends a wait in
  progress, as `net/http` does to abort a read. A `Write` whose deadline
  passes refunds its charge and returns 0 and `os.ErrDeadlineExceeded`, a
  timeout `net.Error`. A `Read` has already received its bytes, so it keeps
  the charge and returns them with `os.ErrDeadlineExceeded`. Moving a deadline
  later does not end a wait. A deadline the wrapped connection rejects, as
  one without deadline support does, does not bound the limiter either,
  unless the connection is closed.
- `Rate.Now` and `Meter.Snapshot` report bytes per second over the last
  sampling window of at least one second, so they lag by up to about two
  seconds. Refunds come off the throughput: one for bytes metered in an
  earlier window, such as a write canceled by `Close`, is taken off the next
  windows. The connection wrappers drop the refund of a charge made before
  its `Rate` was last started, as `Start` cleared that charge from the debt
  and the meter. Each write records whether each level actually charged
  debt: refunding a write charged while unlimited only adjusts its meter,
  even if a limit was enabled before the write failed. Direct `Get` and
  `ReturnBucket` calls carry no receipt: callers must coordinate refunds with
  limit changes and restarts themselves. `NewRateConn` charges both
  directions of a connection to one limiter; `NewDuplexRateConn` and
  `NewDuplexRateReadWriteCloser` take one per direction, so uploads and
  downloads are limited and metered apart.

## Concurrency

`Get`, `Now`, `Meter.Add` and the charges of connection reads and writes are
lock-free. `ReturnBucket`, and the refund of a short or failed write, take
the `Rate`'s lock briefly, so that a refund sees the same limit and start as
the debt it changes. A `Rate` keeps its debt and meter in a generation that
`Start` replaces, rather than clears, when it restarts the `Rate`: a charge
lands in one generation, waits on that generation's `Stop`, and a write's
refund receipt refers to it, so a charge racing with a restart counts in
exactly one generation. The callers waiting on a `Rate` spread over several
channels that `Stop` closes, so that the connections throttled by a shared
`Rate` do not queue for one channel's lock.

Up to eight hierarchy levels keep these receipts and their waits on the
stack; larger hierarchies allocate for them on every write. Apart from that,
charges allocate only when a wait needs a timer: a `Get` waiting longer than
2 ms, or a `GetContext` with a cancelable context, waiting longer than
200 µs, so that a deadline or `Close` ends even a short wait. The connection
wrappers reuse one timer per direction for the waits of a `*Rate` or
`*HierarchicalLimiter`, so a throttled connection does not allocate per
wait. An already-canceled wait returns without allocating a timer. A
hierarchical charge reads the clock once for all levels. A `Rate` takes
about 380 bytes in two allocations, laid out so that the fields every charge
reads or writes share no cache line with other data; once callers wait on
it, it also holds up to eight channels per processor. A `Meter` takes about
300 bytes. Wrapping a connection takes at most three allocations,
about 240 bytes. Setting its deadlines allocates only for the first deadline
and after one has expired, and moving a deadline later, as servers do before
every read, leaves the runtime timer alone. A pending deadline does not keep
a connection that was never closed from being garbage collected.

Tests can use `testing/synctest`: inside a bubble, a `Rate` or `Meter` runs on
the bubble's clock. Create it inside the bubble that uses it, since its
timestamps do not carry over between a bubble and the outside.
