# byterate

Byte rate limiting, byte budgets and traffic counting for Go connections and
streams.

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
Every level measures its own throughput, and a limit changed later applies to
open connections too:

```go
global := byterate.NewRate(0)      // unlimited, kept for its throughput
user := byterate.NewRate(10 << 20) // 10 MiB/s across the user's connections

c := byterate.NewConn(conn, byterate.NewHierarchy(byterate.NewRate(2<<20), user, global))

fmt.Println(user.Throughput()) // bytes per second
user.SetLimit(20 << 20)
```

Limit downloads and uploads apart:

```go
c := byterate.NewDuplexConn(conn, byterate.NewRate(8<<20), byterate.NewRate(1<<20))
```

`NewReadWriteCloser` and `NewDuplexReadWriteCloser` do the same for an
`io.ReadWriteCloser`. The zero value of `Rate` is ready to use, unlimited
until `SetLimit`.

## More

**Budgets.** A `Budget` caps the total bytes. Once it is spent, reads and
writes fail with `ErrBudgetExhausted`:

```go
budget := byterate.NewBudget(10 << 30) // 10 GiB
budget.OnLow(100<<20, func(left int64) { /* ask for more */ })
budget.Grant(1<<30, time.Now().Add(time.Minute)) // 1 GiB more; it all lapses in a minute

c := byterate.NewConn(conn, byterate.NewHierarchy(user, budget, global))
```

**Counting.** A `Counter` adds up the bytes transferred and measures their
throughput. `Swap` takes the totals since the last `Swap`, so that each byte
is reported once:

```go
var traffic byterate.Counter // one per connection; NewSharedCounter for many goroutines
traffic.Add(readBytes, writtenBytes)
in, out := traffic.Swap()
inBps, outBps := traffic.Throughput()
```

**Switching.** A `Switch` moves a connection's traffic to another limiter
while it runs:

```go
sw := byterate.NewSwitch(oldUser)
c := byterate.NewConn(conn, sw)
sw.Set(newUser) // waits in progress start over on newUser
```

**Relays.** A relay that calls a hook before each read and after each write
can use any limiter's `Ready` and `Charge`: `Ready` blocks until the limit is
ready and says how many bytes may pass, and `Charge` records the bytes that
did. A `Counter`'s `ChargeIn` and `ChargeOut` fit the hook after a write:

```go
type hooks struct {
	Wait func(ctx context.Context, max int64) (int64, error)
	Done func(n int64) error
}

l := byterate.NewHierarchy(connRate, user, budget)
upload := hooks{Wait: l.Ready, Done: l.Charge}
```

**Datagrams.** `NewDatagramConn` wraps a connection that carries one message
per `Read` and `Write`; UDP, IP and Unix datagram sockets are recognized by
`NewConn` itself.

**Direct use.** Any limiter can be charged directly: `Wait(ctx, n)` charges
`n` bytes and blocks until the limit allows them, and `Refund(n)` gives back
bytes that were not transferred.

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

**Refusals.** A level that refuses bytes, such as a spent `Budget`, is asked
first and refuses them for the whole `Hierarchy`: the other levels neither
take them nor make the caller wait. `Charge`, which records bytes already
transferred, keeps them on every level and returns the refusal.

**Budgets.** A `Budget` never blocks. A `Write` sends what is left of it and
then fails; a `Read` that overdraws it returns the bytes it read with
`ErrBudgetExhausted`. `Grant` adds bytes and sets when the whole budget lapses,
and `OnLow` reports each time a charge takes it below a mark, so once until a
`Grant` or `Refund` lifts it back. When to grant, and what to do with bytes
left at a lapse, is up to the caller.

**Switching.** After `Switch.Set`, new charges go to the new limiter, and a
wait in progress on a `Rate` of the old one ends, is refunded there, and
starts over on the new one. The wrappers and a `Hierarchy` refund to the
limiter that took a charge; a direct `Refund` goes to the current one.

**Throughput.** `Rate.Throughput` and `Counter.Throughput` report bytes per
second over the last sampling window of at least one second, so they lag by up
to about two seconds. Refunds are taken off a `Rate`'s throughput.

## Writing a Limiter

Most code combines the limiters of this package. A `Limiter` of your own needs
four methods: `Wait` charges bytes before they are sent and blocks until they
may go, or returns an error, keeping no charge, to refuse them; `Refund` gives
bytes back; `Ready` blocks until the limit is ready and says how many bytes
may pass, without charging; and `Charge` records bytes already transferred.
The wrappers use `Ready` to size each `Read` and `Write`.

## Performance

Charges are lock-free; a refund takes the `Rate`'s lock briefly. A throttled
connection reuses one timer per direction and does not allocate per `Read` or
`Write` for hierarchies of up to eight levels. `Wait` and `Ready` called
directly make a timer for each wait that needs one. Waits of up to 200 µs are
accumulated as debt rather than slept. Callers waiting on a shared `Rate` are
spread over several channels, so that many throttled connections do not
contend on one.

`Counter.Add` is an atomic add per direction and does not read the clock; a
shared `Counter` spreads the adds of many goroutines over counters per
processor. An unlimited `Rate` takes memory for its meter once something is
charged to it, so for levels that only count bytes, such as one per tunnel,
prefer a `Counter`.

Wrapping a connection hides the zero-copy `ReadFrom` and `WriteTo` of
`*net.TCPConn`, so wrap only connections that need a limit or a count.

## Testing

`Rate`, `Budget` and `Counter` work inside `testing/synctest` bubbles, on the
bubble's clock. Create them inside the bubble that uses them.
