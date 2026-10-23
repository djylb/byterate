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
`io.ReadWriteCloser`, `NewPacketConn` and `NewDuplexPacketConn` for a
`net.PacketConn`, and `NewReader` and `NewWriter` for a plain `io.Reader` or
`io.Writer`, whose `Close` ends a wait. The zero value of `Rate` is ready to
use, unlimited until `SetLimit`.

A server that limits, counts and caps the connections of many users is
simplest with a `Group` per user under one for the server, below.

## More

**Groups.** A `Group` is a level of a tree, such as a server, a user, a
client and a tunnel. A connection opened in a `Group` is limited by every
level above it, counts in every level's traffic and connections, and each
level can cap its connections:

```go
server := byterate.NewGroup(nil)
user := byterate.NewGroup(server)
tunnel := byterate.NewGroup(user)

user.SetLimiters(byterate.NewRate(10<<20), byterate.NewRate(2<<20)) // reads, writes
user.SetMaxConns(100)

c, err := tunnel.Open(conn) // a *byterate.TooManyConnsError past a cap
if err != nil {
	conn.Close()
	return err
}
defer c.Close() // gives the slot back

user.Conns()                           // connections of the user and below
read, written := user.Traffic().Load() // bytes read and written below the user
tunnel.SetParent(otherUser)            // move the tunnel, its connections with it
```

`OpenDatagram`, `OpenPacketConn` and `OpenReadWriteCloser` do the same as
`NewDuplexDatagramConn`, `NewDuplexPacketConn` and `NewDuplexReadWriteCloser`.
Options change how a connection opens:

```go
c, err := tunnel.Open(conn,
	byterate.WithLimiters(nil, byterate.NewRate(1<<20)), // a limit of the connection's own
	byterate.WithWait(ctx))                              // wait for a slot instead of refusing
```

`errors.Is(err, byterate.ErrTooManyConns)` matches a refusal, and
`errors.As` gives the `*TooManyConnsError`, whose `Group` is the level that
refused. `Acquire`, `AcquireContext` and `Release` take, wait for and give
back a slot alone, and `Reads` and `Writes` return the limiters of a group's
reads and writes, for other wrappers or relays. A level's limiters can be any
limiter, such as a `Budget` or a `NewHierarchy` of several; `SetTraffic`
gives a level a `NewSharedCounter` when many connections pass through it at
once, such as the root.

**Budgets.** A `Budget` counts the bytes used in a period and applies steps
as they grow: past each step's `After`, its `Limit` slows the traffic down,
or `Stop` ends it with `ErrBudgetExhausted`:

```go
daily := byterate.NewBudget(
	byterate.Step{After: 10 << 30, Limit: byterate.NewRate(1 << 20)},  // past 10 GiB: 1 MiB/s
	byterate.Step{After: 20 << 30, Limit: byterate.NewRate(64 << 10)}, // past 20 GiB: 64 KiB/s
	byterate.Step{After: 30 << 30, Stop: true},                        // past 30 GiB: stop
)
daily.OnStep(func(step int, used int64) { /* notify */ })

c := byterate.NewConn(conn, byterate.NewHierarchy(user, daily))

daily.Reset()        // a new period starts over
daily.SetSteps(...)  // new steps, from the next read or write; Steps() returns them
daily.Used()         // the bytes used in the period
daily.Remaining()    // the bytes left before the stop, 0 once stopped
```

None of these changes ends a connection. Periods are up to the caller, such
as each hour, day or week:

```go
var next func()
next = func() { daily.Reset(); time.AfterFunc(24*time.Hour, next) }
time.AfterFunc(time.Until(midnight), next)
```

**Counting.** A `Counter` adds up the bytes read and written and measures
their throughput. `Swap` takes the totals since the last `Swap`, so that each
byte is reported once, and `Total` the running totals, to persist or report
as snapshots; `Restore` adds persisted totals back after a restart and `Set`
replaces the totals, neither counting as traffic. Its `Reads` and `Writes`
views are limiters that never limit, so a connection can count exactly what
its reads and writes take:

```go
var traffic byterate.Counter // per connection; NewSharedCounter for many goroutines
c := byterate.NewDuplexConn(conn,
	byterate.NewHierarchy(user, traffic.Reads()),
	byterate.NewHierarchy(user, traffic.Writes()))

read, written := traffic.Swap()
readBps, writtenBps := traffic.Throughput()
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
did. A `Counter`'s views fit the hook after a write alone, such as
`traffic.Writes().Charge`, and a `Group`'s `Reads` and `Writes` both:

```go
type hooks struct {
	Wait func(ctx context.Context, max int64) (int64, error)
	Done func(n int64) error
}

l := byterate.NewHierarchy(connRate, user, budget)
upload := hooks{Wait: l.Ready, Done: l.Charge}
download := hooks{Wait: tunnel.Reads().Ready, Done: tunnel.Reads().Charge}
```

**Datagrams.** `NewDatagramConn` and `NewDuplexDatagramConn` wrap a
connection that carries one message per `Read` and `Write`, which is never
split; UDP, IP and Unix datagram sockets are recognized by `NewConn` itself.
`NewPacketConn` charges each `ReadFrom` and `WriteTo` of a `net.PacketConn`
whole. With `WithDrop`, it drops the datagrams that would wait instead, as a
UDP relay does: such a datagram is not charged, `WriteTo` reports it sent and
`ReadFrom` reads on to the next one; a refusal such as `ErrBudgetExhausted`
is still returned:

```go
pc := byterate.NewPacketConn(udp, limit, byterate.WithDrop()) // also Group.OpenPacketConn(udp, byterate.WithDrop())
```

Code that sends datagrams itself asks `TryWait`, which charges the bytes only
if they may pass at once, at every level of a hierarchy or none:

```go
if err := limit.TryWait(int64(len(packet))); err != nil {
	continue // byterate.ErrWouldWait, or a refusal such as ErrBudgetExhausted
}
if _, err := pc.WriteTo(packet, addr); err != nil {
	limit.Refund(int64(len(packet))) // not sent
}
```

**Saving limits.** The package has no JSON form of its own, since what an
application stores, and under which names, is its own format;
`ExampleRate_json` shows a small wrapper that saves `Limit` and restores it
with `NewRate` or `SetLimit`.

**Direct use.** Any limiter can be charged directly: `Wait(ctx, n)` charges
`n` bytes and blocks until the limit allows them, `TryWait(n)` charges them
only if it need not block, and `Refund(n)` gives back bytes that were not
transferred.

## How it behaves

**Limits.** Limits are in bytes per second; `0` or less means unlimited. After
an idle period a `Rate` lets a burst through at once: two seconds' worth, or
what `SetBurst` sets; a smaller burst smooths traffic out.
`SetLimit` takes effect from the next charge and keeps what is owed: bytes
charged at the old limit are repaid at the new one. `Reset` instead wakes
blocked callers, drops the debt and starts over with a full burst.

**Disable is not a block.** `Disable` turns a `Rate`'s limit off until
`Enable`: traffic charged to it passes without limit and blocked callers are
woken. It still meters, as an unlimited `Rate` does. To cut traffic, close
the connections.

**Waits are not first come, first served.** A caller that a limit makes wait
sleeps until the debt its own charge added is repaid, so callers sharing a
limit get their share in grants, but are not woken in the order they came.
`AcquireContext` wakes the callers waiting for a group's slot in order, one
per freed slot, but another `Acquire` can take the slot first.

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

**Nested limits.** `NewHierarchy` charges every level and waits for the
strictest. `Disable` on one level ends the wait it imposes; the others still
apply. Every level is charged when the bytes are, so a looser level counts
bytes still waiting on a stricter one: it keeps its long-run limit, but when
many waits on stricter levels end together, it can pass more than its burst at
once. Pass `nil` for levels that should neither limit nor meter.

**Refusals.** A level that refuses bytes, such as a stopped `Budget`, is asked
first and refuses them for the whole hierarchy: the other levels neither
take them nor make the caller wait. `Charge`, which records bytes already
transferred, keeps them on every level and returns the refusal.

**Budgets.** A `Budget` never blocks by itself; the `Limit` of its step does.
Once the usage reaches a `Stop` step, it refuses everything until the next
period. Writes never take the usage past it: a `Write` sends what fits and
fails. A `Read` charges what it read, so the `Read` that reaches the stop
returns its bytes with `ErrBudgetExhausted`, and concurrent reads can each
pass it by their last grant. Steps apply from their `After`, as reads and writes end
a grant there. The bytes used count everything charged to the `Budget`,
whichever step applied, and a step's `Limit` only the bytes of its step; a
`Limit` adds to the other limits, so it can only slow traffic down. For steps
that also speed it up, leave the base speed out of the hierarchy and give
the first step, at `After: 0`, a `Limit` as well.

`Reset`, `SetUsed` and `SetSteps` take effect from the next read or write,
without ending connections; a slowed connection gets its full speed back
from its next grant. `SetUsed` takes back persisted usage after a restart,
or a total that several budgets share, and `SetExpiry` stops all bytes after
a time. `Reset` drops the usage of the period, so keep what is billed in a
`Counter`.

**Groups.** Every read and write is charged to the limits of the group and
each ancestor at that moment, and counted in each one's `Traffic`. After
`SetParent`, a moved group's connections count in the new ancestors, their
slots too, even past a cap, which then refuses new connections until enough
close; the bytes counted before stay with the old ancestors, and a wait in
progress ends on them. `SetMaxConns` limits the connections of a level and
everything below it. `Acquire` takes a slot at every level or none. A limiter
or `Counter` given to several levels is charged once.

**Switching.** After `Switch.Set`, new charges go to the new limiter, and a
wait in progress on a `Rate` of the old one ends, is refunded there, and
starts over on the new one.

**Refunds.** The wrappers, hierarchies, switches, budgets and groups keep a
receipt of each charge and refund exactly what it took: a `Rate` only within
the generation it charged, a `Budget` within the period, also inside a
`Switch` or a budget step. A direct `Refund(n)` has no receipt: it gives the
bytes back to the limiter as it is now, so call it only for bytes the same
limiter charged since its last `Enable`, `Reset`, `SetUsed` or `Set`, such as
the unsent part of a `Wait` of your own.

**Throughput.** `Rate.Throughput` and `Counter.Throughput` both report the
bytes of the last whole second, in one-second windows of a clock that every
`Rate` and `Counter` shares: the window before the current one, so they lag
by one to two seconds. The windows are the same for every
reader: what is reported depends on when the bytes passed, not on how often
or by how many callers it is read. Refunds are taken off, those of an earlier
window off the following ones. `Counter.Add` does not read the clock, so its
bytes count in the window that the next count through a view, or a
`Throughput` call, closes; the views count with the time of each charge.
Count through the views when a `Counter`'s throughput matters.

## Writing a Limiter

Most code combines the limiters of this package. A `Limiter` of your own needs
five methods: `Wait` charges bytes before they are sent and blocks until they
may go, or returns an error, keeping no charge, to refuse them; `TryWait`
charges them only if they may go at once and otherwise returns
`ErrWouldWait`; `Refund` gives bytes back; `Ready` blocks until the limit is
ready and says how many bytes may pass, without charging; and `Charge`
records bytes already transferred. The wrappers use `Ready` to size each
`Read` and `Write`.

## Performance

Charges are lock-free; a refund takes the `Rate`'s lock briefly. A throttled
connection reuses one timer per direction and does not allocate per `Read` or
`Write` while a write charges up to eight limiters, a `Budget` and the
limiter of its step counting as two and `Counter` views not at all; a
group's levels are rebuilt only after it or an ancestor changes. `Wait` and `Ready` called
directly make a timer for each wait that needs one. Waits of up to 200 µs are
accumulated as debt rather than slept. Callers waiting on a shared `Rate` are
spread over several channels, so that many throttled connections do not
contend on one.

`Counter.Add` is an atomic add per direction and does not read the clock; a
shared `Counter` spreads the adds of many goroutines over counters per
processor. An unlimited `Rate` takes memory for its meter once something is
charged to it, so for levels that only count bytes, such as one per connection,
prefer a `Counter`.

`io.Copy` to and from a wrapped connection keeps its zero-copy paths: the
wrapper hands the connection, or the one at the other end, a grant at a time
as an `io.LimitedReader`, so `sendfile` from files and, on Linux, `splice`
between sockets still move the data within the limit. Other copies go
through `Read` and `Write`.

## Testing

`Rate`, `Budget` and `Counter` work inside `testing/synctest` bubbles, on the
bubble's clock. Create them inside the bubble that uses them.
