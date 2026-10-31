# byterate

Byte rate limiting, byte budgets, traffic counting and throughput for Go
connections and streams.

## Install

```bash
go get github.com/djylb/byterate
```

## Usage

Count a connection's bytes, or limit it as well; the result is still a
`net.Conn`:

```go
counted := byterate.NewConn(conn)                                         // counts only
limited := byterate.NewConn(conn, byterate.WithLimiter(byterate.NewRate(1<<20))) // 1 MiB/s both ways

read, written := limited.Meter().Total()
```

A connection without a limit passes its reads and writes straight through
and only counts them. `SetLimiters` adds, changes or lifts a limit at any
time, also for a transfer in progress, whose wait starts over on the new
limit:

```go
counted.SetLimiters(nil, byterate.NewRate(512<<10)) // writes at 512 KiB/s from now on
counted.SetLimiters(nil, nil)                         // unlimited again
```

Nest limits, such as a connection's within its user's within a global one.
A limit changed later applies to open connections too:

```go
global := byterate.NewRate(0)      // unlimited for now
user := byterate.NewRate(10 << 20) // 10 MiB/s across the user's connections

c := byterate.NewConn(conn, byterate.WithLimiter(byterate.NewHierarchy(byterate.NewRate(2<<20), user, global)))
user.SetLimit(20 << 20)
```

Limit downloads and uploads apart:

```go
c := byterate.NewConn(conn, byterate.WithLimiters(byterate.NewRate(8<<20), byterate.NewRate(1<<20)))
```

`NewStream` does the same for an `io.ReadWriteCloser`, `NewReader` and
`NewWriter` for a plain `io.Reader` or `io.Writer`, whose `Close` ends a
wait, and `NewPacketConn` for a `net.PacketConn`. The zero value of `Rate` is
ready to use, unlimited until `SetLimit`.

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
readBps, writtenBps := user.Traffic().Throughput()
tunnel.SetParent(otherUser)            // move the tunnel, its connections with it
```

`OpenPacketConn` and `OpenStream` do the same as `NewPacketConn` and
`NewStream`. The options of the wrappers apply to `Open` too:

```go
c, err := tunnel.Open(conn,
	byterate.WithLimiters(nil, byterate.NewRate(1<<20)), // a limit of the connection's own
	byterate.WithCounter(&perConn),                       // also counted here
	byterate.WithWait(ctx))                               // wait for a slot instead of refusing
```

`errors.Is(err, byterate.ErrTooManyConns)` matches a refusal, and
`errors.As` gives the `*TooManyConnsError`, whose `Group` is the level that
refused. `Acquire`, `AcquireContext` and `Release` take, wait for and give
back a slot alone, and `Reads` and `Writes` return the limiters of a group's
reads and writes, for relays that call hooks per read and write. A level's
limiters can be any limiter, such as a `Budget` or a `NewHierarchy` of
several.

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

c := byterate.NewConn(conn, byterate.WithLimiter(byterate.NewHierarchy(user, daily)))

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

A wrapper leases a Budget's bytes ahead instead of charging it at every
read and write, so a quota costs no more than counting. Code that passes
bytes itself can lease too:

```go
lease, err := daily.Lease(1 << 20) // up to 1 MiB, ErrBudgetExhausted once stopped
// pass up to lease.Bytes bytes
daily.Settle(lease, sent)          // give back what was not used
```

**Counting.** Every wrapper counts its bytes in its `Meter`. A `Counter`
adds up bytes: those of `Add`, and those of the Meters attached to it, by
`WithCounter` or `Attach`, which cost the connections' reads and writes
nothing, since the Counter adds them up when read; a wrapper stays attached,
and so referenced, until its `Close`. `Swap` takes the totals
since the last `Swap`, so that each byte is reported once, and `Total` the
running totals, to persist or report as snapshots; `Restore` adds persisted
totals back after a restart and `Set` replaces the totals, neither counting
as traffic:

```go
var traffic byterate.Counter // shared by many connections
c := byterate.NewConn(conn, byterate.WithCounter(&traffic))

read, written := traffic.Swap()
```

**Throughput.** `Throughput` reports a Counter's bytes per second of the last
whole second. Counting never reads the clock: the totals are sampled once a
second of the Counter's clock has passed, by `Throughput` itself or by
`Sample`. For the throughput of every second, call `Sample` once a second,
such as from one ticker for all Counters; read more rarely, `Throughput`
reports the average since the last sample:

```go
for range time.Tick(time.Second) {
	for _, c := range counters {
		c.Sample()
	}
}
readBps, writtenBps := traffic.Throughput()
```

`SetClock` gives a Counter, and a Budget for its expiry, a clock of the
caller's, such as a fake one in tests: any type with `Now() time.Time`.

**Switching.** A `Switch` moves the traffic of every connection it limits to
another limiter at once, while they run:

```go
sw := byterate.NewSwitch(oldUser)
c := byterate.NewConn(conn, byterate.WithLimiter(sw))
sw.Set(newUser) // waits in progress start over on newUser
```

**Relays.** A relay that calls a hook before each read and after each write
can use any limiter's `Ready` and `Charge`: `Ready` blocks until the limit is
ready and says how many bytes may pass, and `Charge` records the bytes that
did. A `Group`'s `Reads` and `Writes` fit both, and count the bytes in its
traffic; a `Counter`'s `Add` counts them alone:

```go
type hooks struct {
	Wait func(ctx context.Context, max int64) (int64, error)
	Done func(n int64) error
}

l := byterate.NewHierarchy(connRate, user, budget)
upload := hooks{Wait: l.Ready, Done: l.Charge}
download := hooks{Wait: tunnel.Reads().Ready, Done: tunnel.Reads().Charge}
```

**Datagrams.** UDP, IP and Unix datagram sockets are never split: each
message is charged and sent whole. `WithDatagram` does the same for other
connections that carry one message per `Read` and `Write`. `NewPacketConn`
charges each `ReadFrom` and `WriteTo` of a `net.PacketConn` whole. With
`WithDrop`, it drops the datagrams that would wait instead, as a UDP relay
does: such a datagram is not charged, `WriteTo` reports it sent and
`ReadFrom` reads on to the next one; a refusal such as `ErrBudgetExhausted`
is still returned:

```go
pc := byterate.NewPacketConn(udp, byterate.WithLimiter(limit), byterate.WithDrop())
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

**The fast path.** A direction of a wrapper with no limiter, or none but
Budgets with leases left, reads and writes without charging anything. Which
path a read or write takes is chosen anew each time, and both give the same
results: the same bytes counted, the same limits and budgets kept.

**Limits.** Limits are in bytes per second; `0` or less means unlimited. After
an idle period a `Rate` lets a burst through at once: two seconds' worth, or
what `SetBurst` sets; a smaller burst smooths traffic out.
`SetLimit` takes effect from the next charge and keeps what is owed: bytes
charged at the old limit are repaid at the new one. `Reset` instead wakes
blocked callers, drops the debt and starts over with a full burst. An
unlimited `Rate` in a wrapper's levels still costs a charge per read and
write, as it can be limited later; leave it out, or use `SetLimiters`, for
connections that should be counted alone.

**Disable is not a block.** `Disable` turns a `Rate`'s limit off until
`Enable`: traffic charged to it passes without limit and blocked callers are
woken. To cut traffic, close the connections.

**Waits are not first come, first served.** A caller that a limit makes wait
sleeps until the debt its own charge added is repaid, so callers sharing a
limit get their share in grants, but are not woken in the order they came.
`AcquireContext` wakes the callers waiting for a group's slot in order, one
per freed slot, but another `Acquire` can take the slot first.

**Connections pass data in grants.** A grant is what the limit lets through at
once: its credit, or without credit about 20 ms of traffic.

- A `Write` sends its bytes a grant at a time, so a large `Write` neither
  bursts past the limit nor holds a shared limit while other connections wait.
  Concurrent limited `Write`s on one connection do not interleave.
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
once. Pass `nil` for levels that should not limit.

**Refusals.** A level that refuses bytes, such as a stopped `Budget`, is asked
first and refuses them for the whole hierarchy: the other levels neither
take them nor make the caller wait. `Charge`, which records bytes already
transferred, keeps them on every level and returns the refusal.

**Budgets.** A `Budget` never blocks by itself; the `Limit` of its step does.
Once the usage reaches a `Stop` step, it refuses everything until the next
period. Neither writes nor reads take the usage past it: a `Write` sends
what fits and fails, a `Read` reads no more than is left and the next one
fails; a datagram, read whole, can pass it by its size. Steps apply from their `After`. The bytes used count everything
charged to the `Budget`, whichever step applied, and a step's `Limit` only
the bytes of its step; a `Limit` adds to the other limits, so it can only
slow traffic down. For steps that also speed it up, leave the base speed out
of the hierarchy and give the first step, at `After: 0`, a `Limit` as well.

**Leases.** A wrapper leases about half of what a Budget has left, between
16 KiB and 1 MiB, never past the next step or the stop, and passes that
many bytes without asking the Budget again; while a step's `Limit` applies,
it charges the Budget at every read and write instead. Leased bytes count as
used when leased, so connections that share a Budget never pass its stop
together, and `Used` and `Remaining` leave out what the wrappers' leases hold
unused, so they report the bytes that passed. Bounds of what leasing can
change: `OnStep` reports a step when a lease reaches it, at most one lease
before its bytes pass; concurrent reads or writes on one direction of one
connection can each pass the lease by their own size, which is then counted
as used; a lease taken before a `Reset` passes its rest in no period, at most
one lease per direction; and a change of `SetSteps` applies from each
connection's next lease. `Close` gives back what a lease did not use.

`Reset`, `SetUsed` and `SetSteps` take effect from the next read or write,
without ending connections; a slowed connection gets its full speed back
from its next grant. `SetUsed` takes back persisted usage after a restart,
or a total that several budgets share, and `SetExpiry` stops all bytes after
a time. `Reset` drops the usage of the period, so keep what is billed in a
`Counter`.

**Groups.** Every read and write is limited by the limits of the group and
each ancestor at that moment, and counted in each one's `Traffic`. After
`SetParent`, a moved group's connections count in the new ancestors, their
slots too, even past a cap, which then refuses new connections until enough
close; the bytes counted before stay with the old ancestors, and a wait in
progress starts over on the new ones. `SetLimiters` applies to the open
connections at once. `SetMaxConns` limits the connections of a level and
everything below it. `Acquire` takes a slot at every level or none. A
limiter given to several levels is charged once.

**Switching.** After `Switch.Set`, new charges go to the new limiter, and a
wait in progress on a `Rate` of the old one ends, is refunded there, and
starts over on the new one. A wrapper's `SetLimiters` does the same.

**Refunds.** The wrappers, hierarchies, switches, budgets and groups keep a
receipt of each charge and refund exactly what it took: a `Rate` only within
the generation it charged, a `Budget` within the period, also inside a
`Switch` or a budget step. A direct `Refund(n)` has no receipt: it gives the
bytes back to the limiter as it is now, so call it only for bytes the same
limiter charged since its last `Enable`, `Reset`, `SetUsed` or `Set`, such as
the unsent part of a `Wait` of your own.

**Throughput.** `Counter.Throughput` reports the bytes of the last whole
second, in one-second windows of the Counter's clock, the same for every
reader, so it lags by one to two seconds. The bytes between two samples
count evenly over the seconds between them. Refunds are taken off, those of
an earlier window off the following ones; `Restore` and `Set` are not
traffic.

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

A read or write on the fast path costs an atomic load and an uncontended
atomic add on the connection's own cache line; it reads no clock and writes
nothing that other connections share, so it does not slow down with the
number of processors. A Counter or Group adds up its Meters when read, at a
cost per attached connection.

Charges are lock-free; a refund takes the `Rate`'s lock briefly. A throttled
connection reuses one timer per direction and does not allocate per `Read` or
`Write` while a write charges up to eight limiters, a `Budget` and the
limiter of its step counting as two. `Wait` and `Ready` called directly make
a timer for each wait that needs one. Waits of up to 200 µs are accumulated
as debt rather than slept. Callers waiting on a shared `Rate` are spread over
several channels, so that many throttled connections do not contend on one.
`Counter.Add` is an atomic add per direction; a shared `Counter` spreads the
adds of many goroutines over counters per processor.

`io.Copy` to and from a wrapper keeps its zero-copy paths: the wrapper hands
the connection, or the one at the other end, a chunk of at most 256 KiB at a
time as an `io.LimitedReader`, within the limit and the leases, so
`sendfile` from files and, on Linux, `splice` between sockets still move the
data; a change of limit applies from the next chunk. Other copies go through
`Read` and `Write`.

## Testing

`Rate`, `Budget` and `Counter` work inside `testing/synctest` bubbles, on the
bubble's clock. Create them inside the bubble that uses them. A `Counter`
and a `Budget` also take a clock of their own with `SetClock`.

## Changes from earlier versions

- `NewDuplexConn`, `NewDatagramConn`, `NewDuplexDatagramConn`,
  `NewDuplexPacketConn`, `NewReadWriteCloser` and `NewDuplexReadWriteCloser`
  became options of `NewConn`, `NewPacketConn` and `NewStream`:
  `WithLimiters`, `WithDatagram`. The wrappers always wrap, so that they
  count, and return their own types, with `SetLimiters` and `Meter`.
- `Group.OpenDatagram` and `OpenReadWriteCloser` are `Open` with
  `WithDatagram` and `OpenStream`; `SetReadLimiter`, `SetWriteLimiter`,
  `ReadLimiter` and `WriteLimiter` are `SetLimiters` and `Limiters`.
- `Rate.Throughput` is gone: throughput is a Counter's, which a connection
  feeds through `WithCounter` and a Group through its `Traffic`, without a
  charge per read and write.
- `Counter.Reads` and `Counter.Writes`, views that counted through the
  limiter path, are superseded by `WithCounter` and `Attach` for wrappers and
  `Add` for code of one's own.
