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

## Semantics

- Limits are in bytes per second; a limit `<= 0` means unlimited.
- After an idle period a `Rate` lets two seconds' worth of bytes through
  without waiting.
- `NewRate` returns a started `Rate`. The zero value is unlimited until
  `ResetLimit`, or `SetLimit` followed by `Start`.
- `SetLimit` keeps debt accrued at the previous limit and does not wake
  blocked callers. `ResetLimit` (`Stop`, `SetLimit`, `Start`) applies the new
  limit at once.
- `Stop` disables limiting and wakes callers blocked in `Get`.
  `GetContext` also returns early when its context is done; the charge is
  kept, so refund bytes that were not transferred with `ReturnBucket`.
- `NewHierarchicalLimiter` charges every `Rate` and waits for the longest
  delay. Nil rates and rates with no limit are left out; it returns nil when
  none remain.
- `NewRateConn` and `NewRateReadWriteCloser` charge reads after the data
  arrives and writes before sending, refunding short writes. When the limiter
  implements `ContextLimiter` (`*Rate` and `*HierarchicalLimiter` do), `Close`
  wakes a `Read` or `Write` blocked in the limiter, which then returns
  `net.ErrClosed`. A nil limiter returns the connection unchanged.
- `NewRateConn` keeps the `net.Conn` addresses and deadlines, and its
  `RawConn` method returns the wrapped connection, so helpers such as
  `netx.RawConnOf` can unwrap it.
- `Rate.Now` and `Meter.Snapshot` report bytes per second over the last
  sampling window of at least one second.
