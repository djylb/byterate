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
limitedConn := byterate.NewRateConn(conn, limiter)

meter := byterate.NewMeter()
meter.Add(readBytes, writtenBytes)
inBps, outBps, totalBps := meter.Snapshot()
```
