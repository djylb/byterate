# AGENTS.md

- Before you finish, `gofmt -l .` must print nothing and `go vet ./...`,
  `go test -race -shuffle=on ./...`, `GOARCH=386 go test ./...` and the
  js/wasm tests must pass.
- Standard library only, also for tests.
- Charge paths (Limiter methods, wrapper reads and writes) stay
  allocation-free and lock-free.
- A wrapper direction with nothing to charge reads no clock and writes
  nothing shared: one atomic load and its own Meter's add. The fast and the
  limited paths must give the same results; the feature-combination tests
  cover them.
- Wrappers never let I/O bypass the limiter or the leases: no raw socket
  access, zero-copy forwarding only in granted or leased chunks, and
  datagrams are never split.
- Refunds go through the receipt of the charge and never reach a Rate that
  was reset since.
- A Limiter must never charge itself, directly or through other Limiters.
- User callbacks and other Limiters never run under a lock.
- Measure time with the monotonic clock, never wall-clock time.
- Tests must not leak goroutines or timers.
- Commit subjects are short, English and imperative.
- Never commit host names, IP addresses or credentials.
