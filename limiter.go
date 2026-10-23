package byterate

import (
	"context"
	"errors"
)

// ErrWouldWait is returned by TryWait for bytes that the limit would make
// the caller wait for.
var ErrWouldWait = errors.New("byterate: limit would wait")

// Limiter charges and refunds byte budgets. *Rate, *Budget, *Switch, the
// result of NewHierarchy, the views of a Counter and the Reads and Writes of
// a Group implement it, and the connection wrappers take any implementation.
// Most code combines those and never implements Limiter itself.
//
// A transfer is charged one of three ways. Wait charges the bytes before they
// are sent and blocks until the limit allows them, as a Write does. TryWait
// does the same without blocking: it charges the bytes only if the limit lets
// them through at once, so that a sender of datagrams can drop those it may
// not send yet. Ready and Charge instead charge the bytes after: Ready blocks
// until the limit is ready and says how many bytes may pass, without
// charging, and Charge records the bytes that did, as debt that the next
// Ready waits for, as a Read does. Ready and Charge have the signatures of
// the hooks that a relay calls before each read and after each write.
//
// Callers that a limit makes wait are woken when it allows them, not in the
// order they came: a wait is a sleep until the debt the caller's charge
// added is repaid, and Disable, a Switch's Set and the end of ctx wake it
// early. Callers that share a limit get their share in grants, so none
// waits for long, but none is served first by right either.
//
// A charge that was not transferred, wholly or in part, is given back with
// Refund. Refund takes no receipt, so it gives back bytes to the limit as it
// is now: refund only bytes that the same Limiter charged, before any change
// that voids charges, such as a Rate's Enable or Reset, a Budget's Reset or
// SetUsed, or a Switch's Set. The connection wrappers, hierarchies,
// Switches, Budgets and Groups of this package refund their own charges
// themselves, each to the Limiter and the generation, period or step it
// charged, and leave out the charges that such a change voided.
type Limiter interface {
	// Wait charges n bytes and blocks until the limit allows them, or until
	// ctx is done, when it returns ctx.Err() and keeps the charge. A limit
	// that refuses the bytes, such as an exhausted Budget, returns its error
	// and keeps no charge. The connection wrappers pass a context that Close
	// and the deadlines end.
	Wait(ctx context.Context, n int64) error
	// TryWait charges n bytes if the limit lets them through without
	// waiting, as Wait would, and returns nil. Otherwise it charges nothing
	// and returns ErrWouldWait, or the error of a limit that refuses the
	// bytes, such as ErrBudgetExhausted.
	TryWait(n int64) error
	// Refund gives back n bytes charged by Wait, TryWait or Charge that
	// were not transferred.
	Refund(n int64)
	// Ready blocks until the limit is ready for a transfer, or until ctx is
	// done, and returns how many of n bytes it may take, at least 1 for a
	// positive n. It charges nothing. A limit that refuses any more
	// bytes returns its error.
	Ready(ctx context.Context, n int64) (int64, error)
	// Charge records n transferred bytes without waiting, as debt that the
	// next Ready waits for. The bytes stay charged even when it returns an
	// error, which says that the limit refuses further bytes.
	Charge(n int64) error
}

var (
	_ Limiter = (*Rate)(nil)
	_ Limiter = (*hierarchy)(nil)
	_ Limiter = (*Budget)(nil)
	_ Limiter = (*Switch)(nil)
)
