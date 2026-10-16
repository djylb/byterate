package byterate

import "context"

// Limiter charges and refunds byte budgets. *Rate, *Hierarchy, *Budget,
// *Switch and the views of a Counter implement it, and the connection
// wrappers take any implementation.
// Most code combines those and never implements Limiter itself.
//
// A transfer is charged one of two ways. Wait charges the bytes before they
// are sent and blocks until the limit allows them, as a Write does. Ready and
// Charge instead charge them after: Ready blocks until the limit is ready and
// says how many bytes may pass, without charging, and Charge records the
// bytes that did, as debt that the next Ready waits for, as a Read does.
// Ready and Charge have the signatures of the hooks that a relay calls
// before each read and after each write.
type Limiter interface {
	// Wait charges n bytes and blocks until the limit allows them, or until
	// ctx is done, when it returns ctx.Err() and keeps the charge. A limit
	// that refuses the bytes, such as an exhausted Budget, returns its error
	// and keeps no charge. The connection wrappers pass a context that Close
	// and the deadlines end.
	Wait(ctx context.Context, n int64) error
	// Refund gives back n bytes charged by Wait or Charge that were not
	// transferred.
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
	_ Limiter = (*Hierarchy)(nil)
	_ Limiter = (*Budget)(nil)
	_ Limiter = (*Switch)(nil)
)
