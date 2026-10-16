package byterate

import "context"

// Limiter charges and refunds byte budgets. *Rate and *Hierarchy implement
// it, and the connection wrappers take any implementation.
type Limiter interface {
	// Wait charges n bytes and blocks until the limit allows them, or until
	// ctx is done, when it returns ctx.Err() and keeps the charge. The
	// connection wrappers pass a context that Close and the deadlines end,
	// and return any other error as it is.
	Wait(ctx context.Context, n int64) error
	// Refund gives back n bytes charged by Wait that were not transferred.
	Refund(n int64)
}

var (
	_ Limiter = (*Rate)(nil)
	_ Limiter = (*Hierarchy)(nil)
)
