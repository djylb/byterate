package byterate

import (
	"context"
	"errors"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// Generation states. A generation ends once, by a deadline or by Close.
const (
	genOpen uint32 = iota
	genExpired
	genClosed
)

// waitGen is one generation of a direction's wait signal and the context a wait
// passes to its Limiter's Wait. Its Done channel is made on first use, since
// most waits need none, and never changes; Err is non-nil once it is closed, as
// context.Context requires.
type waitGen struct {
	done  atomic.Value // chan struct{}, set by Done or end
	state atomic.Uint32
}

var _ context.Context = (*waitGen)(nil)

// closedChan is the Done channel of a generation that ended before any wait
// asked for one.
var closedChan = func() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}()

// closedGen is the generation of every closed connection whose own
// generation a deadline had already ended.
var closedGen = func() *waitGen {
	g := &waitGen{}
	g.end(genClosed)
	return g
}()

func (g *waitGen) Deadline() (time.Time, bool) { return time.Time{}, false }

func (g *waitGen) Done() <-chan struct{} {
	if ch, ok := g.done.Load().(chan struct{}); ok {
		return ch
	}
	g.done.CompareAndSwap(nil, make(chan struct{})) // loses only to end or another Done
	return g.done.Load().(chan struct{})
}

func (g *waitGen) Err() error {
	var err error
	switch g.state.Load() {
	case genOpen:
		return nil
	case genExpired:
		err = context.DeadlineExceeded
	default:
		err = context.Canceled
	}
	<-g.Done() // end sets the state before it closes the channel
	return err
}

func (g *waitGen) Value(any) any { return nil }

// end ends g with state genExpired or genClosed, unless it has ended.
func (g *waitGen) end(state uint32) {
	if !g.state.CompareAndSwap(genOpen, state) {
		return
	}
	if !g.done.CompareAndSwap(nil, closedChan) {
		close(g.done.Load().(chan struct{}))
	}
}

// causeOf returns g's cause for an error that g's end produced, through a
// wait on g as a context, and any other error as it is.
func (g *waitGen) causeOf(err error) error {
	if err != nil && g.ended() && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
		return g.cause()
	}
	return err
}

func (g *waitGen) ended() bool { return g.state.Load() != genOpen }

// cause returns the error for a wait that g ended.
func (g *waitGen) cause() error {
	if g.state.Load() == genClosed {
		return net.ErrClosed
	}
	return os.ErrDeadlineExceeded
}

// waitCancel is a direction's wait signal. Its current generation is ended
// when the connection is closed or the direction's deadline passes. A
// deadline that has passed and is moved later or cleared starts a new
// generation, as net.Pipe does, and waits on the old one stay ended.
type waitCancel struct {
	gen atomic.Pointer[waitGen] // nil until init

	mu     sync.Mutex
	closed bool           // the connection is closed
	timer  *deadlineTimer // made by the first deadline in the future, then reused
}

// deadlineTimer ends a generation at its deadline. It refers to the
// generation alone, so a pending deadline keeps no connection reachable. A
// deadline moved later, as servers do before every read, leaves the runtime
// timer alone: it fires at the earlier time and sets itself again.
type deadlineTimer struct {
	mu      sync.Mutex
	t       *time.Timer
	gen     *waitGen  // the generation to end
	at      time.Time // the deadline, zero for none
	firesAt time.Time // the deadline t is set for, zero once it has fired
}

func newDeadlineTimer(g *waitGen, at time.Time, d time.Duration) *deadlineTimer {
	dt := &deadlineTimer{gen: g, at: at, firesAt: at}
	dt.mu.Lock() // fire may run before t is set
	dt.t = time.AfterFunc(d, dt.fire)
	dt.mu.Unlock()
	return dt
}

// stop disarms dt for good.
func (dt *deadlineTimer) stop() {
	dt.mu.Lock()
	defer dt.mu.Unlock()
	dt.at, dt.firesAt = time.Time{}, time.Time{}
	dt.t.Stop()
}

func (dt *deadlineTimer) fire() {
	dt.mu.Lock()
	defer dt.mu.Unlock()
	dt.firesAt = time.Time{}
	if dt.at.IsZero() {
		return
	}
	if d := time.Until(dt.at); d > 0 {
		dt.schedule(d) // the deadline was moved later
		return
	}
	dt.gen.end(genExpired)
}

// set makes dt end g at at, which is d from now. dt.mu must be held.
func (dt *deadlineTimer) set(g *waitGen, at time.Time, d time.Duration) {
	dt.gen, dt.at = g, at
	if dt.firesAt.IsZero() || dt.firesAt.After(at) {
		dt.schedule(d)
	}
}

// schedule sets t to fire after d, at dt.at. dt.mu must be held.
func (dt *deadlineTimer) schedule(d time.Duration) {
	dt.firesAt = dt.at
	dt.t.Reset(d)
}

func (w *waitCancel) init() { w.gen.Store(&waitGen{}) }

// current returns the generation a new wait uses.
func (w *waitCancel) current() *waitGen { return w.gen.Load() }

// setDeadline sets the time at which the current generation ends; a zero t
// means never.
func (w *waitCancel) setDeadline(t time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return
	}
	dt := w.timer
	if dt != nil {
		dt.mu.Lock() // keeps fire from ending g while it is replaced
		defer dt.mu.Unlock()
		dt.at = time.Time{} // set again below for a deadline in the future
	}

	g := w.gen.Load()
	var d time.Duration
	if !t.IsZero() {
		now := time.Now()
		if d = t.Sub(now); d <= 0 {
			g.end(genExpired)
			return
		}
		// On the monotonic clock, as the wrapped connection keeps it: a
		// deadline without a monotonic reading would otherwise move with
		// wall-clock steps each time the timer checks it.
		t = now.Add(d)
	}
	if g.ended() {
		g = &waitGen{} // waits on the ended generation stay ended
		w.gen.Store(g)
	}
	switch {
	case t.IsZero():
		if dt != nil {
			dt.t.Stop()
			dt.firesAt = time.Time{}
		}
	case dt == nil:
		w.timer = newDeadlineTimer(g, t, d)
	default:
		dt.set(g, t, d)
	}
}

// close ends the wait signal for good, ending any wait.
func (w *waitCancel) close() {
	w.mu.Lock()
	defer w.mu.Unlock()
	g := w.gen.Load()
	if w.closed || g == nil {
		return
	}
	w.closed = true
	if dt := w.timer; dt != nil {
		dt.stop()
	}
	if g.ended() {
		// Waits on g keep their deadline error; later ones see the close.
		w.gen.Store(closedGen)
		return
	}
	g.end(genClosed)
}
