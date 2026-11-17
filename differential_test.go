package byterate

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

var updateDifferential = flag.Bool("update-differential", false, "rewrite testdata/differential.golden")

// TestDifferential runs one scenario per feature in a testing/synctest
// bubble and compares what it observes, the waits included, with
// testdata/differential.golden, recorded before the scale rework: the
// rework must not change what any feature does.
func TestDifferential(t *testing.T) {
	var out strings.Builder
	for _, sc := range differentialScenarios {
		synctest.Test(t, func(t *testing.T) {
			rec := &diffRecorder{b: &out, start: time.Now()}
			fmt.Fprintf(&out, "== %s\n", sc.name)
			sc.run(rec)
		})
	}
	const path = "testdata/differential.golden"
	if *updateDifferential {
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(out.String()), 0o600); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := out.String(); got != string(want) {
		gl, wl := strings.Split(got, "\n"), strings.Split(string(want), "\n")
		for i := range max(len(gl), len(wl)) {
			var g, w string
			if i < len(gl) {
				g = gl[i]
			}
			if i < len(wl) {
				w = wl[i]
			}
			if g != w {
				t.Errorf("line %d:\n got  %q\n want %q", i+1, g, w)
			}
		}
	}
}

type diffRecorder struct {
	b     *strings.Builder
	start time.Time
}

// log records a line with the bubble time since the scenario began.
func (r *diffRecorder) log(format string, args ...any) {
	fmt.Fprintf(r.b, "%8v  %s\n", time.Since(r.start), fmt.Sprintf(format, args...))
}

func errStr(err error) string {
	if err == nil {
		return "ok"
	}
	return err.Error()
}

var differentialScenarios = []struct {
	name string
	run  func(r *diffRecorder)
}{
	{"rate gcra", func(r *diffRecorder) {
		rt := NewRate(1000)
		for _, n := range []int64{500, 1500, 700, 3000, 10} {
			r.log("wait %d %s", n, errStr(rt.Wait(bg, n)))
		}
		r.log("trywait 100 %s", errStr(rt.TryWait(100)))
		time.Sleep(3 * time.Second)
		r.log("trywait 100 %s", errStr(rt.TryWait(100)))
		rt.Refund(1000)
		r.log("refund; wait 2500 %s", errStr(rt.Wait(bg, 2500)))
		rt.SetLimit(4000)
		r.log("setlimit; wait 8000 %s", errStr(rt.Wait(bg, 8000)))
		n, err := rt.Acquire(bg, 100, 5000)
		r.log("acquire %d %s", n, errStr(err))
		n, err = rt.TryAcquire(1, 5000)
		r.log("tryacquire %d %s", n, errStr(err))
		rt.SetBurst(100 * time.Millisecond)
		r.log("burst %v; wait 1000 %s", rt.Burst(), errStr(rt.Wait(bg, 1000)))
	}},
	{"hierarchy", func(r *diffRecorder) {
		a, b := NewRate(1000), NewRate(4000)
		h := NewHierarchy(a, b)
		for _, n := range []int64{3000, 1000, 500} {
			r.log("wait %d %s", n, errStr(h.Wait(bg, n)))
		}
		r.log("trywait %s", errStr(h.TryWait(100)))
		h.Refund(500)
		r.log("refund; wait 600 %s", errStr(h.Wait(bg, 600)))
		bud := NewBudget(Step{After: 2000, Stop: true})
		h2 := NewHierarchy(a, bud)
		r.log("budget wait 1500 %s used=%d", errStr(h2.Wait(bg, 1500)), bud.Used())
		r.log("budget wait 1500 %s used=%d", errStr(h2.Wait(bg, 1500)), bud.Used())
		ctx, cancel := context.WithTimeout(bg, 200*time.Millisecond)
		r.log("canceled wait %s", errStr(NewHierarchy(a, b).Wait(ctx, 5000)))
		cancel()
		r.log("after cancel wait 100 %s", errStr(a.Wait(bg, 100)))
	}},
	{"budget steps", func(r *diffRecorder) {
		slow := NewRate(1000)
		bud := NewBudget(Step{After: 10000, Limit: slow}, Step{After: 20000, Stop: true})
		var steps []string
		bud.OnStep(func(step int, used int64) { steps = append(steps, fmt.Sprintf("%d@%d", step, used)) })
		for range 4 {
			err := bud.Wait(bg, 6000)
			r.log("wait 6000 %s used=%d step=%d remaining=%d", errStr(err), bud.Used(), bud.Step(), bud.Remaining())
		}
		r.log("charge %s used=%d", errStr(bud.Charge(5000)), bud.Used())
		bud.Refund(3000)
		r.log("refund used=%d", bud.Used())
		bud.Reset()
		r.log("reset used=%d step=%d", bud.Used(), bud.Step())
		l, err := bud.Lease(4000)
		r.log("lease %d %s used=%d", l.Bytes, errStr(err), bud.Used())
		bud.Settle(l, 1000)
		r.log("settle used=%d", bud.Used())
		bud.SetSteps(Step{After: 500, Stop: true})
		r.log("setsteps trywait %s used=%d", errStr(bud.TryWait(1000)), bud.Used())
		bud.SetUsed(100)
		r.log("setused used=%d remaining=%d", bud.Used(), bud.Remaining())
		r.log("steps %v", steps)
	}},
	{"budget leases on conns", func(r *diffRecorder) {
		bud := NewBudget(Step{After: 100 << 10, Stop: true})
		c1 := NewConn(&nopNetConn{}, WithLimiter(bud))
		c2 := NewConn(&nopNetConn{}, WithLimiter(bud))
		buf := make([]byte, 10<<10)
		for i := range 6 {
			n1, e1 := c1.Write(buf)
			n2, e2 := c2.Read(buf)
			r.log("round %d: write %d %s read %d %s used=%d remaining=%d", i, n1, errStr(e1), n2, errStr(e2), bud.Used(), bud.Remaining())
		}
		_ = c1.Close()
		r.log("close c1 used=%d", bud.Used())
		var total int
		for {
			n, err := c2.Write(buf)
			total += n
			if err != nil {
				r.log("c2 wrote %d then %s used=%d", total, errStr(err), bud.Used())
				break
			}
		}
		_ = c2.Close()
		r.log("closed used=%d", bud.Used())
	}},
	{"switch", func(r *diffRecorder) {
		slow, fast := NewRate(1000), NewRate(100000)
		sw := NewSwitch(slow)
		c := NewConn(&nopNetConn{}, WithLimiter(sw))
		buf := make([]byte, 2000)
		_, _ = c.Write(buf)
		r.log("first write")
		var wg sync.WaitGroup
		wg.Go(func() {
			n, err := c.Write(make([]byte, 5000))
			r.log("long write %d %s", n, errStr(err))
		})
		time.Sleep(500 * time.Millisecond)
		sw.Set(fast)
		wg.Wait()
		r.log("switched; write %s", errStr(sw.Wait(bg, 50000)))
		sw.Set(nil)
		n, err := c.Write(make([]byte, 1<<20))
		r.log("unlimited write %d %s", n, errStr(err))
		_ = c.Close()
	}},
	{"group tree", func(r *diffRecorder) {
		root := NewGroup(nil)
		user := NewGroup(root)
		tun := NewGroup(user)
		other := NewGroup(root)
		setWrite(user, NewRate(10000))
		c1, err := tun.Open(&nopNetConn{})
		r.log("open %s", errStr(err))
		c2, _ := user.Open(&nopNetConn{})
		c3, _ := other.Open(&nopNetConn{})
		buf := make([]byte, 5000)
		for range 4 {
			_, _ = c1.Write(buf)
		}
		r.log("c1 wrote 20000")
		_, _ = c2.Read(buf)
		_, _ = c3.Write(buf)
		_ = tun.Writes().Charge(1000)
		_ = tun.Reads().Charge(300)
		logGroups := func(what string) {
			for _, g := range []struct {
				n string
				g *Group
			}{{"root", root}, {"user", user}, {"tun", tun}, {"other", other}} {
				rd, wr := g.g.Traffic().Total()
				r.log("%s %s: traffic %d/%d conns %d", what, g.n, rd, wr, g.g.Conns())
			}
		}
		logGroups("before move")
		r.log("setparent %s", errStr(tun.SetParent(other)))
		r.log("cycle %s", errStr(root.SetParent(tun)))
		setWrite(other, NewRate(2000))
		_, _ = c1.Write(buf)
		r.log("c1 wrote 5000 under other")
		_ = tun.Writes().Charge(700)
		logGroups("after move")
		nc := NewSharedCounter()
		user.SetTraffic(nc)
		_, _ = c2.Write(buf)
		logGroups("after settraffic")
		_ = c1.Close()
		_ = c2.Close()
		_ = c3.Close()
		logGroups("closed")
		setWrite(root, NewRate(1000))
		c4, _ := tun.Open(&nopNetConn{})
		_, _ = c4.Write(buf)
		_, _ = c4.Write(buf)
		r.log("c4 under root rate")
		setWrite(root, nil)
		_, _ = c4.Write(buf)
		r.log("root rate removed")
		_ = c4.Close()
		logGroups("end")
	}},
	{"group budget leases", func(r *diffRecorder) {
		root := NewGroup(nil)
		bud := NewBudget(Step{After: 50 << 10, Stop: true})
		root.SetLimiters(bud, bud)
		c, _ := NewGroup(root).Open(&nopNetConn{})
		buf := make([]byte, 8<<10)
		var total int
		for {
			n, err := c.Write(buf)
			total += n
			if err != nil {
				r.log("wrote %d then %s used=%d", total, errStr(err), bud.Used())
				break
			}
		}
		root.SetLimiters(nil, nil)
		n, err := c.Write(buf)
		r.log("after removing budget write %d %s used=%d", n, errStr(err), bud.Used())
		_ = c.Close()
		r.log("closed used=%d", bud.Used())
	}},
	{"group slots", func(r *diffRecorder) {
		root := NewGroup(nil)
		root.SetMaxConns(2)
		a, b := NewGroup(root), NewGroup(root)
		b.SetMaxConns(1)
		r.log("a %s", errStr(a.Acquire()))
		r.log("b %s", errStr(b.Acquire()))
		err := a.Acquire()
		var tm *TooManyConnsError
		if e, ok := err.(*TooManyConnsError); ok {
			tm = e
		}
		r.log("a again %s refused-by-root=%v", errStr(err), tm != nil && tm.Group() == root)
		var wg sync.WaitGroup
		var order []string
		var mu sync.Mutex
		for i := range 3 {
			wg.Go(func() {
				err := a.AcquireContext(bg)
				mu.Lock()
				order = append(order, fmt.Sprintf("w%d:%s", i, errStr(err)))
				mu.Unlock()
			})
			synctest.Wait()
		}
		b.Release()
		synctest.Wait()
		a.Release()
		synctest.Wait()
		root.SetMaxConns(0)
		wg.Wait()
		r.log("waiters %d conns root=%d a=%d b=%d", len(order), root.Conns(), a.Conns(), b.Conns())
		ctx, cancel := context.WithTimeout(bg, time.Second)
		b.SetMaxConns(0)
		r.log("b %s", errStr(b.Acquire()))
		b.SetMaxConns(1)
		r.log("b ctx %s", errStr(b.AcquireContext(ctx)))
		cancel()
		r.log("move a under b %s conns b=%d root=%d", errStr(a.SetParent(b)), b.Conns(), root.Conns())
	}},
	{"counter", func(r *diffRecorder) {
		var c Counter
		c.Add(100, 200)
		r.log("total %v", fmt2(c.Total()))
		r.log("swap %v", fmt2(c.Swap()))
		c.Add(5, 6)
		r.log("load %v", fmt2(c.Load()))
		c.Restore(1000, 1000)
		r.log("restore total %v load %v", fmt2(c.Total()), fmt2(c.Load()))
		c.Set(10, 20)
		r.log("set total %v load %v", fmt2(c.Total()), fmt2(c.Load()))
		for range 3 {
			c.Add(1000, 3000)
			time.Sleep(time.Second)
			r.log("throughput %v", fmt2(c.Throughput()))
		}
		m := NewConn(&nopNetConn{})
		c.Attach(m.Meter())
		c.Attach(m.Meter())
		_, _ = m.Write(make([]byte, 77))
		r.log("attached total %v", fmt2(c.Total()))
		c.Detach(m.Meter())
		_, _ = m.Write(make([]byte, 3))
		r.log("one detach total %v", fmt2(c.Total()))
		c.Detach(m.Meter())
		_, _ = m.Write(make([]byte, 5))
		r.log("detached total %v meter %v", fmt2(c.Total()), fmt2(m.Meter().Total()))
		sc := NewSharedCounter()
		var wg sync.WaitGroup
		for range 8 {
			wg.Go(func() {
				for range 1000 {
					sc.Add(1, 2)
				}
			})
		}
		wg.Wait()
		r.log("shared total %v", fmt2(sc.Total()))
		wc := &Counter{}
		cc := NewConn(&nopNetConn{}, WithCounter(wc), WithCounter(wc))
		_, _ = cc.Write(make([]byte, 9))
		r.log("withcounter twice total %v", fmt2(wc.Total()))
		_ = cc.Close()
		_ = m.Close()
	}},
	{"conn limiters", func(r *diffRecorder) {
		rt := NewRate(1000)
		c := NewConn(&nopNetConn{}, WithLimiters(nil, rt))
		buf := make([]byte, 1500)
		for range 3 {
			n, err := c.Write(buf)
			r.log("write %d %s", n, errStr(err))
		}
		c.SetLimiters(NewRate(500), nil)
		n, err := c.Write(buf)
		r.log("unlimited write %d %s", n, errStr(err))
		for range 3 {
			n, err = c.Read(buf)
			r.log("read %d %s", n, errStr(err))
		}
		_ = c.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
		c.SetLimiters(nil, NewRate(10))
		_, _ = c.Write(make([]byte, 100))
		n, err = c.Write(make([]byte, 1000))
		r.log("deadline write %d %s", n, errStr(err))
		_ = c.Close()
	}},
}

func fmt2(a, b int64) string { return fmt.Sprintf("%d/%d", a, b) }
