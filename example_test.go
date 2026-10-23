package byterate_test

import (
	"context"
	"fmt"
	"io"
	"net"

	"github.com/djylb/byterate"
)

// Limit a connection to 1 MiB/s in both directions; the result is still a
// net.Conn.
func ExampleNewConn() {
	client, server := net.Pipe()
	defer func() { _ = server.Close() }()
	conn := byterate.NewConn(client, byterate.NewRate(1<<20))
	defer func() { _ = conn.Close() }()

	go func() { _, _ = conn.Write([]byte("hello")) }()
	buf := make([]byte, 5)
	_, _ = io.ReadFull(server, buf)
	fmt.Println(string(buf), conn.RemoteAddr())
	// Output: hello pipe
}

// Nest a connection's limit within its user's and a global one. Every level
// meters the traffic and reports its own throughput, and a limit changed
// later applies to open connections too.
func ExampleNewHierarchy() {
	global := byterate.NewRate(0)     // unlimited, kept for its throughput
	user := byterate.NewRate(8 << 20) // 8 MiB/s across the user's connections

	client, server := net.Pipe()
	defer func() { _ = server.Close() }()
	conn := byterate.NewConn(client, byterate.NewHierarchy(byterate.NewRate(1<<20), user, global))
	defer func() { _ = conn.Close() }()
	go func() { _, _ = conn.Write(make([]byte, 4096)) }()
	_, _ = io.ReadFull(server, make([]byte, 4096))

	fmt.Println(global.Limit(), user.Limit())
	_ = global.Throughput()    // bytes per second over the last second
	global.SetLimit(100 << 20) // now limits the open connection too
	// Output: 0 8388608
}

// Limit downloads and uploads apart, each metered on its own.
func ExampleNewDuplexConn() {
	download, upload := byterate.NewRate(4<<20), byterate.NewRate(1<<20)
	client, server := net.Pipe()
	defer func() { _ = server.Close() }()
	conn := byterate.NewDuplexConn(client, download, upload)
	defer func() { _ = conn.Close() }()
	go func() { _, _ = conn.Write([]byte("up")) }()
	_, _ = io.ReadFull(server, make([]byte, 2))
	fmt.Println(download.Limit(), upload.Limit())
	// Output: 4194304 1048576
}

// The zero value of a Rate is ready to use.
func ExampleRate() {
	var r byterate.Rate // unlimited
	r.SetLimit(64 << 10)
	fmt.Println(r.Limit())
	// Output: 65536
}

// Count the bytes of a transfer and take the totals for a report.
func ExampleCounter() {
	var c byterate.Counter
	c.Add(1500, 0)
	c.Add(0, 300)
	in, out := c.Swap() // the totals so far, and start over
	fmt.Println(in, out)
	in, out = c.Load()
	fmt.Println(in, out)
	// Output:
	// 1500 300
	// 0 0
}

// Move a connection's traffic to another limiter while it runs.
func ExampleSwitch() {
	oldUser, newUser := byterate.NewRate(1<<20), byterate.NewRate(4<<20)
	sw := byterate.NewSwitch(oldUser)
	client, server := net.Pipe()
	defer func() { _ = server.Close() }()
	conn := byterate.NewConn(client, sw)
	defer func() { _ = conn.Close() }()

	sw.Set(newUser) // charges, and waits in progress, move to newUser
	fmt.Println(sw.Limiter() == byterate.Limiter(newUser))
	// Output: true
}

// Drive a relay's hooks with a limiter: Ready before each read says how many
// bytes may pass, and Charge after each write records them.
func ExampleRate_Ready() {
	r := byterate.NewRate(1 << 20)
	src := make([]byte, 100<<10)
	sent := 0
	for sent < len(src) {
		n, err := r.Ready(context.Background(), int64(len(src)-sent))
		if err != nil {
			break
		}
		sent += int(n) // read and write n bytes here
		_ = r.Charge(n)
	}
	fmt.Println(sent)
	// Output: 102400
}

// Count exactly what a connection's reads and writes take.
func ExampleCounter_Reads() {
	client, server := net.Pipe()
	defer func() { _ = server.Close() }()
	go func() { _, _ = server.Write([]byte("hello")) }()
	var traffic byterate.Counter
	conn := byterate.NewDuplexConn(client, traffic.Reads(), traffic.Writes())
	defer func() { _ = conn.Close() }()

	_, _ = io.ReadFull(conn, make([]byte, 5))
	fmt.Println(traffic.Total())
	// Output: 5 0
}

// Stop a connection once it has used its allowance: Write sends what fits
// and fails.
func ExampleBudget() {
	client, server := net.Pipe()
	defer func() { _ = server.Close() }()
	go func() { _, _ = io.Copy(io.Discard, server) }()
	budget := byterate.NewBudget(byterate.Step{After: 1000, Stop: true})
	conn := byterate.NewConn(client, byterate.NewHierarchy(byterate.NewRate(1<<20), budget))
	defer func() { _ = conn.Close() }()

	n, err := conn.Write(make([]byte, 4000))
	fmt.Println(n, err, budget.Used())
	// Output: 1000 byterate: budget exhausted 1000
}

// Slow traffic down in stages as it is used, and stop it at the end. Each
// period, such as a day, starts over with Reset; changing the steps or their
// limits keeps connections open.
func ExampleNewBudget() {
	slow, slower := byterate.NewRate(1<<20), byterate.NewRate(64<<10)
	daily := byterate.NewBudget(
		byterate.Step{After: 10 << 30, Limit: slow},   // past 10 GiB: 1 MiB/s
		byterate.Step{After: 20 << 30, Limit: slower}, // past 20 GiB: 64 KiB/s
		byterate.Step{After: 30 << 30, Stop: true},    // past 30 GiB: stop
	)
	daily.OnStep(func(step int, used int64) { fmt.Println("step", step, "at", used) })

	_ = daily.Charge(15 << 30)
	fmt.Println(daily.Step(), daily.Used())

	daily.Reset()          // the next day starts over
	slow.SetLimit(2 << 20) // a new speed for the first step
	fmt.Println(daily.Step(), daily.Used())
	// Output:
	// step 0 at 16106127360
	// 0 16106127360
	// -1 0
}

// Account for and limit connections in a tree of levels, such as a server, a
// user, a client and a tunnel: every level counts the bytes and connections
// below it, caps connections and can set limits of its own.
func ExampleGroup() {
	server := byterate.NewGroup(nil)
	user := byterate.NewGroup(server)
	tunnel := byterate.NewGroup(user)
	user.SetLimiters(byterate.NewRate(1<<20), byterate.NewRate(1<<20)) // 1 MiB/s each way
	user.SetMaxConns(100)

	client, peer := net.Pipe()
	defer func() { _ = peer.Close() }()
	go func() { _, _ = io.Copy(io.Discard, peer) }()
	conn, err := tunnel.Open(client)
	if err != nil {
		_ = client.Close() // a *byterate.TooManyConnsError, matching ErrTooManyConns
		return
	}
	_, _ = conn.Write(make([]byte, 1000))
	fmt.Println(server.Conns(), user.Conns())
	_, out := user.Traffic().Load()
	fmt.Println(out)
	_ = conn.Close()
	fmt.Println(server.Conns())
	// Output:
	// 1 1
	// 1000
	// 0
}

// Give a user a speed and a monthly allowance shared by both directions: past
// 100 GiB its traffic slows to 1 MiB/s, and at 120 GiB it stops until Reset
// starts the next month. Open refuses connections past the user's cap.
func ExampleGroup_budget() {
	server := byterate.NewGroup(nil)
	user := byterate.NewGroup(server)
	monthly := byterate.NewBudget(
		byterate.Step{After: 100 << 30, Limit: byterate.NewRate(1 << 20)},
		byterate.Step{After: 120 << 30, Stop: true},
	)
	limit := byterate.NewHierarchy(byterate.NewRate(10<<20), monthly) // 10 MiB/s
	user.SetLimiters(limit, limit)
	user.SetMaxConns(10)

	client, peer := net.Pipe()
	defer func() { _ = peer.Close() }()
	go func() { _, _ = io.Copy(io.Discard, peer) }()
	conn, err := user.Open(client)
	if err != nil {
		_ = client.Close()
		return
	}
	defer func() { _ = conn.Close() }()
	_, _ = conn.Write(make([]byte, 1500))
	in, out := user.Traffic().Total()
	fmt.Println(monthly.Used(), in, out)

	monthly.Reset() // at the start of the next month
	fmt.Println(monthly.Used())
	// Output:
	// 1500 0 1500
	// 0
}

// Drop the datagrams that a limit does not let through at once, as a UDP
// relay does, instead of waiting for them.
func ExampleRate_TryWait() {
	limit := byterate.NewRate(1000) // a burst of 2000 bytes
	sent, dropped := 0, 0
	for range 5 {
		packet := make([]byte, 600)
		if err := limit.TryWait(int64(len(packet))); err != nil {
			dropped++ // byterate.ErrWouldWait, or a refusal such as ErrBudgetExhausted
			continue
		}
		sent++ // conn.WriteTo(packet, addr)
	}
	fmt.Println(sent, dropped)
	// Output:
	// 3 2
}
