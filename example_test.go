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

// Cap the total bytes of a connection: once the Budget is spent, Write sends
// what is left and fails.
func ExampleBudget() {
	client, server := net.Pipe()
	defer func() { _ = server.Close() }()
	go func() { _, _ = io.Copy(io.Discard, server) }()
	budget := byterate.NewBudget(1000)
	conn := byterate.NewConn(client, byterate.NewHierarchy(byterate.NewRate(1<<20), budget))
	defer func() { _ = conn.Close() }()

	n, err := conn.Write(make([]byte, 4000))
	fmt.Println(n, err, budget.Left())
	// Output: 1000 byterate: budget exhausted 0
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
