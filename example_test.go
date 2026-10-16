package byterate_test

import (
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
