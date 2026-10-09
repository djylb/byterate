package byterate_test

import (
	"fmt"
	"io"
	"net"

	"github.com/djylb/byterate"
)

// Limit a connection to 1 MiB/s while keeping it a net.Conn.
func ExampleNewRateConn() {
	client, server := net.Pipe()
	defer func() { _ = server.Close() }()
	limited := byterate.NewRateConn(client, byterate.NewRate(1<<20))
	defer func() { _ = limited.Close() }()

	go func() { _, _ = limited.Write([]byte("hello")) }()
	buf := make([]byte, 5)
	_, _ = io.ReadFull(server, buf)
	fmt.Println(string(buf), limited.RemoteAddr())
	// Output: hello pipe
}

// Nest a connection's limit within its user's and an unlimited global Rate,
// and read each level's throughput.
func ExampleNewHierarchicalLimiter() {
	global := byterate.NewRate(0)     // unlimited, kept for its throughput
	user := byterate.NewRate(8 << 20) // 8 MiB/s across the user's connections

	client, server := net.Pipe()
	defer func() { _ = server.Close() }()
	conn := byterate.NewRateConn(client, byterate.NewHierarchicalLimiter(byterate.NewRate(1<<20), user, global))
	defer func() { _ = conn.Close() }()
	go func() { _, _ = conn.Write(make([]byte, 4096)) }()
	_, _ = io.ReadFull(server, make([]byte, 4096))

	fmt.Println(global.Limit(), user.Limit())
	_ = global.Now()           // bytes per second over the last sampling window
	global.SetLimit(100 << 20) // also limits the open connection
	// Output: 0 8388608
}

// Limit uploads and downloads separately and meter each direction.
func ExampleNewDuplexRateConn() {
	download, upload := byterate.NewRate(4<<20), byterate.NewRate(1<<20)
	client, server := net.Pipe()
	defer func() { _ = server.Close() }()
	conn := byterate.NewDuplexRateConn(client, download, upload)
	defer func() { _ = conn.Close() }()
	go func() { _, _ = conn.Write([]byte("up")) }()
	_, _ = io.ReadFull(server, make([]byte, 2))
	fmt.Println(download.Limit(), upload.Limit())
	// Output: 4194304 1048576
}
