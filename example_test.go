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
