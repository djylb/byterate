//go:build arm || mips || mipsle || mips64 || mips64le

package byterate

// cacheLineSize covers the 32-byte lines of the small cores these run on.
const cacheLineSize = 32
