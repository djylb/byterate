//go:build amd64 || arm64 || ppc64 || ppc64le

package byterate

// cacheLineSize covers the 128-byte lines of Apple silicon and POWER, and the adjacent-line prefetch of x86-64.
const cacheLineSize = 128
