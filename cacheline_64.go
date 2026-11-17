//go:build !(amd64 || arm64 || ppc64 || ppc64le || s390x || arm || mips || mipsle || mips64 || mips64le)

package byterate

// cacheLineSize is the 64-byte line of most other processors.
const cacheLineSize = 64
