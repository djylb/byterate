package byterate

import (
	"math/bits"
	"time"
)

// bytesToNsCeil returns ceil(bytes*1e9/rate), saturated to maxI64.
func bytesToNsCeil(bytes, rate int64) int64 {
	if bytes <= 0 || rate <= 0 {
		return 0
	}
	hi, lo := bits.Mul64(uint64(bytes), uint64(time.Second))
	if hi >= uint64(rate) { // quotient does not fit in 64 bits
		return maxI64
	}
	q, r := bits.Div64(hi, lo, uint64(rate))
	if q >= uint64(maxI64) {
		return maxI64
	}
	if r != 0 {
		q++
	}
	return int64(q)
}

// mulDiv returns floor(a*b/c) for a, b >= 0 and c > 0, saturated to maxI64.
func mulDiv(a, b, c int64) int64 {
	hi, lo := bits.Mul64(uint64(a), uint64(b))
	if hi >= uint64(c) { // quotient does not fit in 64 bits
		return maxI64
	}
	q, _ := bits.Div64(hi, lo, uint64(c))
	if q > uint64(maxI64) {
		return maxI64
	}
	return int64(q)
}

// addSat returns a+b, saturated to the int64 range.
func addSat(a, b int64) int64 {
	s := a + b
	if (a < 0) == (b < 0) && (s < 0) != (a < 0) {
		if a < 0 {
			return minI64
		}
		return maxI64
	}
	return s
}

// subSat returns a-b, saturated to the int64 range.
func subSat(a, b int64) int64 {
	d := a - b
	if (a < 0) != (b < 0) && (d < 0) != (a < 0) {
		if a < 0 {
			return minI64
		}
		return maxI64
	}
	return d
}

// covers reports whether creditNs of traffic at limit, as Rate.credit
// returns them, cover size bytes. It multiplies instead of dividing, as most
// charges are covered.
func covers(size, creditNs, limit int64) bool {
	if limit <= 0 {
		return true
	}
	hi, lo := bits.Mul64(uint64(creditNs), uint64(limit))
	shi, slo := bits.Mul64(uint64(size), uint64(time.Second))
	return hi > shi || hi == shi && lo >= slo
}

// mix32 spreads the bits of x over 32 bits by Fibonacci hashing, for keys
// that pick a shard.
func mix32(x uintptr) uint32 {
	return uint32(uint64(x) * 0x9E3779B97F4A7C15 >> 32)
}

// pow2AtLeast returns the least power of two that is at least n, and 1 for
// n <= 1.
func pow2AtLeast(n int) int {
	p := 1
	for p < n {
		p <<= 1
	}
	return p
}
