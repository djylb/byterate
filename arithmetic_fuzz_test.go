package byterate

import (
	"math/big"
	"testing"
)

// Compare the bounded integer arithmetic against an arbitrary-precision
// reference, including products that overflow both signed and unsigned int64.
func FuzzRateArithmetic(f *testing.F) {
	for _, seed := range [][3]int64{{0, 0, 1}, {1, 1, 3}, {32768, 1000000000, 1048576}, {maxI64, maxI64, 1}, {maxI64, maxI64, maxI64}, {minI64, 1, 1}} {
		f.Add(seed[0], seed[1], seed[2])
	}
	f.Fuzz(func(t *testing.T, a, b, divisor int64) {
		a, b = a&maxI64, b&maxI64
		divisor = max(divisor&maxI64, 1)
		bound := big.NewInt(maxI64)
		check := func(got int64, factor int64, ceil bool) {
			t.Helper()
			product := new(big.Int).Mul(big.NewInt(a), big.NewInt(factor))
			quotient, remainder := new(big.Int), new(big.Int)
			quotient.QuoRem(product, big.NewInt(divisor), remainder)
			if ceil && remainder.Sign() != 0 {
				quotient.Add(quotient, big.NewInt(1))
			}
			if quotient.Cmp(bound) > 0 {
				quotient.Set(bound)
			}
			if got != quotient.Int64() {
				t.Fatalf("a=%d factor=%d divisor=%d ceil=%v: got %d want %s", a, factor, divisor, ceil, got, quotient)
			}
		}
		check(mulDiv(a, b, divisor), b, false)
		check(bytesToNsCeil(a, divisor), 1000000000, true)
	})
}
