// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package decimal128_test

import (
	"fmt"
	"math"
	"math/big"
	"testing"

	"github.com/apache/arrow/go/v14/arrow/decimal128"
	"github.com/stretchr/testify/assert"
)

func TestFromU64(t *testing.T) {
	for _, tc := range []struct {
		v    uint64
		want decimal128.Num
		sign int
	}{
		{0, decimal128.New(0, 0), 0},
		{1, decimal128.New(0, 1), +1},
		{2, decimal128.New(0, 2), +1},
		{math.MaxInt64, decimal128.New(0, math.MaxInt64), +1},
		{math.MaxUint64, decimal128.New(0, math.MaxUint64), +1},
	} {
		t.Run(fmt.Sprintf("%+0#x", tc.v), func(t *testing.T) {
			v := decimal128.FromU64(tc.v)
			ref := new(big.Int).SetUint64(tc.v)
			if got, want := v, tc.want; got != want {
				t.Fatalf("invalid value. got=%+0#x, want=%+0#x (big-int=%+0#x)", got, want, ref)
			}
			if got, want := v.Sign(), tc.sign; got != want {
				t.Fatalf("invalid sign for %+0#x: got=%v, want=%v", v, got, want)
			}
			if got, want := v.Sign(), ref.Sign(); got != want {
				t.Fatalf("invalid sign for %+0#x: got=%v, want=%v", v, got, want)
			}
			if got, want := v.LowBits(), tc.want.LowBits(); got != want {
				t.Fatalf("invalid low-bits: got=%+0#x, want=%+0#x", got, want)
			}
			if got, want := v.HighBits(), tc.want.HighBits(); got != want {
				t.Fatalf("invalid high-bits: got=%+0#x, want=%+0#x", got, want)
			}
		})
	}
}

func TestFromI64(t *testing.T) {
	for _, tc := range []struct {
		v    int64
		want decimal128.Num
		sign int
	}{
		{0, decimal128.New(0, 0), 0},
		{1, decimal128.New(0, 1), 1},
		{2, decimal128.New(0, 2), 1},
		{math.MaxInt64, decimal128.New(0, math.MaxInt64), 1},
		{math.MinInt64, decimal128.New(-1, u64Cnv(math.MinInt64)), -1},
	} {
		t.Run(fmt.Sprintf("%+0#x", tc.v), func(t *testing.T) {
			v := decimal128.FromI64(tc.v)
			ref := big.NewInt(tc.v)
			if got, want := v, tc.want; got != want {
				t.Fatalf("invalid value. got=%+0#x, want=%+0#x (big-int=%+0#x)", got, want, ref)
			}
			if got, want := v.Sign(), tc.sign; got != want {
				t.Fatalf("invalid sign for %+0#x: got=%v, want=%v", v, got, want)
			}
			if got, want := v.Sign(), ref.Sign(); got != want {
				t.Fatalf("invalid sign for %+0#x: got=%v, want=%v", v, got, want)
			}
			if got, want := v.LowBits(), tc.want.LowBits(); got != want {
				t.Fatalf("invalid low-bits: got=%+0#x, want=%+0#x", got, want)
			}
			if got, want := v.HighBits(), tc.want.HighBits(); got != want {
				t.Fatalf("invalid high-bits: got=%+0#x, want=%+0#x", got, want)
			}
		})
	}
}

func u64Cnv(i int64) uint64 { return uint64(i) }

func BenchmarkBigIntToDecimal(b *testing.B) {
	var (
		n     decimal128.Num
		bi, _ = (&big.Int{}).SetString("-340282366920938463463374607431711455", 10)
	)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n = decimal128.FromBigInt(bi)
		if n.Sign() >= 0 {
			b.FailNow()
		}
	}
}

func TestAdd(t *testing.T) {
	for _, tc := range []struct {
		n    decimal128.Num
		rhs  decimal128.Num
		want decimal128.Num
	}{
		{decimal128.New(0, 1), decimal128.New(0, 2), decimal128.New(0, 3)},
		{decimal128.New(1, 0), decimal128.New(2, 0), decimal128.New(3, 0)},
		{decimal128.New(2, 1), decimal128.New(1, 2), decimal128.New(3, 3)},
		{decimal128.New(0, 1), decimal128.New(0, math.MaxUint64), decimal128.New(1, 0)},
		{decimal128.New(0, math.MaxUint64), decimal128.New(0, 1), decimal128.New(1, 0)},
		{decimal128.New(0, 1), decimal128.New(0, 0), decimal128.New(0, 1)},
		{decimal128.New(0, 0), decimal128.New(0, 1), decimal128.New(0, 1)},
	} {
		t.Run("add", func(t *testing.T) {
			n := tc.n.Add(tc.rhs)
			if got, want := n, tc.want; got != want {
				t.Fatalf("invalid value. got=%v, want=%v", got, want)
			}
		})
	}
}

func TestSub(t *testing.T) {
	for _, tc := range []struct {
		n    decimal128.Num
		rhs  decimal128.Num
		want decimal128.Num
	}{
		{decimal128.New(0, 3), decimal128.New(0, 2), decimal128.New(0, 1)},
		{decimal128.New(3, 0), decimal128.New(2, 0), decimal128.New(1, 0)},
		{decimal128.New(3, 3), decimal128.New(1, 2), decimal128.New(2, 1)},
		{decimal128.New(0, 0), decimal128.New(0, math.MaxUint64), decimal128.New(-1, 1)},
		{decimal128.New(1, 0), decimal128.New(0, math.MaxUint64), decimal128.New(0, 1)},
		{decimal128.New(0, 1), decimal128.New(0, 0), decimal128.New(0, 1)},
		{decimal128.New(0, 0), decimal128.New(0, 1), decimal128.New(-1, math.MaxUint64)},
	} {
		t.Run("sub", func(t *testing.T) {
			n := tc.n.Sub(tc.rhs)
			if got, want := n, tc.want; got != want {
				t.Fatalf("invalid value. got=%v, want=%v", got, want)
			}
		})
	}
}

func TestMul(t *testing.T) {
	for _, tc := range []struct {
		n    decimal128.Num
		rhs  decimal128.Num
		want decimal128.Num
	}{
		{decimal128.New(0, 2), decimal128.New(0, 3), decimal128.New(0, 6)},
		{decimal128.New(2, 0), decimal128.New(0, 3), decimal128.New(6, 0)},
		{decimal128.New(3, 3), decimal128.New(0, 2), decimal128.New(6, 6)},
		{decimal128.New(0, 2), decimal128.New(3, 3), decimal128.New(6, 6)},
		{decimal128.New(0, 2), decimal128.New(0, math.MaxUint64), decimal128.New(1, math.MaxUint64-1)},
		{decimal128.New(0, 1), decimal128.New(0, 0), decimal128.New(0, 0)},
		{decimal128.New(0, 0), decimal128.New(0, 1), decimal128.New(0, 0)},
	} {
		t.Run("mul", func(t *testing.T) {
			n := tc.n.Mul(tc.rhs)
			if got, want := n, tc.want; got != want {
				t.Fatalf("invalid value. got=%v, want=%v", got, want)
			}
		})
	}
}

func TestDiv(t *testing.T) {
	for _, tc := range []struct {
		n        decimal128.Num
		rhs      decimal128.Num
		want_res decimal128.Num
		want_rem decimal128.Num
	}{
		{decimal128.New(0, 3), decimal128.New(0, 2), decimal128.New(0, 1), decimal128.New(0, 1)},
		{decimal128.New(3, 0), decimal128.New(2, 0), decimal128.New(0, 1), decimal128.New(1, 0)},
		{decimal128.New(3, 2), decimal128.New(2, 3), decimal128.New(0, 1), decimal128.New(0, math.MaxUint64)},
		{decimal128.New(0, math.MaxUint64), decimal128.New(0, 1), decimal128.New(0, math.MaxUint64), decimal128.New(0, 0)},
		{decimal128.New(math.MaxInt64, 0), decimal128.New(0, 1), decimal128.New(math.MaxInt64, 0), decimal128.New(0, 0)},
		{decimal128.New(0, 0), decimal128.New(0, 1), decimal128.New(0, 0), decimal128.New(0, 0)},
	} {
		t.Run("div", func(t *testing.T) {
			res, rem := tc.n.Div(tc.rhs)
			if got, want := res, tc.want_res; got != want {
				t.Fatalf("invalid res value. got=%v, want=%v", got, want)
			}
			if got, want := rem, tc.want_rem; got != want {
				t.Fatalf("invalid rem value. got=%v, want=%v", got, want)
			}
		})
	}
}

func TestPow(t *testing.T) {
	for _, tc := range []struct {
		n    decimal128.Num
		rhs  decimal128.Num
		want decimal128.Num
	}{
		{decimal128.New(0, 2), decimal128.New(0, 3), decimal128.New(0, 8)},
		{decimal128.New(0, 2), decimal128.New(0, 65), decimal128.New(2, 0)},
		{decimal128.New(0, 1), decimal128.New(0, 0), decimal128.New(0, 1)},
		{decimal128.New(0, 0), decimal128.New(0, 1), decimal128.New(0, 0)},
	} {
		t.Run("pow", func(t *testing.T) {
			n := tc.n.Pow(tc.rhs)
			if got, want := n, tc.want; got != want {
				t.Fatalf("invalid value. got=%v, want=%v", got, want)
			}
		})
	}
}

func TestMax(t *testing.T) {
	for _, tc := range []struct {
		n    decimal128.Num
		rhs  []decimal128.Num
		want decimal128.Num
	}{
		{decimal128.New(0, 2), []decimal128.Num{decimal128.New(2, 1), decimal128.New(0, 8), decimal128.New(0, 0)}, decimal128.New(2, 1)},
		{decimal128.New(0, 10), []decimal128.Num{decimal128.New(0, 1), decimal128.New(-1, 8), decimal128.New(3, 0)}, decimal128.New(3, 0)},
	} {
		t.Run("max", func(t *testing.T) {
			n := decimal128.Max(tc.n, tc.rhs...)
			if got, want := n, tc.want; got != want {
				t.Fatalf("invalid value. got=%v, want=%v", got, want)
			}
		})
	}
}

func TestMin(t *testing.T) {
	for _, tc := range []struct {
		n    decimal128.Num
		rhs  []decimal128.Num
		want decimal128.Num
	}{
		{decimal128.New(0, 2), []decimal128.Num{decimal128.New(2, 1), decimal128.New(0, 8), decimal128.New(0, 0)}, decimal128.New(0, 0)},
		{decimal128.New(0, 10), []decimal128.Num{decimal128.New(-1, 0), decimal128.New(0, 8), decimal128.New(3, 0)}, decimal128.New(-1, 0)},
	} {
		t.Run("min", func(t *testing.T) {
			n := decimal128.Min(tc.n, tc.rhs...)
			if got, want := n, tc.want; got != want {
				t.Fatalf("invalid value. got=%v, want=%v", got, want)
			}
		})
	}
}

func TestGreater(t *testing.T) {
	for _, tc := range []struct {
		n    decimal128.Num
		rhs  decimal128.Num
		want bool
	}{
		{decimal128.New(0, 2), decimal128.New(0, 1), true},
		{decimal128.New(2, 0), decimal128.New(1, 0), true},
		{decimal128.New(-1, 0), decimal128.New(-2, 0), true},
		{decimal128.New(0, 2), decimal128.New(0, 3), false},
		{decimal128.New(2, 0), decimal128.New(3, 0), false},
		{decimal128.New(-3, 0), decimal128.New(-2, 0), false},
		{decimal128.New(0, 2), decimal128.New(0, 2), false},
		{decimal128.New(2, 0), decimal128.New(2, 0), false},
		{decimal128.New(-2, 0), decimal128.New(-2, 0), false},
		{decimal128.New(2, math.MaxUint64), decimal128.New(2, 1), true},
		{decimal128.New(2, math.MaxUint64), decimal128.New(3, 1), false},
		{decimal128.New(2, math.MaxUint64), decimal128.New(2, math.MaxUint64), false},
		{decimal128.New(-2, math.MaxUint64), decimal128.New(-2, math.MaxUint64), false},
	} {
		t.Run("greater", func(t *testing.T) {
			n := tc.n.Greater(tc.rhs)
			if got, want := n, tc.want; got != want {
				t.Fatalf("invalid value. got=%v, want=%v", got, want)
			}
		})
	}
}

func TestLess(t *testing.T) {
	for _, tc := range []struct {
		n    decimal128.Num
		rhs  decimal128.Num
		want bool
	}{
		{decimal128.New(0, 2), decimal128.New(0, 1), false},
		{decimal128.New(2, 0), decimal128.New(1, 0), false},
		{decimal128.New(-1, 0), decimal128.New(-2, 0), false},
		{decimal128.New(0, 2), decimal128.New(0, 3), true},
		{decimal128.New(2, 0), decimal128.New(3, 0), true},
		{decimal128.New(-3, 0), decimal128.New(-2, 0), true},
		{decimal128.New(0, 2), decimal128.New(0, 2), false},
		{decimal128.New(2, 0), decimal128.New(2, 0), false},
		{decimal128.New(-2, 0), decimal128.New(-2, 0), false},
		{decimal128.New(2, math.MaxUint64), decimal128.New(2, 1), false},
		{decimal128.New(2, math.MaxUint64), decimal128.New(3, 1), true},
		{decimal128.New(2, math.MaxUint64), decimal128.New(2, math.MaxUint64), false},
		{decimal128.New(-2, math.MaxUint64), decimal128.New(-2, math.MaxUint64), false},
	} {
		t.Run("less", func(t *testing.T) {
			n := tc.n.Less(tc.rhs)
			if got, want := n, tc.want; got != want {
				t.Fatalf("invalid value. got=%v, want=%v", got, want)
			}
		})
	}
}

func TestCmp(t *testing.T) {
	for _, tc := range []struct {
		n    decimal128.Num
		rhs  decimal128.Num
		want int
	}{
		{decimal128.New(0, 2), decimal128.New(0, 1), 1},
		{decimal128.New(2, 0), decimal128.New(1, 0), 1},
		{decimal128.New(-1, 0), decimal128.New(-2, 0), 1},
		{decimal128.New(0, 2), decimal128.New(0, 3), -1},
		{decimal128.New(-3, 0), decimal128.New(-2, 0), -1},
		{decimal128.New(2, 0), decimal128.New(3, 0), -1},
		{decimal128.New(0, 2), decimal128.New(0, 2), 0},
		{decimal128.New(2, 0), decimal128.New(2, 0), 0},
		{decimal128.New(-2, 0), decimal128.New(-2, 0), 0},
		{decimal128.New(2, math.MaxUint64), decimal128.New(2, 1), 1},
		{decimal128.New(2, math.MaxUint64), decimal128.New(3, 1), -1},
		{decimal128.New(2, math.MaxUint64), decimal128.New(2, math.MaxUint64), 0},
		{decimal128.New(-2, math.MaxUint64), decimal128.New(-2, math.MaxUint64), 0},
	} {
		t.Run("cmp", func(t *testing.T) {
			n := tc.n.Cmp(tc.rhs)
			if got, want := n, tc.want; got != want {
				t.Fatalf("invalid value. got=%v, want=%v", got, want)
			}
		})
	}
}

func BenchmarkDecimalToBigInt(b *testing.B) {
	var (
		bi *big.Int
		n  = decimal128.New(-18446744073709552, 7083549724304524577)
	)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bi = n.BigInt()
		if bi.Sign() >= 0 {
			b.FailNow()
		}
	}
}

func TestDecimalToBigInt(t *testing.T) {
	tests := []struct {
		hi  int64
		lo  uint64
		exp string
	}{
		{-18446744073709552, 7083549724304524577, "-340282366920938463463374607431711455"},
		{1, 4611686018427387904, "23058430092136939520"},
		{0, 0, "0"},
	}
	for _, tc := range tests {
		t.Run("", func(t *testing.T) {
			n := decimal128.New(tc.hi, tc.lo)
			bi := n.BigInt()

			assert.Equal(t, tc.exp, bi.String())
			n2 := decimal128.FromBigInt(bi)
			assert.Equal(t, n.LowBits(), n2.LowBits())
			assert.Equal(t, n.HighBits(), n2.HighBits())
		})
	}
}

func ulps64(actual, expected float64) int64 {
	ulp := math.Nextafter(actual, math.Inf(1)) - actual
	return int64(math.Abs((expected - actual) / ulp))
}

func ulps32(actual, expected float32) int64 {
	ulp := math.Nextafter32(actual, float32(math.Inf(1))) - actual
	return int64(math.Abs(float64((expected - actual) / ulp)))
}

func assertFloat32Approx(t *testing.T, x, y float32) bool {
	const maxulps int64 = 4
	ulps := ulps32(x, y)
	return assert.LessOrEqualf(t, ulps, maxulps, "%f not equal to %f (%d ulps)", x, y, ulps)
}

func assertFloat64Approx(t *testing.T, x, y float64) bool {
	const maxulps int64 = 4
	ulps := ulps64(x, y)
	return assert.LessOrEqualf(t, ulps, maxulps, "%f not equal to %f (%d ulps)", x, y, ulps)
}

func TestDecimalToReal(t *testing.T) {
	tests := []struct {
		decimalVal string
		scale      int32
		exp        float64
	}{
		{"0", 0, 0},
		{"0", 10, 0.0},
		{"0", -10, 0.0},
		{"1", 0, 1.0},
		{"12345", 0, 12345.0},
		{"12345", 1, 1234.5},
		// 2**62
		{"4611686018427387904", 0, math.Pow(2, 62)},
		// 2**63 + 2**62
		{"13835058055282163712", 0, math.Pow(2, 63) + math.Pow(2, 62)},
		// 2**64 + 2**62
		{"23058430092136939520", 0, math.Pow(2, 64) + math.Pow(2, 62)},
		// 10**38 - 2**103
		{"99999989858795198174164788026374356992", 0, math.Pow10(38) - math.Pow(2, 103)},
	}

	t.Run("float32", func(t *testing.T) {
		checkDecimalToFloat := func(t *testing.T, str string, v float32, scale int32) {
			bi, _ := (&big.Int{}).SetString(str, 10)
			dec := decimal128.FromBigInt(bi)
			assert.Equalf(t, v, dec.ToFloat32(scale), "Decimal Val: %s, Scale: %d", str, scale)
		}
		for _, tt := range tests {
			t.Run(tt.decimalVal, func(t *testing.T) {
				checkDecimalToFloat(t, tt.decimalVal, float32(tt.exp), tt.scale)
				if tt.decimalVal != "0" {
					checkDecimalToFloat(t, "-"+tt.decimalVal, float32(-tt.exp), tt.scale)
				}
			})
		}

		t.Run("precision", func(t *testing.T) {
			// 2**63 + 2**40 (exactly representable in a float's 24 bits of precision)
			checkDecimalToFloat(t, "9223373136366403584", float32(9.223373e+18), 0)
			checkDecimalToFloat(t, "-9223373136366403584", float32(-9.223373e+18), 0)
			// 2**64 + 2**41 exactly representable in a float
			checkDecimalToFloat(t, "18446746272732807168", float32(1.8446746e+19), 0)
			checkDecimalToFloat(t, "-18446746272732807168", float32(-1.8446746e+19), 0)
		})

		t.Run("large values", func(t *testing.T) {
			checkApproxDecimalToFloat := func(str string, v float32, scale int32) {
				bi, _ := (&big.Int{}).SetString(str, 10)
				dec := decimal128.FromBigInt(bi)
				assertFloat32Approx(t, v, dec.ToFloat32(scale))
			}
			// exact comparisons would succeed on most platforms, but not all power-of-ten
			// factors are exactly representable in binary floating point, so we'll use
			// approx and ensure that the values are within 4 ULP (unit of least precision)
			for scale := int32(-38); scale <= 38; scale++ {
				checkApproxDecimalToFloat("1", float32(math.Pow10(-int(scale))), scale)
				checkApproxDecimalToFloat("123", float32(123)*float32(math.Pow10(-int(scale))), scale)
			}
		})
	})

	t.Run("float64", func(t *testing.T) {
		checkDecimalToFloat := func(t *testing.T, str string, v float64, scale int32) {
			bi, _ := (&big.Int{}).SetString(str, 10)
			dec := decimal128.FromBigInt(bi)
			assert.Equalf(t, v, dec.ToFloat64(scale), "Decimal Val: %s, Scale: %d", str, scale)
		}
		for _, tt := range tests {
			t.Run(tt.decimalVal, func(t *testing.T) {
				checkDecimalToFloat(t, tt.decimalVal, tt.exp, tt.scale)
				if tt.decimalVal != "0" {
					checkDecimalToFloat(t, "-"+tt.decimalVal, -tt.exp, tt.scale)
				}
			})
		}

		t.Run("precision", func(t *testing.T) {
			// 2**63 + 2**11 (exactly representable in float64's 53 bits of precision)
			checkDecimalToFloat(t, "9223373136366403584", float64(9.223373136366404e+18), 0)
			checkDecimalToFloat(t, "-9223373136366403584", float64(-9.223373136366404e+18), 0)

			// 2**64 - 2**11 (exactly represntable in a float64)
			checkDecimalToFloat(t, "18446746272732807168", float64(1.8446746272732807e+19), 0)
			checkDecimalToFloat(t, "-18446746272732807168", float64(-1.8446746272732807e+19), 0)

			// 2**64 + 2**11 (exactly representable in a float64)
			checkDecimalToFloat(t, "18446744073709555712", float64(1.8446744073709556e+19), 0)
			checkDecimalToFloat(t, "-18446744073709555712", float64(-1.8446744073709556e+19), 0)

			// Almost 10**38 (minus 2**73)
			checkDecimalToFloat(t, "99999999999999978859343891977453174784", 9.999999999999998e+37, 0)
			checkDecimalToFloat(t, "-99999999999999978859343891977453174784", -9.999999999999998e+37, 0)
			checkDecimalToFloat(t, "99999999999999978859343891977453174784", 9.999999999999998e+27, 10)
			checkDecimalToFloat(t, "-99999999999999978859343891977453174784", -9.999999999999998e+27, 10)
			checkDecimalToFloat(t, "99999999999999978859343891977453174784", 9.999999999999998e+47, -10)
			checkDecimalToFloat(t, "-99999999999999978859343891977453174784", -9.999999999999998e+47, -10)
		})

		t.Run("large values", func(t *testing.T) {
			checkApproxDecimalToFloat := func(str string, v float64, scale int32) {
				bi, _ := (&big.Int{}).SetString(str, 10)
				dec := decimal128.FromBigInt(bi)
				assertFloat64Approx(t, v, dec.ToFloat64(scale))
			}
			// exact comparisons would succeed on most platforms, but not all power-of-ten
			// factors are exactly representable in binary floating point, so we'll use
			// approx and ensure that the values are within 4 ULP (unit of least precision)
			for scale := int32(-308); scale <= 306; scale++ {
				checkApproxDecimalToFloat("1", math.Pow10(-int(scale)), scale)
				checkApproxDecimalToFloat("123", float64(123)*math.Pow10(-int(scale)), scale)
			}
		})
	})
}

func TestDecimalFromFloat(t *testing.T) {
	tests := []struct {
		val              float64
		precision, scale int32
		expected         string
	}{
		{0, 1, 0, "0"},
		{-0, 1, 0, "0"},
		{0, 19, 4, "0.0000"},
		{math.Copysign(0.0, -1), 19, 4, "0.0000"},
		{123, 7, 4, "123.0000"},
		{-123, 7, 4, "-123.0000"},
		{456.78, 7, 4, "456.7800"},
		{-456.78, 7, 4, "-456.7800"},
		{456.784, 5, 2, "456.78"},
		{-456.784, 5, 2, "-456.78"},
		{456.786, 5, 2, "456.79"},
		{-456.786, 5, 2, "-456.79"},
		{999.99, 5, 2, "999.99"},
		{-999.99, 5, 2, "-999.99"},
		{123, 19, 0, "123"},
		{-123, 19, 0, "-123"},
		{123.4, 19, 0, "123"},
		{-123.4, 19, 0, "-123"},
		{123.6, 19, 0, "124"},
		{-123.6, 19, 0, "-124"},
		// 2**62
		{4.611686018427387904e+18, 19, 0, "4611686018427387904"},
		{-4.611686018427387904e+18, 19, 0, "-4611686018427387904"},
		// 2**63
		{9.223372036854775808e+18, 19, 0, "9223372036854775808"},
		{-9.223372036854775808e+18, 19, 0, "-9223372036854775808"},
		// 2**64
		{1.8446744073709551616e+19, 20, 0, "18446744073709551616"},
		{-1.8446744073709551616e+19, 20, 0, "-18446744073709551616"},
	}

	t.Run("float64", func(t *testing.T) {
		for _, tt := range tests {
			t.Run(tt.expected, func(t *testing.T) {
				n, err := decimal128.FromFloat64(tt.val, tt.precision, tt.scale)
				assert.NoError(t, err)

				assert.Equal(t, tt.expected, big.NewFloat(n.ToFloat64(tt.scale)).Text('f', int(tt.scale)))
			})
		}

		t.Run("large values", func(t *testing.T) {
			// test entire float64 range
			for scale := int32(-308); scale <= 308; scale++ {
				val := math.Pow10(int(scale))
				n, err := decimal128.FromFloat64(val, 1, -scale)
				assert.NoError(t, err)
				assert.Equal(t, "1", n.BigInt().String())
			}

			for scale := int32(-307); scale <= 306; scale++ {
				val := 123 * math.Pow10(int(scale))
				n, err := decimal128.FromFloat64(val, 2, -scale-1)
				assert.NoError(t, err)
				assert.Equal(t, "12", n.BigInt().String())
				n, err = decimal128.FromFloat64(val, 3, -scale)
				assert.NoError(t, err)
				assert.Equal(t, "123", n.BigInt().String())
				n, err = decimal128.FromFloat64(val, 4, -scale+1)
				assert.NoError(t, err)
				assert.Equal(t, "1230", n.BigInt().String())
			}
		})
	})

	t.Run("float32", func(t *testing.T) {
		for _, tt := range tests {
			t.Run(tt.expected, func(t *testing.T) {
				n, err := decimal128.FromFloat32(float32(tt.val), tt.precision, tt.scale)
				assert.NoError(t, err)

				assert.Equal(t, tt.expected, big.NewFloat(float64(n.ToFloat32(tt.scale))).Text('f', int(tt.scale)))
			})
		}

		t.Run("large values", func(t *testing.T) {
			// test entire float32 range
			for scale := int32(-38); scale <= 38; scale++ {
				val := float32(math.Pow10(int(scale)))
				n, err := decimal128.FromFloat32(val, 1, -scale)
				assert.NoError(t, err)
				assert.Equal(t, "1", n.BigInt().String())
			}

			for scale := int32(-37); scale <= 36; scale++ {
				val := 123 * float32(math.Pow10(int(scale)))
				n, err := decimal128.FromFloat32(val, 2, -scale-1)
				assert.NoError(t, err)
				assert.Equal(t, "12", n.BigInt().String())
				n, err = decimal128.FromFloat32(val, 3, -scale)
				assert.NoError(t, err)
				assert.Equal(t, "123", n.BigInt().String())
				n, err = decimal128.FromFloat32(val, 4, -scale+1)
				assert.NoError(t, err)
				assert.Equal(t, "1230", n.BigInt().String())
			}
		})
	})
}

func TestFromString(t *testing.T) {
	tests := []struct {
		s             string
		expected      int64
		expectedScale int32
	}{
		{"12.3", 123, 1},
		{"0.00123", 123, 5},
		{"1.23e-8", 123, 10},
		{"-1.23E-8", -123, 10},
		{"1.23e+3", 1230, 0},
		{"-1.23E+3", -1230, 0},
		{"1.23e+5", 123000, 0},
		{"1.2345E+7", 12345000, 0},
		{"1.23e-8", 123, 10},
		{"-1.23E-8", -123, 10},
		{"1.23E+3", 1230, 0},
		{"-1.23e+3", -1230, 0},
		{"1.23e+5", 123000, 0},
		{"1.2345e+7", 12345000, 0},
		{"0000000", 0, 0},
		{"000.0000", 0, 4},
		{".00000", 0, 5},
		{"1e1", 10, 0},
		{"+234.567", 234567, 3},
		{"1e-37", 1, 37},
		{"2112.33", 211233, 2},
		{"-2112.33", -211233, 2},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s_%d", tt.s, tt.expectedScale), func(t *testing.T) {
			n, err := decimal128.FromString(tt.s, 37, tt.expectedScale)
			assert.NoError(t, err)

			ex := decimal128.FromI64(tt.expected)
			assert.Equal(t, ex, n)
		})
	}
}

func TestInvalidNonNegScaleFromString(t *testing.T) {
	tests := []string{"1e39", "-1e39", "9e39", "-9e39", "9.9e40", "-9.9e40"}
	for _, tt := range tests {
		t.Run(tt, func(t *testing.T) {
			_, err := decimal128.FromString(tt, 38, 0)
			assert.Error(t, err)
		})
	}
}
