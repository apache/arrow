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

package decimal_test

import (
	"fmt"
	"math"
	"strconv"
	"testing"

	"github.com/apache/arrow/go/v18/arrow/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
		{"536870912", 0, math.Pow(2, 29)},
	}

	t.Run("float32", func(t *testing.T) {
		checkDecimalToFloat := func(t *testing.T, str string, v float32, scale int32) {
			n, err := decimal.Decimal32FromString(str, 9, 0)
			require.NoError(t, err)
			assert.Equalf(t, v, n.ToFloat32(scale), "Decimal Val: %s, Scale: %d", str, scale)

			n64, err := decimal.Decimal64FromString(str, 18, 0)
			require.NoError(t, err)
			assert.Equalf(t, v, n64.ToFloat32(scale), "Decimal Val: %s, Scale: %d", str, scale)
		}
		for _, tt := range tests {
			t.Run(tt.decimalVal, func(t *testing.T) {
				checkDecimalToFloat(t, tt.decimalVal, float32(tt.exp), tt.scale)
				if tt.decimalVal != "0" {
					checkDecimalToFloat(t, "-"+tt.decimalVal, float32(-tt.exp), tt.scale)
				}
			})
		}

		t.Run("large values", func(t *testing.T) {
			checkApproxDecimaltoFloat := func(str string, v float32, scale int32) {
				n, err := decimal.Decimal32FromString(str, 9, 0)
				require.NoError(t, err)
				assertFloat32Approx(t, v, n.ToFloat32(scale))
			}

			checkApproxDecimal64toFloat := func(str string, v float32, scale int32) {
				n, err := decimal.Decimal64FromString(str, 9, 0)
				require.NoError(t, err)
				assertFloat32Approx(t, v, n.ToFloat32(scale))
			}

			// exact comparisons would succeed on most platforms, but not all power-of-ten
			// factors are exactly representable in binary floating point, so we'll use
			// approx and ensure that the values are within 4 ULP (unit of least precision)
			for scale := int32(-9); scale <= 9; scale++ {
				checkApproxDecimaltoFloat("1", float32(math.Pow10(-int(scale))), scale)
				checkApproxDecimaltoFloat("123", float32(123)*float32(math.Pow10(-int(scale))), scale)
			}

			for scale := int32(-18); scale <= 18; scale++ {
				checkApproxDecimal64toFloat("1", float32(math.Pow10(-int(scale))), scale)
				checkApproxDecimal64toFloat("123", float32(123)*float32(math.Pow10(-int(scale))), scale)
			}
		})
	})

	t.Run("float32", func(t *testing.T) {
		checkDecimalToFloat := func(t *testing.T, str string, v float64, scale int32) {
			n, err := decimal.Decimal32FromString(str, 9, 0)
			require.NoError(t, err)
			assert.Equalf(t, v, n.ToFloat64(scale), "Decimal Val: %s, Scale: %d", str, scale)

			n64, err := decimal.Decimal64FromString(str, 18, 0)
			require.NoError(t, err)
			assert.Equalf(t, v, n64.ToFloat64(scale), "Decimal Val: %s, Scale: %d", str, scale)
		}
		for _, tt := range tests {
			t.Run(tt.decimalVal, func(t *testing.T) {
				checkDecimalToFloat(t, tt.decimalVal, tt.exp, tt.scale)
				if tt.decimalVal != "0" {
					checkDecimalToFloat(t, "-"+tt.decimalVal, -tt.exp, tt.scale)
				}
			})
		}

		t.Run("large values", func(t *testing.T) {
			checkApproxDecimaltoFloat := func(str string, v float64, scale int32) {
				n, err := decimal.Decimal32FromString(str, 9, 0)
				require.NoError(t, err)
				assertFloat64Approx(t, v, n.ToFloat64(scale))
			}

			checkApproxDecimal64toFloat := func(str string, v float64, scale int32) {
				n, err := decimal.Decimal64FromString(str, 9, 0)
				require.NoError(t, err)
				assertFloat64Approx(t, v, n.ToFloat64(scale))
			}

			// exact comparisons would succeed on most platforms, but not all power-of-ten
			// factors are exactly representable in binary floating point, so we'll use
			// approx and ensure that the values are within 4 ULP (unit of least precision)
			for scale := int32(-9); scale <= 9; scale++ {
				checkApproxDecimaltoFloat("1", math.Pow10(-int(scale)), scale)
				checkApproxDecimaltoFloat("123", float64(123)*math.Pow10(-int(scale)), scale)
			}

			for scale := int32(-18); scale <= 18; scale++ {
				checkApproxDecimal64toFloat("1", math.Pow10(-int(scale)), scale)
				checkApproxDecimal64toFloat("123", float64(123)*math.Pow10(-int(scale)), scale)
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
		{0, 9, 4, "0.0000"},
		{math.Copysign(0.0, -1), 9, 4, "0.0000"},
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
		{123, 9, 0, "123"},
		{-123, 9, 0, "-123"},
		{123.4, 9, 0, "123"},
		{-123.4, 9, 0, "-123"},
		{123.6, 9, 0, "124"},
		{-123.6, 9, 0, "-124"},
	}

	t.Run("float64", func(t *testing.T) {
		for _, tt := range tests {
			t.Run(tt.expected, func(t *testing.T) {
				n, err := decimal.Decimal32FromFloat(tt.val, tt.precision, tt.scale)
				require.NoError(t, err)
				assert.Equal(t, tt.expected, fmt.Sprintf("%."+strconv.Itoa(int(tt.scale))+"f", n.ToFloat64(tt.scale)))
			})
		}

		t.Run("large values", func(t *testing.T) {
			// test entire float64 range
			for scale := int32(-308); scale <= 308; scale++ {
				val := math.Pow10(int(scale))
				n, err := decimal.Decimal64FromFloat(val, 1, -scale)
				require.NoError(t, err)
				assert.EqualValues(t, 1, n)
			}

			for scale := int32(-307); scale <= 306; scale++ {
				val := 123 * math.Pow10(int(scale))
				n, err := decimal.Decimal64FromFloat(val, 2, -scale-1)
				require.NoError(t, err)
				assert.EqualValues(t, 12, n)
				n, err = decimal.Decimal64FromFloat(val, 3, -scale)
				require.NoError(t, err)
				assert.EqualValues(t, 123, n)
				n, err = decimal.Decimal64FromFloat(val, 4, -scale+1)
				require.NoError(t, err)
				assert.EqualValues(t, 1230, n)
			}
		})
	})

	t.Run("float32", func(t *testing.T) {
		for _, tt := range tests {
			t.Run(tt.expected, func(t *testing.T) {
				n, err := decimal.Decimal32FromFloat(float32(tt.val), tt.precision, tt.scale)
				require.NoError(t, err)
				assert.Equal(t, tt.expected, fmt.Sprintf("%."+strconv.Itoa(int(tt.scale))+"f", n.ToFloat32(tt.scale)))
			})
		}

		t.Run("large values", func(t *testing.T) {
			// test entire float32 range
			for scale := int32(-38); scale <= 38; scale++ {
				val := float32(math.Pow10(int(scale)))
				n, err := decimal.Decimal64FromFloat(val, 1, -scale)
				require.NoError(t, err)
				assert.EqualValues(t, 1, n)
			}

			for scale := int32(-37); scale <= 36; scale++ {
				val := 123 * float32(math.Pow10(int(scale)))
				n, err := decimal.Decimal64FromFloat(val, 2, -scale-1)
				require.NoError(t, err)
				assert.EqualValues(t, 12, n)
				n, err = decimal.Decimal64FromFloat(val, 3, -scale)
				require.NoError(t, err)
				assert.EqualValues(t, 123, n)
				n, err = decimal.Decimal64FromFloat(val, 4, -scale+1)
				require.NoError(t, err)
				assert.EqualValues(t, 1230, n)
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
		{"0000000", 0, 0},
		{"000.0000", 0, 4},
		{".0000", 0, 5},
		{"1e1", 10, 0},
		{"+234.567", 234567, 3},
		{"1e-8", 1, 8},
		{"2112.33", 211233, 2},
		{"-2112.33", -211233, 2},
		{"12E2", 12, -2},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s_%d", tt.s, tt.expectedScale), func(t *testing.T) {
			n, err := decimal.Decimal32FromString(tt.s, 8, tt.expectedScale)
			require.NoError(t, err)

			ex := decimal.Decimal32(tt.expected)
			assert.Equal(t, ex, n)

			n64, err := decimal.Decimal64FromString(tt.s, 8, tt.expectedScale)
			require.NoError(t, err)

			ex64 := decimal.Decimal64(tt.expected)
			assert.Equal(t, ex64, n64)
		})
	}
}

func TestCmp(t *testing.T) {
	for _, tc := range []struct {
		n    decimal.Decimal32
		rhs  decimal.Decimal32
		want int
	}{
		{decimal.Decimal32(2), decimal.Decimal32(1), 1},
		{decimal.Decimal32(-1), decimal.Decimal32(-2), 1},
		{decimal.Decimal32(2), decimal.Decimal32(3), -1},
		{decimal.Decimal32(-3), decimal.Decimal32(-2), -1},
		{decimal.Decimal32(2), decimal.Decimal32(2), 0},
		{decimal.Decimal32(-2), decimal.Decimal32(-2), 0},
	} {
		t.Run("cmp", func(t *testing.T) {
			n := tc.n.Cmp(tc.rhs)
			if got, want := n, tc.want; got != want {
				t.Fatalf("invalid value. got=%v, want=%v", got, want)
			}
		})
	}

	for _, tc := range []struct {
		n    decimal.Decimal64
		rhs  decimal.Decimal64
		want int
	}{
		{decimal.Decimal64(2), decimal.Decimal64(1), 1},
		{decimal.Decimal64(-1), decimal.Decimal64(-2), 1},
		{decimal.Decimal64(2), decimal.Decimal64(3), -1},
		{decimal.Decimal64(-3), decimal.Decimal64(-2), -1},
		{decimal.Decimal64(2), decimal.Decimal64(2), 0},
		{decimal.Decimal64(-2), decimal.Decimal64(-2), 0},
	} {
		t.Run("cmp", func(t *testing.T) {
			n := tc.n.Cmp(tc.rhs)
			if got, want := n, tc.want; got != want {
				t.Fatalf("invalid value. got=%v, want=%v", got, want)
			}
		})
	}
}

func TestDecimalRescale(t *testing.T) {
	tests := []struct {
		orig, exp          int32
		oldScale, newScale int32
	}{
		{111, 11100, 0, 2},
		{11100, 111, 2, 0},
		{500000, 5, 6, 1},
		{5, 500000, 1, 6},
		{-111, -11100, 0, 2},
		{-11100, -111, 2, 0},
		{555, 555, 2, 2},
	}

	for _, tt := range tests {
		t.Run("decimal32", func(t *testing.T) {
			out, err := decimal.Decimal32(tt.orig).Rescale(tt.oldScale, tt.newScale)
			require.NoError(t, err)
			assert.Equal(t, decimal.Decimal32(tt.exp), out)
		})
		t.Run("decimal64", func(t *testing.T) {
			out, err := decimal.Decimal64(tt.orig).Rescale(tt.oldScale, tt.newScale)
			require.NoError(t, err)
			assert.Equal(t, decimal.Decimal64(tt.exp), out)
		})
	}

	_, err := decimal.Decimal32(555555).Rescale(6, 1)
	assert.Error(t, err)
	_, err = decimal.Decimal64(555555).Rescale(6, 1)
	assert.Error(t, err)

	_, err = decimal.Decimal32(555555).Rescale(0, 5)
	assert.ErrorContains(t, err, "rescale data loss")
	_, err = decimal.Decimal64(555555).Rescale(0, 5)
	assert.ErrorContains(t, err, "rescale data loss")
}

func TestDecimalIncreaseScale(t *testing.T) {
	assert.Equal(t, decimal.Decimal32(1234), decimal.Decimal32(1234).IncreaseScaleBy(0))
	assert.Equal(t, decimal.Decimal32(1234000), decimal.Decimal32(1234).IncreaseScaleBy(3))
	assert.Equal(t, decimal.Decimal32(-1234000), decimal.Decimal32(-1234).IncreaseScaleBy(3))

	assert.Equal(t, decimal.Decimal64(1234), decimal.Decimal64(1234).IncreaseScaleBy(0))
	assert.Equal(t, decimal.Decimal64(1234000), decimal.Decimal64(1234).IncreaseScaleBy(3))
	assert.Equal(t, decimal.Decimal64(-1234000), decimal.Decimal64(-1234).IncreaseScaleBy(3))
}

func TestDecimalReduceScale(t *testing.T) {
	tests := []struct {
		value    int32
		scale    int32
		round    bool
		expected int32
	}{
		{123456, 0, false, 123456},
		{123456, 1, false, 12345},
		{123456, 1, true, 12346},
		{123451, 1, true, 12345},
		{123789, 2, true, 1238},
		{123749, 2, true, 1237},
		{123750, 2, true, 1238},
		{5, 1, true, 1},
		{0, 1, true, 0},
	}

	for _, tt := range tests {
		assert.Equal(t, decimal.Decimal32(tt.expected),
			decimal.Decimal32(tt.value).ReduceScaleBy(tt.scale, tt.round), "decimal32")
		assert.Equal(t, decimal.Decimal32(tt.expected).Negate(),
			decimal.Decimal32(tt.value).Negate().ReduceScaleBy(tt.scale, tt.round), "decimal32")
		assert.Equal(t, decimal.Decimal64(tt.expected),
			decimal.Decimal64(tt.value).ReduceScaleBy(tt.scale, tt.round), "decimal64")
		assert.Equal(t, decimal.Decimal64(tt.expected).Negate(),
			decimal.Decimal64(tt.value).Negate().ReduceScaleBy(tt.scale, tt.round), "decimal64")
	}
}

func TestDecimalBasics(t *testing.T) {
	tests := []struct {
		lhs, rhs int32
	}{
		{100, 3},
		{200, 3},
		{20100, 301},
		{-20100, 301},
		{20100, -301},
		{-20100, -301},
	}

	for _, tt := range tests {
		assert.EqualValues(t, tt.lhs+tt.rhs,
			decimal.Decimal32(tt.lhs).Add(decimal.Decimal32(tt.rhs)))
		assert.EqualValues(t, tt.lhs+tt.rhs,
			decimal.Decimal64(tt.lhs).Add(decimal.Decimal64(tt.rhs)))

		assert.EqualValues(t, tt.lhs-tt.rhs,
			decimal.Decimal32(tt.lhs).Sub(decimal.Decimal32(tt.rhs)))
		assert.EqualValues(t, tt.lhs-tt.rhs,
			decimal.Decimal64(tt.lhs).Sub(decimal.Decimal64(tt.rhs)))

		assert.EqualValues(t, tt.lhs*tt.rhs,
			decimal.Decimal32(tt.lhs).Mul(decimal.Decimal32(tt.rhs)))
		assert.EqualValues(t, tt.lhs*tt.rhs,
			decimal.Decimal64(tt.lhs).Mul(decimal.Decimal64(tt.rhs)))

		expdiv, expmod := tt.lhs/tt.rhs, tt.lhs%tt.rhs
		div, mod := decimal.Decimal32(tt.lhs).Div(decimal.Decimal32(tt.rhs))
		assert.EqualValues(t, expdiv, div)
		assert.EqualValues(t, expmod, mod)

		div64, mod64 := decimal.Decimal64(tt.lhs).Div(decimal.Decimal64(tt.rhs))
		assert.EqualValues(t, expdiv, div64)
		assert.EqualValues(t, expmod, mod64)
	}
}
