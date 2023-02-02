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

package decimal256_test

import (
	"fmt"
	"math"
	"math/big"
	"testing"

	"github.com/apache/arrow/go/v12/arrow/decimal256"
	"github.com/stretchr/testify/assert"
)

func TestFromU64(t *testing.T) {
	for _, tc := range []struct {
		v    uint64
		want decimal256.Num
		sign int
	}{
		{0, decimal256.New(0, 0, 0, 0), 0},
		{1, decimal256.New(0, 0, 0, 1), +1},
		{2, decimal256.New(0, 0, 0, 2), +1},
		{math.MaxInt64, decimal256.New(0, 0, 0, math.MaxInt64), +1},
		{math.MaxUint64, decimal256.New(0, 0, 0, math.MaxUint64), +1},
	} {
		t.Run(fmt.Sprintf("%+0#x", tc.v), func(t *testing.T) {
			v := decimal256.FromU64(tc.v)
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
			if got, want := v.Array(), tc.want.Array(); got != want {
				t.Fatalf("invalid array: got=%+0#v, want=%+0#v", got, want)
			}
		})
	}
}

func u64Cnv(i int64) uint64 { return uint64(i) }

func TestFromI64(t *testing.T) {
	for _, tc := range []struct {
		v    int64
		want decimal256.Num
		sign int
	}{
		{0, decimal256.New(0, 0, 0, 0), 0},
		{1, decimal256.New(0, 0, 0, 1), 1},
		{2, decimal256.New(0, 0, 0, 2), 1},
		{math.MaxInt64, decimal256.New(0, 0, 0, math.MaxInt64), 1},
		{math.MinInt64, decimal256.New(math.MaxUint64, math.MaxUint64, math.MaxUint64, u64Cnv(math.MinInt64)), -1},
	} {
		t.Run(fmt.Sprintf("%+0#x", tc.v), func(t *testing.T) {
			v := decimal256.FromI64(tc.v)
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
			if got, want := v.Array(), tc.want.Array(); got != want {
				t.Fatalf("invalid array: got=%+0#v, want=%+0#v", got, want)
			}
		})
	}
}

func TestDecimalToBigInt(t *testing.T) {
	tests := []struct {
		arr [4]uint64
		exp string
	}{
		{[4]uint64{0, 10084168908774762496, 12965995782233477362, 159309191113245227}, "1000000000000000000000000000000000000000000000000000000000000000000000000000"},
		{[4]uint64{0, 8362575164934789120, 5480748291476074253, 18287434882596306388}, "-1000000000000000000000000000000000000000000000000000000000000000000000000000"},
		{[4]uint64{0, 0, 0, 0}, "0"},
		{[4]uint64{17877984925544397504, 5352188884907840935, 234631617561833724, 196678011949953713}, "1234567890123456789012345678901234567890123456789012345678901234567890123456"},
		{[4]uint64{568759148165154112, 13094555188801710680, 18212112456147717891, 18250066061759597902}, "-1234567890123456789012345678901234567890123456789012345678901234567890123456"},
	}
	for _, tc := range tests {
		t.Run("", func(t *testing.T) {
			n := decimal256.New(tc.arr[3], tc.arr[2], tc.arr[1], tc.arr[0])
			bi := n.BigInt()

			assert.Equal(t, tc.exp, bi.String())
			n2 := decimal256.FromBigInt(bi)
			assert.Equal(t, n2.Array(), n.Array())
		})
	}
}

func TestDecimalFromFloat(t *testing.T) {
	tests := []struct {
		val              float64
		precision, scale int32
		expected         string
	}{
		{0, 1, 0, "0"},
		{math.Copysign(0, -1), 1, 0, "0"},
		{0, 19, 4, "0.0000"},
		{math.Copysign(0, -1), 19, 4, "0.0000"},
		{123.0, 7, 4, "123.0000"},
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
		{9.999999999999999e+75, 76, 0, "9999999999999998863663300700064420349597509066704028242075715752105414230016"},
		{-9.999999999999999e+75, 76, 0, "-9999999999999998863663300700064420349597509066704028242075715752105414230016"},
	}

	t.Run("float64", func(t *testing.T) {
		for _, tt := range tests {
			t.Run(tt.expected, func(t *testing.T) {
				n, err := decimal256.FromFloat64(tt.val, tt.precision, tt.scale)
				assert.NoError(t, err)

				assert.Equal(t, tt.expected, big.NewFloat(n.ToFloat64(tt.scale)).Text('f', int(tt.scale)))
			})
		}

		t.Run("large values", func(t *testing.T) {
			// test entire float64 range
			for scale := int32(-308); scale <= 308; scale++ {
				val := math.Pow10(int(scale))
				n, err := decimal256.FromFloat64(val, 1, -scale)
				assert.NoError(t, err)
				assert.Equal(t, "1", n.BigInt().String())
			}

			for scale := int32(-307); scale <= 306; scale++ {
				val := 123 * math.Pow10(int(scale))
				n, err := decimal256.FromFloat64(val, 2, -scale-1)
				assert.NoError(t, err)
				assert.Equal(t, "12", n.BigInt().String())
				n, err = decimal256.FromFloat64(val, 3, -scale)
				assert.NoError(t, err)
				assert.Equal(t, "123", n.BigInt().String())
				n, err = decimal256.FromFloat64(val, 4, -scale+1)
				assert.NoError(t, err)
				assert.Equal(t, "1230", n.BigInt().String())
			}
		})
	})

	t.Run("float32", func(t *testing.T) {
		for _, tt := range tests {
			if tt.precision > 38 {
				continue
			}
			t.Run(tt.expected, func(t *testing.T) {
				n, err := decimal256.FromFloat32(float32(tt.val), tt.precision, tt.scale)
				assert.NoError(t, err)

				assert.Equal(t, tt.expected, big.NewFloat(float64(n.ToFloat32(tt.scale))).Text('f', int(tt.scale)))
			})
		}

		t.Run("large values", func(t *testing.T) {
			// test entire float32 range
			for scale := int32(-38); scale <= 38; scale++ {
				val := float32(math.Pow10(int(scale)))
				n, err := decimal256.FromFloat32(val, 1, -scale)
				assert.NoError(t, err)
				assert.Equal(t, "1", n.BigInt().String())
			}

			for scale := int32(-37); scale <= 36; scale++ {
				val := 123 * float32(math.Pow10(int(scale)))
				n, err := decimal256.FromFloat32(val, 2, -scale-1)
				assert.NoError(t, err)
				assert.Equal(t, "12", n.BigInt().String())
				n, err = decimal256.FromFloat32(val, 3, -scale)
				assert.NoError(t, err)
				assert.Equal(t, "123", n.BigInt().String())
				n, err = decimal256.FromFloat32(val, 4, -scale+1)
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
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s_%d", tt.s, tt.expectedScale), func(t *testing.T) {
			n, err := decimal256.FromString(tt.s, 8, tt.expectedScale)
			assert.NoError(t, err)

			ex := decimal256.FromI64(tt.expected)
			assert.Equal(t, ex, n)
		})
	}
}
