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

package decimal128_test // import "github.com/apache/arrow/go/arrow/decimal128"

import (
	"fmt"
	"math"
	"math/big"
	"testing"

	"github.com/apache/arrow/go/arrow/decimal128"
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
