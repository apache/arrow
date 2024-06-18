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
	"strings"
	"testing"

	"github.com/apache/arrow/go/v17/arrow/decimal256"
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

func TestAdd(t *testing.T) {
	for _, tc := range []struct {
		n    decimal256.Num
		rhs  decimal256.Num
		want decimal256.Num
	}{
		{decimal256.New(0, 0, 0, 1), decimal256.New(0, 0, 0, 2), decimal256.New(0, 0, 0, 3)},
		{decimal256.New(0, 0, 1, 0), decimal256.New(0, 0, 2, 0), decimal256.New(0, 0, 3, 0)},
		{decimal256.New(0, 1, 0, 0), decimal256.New(0, 2, 0, 0), decimal256.New(0, 3, 0, 0)},
		{decimal256.New(1, 0, 0, 0), decimal256.New(2, 0, 0, 0), decimal256.New(3, 0, 0, 0)},
		{decimal256.New(0, 0, 2, 1), decimal256.New(0, 0, 1, 2), decimal256.New(0, 0, 3, 3)},
		{decimal256.New(0, 2, 1, 0), decimal256.New(0, 1, 2, 0), decimal256.New(0, 3, 3, 0)},
		{decimal256.New(2, 1, 0, 0), decimal256.New(1, 2, 0, 0), decimal256.New(3, 3, 0, 0)},
		{decimal256.New(0, 0, 0, 1), decimal256.New(0, 0, 0, math.MaxUint64), decimal256.New(0, 0, 1, 0)},
		{decimal256.New(0, 0, 0, math.MaxUint64), decimal256.New(0, 0, 0, 1), decimal256.New(0, 0, 1, 0)},
		{decimal256.New(0, 0, 1, 0), decimal256.New(0, 0, math.MaxUint64, 0), decimal256.New(0, 1, 0, 0)},
		{decimal256.New(0, 0, math.MaxUint64, 0), decimal256.New(0, 0, 1, 0), decimal256.New(0, 1, 0, 0)},
		{decimal256.New(0, 1, 0, 0), decimal256.New(0, math.MaxUint64, 0, 0), decimal256.New(1, 0, 0, 0)},
		{decimal256.New(0, math.MaxUint64, 0, 0), decimal256.New(0, 1, 0, 0), decimal256.New(1, 0, 0, 0)},
		{decimal256.New(0, 0, 0, 1), decimal256.New(0, 0, 0, 0), decimal256.New(0, 0, 0, 1)},
		{decimal256.New(0, 0, 0, 0), decimal256.New(0, 0, 0, 1), decimal256.New(0, 0, 0, 1)},
		{decimal256.New(0, 0, 1, 0), decimal256.New(0, 0, 0, 0), decimal256.New(0, 0, 1, 0)},
		{decimal256.New(0, 0, 0, 0), decimal256.New(0, 0, 1, 0), decimal256.New(0, 0, 1, 0)},
		{decimal256.New(0, 1, 0, 0), decimal256.New(0, 0, 0, 0), decimal256.New(0, 1, 0, 0)},
		{decimal256.New(0, 0, 0, 0), decimal256.New(0, 1, 0, 0), decimal256.New(0, 1, 0, 0)},
		{decimal256.New(1, 0, 0, 0), decimal256.New(0, 0, 0, 0), decimal256.New(1, 0, 0, 0)},
		{decimal256.New(0, 0, 0, 0), decimal256.New(1, 0, 0, 0), decimal256.New(1, 0, 0, 0)},
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
		n    decimal256.Num
		rhs  decimal256.Num
		want decimal256.Num
	}{
		{decimal256.New(0, 0, 0, 3), decimal256.New(0, 0, 0, 2), decimal256.New(0, 0, 0, 1)},
		{decimal256.New(0, 0, 3, 0), decimal256.New(0, 0, 2, 0), decimal256.New(0, 0, 1, 0)},
		{decimal256.New(0, 3, 0, 0), decimal256.New(0, 2, 0, 0), decimal256.New(0, 1, 0, 0)},
		{decimal256.New(3, 0, 0, 0), decimal256.New(2, 0, 0, 0), decimal256.New(1, 0, 0, 0)},
		{decimal256.New(0, 0, 3, 3), decimal256.New(0, 0, 1, 2), decimal256.New(0, 0, 2, 1)},
		{decimal256.New(0, 3, 3, 0), decimal256.New(0, 1, 2, 0), decimal256.New(0, 2, 1, 0)},
		{decimal256.New(3, 3, 0, 0), decimal256.New(1, 2, 0, 0), decimal256.New(2, 1, 0, 0)},
		{decimal256.New(0, 0, 0, 0), decimal256.New(0, 0, 0, math.MaxUint64), decimal256.New(math.MaxUint64, math.MaxUint64, math.MaxUint64, 1)},
		{decimal256.New(0, 0, 1, 0), decimal256.New(0, 0, 0, math.MaxUint64), decimal256.New(0, 0, 0, 1)},
		{decimal256.New(0, 0, 0, 0), decimal256.New(0, 0, math.MaxUint64, 0), decimal256.New(math.MaxUint64, math.MaxUint64, 1, 0)},
		{decimal256.New(0, 1, 0, 0), decimal256.New(0, 0, math.MaxUint64, 0), decimal256.New(0, 0, 1, 0)},
		{decimal256.New(0, 0, 0, 0), decimal256.New(0, math.MaxUint64, 0, 0), decimal256.New(math.MaxUint64, 1, 0, 0)},
		{decimal256.New(1, 0, 0, 0), decimal256.New(0, math.MaxUint64, 0, 0), decimal256.New(0, 1, 0, 0)},
		{decimal256.New(0, 0, 0, 1), decimal256.New(0, 0, 0, 0), decimal256.New(0, 0, 0, 1)},
		{decimal256.New(0, 0, 1, 0), decimal256.New(0, 0, 0, 0), decimal256.New(0, 0, 1, 0)},
		{decimal256.New(0, 1, 0, 0), decimal256.New(0, 0, 0, 0), decimal256.New(0, 1, 0, 0)},
		{decimal256.New(1, 0, 0, 0), decimal256.New(0, 0, 0, 0), decimal256.New(1, 0, 0, 0)},
		{decimal256.New(0, 0, 0, 0), decimal256.New(0, 0, 0, 1), decimal256.New(math.MaxUint64, math.MaxUint64, math.MaxUint64, math.MaxUint64)},
		{decimal256.New(0, 0, 0, 0), decimal256.New(0, 0, 1, 0), decimal256.New(math.MaxUint64, math.MaxUint64, math.MaxUint64, 0)},
		{decimal256.New(0, 0, 0, 0), decimal256.New(0, 1, 0, 0), decimal256.New(math.MaxUint64, math.MaxUint64, 0, 0)},
		{decimal256.New(0, 0, 0, 0), decimal256.New(1, 0, 0, 0), decimal256.New(math.MaxUint64, 0, 0, 0)},
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
		n    decimal256.Num
		rhs  decimal256.Num
		want decimal256.Num
	}{
		{decimal256.New(0, 0, 0, 2), decimal256.New(0, 0, 0, 3), decimal256.New(0, 0, 0, 6)},
		{decimal256.New(0, 0, 2, 0), decimal256.New(0, 0, 0, 3), decimal256.New(0, 0, 6, 0)},
		{decimal256.New(0, 2, 0, 0), decimal256.New(0, 0, 0, 3), decimal256.New(0, 6, 0, 0)},
		{decimal256.New(2, 0, 0, 0), decimal256.New(0, 0, 0, 3), decimal256.New(6, 0, 0, 0)},
		{decimal256.New(0, 0, 3, 3), decimal256.New(0, 0, 0, 2), decimal256.New(0, 0, 6, 6)},
		{decimal256.New(0, 3, 3, 0), decimal256.New(0, 0, 0, 2), decimal256.New(0, 6, 6, 0)},
		{decimal256.New(3, 3, 0, 0), decimal256.New(0, 0, 0, 2), decimal256.New(6, 6, 0, 0)},
		{decimal256.New(0, 0, 0, 2), decimal256.New(0, 0, 3, 3), decimal256.New(0, 0, 6, 6)},
		{decimal256.New(0, 0, 2, 0), decimal256.New(0, 0, 3, 3), decimal256.New(0, 6, 6, 0)},
		{decimal256.New(0, 2, 0, 0), decimal256.New(0, 0, 3, 3), decimal256.New(6, 6, 0, 0)},
		{decimal256.New(0, 0, 0, 2), decimal256.New(0, 0, 0, math.MaxUint64), decimal256.New(0, 0, 1, math.MaxUint64-1)},
		{decimal256.New(0, 0, 0, 2), decimal256.New(0, 0, math.MaxUint64, 0), decimal256.New(0, 1, math.MaxUint64-1, 0)},
		{decimal256.New(0, 0, 0, 2), decimal256.New(0, math.MaxUint64, 0, 0), decimal256.New(1, math.MaxUint64-1, 0, 0)},
		{decimal256.New(0, 0, 0, 1), decimal256.New(0, 0, 0, 0), decimal256.New(0, 0, 0, 0)},
		{decimal256.New(0, 0, 1, 0), decimal256.New(0, 0, 0, 0), decimal256.New(0, 0, 0, 0)},
		{decimal256.New(0, 1, 0, 0), decimal256.New(0, 0, 0, 0), decimal256.New(0, 0, 0, 0)},
		{decimal256.New(1, 0, 0, 0), decimal256.New(0, 0, 0, 0), decimal256.New(0, 0, 0, 0)},
		{decimal256.New(0, 0, 0, 0), decimal256.New(0, 0, 0, 1), decimal256.New(0, 0, 0, 0)},
		{decimal256.New(0, 0, 0, 0), decimal256.New(0, 0, 1, 0), decimal256.New(0, 0, 0, 0)},
		{decimal256.New(0, 0, 0, 0), decimal256.New(0, 1, 0, 0), decimal256.New(0, 0, 0, 0)},
		{decimal256.New(0, 0, 0, 0), decimal256.New(1, 0, 0, 0), decimal256.New(0, 0, 0, 0)},
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
		n        decimal256.Num
		rhs      decimal256.Num
		want_res decimal256.Num
		want_rem decimal256.Num
	}{
		{decimal256.New(0, 0, 0, 3), decimal256.New(0, 0, 0, 2), decimal256.New(0, 0, 0, 1), decimal256.New(0, 0, 0, 1)},
		{decimal256.New(0, 0, 3, 0), decimal256.New(0, 0, 2, 0), decimal256.New(0, 0, 0, 1), decimal256.New(0, 0, 1, 0)},
		{decimal256.New(0, 3, 0, 0), decimal256.New(0, 2, 0, 0), decimal256.New(0, 0, 0, 1), decimal256.New(0, 1, 0, 0)},
		{decimal256.New(3, 0, 0, 0), decimal256.New(2, 0, 0, 0), decimal256.New(0, 0, 0, 1), decimal256.New(1, 0, 0, 0)},
		{decimal256.New(0, 0, 3, 2), decimal256.New(0, 0, 2, 3), decimal256.New(0, 0, 0, 1), decimal256.New(0, 0, 0, math.MaxUint64)},
		{decimal256.New(0, 3, 2, 0), decimal256.New(0, 2, 3, 0), decimal256.New(0, 0, 0, 1), decimal256.New(0, 0, math.MaxUint64, 0)},
		{decimal256.New(3, 2, 0, 0), decimal256.New(2, 3, 0, 0), decimal256.New(0, 0, 0, 1), decimal256.New(0, math.MaxUint64, 0, 0)},
		{decimal256.New(0, 0, 0, math.MaxUint64), decimal256.New(0, 0, 0, 1), decimal256.New(0, 0, 0, math.MaxUint64), decimal256.New(0, 0, 0, 0)},
		{decimal256.New(0, 0, math.MaxUint64, 0), decimal256.New(0, 0, 0, 1), decimal256.New(0, 0, math.MaxUint64, 0), decimal256.New(0, 0, 0, 0)},
		{decimal256.New(0, math.MaxUint64, 0, 0), decimal256.New(0, 0, 0, 1), decimal256.New(0, math.MaxUint64, 0, 0), decimal256.New(0, 0, 0, 0)},
		{decimal256.New(math.MaxUint64, 0, 0, 0), decimal256.New(0, 0, 0, 1), decimal256.New(math.MaxUint64, 0, 0, 0), decimal256.New(0, 0, 0, 0)},
		{decimal256.New(0, 0, 0, 0), decimal256.New(0, 0, 0, 1), decimal256.New(0, 0, 0, 0), decimal256.New(0, 0, 0, 0)},
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
		n    decimal256.Num
		rhs  decimal256.Num
		want decimal256.Num
	}{
		{decimal256.New(0, 0, 0, 2), decimal256.New(0, 0, 0, 3), decimal256.New(0, 0, 0, 8)},
		{decimal256.New(0, 0, 2, 0), decimal256.New(0, 0, 0, 3), decimal256.New(8, 0, 0, 0)},
		{decimal256.New(0, 0, 2, 2), decimal256.New(0, 0, 0, 3), decimal256.New(8, 24, 24, 8)},
		{decimal256.New(0, 0, 0, 1), decimal256.New(0, 0, 0, 0), decimal256.New(0, 0, 0, 1)},
		{decimal256.New(0, 0, 0, 0), decimal256.New(0, 0, 0, 1), decimal256.New(0, 0, 0, 0)},
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
		n    decimal256.Num
		rhs  []decimal256.Num
		want decimal256.Num
	}{
		{decimal256.New(0, 0, 0, 2), []decimal256.Num{decimal256.New(8, 4, 2, 1), decimal256.New(9, 0, 0, 8), decimal256.New(0, 17, 0, 0)}, decimal256.New(9, 0, 0, 8)},
		{decimal256.New(0, 0, 0, 10), []decimal256.Num{decimal256.New(0, 4, 0, 1), decimal256.New(0, 0, 0, 8), decimal256.New(0, 0, 3, 0)}, decimal256.New(0, 4, 0, 1)},
	} {
		t.Run("max", func(t *testing.T) {
			n := decimal256.Max(tc.n, tc.rhs...)
			if got, want := n, tc.want; got != want {
				t.Fatalf("invalid value. got=%v, want=%v", got, want)
			}
		})
	}
}

func TestMin(t *testing.T) {
	for _, tc := range []struct {
		n    decimal256.Num
		rhs  []decimal256.Num
		want decimal256.Num
	}{
		{decimal256.New(0, 0, 0, 2), []decimal256.Num{decimal256.New(8, 4, 2, 1), decimal256.New(9, 0, 0, 8), decimal256.New(0, 17, 0, 0)}, decimal256.New(0, 0, 0, 2)},
		{decimal256.New(0, 0, 0, 10), []decimal256.Num{decimal256.New(0, 4, 0, 1), decimal256.New(0, 0, 0, 8), decimal256.New(0, 0, 3, 0)}, decimal256.New(0, 0, 0, 8)},
	} {
		t.Run("min", func(t *testing.T) {
			n := decimal256.Min(tc.n, tc.rhs...)
			if got, want := n, tc.want; got != want {
				t.Fatalf("invalid value. got=%v, want=%v", got, want)
			}
		})
	}
}

func TestGreater(t *testing.T) {
	for _, tc := range []struct {
		n    decimal256.Num
		rhs  decimal256.Num
		want bool
	}{
		{decimal256.New(0, 0, 0, 2), decimal256.New(0, 0, 0, 1), true},
		{decimal256.New(0, 0, 2, 0), decimal256.New(0, 0, 1, 0), true},
		{decimal256.New(0, 2, 0, 0), decimal256.New(0, 1, 0, 0), true},
		{decimal256.New(2, 0, 0, 0), decimal256.New(1, 0, 0, 0), true},
		{decimal256.New(0, 0, 0, 2), decimal256.New(0, 0, 0, 3), false},
		{decimal256.New(0, 0, 2, 0), decimal256.New(0, 0, 3, 0), false},
		{decimal256.New(0, 2, 0, 0), decimal256.New(0, 3, 0, 0), false},
		{decimal256.New(2, 0, 0, 0), decimal256.New(3, 0, 0, 0), false},
		{decimal256.New(0, 0, 0, 2), decimal256.New(0, 0, 0, 2), false},
		{decimal256.New(0, 0, 2, 0), decimal256.New(0, 0, 2, 0), false},
		{decimal256.New(0, 2, 0, 0), decimal256.New(0, 2, 0, 0), false},
		{decimal256.New(2, 0, 0, 0), decimal256.New(2, 0, 0, 0), false},
		{decimal256.New(0, 0, 2, math.MaxUint64), decimal256.New(0, 0, 2, 1), true},
		{decimal256.New(0, 2, math.MaxUint64, 0), decimal256.New(0, 2, 1, 0), true},
		{decimal256.New(2, math.MaxUint64, 0, 0), decimal256.New(2, 1, 0, 0), true},
		{decimal256.New(0, 0, 2, math.MaxUint64), decimal256.New(0, 0, 3, 1), false},
		{decimal256.New(0, 2, math.MaxUint64, 0), decimal256.New(0, 3, 1, 0), false},
		{decimal256.New(2, math.MaxUint64, 0, 0), decimal256.New(3, 1, 0, 0), false},
		{decimal256.New(0, 0, 2, math.MaxUint64), decimal256.New(0, 0, 2, math.MaxUint64), false},
		{decimal256.New(0, 2, math.MaxUint64, 0), decimal256.New(0, 2, math.MaxUint64, 0), false},
		{decimal256.New(2, math.MaxUint64, 0, 0), decimal256.New(2, math.MaxUint64, 0, 0), false},
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
		n    decimal256.Num
		rhs  decimal256.Num
		want bool
	}{
		{decimal256.New(0, 0, 0, 2), decimal256.New(0, 0, 0, 1), false},
		{decimal256.New(0, 0, 2, 0), decimal256.New(0, 0, 1, 0), false},
		{decimal256.New(0, 2, 0, 0), decimal256.New(0, 1, 0, 0), false},
		{decimal256.New(2, 0, 0, 0), decimal256.New(1, 0, 0, 0), false},
		{decimal256.New(0, 0, 0, 2), decimal256.New(0, 0, 0, 3), true},
		{decimal256.New(0, 0, 2, 0), decimal256.New(0, 0, 3, 0), true},
		{decimal256.New(0, 2, 0, 0), decimal256.New(0, 3, 0, 0), true},
		{decimal256.New(2, 0, 0, 0), decimal256.New(3, 0, 0, 0), true},
		{decimal256.New(0, 0, 0, 2), decimal256.New(0, 0, 0, 2), false},
		{decimal256.New(0, 0, 2, 0), decimal256.New(0, 0, 2, 0), false},
		{decimal256.New(0, 2, 0, 0), decimal256.New(0, 2, 0, 0), false},
		{decimal256.New(2, 0, 0, 0), decimal256.New(2, 0, 0, 0), false},
		{decimal256.New(0, 0, 2, math.MaxUint64), decimal256.New(0, 0, 2, 1), false},
		{decimal256.New(0, 2, math.MaxUint64, 0), decimal256.New(0, 2, 1, 0), false},
		{decimal256.New(2, math.MaxUint64, 0, 0), decimal256.New(2, 1, 0, 0), false},
		{decimal256.New(0, 0, 2, math.MaxUint64), decimal256.New(0, 0, 3, 1), true},
		{decimal256.New(0, 2, math.MaxUint64, 0), decimal256.New(0, 3, 1, 0), true},
		{decimal256.New(2, math.MaxUint64, 0, 0), decimal256.New(3, 1, 0, 0), true},
		{decimal256.New(0, 0, 2, math.MaxUint64), decimal256.New(0, 0, 2, math.MaxUint64), false},
		{decimal256.New(0, 2, math.MaxUint64, 0), decimal256.New(0, 2, math.MaxUint64, 0), false},
		{decimal256.New(2, math.MaxUint64, 0, 0), decimal256.New(2, math.MaxUint64, 0, 0), false},
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
		n    decimal256.Num
		rhs  decimal256.Num
		want int
	}{
		{decimal256.New(0, 0, 0, 2), decimal256.New(0, 0, 0, 1), 1},
		{decimal256.New(0, 0, 2, 0), decimal256.New(0, 0, 1, 0), 1},
		{decimal256.New(0, 2, 0, 0), decimal256.New(0, 1, 0, 0), 1},
		{decimal256.New(2, 0, 0, 0), decimal256.New(1, 0, 0, 0), 1},
		{decimal256.New(0, 0, 0, 2), decimal256.New(0, 0, 0, 3), -1},
		{decimal256.New(0, 0, 2, 0), decimal256.New(0, 0, 3, 0), -1},
		{decimal256.New(0, 2, 0, 0), decimal256.New(0, 3, 0, 0), -1},
		{decimal256.New(2, 0, 0, 0), decimal256.New(3, 0, 0, 0), -1},
		{decimal256.New(0, 0, 0, 2), decimal256.New(0, 0, 0, 2), 0},
		{decimal256.New(0, 0, 2, 0), decimal256.New(0, 0, 2, 0), 0},
		{decimal256.New(0, 2, 0, 0), decimal256.New(0, 2, 0, 0), 0},
		{decimal256.New(2, 0, 0, 0), decimal256.New(2, 0, 0, 0), 0},
		{decimal256.New(0, 0, 2, math.MaxUint64), decimal256.New(0, 0, 2, 1), 1},
		{decimal256.New(0, 2, math.MaxUint64, 0), decimal256.New(0, 2, 1, 0), 1},
		{decimal256.New(2, math.MaxUint64, 0, 0), decimal256.New(2, 1, 0, 0), 1},
		{decimal256.New(0, 0, 2, math.MaxUint64), decimal256.New(0, 0, 3, 1), -1},
		{decimal256.New(0, 2, math.MaxUint64, 0), decimal256.New(0, 3, 1, 0), -1},
		{decimal256.New(2, math.MaxUint64, 0, 0), decimal256.New(3, 1, 0, 0), -1},
		{decimal256.New(0, 0, 2, math.MaxUint64), decimal256.New(0, 0, 2, math.MaxUint64), 0},
		{decimal256.New(0, 2, math.MaxUint64, 0), decimal256.New(0, 2, math.MaxUint64, 0), 0},
		{decimal256.New(2, math.MaxUint64, 0, 0), decimal256.New(2, math.MaxUint64, 0, 0), 0},
	} {
		t.Run("cmp", func(t *testing.T) {
			n := tc.n.Cmp(tc.rhs)
			if got, want := n, tc.want; got != want {
				t.Fatalf("invalid value. got=%v, want=%v", got, want)
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
		{"2112.33", 211233, 2},
		{"-2112.33", -211233, 2},
		{"12E2", 12, -2},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s_%d", tt.s, tt.expectedScale), func(t *testing.T) {
			n, err := decimal256.FromString(tt.s, 35, tt.expectedScale)
			assert.NoError(t, err)

			ex := decimal256.FromI64(tt.expected)
			assert.Equal(t, ex, n)
		})
	}
}

// Test issues from GH-38395
func TestToString(t *testing.T) {
	const decStr = "3379334159166193114608287418738414931564221155305735605033949613740461239999"

	integer, _ := (&big.Int{}).SetString(decStr, 10)
	dec := decimal256.FromBigInt(integer)

	expected := "0." + decStr
	assert.Equal(t, expected, dec.ToString(int32(len(decStr))))
	assert.Equal(t, decStr+"0000", dec.ToString(-4))
}

// Test issues from GH-38395
func TestHexFromString(t *testing.T) {
	const decStr = "11111111111111111111111111111111111111.00000000000000000000000000000000000000"

	num, err := decimal256.FromString(decStr, 76, 38)
	if err != nil {
		t.Error(err)
	} else if decStr != num.ToString(38) {
		t.Errorf("expected: %s, actual: %s\n", decStr, num.ToString(38))

		actualCoeff := num.BigInt()
		expectedCoeff, _ := (&big.Int{}).SetString(strings.Replace(decStr, ".", "", -1), 10)
		t.Errorf("expected(hex): %X, actual(hex): %X\n", expectedCoeff.Bytes(), actualCoeff.Bytes())
	}
}

func TestBitLen(t *testing.T) {
	n := decimal256.GetScaleMultiplier(76)
	b := n.BigInt()
	b.Mul(b, big.NewInt(25))
	assert.Greater(t, b.BitLen(), 255)

	assert.Panics(t, func() {
		decimal256.FromBigInt(b)
	})

	_, err := decimal256.FromString(b.String(), decimal256.MaxPrecision, 0)
	assert.ErrorContains(t, err, "bitlen too large for decimal256")
	_, err = decimal256.FromString(b.String(), decimal256.MaxPrecision, -1)
	assert.ErrorContains(t, err, "bitlen too large for decimal256")
}
