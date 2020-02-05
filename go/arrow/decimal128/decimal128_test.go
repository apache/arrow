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

package decimal128 // import "github.com/apache/arrow/go/arrow/decimal128"

import (
	"fmt"
	"math"
	"math/big"
	"testing"
)

func TestFromU64(t *testing.T) {
	for _, tc := range []struct {
		v    uint64
		want Num
		sign int
	}{
		{0, Num{0, 0}, 0},
		{1, Num{1, 0}, +1},
		{2, Num{2, 0}, +1},
		{math.MaxInt64, Num{math.MaxInt64, 0}, +1},
		{math.MaxUint64, Num{math.MaxUint64, 0}, +1},
	} {
		t.Run(fmt.Sprintf("%+0#x", tc.v), func(t *testing.T) {
			v := FromU64(tc.v)
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
			if got, want := v.LowBits(), tc.want.lo; got != want {
				t.Fatalf("invalid low-bits: got=%+0#x, want=%+0#x", got, want)
			}
			if got, want := v.HighBits(), tc.want.hi; got != want {
				t.Fatalf("invalid high-bits: got=%+0#x, want=%+0#x", got, want)
			}
		})
	}
}

func TestFromI64(t *testing.T) {
	for _, tc := range []struct {
		v    int64
		want Num
		sign int
	}{
		{0, Num{0, 0}, 0},
		{1, Num{1, 0}, 1},
		{2, Num{2, 0}, 1},
		{math.MaxInt64, Num{math.MaxInt64, 0}, 1},
		{math.MinInt64, Num{u64Cnv(math.MinInt64), -1}, -1},
	} {
		t.Run(fmt.Sprintf("%+0#x", tc.v), func(t *testing.T) {
			v := FromI64(tc.v)
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
			if got, want := v.LowBits(), tc.want.lo; got != want {
				t.Fatalf("invalid low-bits: got=%+0#x, want=%+0#x", got, want)
			}
			if got, want := v.HighBits(), tc.want.hi; got != want {
				t.Fatalf("invalid high-bits: got=%+0#x, want=%+0#x", got, want)
			}
		})
	}
}

func u64Cnv(i int64) uint64 { return uint64(i) }
