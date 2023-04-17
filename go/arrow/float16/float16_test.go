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

package float16

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFloat16(t *testing.T) {
	cases := map[Num]float32{
		{bits: 0x3c00}: 1,
		{bits: 0x4000}: 2,
		{bits: 0xc000}: -2,
		{bits: 0x0000}: 0,
		{bits: 0x5b8f}: 241.875,
		{bits: 0xdb8f}: -241.875,
		{bits: 0x48c8}: 9.5625,
		{bits: 0xc8c8}: -9.5625,
	}
	for k, v := range cases {
		f := k.Float32()
		assert.Equal(t, v, f, "float32 values should be the same")
		i := New(v)
		assert.Equal(t, k.bits, i.bits, "float16 values should be the same")
		assert.Equal(t, k.Uint16(), i.Uint16(), "float16 values should be the same")
		assert.Equal(t, k.String(), fmt.Sprintf("%v", v), "string representation differ")
	}
}

func TestAdd(t *testing.T) {
	for _, tc := range []struct {
		n    Num
		rhs  Num
		want Num
	}{
		{Num{bits: 0x0000}, Num{bits: 0x0000}, Num{bits: 0x0000}}, // 0 + 0 = 0
		{Num{bits: 0x3c00}, Num{bits: 0x4000}, Num{bits: 0x4200}}, // 1 + 2 = 3
		{Num{bits: 0x4248}, Num{bits: 0x3245}, Num{bits: 0x42AC}}, // 3.141 + 0.196 = 3.336
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
		n    Num
		rhs  Num
		want Num
	}{
		{Num{bits: 0x0000}, Num{bits: 0x0000}, Num{bits: 0x0000}}, // 0 - 0 = 0
		{Num{bits: 0x3c00}, Num{bits: 0x4000}, Num{bits: 0xBC00}}, // 1 - 2 = -1
		{Num{bits: 0x4248}, Num{bits: 0x3245}, Num{bits: 0x41E3}}, // 3.141 - 0.196 = 2.944
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
		n    Num
		rhs  Num
		want Num
	}{
		{Num{bits: 0x0000}, Num{bits: 0x0000}, Num{bits: 0x0000}}, // 0 * 0 = 0
		{Num{bits: 0x3c00}, Num{bits: 0x4000}, Num{bits: 0x4000}}, // 1 * 2 = 2
		{Num{bits: 0x4248}, Num{bits: 0x3245}, Num{bits: 0x38EC}}, // 3.141 * 0.196 = 0.6153
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
		n    Num
		rhs  Num
		want Num
	}{
		{Num{bits: 0x0000}, Num{bits: 0x3c00}, Num{bits: 0x0000}}, // 0 / 1 = 0
		{Num{bits: 0x3c00}, Num{bits: 0x4000}, Num{bits: 0x3800}}, // 1 / 2 = 0.5
		{Num{bits: 0x4248}, Num{bits: 0x3245}, Num{bits: 0x4C01}}, // 3.141 * 0.196 = 16.02
	} {
		t.Run("div", func(t *testing.T) {
			n := tc.n.Div(tc.rhs)
			if got, want := n, tc.want; got != want {
				t.Fatalf("invalid value. got=%v, want=%v", got, want)
			}
		})
	}
}

func TestGreater(t *testing.T) {
	for _, tc := range []struct {
		n    Num
		rhs  Num
		want bool
	}{
		{Num{bits: 0x3c00}, Num{bits: 0x4000}, false}, // 1 > 2 = false
		{Num{bits: 0x4900}, Num{bits: 0x4900}, false}, // 10 == 10 = false
		{Num{bits: 0x4248}, Num{bits: 0x3245}, true},  // 3.141 > 0.196 = true
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
		n    Num
		rhs  Num
		want bool
	}{
		{Num{bits: 0x3c00}, Num{bits: 0x4000}, true},  // 1 < 2 = true
		{Num{bits: 0x4900}, Num{bits: 0x4900}, false}, // 10 == 10 = false
		{Num{bits: 0x4248}, Num{bits: 0x3245}, false}, // 3.141 < 0.196 = false
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
		n    Num
		rhs  Num
		want int
	}{
		{Num{bits: 0x3c00}, Num{bits: 0x4000}, -1}, // cmp(1, 2) = -1
		{Num{bits: 0x4900}, Num{bits: 0x4900}, 0}, // cmp(10, 10) = 0
		{Num{bits: 0x4248}, Num{bits: 0x3245}, 1},  // cmp(3.141, 0.196) = 1
	} {
		t.Run("cmp", func(t *testing.T) {
			n := tc.n.Cmp(tc.rhs)
			if got, want := n, tc.want; got != want {
				t.Fatalf("invalid value. got=%v, want=%v", got, want)
			}
		})
	}
}

func TestMax(t *testing.T) {
	for _, tc := range []struct {
		n    Num
		rhs  []Num
		want Num
	}{
		{Num{bits: 0x3c00}, []Num{{bits: 0x4000}, {bits: 0x4580}, {bits: 0x3C00}, {bits: 0x4247}}, Num{bits: 0x4580}}, // max(2, 5.5, 1, 3.14) = 5.5
		{Num{bits: 0x4248}, []Num{{bits: 0xC000}, {bits: 0xC580}, {bits: 0x3C00}, {bits: 0x4247}}, Num{bits: 0x4248}}, // max(-2, -5.5, 1, 3.14) = 3.14
	} {
		t.Run("max", func(t *testing.T) {
			n := Max(tc.n, tc.rhs...)
			if got, want := n, tc.want; got != want {
				t.Fatalf("invalid value. got=%v, want=%v", got, want)
			}
		})
	}
}

func TestMin(t *testing.T) {
	for _, tc := range []struct {
		n    Num
		rhs  []Num
		want Num
	}{
		{Num{bits: 0x3c00}, []Num{{bits: 0x4000}, {bits: 0x4580}, {bits: 0x3C00}, {bits: 0x4247}}, Num{bits: 0x3C00}}, // min(2, 5.5, 1, 3.14) = 1
		{Num{bits: 0x4248}, []Num{{bits: 0x4000}, {bits: 0xC580}, {bits: 0xBC00}, {bits: 0x4247}}, Num{bits: 0xC580}}, // min(2, -5.5, -1, 3.14) = -5.5
	} {
		t.Run("min", func(t *testing.T) {
			n := Min(tc.n, tc.rhs...)
			if got, want := n, tc.want; got != want {
				t.Fatalf("invalid value. got=%v, want=%v", got, want)
			}
		})
	}
}

func TestAbs(t *testing.T) {
	for _, tc := range []struct {
		n    Num
		want Num
	}{
		{Num{bits: 0x4580}, Num{bits: 0x4580}}, // 5.5
		{Num{bits: 0x0000}, Num{bits: 0x0000}}, // 0
		{Num{bits: 0xC580}, Num{bits: 0x4580}}, // -5.5
	} {
		t.Run("abs", func(t *testing.T) {
			n := tc.n.Abs()
			if got, want := n, tc.want; got != want {
				t.Fatalf("invalid value. got=%v, want=%v", got, want)
			}
		})
	}
}

func TestSign(t *testing.T) {
	for _, tc := range []struct {
		n    Num
		want int
	}{
		{Num{bits: 0x4580}, 1},  // 5.5
		{Num{bits: 0x0000}, 0},  // 0
		{Num{bits: 0xC580}, -1}, // -5.5
	} {
		t.Run("sign", func(t *testing.T) {
			n := tc.n.Sign()
			if got, want := n, tc.want; got != want {
				t.Fatalf("invalid value. got=%v, want=%v", got, want)
			}
		})
	}
}
