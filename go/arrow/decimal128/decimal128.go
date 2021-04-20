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
	"encoding/binary"
	"math/big"
)

var (
	MaxDecimal128 = New(542101086242752217, 687399551400673280-1)
)

// Num represents a signed 128-bit integer in two's complement.
// Calculations wrap around and overflow is ignored.
//
// For a discussion of the algorithms, look at Knuth's volume 2,
// Semi-numerical Algorithms section 4.3.1.
//
// Adapted from the Apache ORC C++ implementation
type Num struct {
	lo uint64 // low bits
	hi int64  // high bits
}

// New returns a new signed 128-bit integer value.
func New(hi int64, lo uint64) Num {
	return Num{lo: lo, hi: hi}
}

// FromU64 returns a new signed 128-bit integer value from the provided uint64 one.
func FromU64(v uint64) Num {
	return New(0, v)
}

// FromI64 returns a new signed 128-bit integer value from the provided int64 one.
func FromI64(v int64) Num {
	switch {
	case v > 0:
		return New(0, uint64(v))
	case v < 0:
		return New(-1, uint64(v))
	default:
		return Num{}
	}
}

func fromBigIntPositive(v *big.Int) Num {
	var buf [16]byte
	v.FillBytes(buf[:])
	return Num{
		lo: binary.BigEndian.Uint64(buf[8:]),
		hi: int64(binary.BigEndian.Uint64(buf[:8])),
	}
}

func FromBigInt(v *big.Int) Num {
	if v.Sign() < 0 {
		n := fromBigIntPositive((&big.Int{}).Abs(v))
		n.lo = ^n.lo + 1
		n.hi = ^n.hi
		if n.lo == 0 {
			n.hi += 1
		}
		return n
	}
	return fromBigIntPositive(v)
}

// LowBits returns the low bits of the two's complement representation of the number.
func (n Num) LowBits() uint64 { return n.lo }

// HighBits returns the high bits of the two's complement representation of the number.
func (n Num) HighBits() int64 { return n.hi }

// Sign returns:
//
// -1 if x <  0
//  0 if x == 0
// +1 if x >  0
func (n Num) Sign() int {
	if n == (Num{}) {
		return 0
	}
	return int(1 | (n.hi >> 63))
}

func toBigInt(n Num) *big.Int {
	hi := big.NewInt(n.hi)
	return hi.Lsh(hi, 64).Add(hi, (&big.Int{}).SetUint64(n.lo))
}

func (n Num) BigInt() *big.Int {
	if n.Sign() < 0 {
		n.lo = ^n.lo + 1
		n.hi = ^n.hi
		if n.lo == 0 {
			n.hi += 1
		}
		ret := toBigInt(n)
		return ret.Neg(ret)
	}
	return toBigInt(n)
}
