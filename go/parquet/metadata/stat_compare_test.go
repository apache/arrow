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

package metadata

import (
	"encoding/binary"
	"testing"

	"github.com/apache/arrow/go/v14/parquet"
	"github.com/apache/arrow/go/v14/parquet/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSignedByteArrayCompare(t *testing.T) {
	s := ByteArrayStatistics{
		statistics: statistics{
			order: schema.SortSIGNED,
		},
	}

	// signed byte array comparison is only used for Decimal comparison.
	// when decimals are encoded as byte arrays they use twos compliment
	// big-endian encoded values. Comparisons of byte arrays of unequal
	// types need to handle sign extension.

	tests := []struct {
		b     []byte
		order int
	}{
		{[]byte{0x80, 0x80, 0, 0}, 0},
		{[]byte{ /*0xFF,*/ 0x80, 0, 0}, 1},
		{[]byte{0xFF, 0x80, 0, 0}, 1},
		{[]byte{ /*0xFF,*/ 0xFF, 0x01, 0}, 2},
		{[]byte{ /*0xFF, 0xFF,*/ 0x80, 0}, 3},
		{[]byte{ /*0xFF,*/ 0xFF, 0x80, 0}, 3},
		{[]byte{0xFF, 0xFF, 0x80, 0}, 3},
		{[]byte{ /*0xFF,0xFF,0xFF,*/ 0x80}, 4},
		{[]byte{ /*0xFF,0xFF,0xFF*/ 0xFF}, 5},
		{[]byte{ /*0, 0,*/ 0x01, 0x01}, 6},
		{[]byte{ /*0,*/ 0, 0x01, 0x01}, 6},
		{[]byte{0, 0, 0x01, 0x01}, 6},
		{[]byte{ /*0,*/ 0x01, 0x01, 0}, 7},
		{[]byte{0x01, 0x01, 0, 0}, 8},
	}

	for i, tt := range tests {
		// empty array is always the smallest
		assert.Truef(t, s.less(parquet.ByteArray{}, parquet.ByteArray(tt.b)), "case: %d", i)
		assert.Falsef(t, s.less(parquet.ByteArray(tt.b), parquet.ByteArray{}), "case: %d", i)
		// equals is always false
		assert.Falsef(t, s.less(parquet.ByteArray(tt.b), parquet.ByteArray(tt.b)), "case: %d", i)

		for j, case2 := range tests {
			var fn func(assert.TestingT, bool, string, ...interface{}) bool
			if tt.order < case2.order {
				fn = assert.Truef
			} else {
				fn = assert.Falsef
			}
			fn(t, s.less(parquet.ByteArray(tt.b), parquet.ByteArray(case2.b)),
				"%d (order: %d) %d (order: %d)", i, tt.order, j, case2.order)
		}
	}
}

func TestUnsignedByteArrayCompare(t *testing.T) {
	s := ByteArrayStatistics{
		statistics: statistics{
			order: schema.SortUNSIGNED,
		},
	}

	s1ba := parquet.ByteArray("arrange")
	s2ba := parquet.ByteArray("arrangement")
	assert.True(t, s.less(s1ba, s2ba))

	// multi-byte utf-8 characters
	s1ba = parquet.ByteArray("braten")
	s2ba = parquet.ByteArray("bügeln")
	assert.True(t, s.less(s1ba, s2ba))

	s1ba = parquet.ByteArray("ünk123456") // ü = 252
	s2ba = parquet.ByteArray("ănk123456") // ă = 259
	assert.True(t, s.less(s1ba, s2ba))
}

func TestSignedCompareFLBA(t *testing.T) {
	s := FixedLenByteArrayStatistics{
		statistics: statistics{order: schema.SortSIGNED},
	}

	values := []parquet.FixedLenByteArray{
		[]byte{0x80, 0, 0, 0},
		[]byte{0xFF, 0xFF, 0x01, 0},
		[]byte{0xFF, 0xFF, 0x80, 0},
		[]byte{0xFF, 0xFF, 0xFF, 0x80},
		[]byte{0xFF, 0xFF, 0xFF, 0xFF},
		[]byte{0, 0, 0x01, 0x01},
		[]byte{0, 0x01, 0x01, 0},
		[]byte{0x01, 0x01, 0, 0},
	}

	for i, v := range values {
		assert.Falsef(t, s.less(v, v), "%d", i)
		for j, v2 := range values[i+1:] {
			assert.Truef(t, s.less(v, v2), "%d %d", i, j)
			assert.Falsef(t, s.less(v2, v), "%d %d", j, i)
		}
	}
}

func TestUnsignedCompareFLBA(t *testing.T) {
	s := FixedLenByteArrayStatistics{
		statistics: statistics{order: schema.SortUNSIGNED},
	}

	s1flba := parquet.FixedLenByteArray("Anti123456")
	s2flba := parquet.FixedLenByteArray("Bunkd123456")
	assert.True(t, s.less(s1flba, s2flba))

	s1flba = parquet.FixedLenByteArray("Bunk123456")
	s2flba = parquet.FixedLenByteArray("Bünk123456")
	assert.True(t, s.less(s1flba, s2flba))
}

func TestSignedCompareInt96(t *testing.T) {
	s := Int96Statistics{
		statistics: statistics{order: schema.SortSIGNED},
	}

	val := -14

	var (
		a   = parquet.NewInt96([3]uint32{1, 41, 14})
		b   = parquet.NewInt96([3]uint32{1, 41, 42})
		aa  = parquet.NewInt96([3]uint32{1, 41, 14})
		bb  = parquet.NewInt96([3]uint32{1, 41, 14})
		aaa = parquet.NewInt96([3]uint32{1, 41, uint32(val)})
		bbb = parquet.NewInt96([3]uint32{1, 41, 42})
	)

	assert.True(t, s.less(a, b))
	assert.True(t, !s.less(aa, bb) && !s.less(bb, aa))
	assert.True(t, s.less(aaa, bbb))
}

func TestUnsignedCompareInt96(t *testing.T) {
	s := Int96Statistics{
		statistics: statistics{order: schema.SortUNSIGNED},
	}

	valb := -41
	valbb := -14

	var (
		a   = parquet.NewInt96([3]uint32{1, 41, 14})
		b   = parquet.NewInt96([3]uint32{1, uint32(valb), 42})
		aa  = parquet.NewInt96([3]uint32{1, 41, 14})
		bb  = parquet.NewInt96([3]uint32{1, 41, uint32(valbb)})
		aaa parquet.Int96
		bbb parquet.Int96
	)

	assert.True(t, s.less(a, b))
	assert.True(t, s.less(aa, bb))

	binary.LittleEndian.PutUint32(aaa[8:], 2451545) // 2000-01-01
	binary.LittleEndian.PutUint32(bbb[8:], 2451546) // 2000-01-02
	// 12 hours + 34 minutes + 56 seconds
	aaa.SetNanoSeconds(45296000000000)
	// 12 hours + 34 minutes + 50 seconds
	bbb.SetNanoSeconds(45290000000000)
	assert.True(t, s.less(aaa, bbb))

	binary.LittleEndian.PutUint32(aaa[8:], 2451545) // 2000-01-01
	binary.LittleEndian.PutUint32(bbb[8:], 2451545) // 2000-01-01
	// 11 hours + 34 minutes + 56 seconds
	aaa.SetNanoSeconds(41696000000000)
	// 12 hours + 34 minutes + 50 seconds
	bbb.SetNanoSeconds(45290000000000)
	assert.True(t, s.less(aaa, bbb))

	binary.LittleEndian.PutUint32(aaa[8:], 2451545) // 2000-01-01
	binary.LittleEndian.PutUint32(bbb[8:], 2451545) // 2000-01-01
	// 12 hours + 34 minutes + 55 seconds
	aaa.SetNanoSeconds(45295000000000)
	// 12 hours + 34 minutes + 56 seconds
	bbb.SetNanoSeconds(45296000000000)
	assert.True(t, s.less(aaa, bbb))
}

func TestCompareSignedInt64(t *testing.T) {
	var (
		a   int64 = 1
		b   int64 = 4
		aa  int64 = 1
		bb  int64 = 1
		aaa int64 = -1
		bbb int64 = 1
	)

	n := schema.NewInt64Node("signedint64", parquet.Repetitions.Required, -1)
	descr := schema.NewColumn(n, 0, 0)
	s := NewStatistics(descr, nil).(*Int64Statistics)

	assert.True(t, s.less(a, b))
	assert.True(t, !s.less(aa, bb) && !s.less(bb, aa))
	assert.True(t, s.less(aaa, bbb))
}

func TestCompareUnsignedInt64(t *testing.T) {
	var (
		a   int64 = 1
		b   int64 = 4
		aa  int64 = 1
		bb  int64 = 1
		aaa int64 = 1
		bbb int64 = -1
	)

	n, err := schema.NewPrimitiveNodeConverted("unsigned int64", parquet.Repetitions.Required, parquet.Types.Int64, schema.ConvertedTypes.Uint64, 0, 0, 0, 0)
	require.NoError(t, err)
	descr := schema.NewColumn(n, 0, 0)

	assert.Equal(t, schema.SortUNSIGNED, descr.SortOrder())
	s := NewStatistics(descr, nil).(*Int64Statistics)

	assert.True(t, s.less(a, b))
	assert.True(t, !s.less(aa, bb) && !s.less(bb, aa))
	assert.True(t, s.less(aaa, bbb))
}

func TestCompareUnsignedInt32(t *testing.T) {
	var (
		a   int32 = 1
		b   int32 = 4
		aa  int32 = 1
		bb  int32 = 1
		aaa int32 = 1
		bbb int32 = -1
	)

	n, err := schema.NewPrimitiveNodeConverted("unsigned int32", parquet.Repetitions.Required, parquet.Types.Int32, schema.ConvertedTypes.Uint32, 0, 0, 0, 0)
	require.NoError(t, err)
	descr := schema.NewColumn(n, 0, 0)

	assert.Equal(t, schema.SortUNSIGNED, descr.SortOrder())
	s := NewStatistics(descr, nil).(*Int32Statistics)

	assert.True(t, s.less(a, b))
	assert.True(t, !s.less(aa, bb) && !s.less(bb, aa))
	assert.True(t, s.less(aaa, bbb))
}
