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

package array_test

import (
	"fmt"
	"math/big"
	"testing"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/bitutil"
	"github.com/apache/arrow/go/v13/arrow/decimal128"
	"github.com/apache/arrow/go/v13/arrow/decimal256"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/stretchr/testify/suite"
)

type decimalValue interface{}

func bitmapFromSlice(vals []bool) []byte {
	out := make([]byte, int(bitutil.BytesForBits(int64(len(vals)))))
	writer := bitutil.NewBitmapWriter(out, 0, len(vals))
	for _, val := range vals {
		if val {
			writer.Set()
		} else {
			writer.Clear()
		}
		writer.Next()
	}
	writer.Finish()
	return out
}

type DecimalTestSuite struct {
	suite.Suite

	dt  arrow.DataType
	mem *memory.CheckedAllocator
}

func (d *DecimalTestSuite) SetupTest() {
	d.mem = memory.NewCheckedAllocator(memory.DefaultAllocator)
}

func (d *DecimalTestSuite) TearDownTest() {
	d.mem.AssertSize(d.T(), 0)
}

func (d *DecimalTestSuite) makeData(input []decimalValue, out []byte) {
	switch d.dt.ID() {
	case arrow.DECIMAL128:
		for _, v := range input {
			arrow.Decimal128Traits.PutValue(out, v.(decimal128.Num))
			out = out[arrow.Decimal128SizeBytes:]
		}
	case arrow.DECIMAL256:
		for _, v := range input {
			arrow.Decimal256Traits.PutValue(out, v.(decimal256.Num))
			out = out[arrow.Decimal256SizeBytes:]
		}
	}
}

func (d *DecimalTestSuite) testCreate(bitWidth int, prec int32, draw []decimalValue, valids []bool, offset int64) arrow.Array {
	switch bitWidth {
	case 128:
		d.dt = &arrow.Decimal128Type{Precision: prec, Scale: 4}
	case 256:
		d.dt = &arrow.Decimal256Type{Precision: prec, Scale: 4}
	}

	bldr := array.NewBuilder(d.mem, d.dt)
	defer bldr.Release()
	bldr.Reserve(len(draw))

	nullCount := 0
	for i, b := range valids {
		if b {
			switch v := draw[i].(type) {
			case decimal128.Num:
				bldr.(*array.Decimal128Builder).Append(v)
			case decimal256.Num:
				bldr.(*array.Decimal256Builder).Append(v)
			}
		} else {
			bldr.AppendNull()
			nullCount++
		}
	}

	arr := bldr.NewArray()
	d.EqualValues(0, bldr.Len())

	rawBytes := make([]byte, len(draw)*(d.dt.(arrow.FixedWidthDataType).BitWidth()/8))
	d.makeData(draw, rawBytes)

	expectedData := memory.NewBufferBytes(rawBytes)
	expectedNullBitmap := bitmapFromSlice(valids)
	expectedNullCount := len(draw) - bitutil.CountSetBits(expectedNullBitmap, 0, len(valids))

	expected := array.NewData(d.dt, len(valids), []*memory.Buffer{memory.NewBufferBytes(expectedNullBitmap), expectedData}, nil, expectedNullCount, 0)
	defer expected.Release()

	expectedArr := array.MakeFromData(expected)
	defer expectedArr.Release()

	lhs := array.NewSlice(arr, offset, int64(arr.Len())-offset)
	rhs := array.NewSlice(expectedArr, offset, int64(expectedArr.Len())-offset)
	defer func() {
		lhs.Release()
		rhs.Release()
	}()

	d.Truef(array.Equal(lhs, rhs), "expected: %s, got: %s\n", rhs, lhs)
	return arr
}

type Decimal128TestSuite struct {
	DecimalTestSuite
}

func (d *Decimal128TestSuite) runTest(f func(prec int32)) {
	for prec := int32(1); prec <= 38; prec++ {
		d.Run(fmt.Sprintf("prec=%d", prec), func() { f(prec) })
	}
}

func (d *Decimal128TestSuite) TestNoNulls() {
	d.runTest(func(prec int32) {
		draw := []decimalValue{decimal128.FromU64(1), decimal128.FromI64(-2),
			decimal128.FromU64(2389), decimal128.FromU64(4),
			decimal128.FromI64(-12348)}
		valids := []bool{true, true, true, true, true}
		arr := d.testCreate(128, prec, draw, valids, 0)
		arr.Release()
		arr = d.testCreate(128, prec, draw, valids, 2)
		arr.Release()
	})
}

func (d *Decimal128TestSuite) TestWithNulls() {
	d.runTest(func(prec int32) {
		draw := []decimalValue{decimal128.FromU64(1), decimal128.FromU64(2),
			decimal128.FromI64(-1), decimal128.FromI64(4), decimal128.FromI64(-1),
			decimal128.FromI64(1), decimal128.FromI64(2)}
		bigVal, _ := (&big.Int{}).SetString("230342903942234234", 10)
		draw = append(draw, decimal128.FromBigInt(bigVal))

		bigNeg, _ := (&big.Int{}).SetString("-23049302932235234", 10)
		draw = append(draw, decimal128.FromBigInt(bigNeg))

		valids := []bool{true, true, false, true, false, true, true, true, true}
		arr := d.testCreate(128, prec, draw, valids, 0)
		arr.Release()
		arr = d.testCreate(128, prec, draw, valids, 2)
		arr.Release()
	})
}

type Decimal256TestSuite struct {
	DecimalTestSuite
}

func (d *Decimal256TestSuite) runTest(f func(prec int32)) {
	for _, prec := range []int32{1, 2, 5, 10, 38, 39, 40, 75, 76} {
		d.Run(fmt.Sprintf("prec=%d", prec), func() { f(prec) })
	}
}

func (d *Decimal256TestSuite) TestNoNulls() {
	d.runTest(func(prec int32) {
		draw := []decimalValue{decimal256.FromU64(1), decimal256.FromI64(-2),
			decimal256.FromU64(2389), decimal256.FromU64(4),
			decimal256.FromI64(-12348)}
		valids := []bool{true, true, true, true, true}
		arr := d.testCreate(256, prec, draw, valids, 0)
		arr.Release()
		arr = d.testCreate(256, prec, draw, valids, 2)
		arr.Release()
	})
}

func (d *Decimal256TestSuite) TestWithNulls() {
	d.runTest(func(prec int32) {
		draw := []decimalValue{decimal256.FromU64(1), decimal256.FromU64(2),
			decimal256.FromI64(-1), decimal256.FromI64(4), decimal256.FromI64(-1),
			decimal256.FromI64(1), decimal256.FromI64(2)}

		// (pow(2, 255) - 1)
		bigVal, _ := (&big.Int{}).SetString("57896044618658097711785492504343953926634992332820282019728792003956564819967", 10)
		draw = append(draw, decimal256.FromBigInt(bigVal))

		draw = append(draw, decimal256.FromBigInt(bigVal.Neg(bigVal)))

		valids := []bool{true, true, false, true, false, true, true, true, true}
		arr := d.testCreate(256, prec, draw, valids, 0)
		arr.Release()
		arr = d.testCreate(256, prec, draw, valids, 2)
		arr.Release()
	})
}

func TestDecimal(t *testing.T) {
	suite.Run(t, new(Decimal128TestSuite))
	suite.Run(t, new(Decimal256TestSuite))
}
