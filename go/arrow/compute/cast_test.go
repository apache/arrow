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

//go:build go1.18

package compute_test

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/bitutil"
	"github.com/apache/arrow/go/v12/arrow/compute"
	"github.com/apache/arrow/go/v12/arrow/decimal128"
	"github.com/apache/arrow/go/v12/arrow/decimal256"
	"github.com/apache/arrow/go/v12/arrow/internal/testing/gen"
	"github.com/apache/arrow/go/v12/arrow/internal/testing/types"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/apache/arrow/go/v12/arrow/scalar"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

func getScalars(inputs []compute.Datum, idx int) []scalar.Scalar {
	out := make([]scalar.Scalar, len(inputs))
	for i, in := range inputs {
		if in.Kind() == compute.KindArray {
			arr := in.(*compute.ArrayDatum).MakeArray()
			defer arr.Release()
			out[i], _ = scalar.GetScalar(arr, idx)
		} else {
			out[i] = in.(*compute.ScalarDatum).Value
		}
	}
	return out
}

func getDatums[T any](inputs []T) []compute.Datum {
	out := make([]compute.Datum, len(inputs))
	for i, in := range inputs {
		out[i] = compute.NewDatum(in)
	}
	return out
}

func assertArraysEqual(t *testing.T, expected, actual arrow.Array, opts ...array.EqualOption) bool {
	return assert.Truef(t, array.ApproxEqual(expected, actual, opts...), "expected: %s\ngot: %s", expected, actual)
}

func assertDatumsEqual(t *testing.T, expected, actual compute.Datum, opts []array.EqualOption, scalarOpts []scalar.EqualOption) {
	require.Equal(t, expected.Kind(), actual.Kind())

	switch expected.Kind() {
	case compute.KindScalar:
		want := expected.(*compute.ScalarDatum).Value
		got := actual.(*compute.ScalarDatum).Value
		assert.Truef(t, scalar.ApproxEquals(want, got, scalarOpts...), "expected: %s\ngot: %s", want, got)
	case compute.KindArray:
		want := expected.(*compute.ArrayDatum).MakeArray()
		got := actual.(*compute.ArrayDatum).MakeArray()
		assertArraysEqual(t, want, got, opts...)
		want.Release()
		got.Release()
	case compute.KindChunked:
		want := expected.(*compute.ChunkedDatum).Value
		got := actual.(*compute.ChunkedDatum).Value
		assert.Truef(t, array.ChunkedEqual(want, got), "expected: %s\ngot: %s", want, got)
	default:
		assert.Truef(t, actual.Equals(expected), "expected: %s\ngot: %s", expected, actual)
	}
}

func checkScalarNonRecursive(t *testing.T, funcName string, inputs []compute.Datum, expected compute.Datum, opts compute.FunctionOptions) {
	out, err := compute.CallFunction(context.Background(), funcName, opts, inputs...)
	assert.NoError(t, err)
	defer out.Release()
	assertDatumsEqual(t, expected, out, nil, nil)
}

func checkScalarWithScalars(t *testing.T, funcName string, inputs []scalar.Scalar, expected scalar.Scalar, opts compute.FunctionOptions) {
	datums := getDatums(inputs)
	defer func() {
		for _, s := range inputs {
			if r, ok := s.(scalar.Releasable); ok {
				r.Release()
			}
		}
		for _, d := range datums {
			d.Release()
		}
	}()
	out, err := compute.CallFunction(context.Background(), funcName, opts, datums...)
	assert.NoError(t, err)
	defer out.Release()
	if !scalar.Equals(out.(*compute.ScalarDatum).Value, expected) {
		var b strings.Builder
		b.WriteString(funcName + "(")
		for i, in := range inputs {
			if i != 0 {
				b.WriteByte(',')
			}
			b.WriteString(in.String())
		}
		b.WriteByte(')')
		b.WriteString(" = " + out.(*compute.ScalarDatum).Value.String())
		b.WriteString(" != " + expected.String())

		if !arrow.TypeEqual(out.(*compute.ScalarDatum).Type(), expected.DataType()) {
			fmt.Fprintf(&b, " (types differed: %s vs %s)",
				out.(*compute.ScalarDatum).Type(), expected.DataType())
		}
		t.Fatalf(b.String())
	}
}

func checkScalar(t *testing.T, funcName string, inputs []compute.Datum, expected compute.Datum, opts compute.FunctionOptions) {
	checkScalarNonRecursive(t, funcName, inputs, expected, opts)

	if expected.Kind() == compute.KindScalar {
		return
	}

	exp := expected.(*compute.ArrayDatum).MakeArray()
	defer exp.Release()

	// check for at least 1 array, and make sure the others are of equal len
	hasArray := false
	for _, in := range inputs {
		if in.Kind() == compute.KindArray {
			assert.EqualValues(t, exp.Len(), in.(*compute.ArrayDatum).Len())
			hasArray = true
		}
	}

	require.True(t, hasArray)

	// check all the input scalars
	for i := 0; i < exp.Len(); i++ {
		e, _ := scalar.GetScalar(exp, i)
		checkScalarWithScalars(t, funcName, getScalars(inputs, i), e, opts)
		if r, ok := e.(scalar.Releasable); ok {
			r.Release()
		}
	}
}

func assertBufferSame(t *testing.T, left, right arrow.Array, idx int) {
	assert.Same(t, left.Data().Buffers()[idx], right.Data().Buffers()[idx])
}

func checkScalarUnary(t *testing.T, funcName string, input compute.Datum, exp compute.Datum, opt compute.FunctionOptions) {
	checkScalar(t, funcName, []compute.Datum{input}, exp, opt)
}

func checkCast(t *testing.T, input arrow.Array, exp arrow.Array, opts compute.CastOptions) {
	opts.ToType = exp.DataType()
	in, out := compute.NewDatum(input), compute.NewDatum(exp)
	defer in.Release()
	defer out.Release()
	checkScalarUnary(t, "cast", in, out, &opts)
}

func checkCastFails(t *testing.T, input arrow.Array, opt compute.CastOptions) {
	_, err := compute.CastArray(context.Background(), input, &opt)
	assert.ErrorIs(t, err, arrow.ErrInvalid)

	// for scalars, check that at least one of the input fails
	// since many of the tests contain a mix of passing and failing values.
	// in some cases we will want to check more precisely
	nfail := 0
	for i := 0; i < input.Len(); i++ {
		sc, _ := scalar.GetScalar(input, i)
		if r, ok := sc.(scalar.Releasable); ok {
			defer r.Release()
		}
		d := compute.NewDatum(sc)
		defer d.Release()
		out, err := compute.CastDatum(context.Background(), d, &opt)
		if err != nil {
			nfail++
		} else {
			out.Release()
		}
	}
	assert.Greater(t, nfail, 0)
}

func checkCastZeroCopy(t *testing.T, input arrow.Array, toType arrow.DataType, opts *compute.CastOptions) {
	opts.ToType = toType
	out, err := compute.CastArray(context.Background(), input, opts)
	assert.NoError(t, err)
	defer out.Release()

	assert.Len(t, out.Data().Buffers(), len(input.Data().Buffers()))
	for i := range out.Data().Buffers() {
		assertBufferSame(t, out, input, i)
	}
}

var (
	signedIntTypes = []arrow.DataType{
		arrow.PrimitiveTypes.Int8,
		arrow.PrimitiveTypes.Int16,
		arrow.PrimitiveTypes.Int32,
		arrow.PrimitiveTypes.Int64,
	}
	unsignedIntTypes = []arrow.DataType{
		arrow.PrimitiveTypes.Uint8,
		arrow.PrimitiveTypes.Uint16,
		arrow.PrimitiveTypes.Uint32,
		arrow.PrimitiveTypes.Uint64,
	}
	integerTypes  = append(signedIntTypes, unsignedIntTypes...)
	floatingTypes = []arrow.DataType{
		arrow.PrimitiveTypes.Float32,
		arrow.PrimitiveTypes.Float64,
	}
	numericTypes    = append(integerTypes, floatingTypes...)
	baseBinaryTypes = []arrow.DataType{
		arrow.BinaryTypes.Binary,
		arrow.BinaryTypes.LargeBinary,
		arrow.BinaryTypes.String,
		arrow.BinaryTypes.LargeString,
	}
	dictIndexTypes = integerTypes
)

type CastSuite struct {
	suite.Suite

	mem *memory.CheckedAllocator
}

func (c *CastSuite) allocateEmptyBitmap(len int) *memory.Buffer {
	buf := memory.NewResizableBuffer(c.mem)
	buf.Resize(int(bitutil.BytesForBits(int64(len))))
	return buf
}

func (c *CastSuite) maskArrayWithNullsAt(input arrow.Array, toMask []int) arrow.Array {
	masked := input.Data().(*array.Data).Copy()
	defer masked.Release()
	if masked.Buffers()[0] != nil {
		masked.Buffers()[0].Release()
	}
	masked.Buffers()[0] = c.allocateEmptyBitmap(input.Len())
	masked.SetNullN(array.UnknownNullCount)

	if original := input.NullBitmapBytes(); len(original) > 0 {
		bitutil.CopyBitmap(original, input.Data().Offset(), input.Len(), masked.Buffers()[0].Bytes(), 0)
	} else {
		bitutil.SetBitsTo(masked.Buffers()[0].Bytes(), 0, int64(input.Len()), true)
	}

	for _, i := range toMask {
		bitutil.SetBitTo(masked.Buffers()[0].Bytes(), i, false)
	}

	return array.MakeFromData(masked)
}

func (c *CastSuite) invalidUtf8Arr(dt arrow.DataType) arrow.Array {
	bldr := array.NewBinaryBuilder(c.mem, dt.(arrow.BinaryDataType))
	defer bldr.Release()

	bldr.AppendValues([][]byte{
		[]byte("Hi"),
		[]byte("olá mundo"),
		[]byte("你好世界"),
		[]byte(""),
		[]byte("\xa0\xa1"), // invalid utf8!
	}, nil)

	return bldr.NewArray()
}

type binaryBuilderAppend interface {
	array.Builder
	AppendValues([][]byte, []bool)
}

func (c *CastSuite) fixedSizeInvalidUtf8(dt arrow.DataType) arrow.Array {
	var bldr binaryBuilderAppend
	if dt.ID() == arrow.FIXED_SIZE_BINARY {
		c.Require().Equal(3, dt.(*arrow.FixedSizeBinaryType).ByteWidth)
		bldr = array.NewFixedSizeBinaryBuilder(c.mem, dt.(*arrow.FixedSizeBinaryType))
	} else {
		bldr = array.NewBinaryBuilder(c.mem, dt.(arrow.BinaryDataType))
	}

	defer bldr.Release()

	bldr.AppendValues([][]byte{
		[]byte("Hi!"),
		[]byte("lá"),
		[]byte("你"),
		[]byte("   "),
		[]byte("\xa0\xa1\xa2"), // invalid utf8!
	}, nil)

	return bldr.NewArray()
}

func (c *CastSuite) SetupTest() {
	c.mem = memory.NewCheckedAllocator(memory.DefaultAllocator)
}

func (c *CastSuite) TearDownTest() {
	c.mem.AssertSize(c.T(), 0)
}

func (c *CastSuite) TestCanCast() {
	expectCanCast := func(from arrow.DataType, toSet []arrow.DataType, expected bool) {
		for _, to := range toSet {
			c.Equalf(expected, compute.CanCast(from, to), "CanCast from: %s, to: %s, expected: %t",
				from, to, expected)
		}
	}

	canCast := func(from arrow.DataType, toSet []arrow.DataType) {
		expectCanCast(from, toSet, true)
	}

	cannotCast := func(from arrow.DataType, toSet []arrow.DataType) {
		expectCanCast(from, toSet, false)
	}

	canCast(arrow.Null, []arrow.DataType{arrow.FixedWidthTypes.Boolean})
	canCast(arrow.Null, numericTypes)
	canCast(arrow.Null, baseBinaryTypes)
	canCast(arrow.Null, []arrow.DataType{
		arrow.FixedWidthTypes.Date32, arrow.FixedWidthTypes.Date64, arrow.FixedWidthTypes.Time32ms, arrow.FixedWidthTypes.Timestamp_s,
	})
	cannotCast(&arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Uint16, ValueType: arrow.Null}, []arrow.DataType{arrow.Null})

	canCast(arrow.FixedWidthTypes.Boolean, []arrow.DataType{arrow.FixedWidthTypes.Boolean})
	canCast(arrow.FixedWidthTypes.Boolean, numericTypes)
	canCast(arrow.FixedWidthTypes.Boolean, []arrow.DataType{arrow.BinaryTypes.String, arrow.BinaryTypes.LargeString})
	cannotCast(&arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: arrow.FixedWidthTypes.Boolean}, []arrow.DataType{arrow.FixedWidthTypes.Boolean})

	cannotCast(arrow.FixedWidthTypes.Boolean, []arrow.DataType{arrow.Null})
	cannotCast(arrow.FixedWidthTypes.Boolean, []arrow.DataType{arrow.BinaryTypes.Binary, arrow.BinaryTypes.LargeBinary})
	cannotCast(arrow.FixedWidthTypes.Boolean, []arrow.DataType{
		arrow.FixedWidthTypes.Date32, arrow.FixedWidthTypes.Date64, arrow.FixedWidthTypes.Time32ms, arrow.FixedWidthTypes.Timestamp_s})

	for _, from := range numericTypes {
		canCast(from, []arrow.DataType{arrow.FixedWidthTypes.Boolean})
		canCast(from, numericTypes)
		canCast(from, []arrow.DataType{arrow.BinaryTypes.String, arrow.BinaryTypes.LargeString})
		canCast(&arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: from}, []arrow.DataType{from})

		cannotCast(from, []arrow.DataType{arrow.Null})
	}

	for _, from := range baseBinaryTypes {
		canCast(from, []arrow.DataType{arrow.FixedWidthTypes.Boolean})
		canCast(from, numericTypes)
		canCast(from, baseBinaryTypes)
		canCast(&arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int64, ValueType: from}, []arrow.DataType{from})

		// any cast which is valid for the dictionary is valid for the dictionary array
		canCast(&arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Uint32, ValueType: from}, baseBinaryTypes)
		canCast(&arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int16, ValueType: from}, baseBinaryTypes)

		cannotCast(from, []arrow.DataType{arrow.Null})
	}

	canCast(arrow.BinaryTypes.String, []arrow.DataType{arrow.FixedWidthTypes.Timestamp_ms})
	canCast(arrow.BinaryTypes.LargeString, []arrow.DataType{arrow.FixedWidthTypes.Timestamp_ns})
	// no formatting supported
	cannotCast(arrow.FixedWidthTypes.Timestamp_us, []arrow.DataType{arrow.BinaryTypes.Binary, arrow.BinaryTypes.LargeBinary})

	canCast(&arrow.FixedSizeBinaryType{ByteWidth: 3}, []arrow.DataType{
		arrow.BinaryTypes.Binary, arrow.BinaryTypes.LargeBinary, arrow.BinaryTypes.String, arrow.BinaryTypes.LargeString,
		&arrow.FixedSizeBinaryType{ByteWidth: 3}})

	arrow.RegisterExtensionType(types.NewSmallintType())
	defer arrow.UnregisterExtensionType("smallint")
	canCast(types.NewSmallintType(), []arrow.DataType{arrow.PrimitiveTypes.Int16})
	canCast(types.NewSmallintType(), numericTypes) // any cast which is valid for storage is supported
	canCast(arrow.Null, []arrow.DataType{types.NewSmallintType()})

	canCast(arrow.FixedWidthTypes.Date32, []arrow.DataType{arrow.BinaryTypes.String, arrow.BinaryTypes.LargeString})
	canCast(arrow.FixedWidthTypes.Date64, []arrow.DataType{arrow.BinaryTypes.String, arrow.BinaryTypes.LargeString})
	canCast(arrow.FixedWidthTypes.Timestamp_ns, []arrow.DataType{arrow.BinaryTypes.String, arrow.BinaryTypes.LargeString})
	canCast(arrow.FixedWidthTypes.Timestamp_us, []arrow.DataType{arrow.BinaryTypes.String, arrow.BinaryTypes.LargeString})
	canCast(arrow.FixedWidthTypes.Time32ms, []arrow.DataType{arrow.BinaryTypes.String, arrow.BinaryTypes.LargeString})
	canCast(arrow.FixedWidthTypes.Time64ns, []arrow.DataType{arrow.BinaryTypes.String, arrow.BinaryTypes.LargeString})
}

func (c *CastSuite) checkCastFails(dt arrow.DataType, input string, opts *compute.CastOptions) {
	inArr, _, _ := array.FromJSON(c.mem, dt, strings.NewReader(input), array.WithUseNumber())
	defer inArr.Release()

	checkCastFails(c.T(), inArr, *opts)
}

func (c *CastSuite) checkCastOpts(dtIn, dtOut arrow.DataType, inJSON, outJSON string, opts compute.CastOptions) {
	inArr, _, _ := array.FromJSON(c.mem, dtIn, strings.NewReader(inJSON), array.WithUseNumber())
	outArr, _, _ := array.FromJSON(c.mem, dtOut, strings.NewReader(outJSON), array.WithUseNumber())
	defer inArr.Release()
	defer outArr.Release()

	checkCast(c.T(), inArr, outArr, opts)
}

func (c *CastSuite) checkCast(dtIn, dtOut arrow.DataType, inJSON, outJSON string) {
	c.checkCastOpts(dtIn, dtOut, inJSON, outJSON, *compute.DefaultCastOptions(true))
}

func (c *CastSuite) checkCastArr(in arrow.Array, dtOut arrow.DataType, json string, opts compute.CastOptions) {
	outArr, _, _ := array.FromJSON(c.mem, dtOut, strings.NewReader(json), array.WithUseNumber())
	defer outArr.Release()
	checkCast(c.T(), in, outArr, opts)
}

func (c *CastSuite) checkCastExp(dtIn arrow.DataType, inJSON string, exp arrow.Array) {
	inArr, _, _ := array.FromJSON(c.mem, dtIn, strings.NewReader(inJSON), array.WithUseNumber())
	defer inArr.Release()
	checkCast(c.T(), inArr, exp, *compute.DefaultCastOptions(true))
}

func (c *CastSuite) TestNumericToBool() {
	for _, dt := range numericTypes {
		c.checkCast(dt, arrow.FixedWidthTypes.Boolean,
			`[0, null, 127, 1, 0]`, `[false, null, true, true, false]`)
	}

	// check negative numbers
	for _, dt := range []arrow.DataType{arrow.PrimitiveTypes.Int8, arrow.PrimitiveTypes.Float64} {
		c.checkCast(dt, arrow.FixedWidthTypes.Boolean,
			`[0, null, 127, -1, 0]`, `[false, null, true, true, false]`)
	}
}

func (c *CastSuite) StringToBool() {
	for _, dt := range []arrow.DataType{arrow.BinaryTypes.String, arrow.BinaryTypes.LargeString} {
		c.checkCast(dt, arrow.FixedWidthTypes.Boolean,
			`["False", null, "true", "True", "false"]`, `[false, null, true, true, false]`)

		c.checkCast(dt, arrow.FixedWidthTypes.Boolean,
			`["0", null, "1", "1", "0"]`, `[false, null, true, true, false]`)

		opts := compute.NewCastOptions(arrow.FixedWidthTypes.Boolean, true)
		c.checkCastFails(dt, `["false "]`, opts)
		c.checkCastFails(dt, `["T"]`, opts)
	}
}

func (c *CastSuite) TestToIntUpcast() {
	c.checkCast(arrow.PrimitiveTypes.Int8, arrow.PrimitiveTypes.Int32,
		`[0, null, 127, -1, 0]`, `[0, null, 127, -1, 0]`)

	c.checkCast(arrow.PrimitiveTypes.Uint8, arrow.PrimitiveTypes.Int16,
		`[0, 100, 200, 255, 0]`, `[0, 100, 200, 255, 0]`)
}

func (c *CastSuite) TestToIntDowncastSafe() {
	// int16 to uint8 no overflow/underflow
	c.checkCast(arrow.PrimitiveTypes.Int16, arrow.PrimitiveTypes.Uint8,
		`[0, null, 200, 1, 2]`, `[0, null, 200, 1, 2]`)

	// int16 to uint8, overflow
	c.checkCastFails(arrow.PrimitiveTypes.Int16, `[0, null, 256, 0, 0]`,
		compute.NewCastOptions(arrow.PrimitiveTypes.Uint8, true))
	// and underflow
	c.checkCastFails(arrow.PrimitiveTypes.Int16, `[0, null, -1, 0, 0]`,
		compute.NewCastOptions(arrow.PrimitiveTypes.Uint8, true))

	// int32 to int16, no overflow/underflow
	c.checkCast(arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Int16,
		`[0, null, 2000, 1, 2]`, `[0, null, 2000, 1, 2]`)

	// int32 to int16, overflow
	c.checkCastFails(arrow.PrimitiveTypes.Int32, `[0, null, 2000, 70000, 2]`,
		compute.NewCastOptions(arrow.PrimitiveTypes.Int16, true))

	// and underflow
	c.checkCastFails(arrow.PrimitiveTypes.Int32, `[0, null, 2000, -70000, 2]`,
		compute.NewCastOptions(arrow.PrimitiveTypes.Int16, true))

	c.checkCastFails(arrow.PrimitiveTypes.Int32, `[0, null, 2000, -70000, 2]`,
		compute.NewCastOptions(arrow.PrimitiveTypes.Uint8, true))

}

func (c *CastSuite) TestIntegerSignedToUnsigned() {
	i32s, _, _ := array.FromJSON(c.mem, arrow.PrimitiveTypes.Int32, strings.NewReader(`[-2147483648, null, -1, 65535, 2147483647]`))
	defer i32s.Release()

	// same width
	checkCastFails(c.T(), i32s, *compute.NewCastOptions(arrow.PrimitiveTypes.Uint32, true))
	// wider
	checkCastFails(c.T(), i32s, *compute.NewCastOptions(arrow.PrimitiveTypes.Uint64, true))
	// narrower
	checkCastFails(c.T(), i32s, *compute.NewCastOptions(arrow.PrimitiveTypes.Uint16, true))

	var options compute.CastOptions
	options.AllowIntOverflow = true

	u32s, _, _ := array.FromJSON(c.mem, arrow.PrimitiveTypes.Uint32,
		strings.NewReader(`[2147483648, null, 4294967295, 65535, 2147483647]`))
	defer u32s.Release()
	checkCast(c.T(), i32s, u32s, options)

	u64s, _, _ := array.FromJSON(c.mem, arrow.PrimitiveTypes.Uint64,
		strings.NewReader(`[18446744071562067968, null, 18446744073709551615, 65535, 2147483647]`),
		array.WithUseNumber()) // have to use WithUseNumber so it doesn't lose precision converting to float64
	defer u64s.Release()
	checkCast(c.T(), i32s, u64s, options)

	// fail because of overflow, instead of underflow
	i32s, _, _ = array.FromJSON(c.mem, arrow.PrimitiveTypes.Int32, strings.NewReader(`[0, null, 0, 65536, 2147483647]`))
	defer i32s.Release()
	checkCastFails(c.T(), i32s, *compute.NewCastOptions(arrow.PrimitiveTypes.Uint16, true))

	u16s, _, _ := array.FromJSON(c.mem, arrow.PrimitiveTypes.Uint16, strings.NewReader(`[0, null, 0, 0, 65535]`))
	defer u16s.Release()
	checkCast(c.T(), i32s, u16s, options)
}

func (c *CastSuite) TestIntegerUnsignedToSigned() {
	u32s, _, _ := array.FromJSON(c.mem, arrow.PrimitiveTypes.Uint32, strings.NewReader(`[4294967295, null, 0, 32768]`))
	defer u32s.Release()
	// same width
	checkCastFails(c.T(), u32s, *compute.SafeCastOptions(arrow.PrimitiveTypes.Int32))

	// narrower
	checkCastFails(c.T(), u32s, *compute.SafeCastOptions(arrow.PrimitiveTypes.Int16))
	sl := array.NewSlice(u32s, 1, int64(u32s.Len()))
	defer sl.Release()
	checkCastFails(c.T(), sl, *compute.SafeCastOptions(arrow.PrimitiveTypes.Int16))

	var opts compute.CastOptions
	opts.AllowIntOverflow = true
	c.checkCastArr(u32s, arrow.PrimitiveTypes.Int32, `[-1, null, 0, 32768]`, opts)
	c.checkCastArr(u32s, arrow.PrimitiveTypes.Int64, `[4294967295, null, 0, 32768]`, opts)
	c.checkCastArr(u32s, arrow.PrimitiveTypes.Int16, `[-1, null, 0, -32768]`, opts)
}

func (c *CastSuite) TestToIntDowncastUnsafe() {
	opts := compute.CastOptions{AllowIntOverflow: true}
	c.checkCastOpts(arrow.PrimitiveTypes.Int16, arrow.PrimitiveTypes.Uint8,
		`[0, null, 200, 1, 2]`, `[0, null, 200, 1, 2]`, opts)

	c.checkCastOpts(arrow.PrimitiveTypes.Int16, arrow.PrimitiveTypes.Uint8,
		`[0, null, 256, 1, 2, -1]`, `[0, null, 0, 1, 2, 255]`, opts)

	c.checkCastOpts(arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Int16,
		`[0, null, 2000, 1, 2, -1]`, `[0, null, 2000, 1, 2, -1]`, opts)

	c.checkCastOpts(arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Int16,
		`[0, null, 2000, 70000, -70000]`, `[0, null, 2000, 4464, -4464]`, opts)
}

func (c *CastSuite) TestFloatingToInt() {
	for _, from := range []arrow.DataType{arrow.PrimitiveTypes.Float32, arrow.PrimitiveTypes.Float64} {
		for _, to := range []arrow.DataType{arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Int64} {
			// float to int no truncation
			c.checkCast(from, to, `[1.0, null, 0.0, -1.0, 5.0]`, `[1, null, 0, -1, 5]`)

			// float to int truncate error
			opts := compute.SafeCastOptions(to)
			c.checkCastFails(from, `[1.5, 0.0, null, 0.5, -1.5, 5.5]`, opts)

			// float to int truncate allowed
			opts.AllowFloatTruncate = true
			c.checkCastOpts(from, to, `[1.5, 0.0, null, 0.5, -1.5, 5.5]`, `[1, 0, null, 0, -1, 5]`, *opts)
		}
	}
}

func (c *CastSuite) TestIntToFloating() {
	for _, from := range []arrow.DataType{arrow.PrimitiveTypes.Uint32, arrow.PrimitiveTypes.Int32} {
		two24 := `[16777216, 16777217]`
		c.checkCastFails(from, two24, compute.SafeCastOptions(arrow.PrimitiveTypes.Float32))
		one24 := `[16777216]`
		c.checkCast(from, arrow.PrimitiveTypes.Float32, one24, one24)
	}

	i64s, _, _ := array.FromJSON(c.mem, arrow.PrimitiveTypes.Int64,
		strings.NewReader(`[-9223372036854775808, -9223372036854775807, 0, 9223372036854775806,  9223372036854775807]`),
		array.WithUseNumber())
	defer i64s.Release()

	checkCastFails(c.T(), i64s, *compute.SafeCastOptions(arrow.PrimitiveTypes.Float64))
	masked := c.maskArrayWithNullsAt(i64s, []int{0, 1, 3, 4})
	defer masked.Release()
	c.checkCastArr(masked, arrow.PrimitiveTypes.Float64, `[null, null, 0, null, null]`, *compute.DefaultCastOptions(true))

	c.checkCastFails(arrow.PrimitiveTypes.Uint64, `[9007199254740992, 9007199254740993]`, compute.SafeCastOptions(arrow.PrimitiveTypes.Float64))
}

func (c *CastSuite) TestDecimal128ToInt() {
	opts := compute.SafeCastOptions(arrow.PrimitiveTypes.Int64)

	c.Run("no overflow no truncate", func() {
		for _, allowIntOverflow := range []bool{false, true} {
			c.Run(fmt.Sprintf("int_overflow=%t", allowIntOverflow), func() {
				for _, allowDecTruncate := range []bool{false, true} {
					c.Run(fmt.Sprintf("dec_truncate=%t", allowDecTruncate), func() {
						opts.AllowIntOverflow = allowIntOverflow
						opts.AllowDecimalTruncate = allowDecTruncate

						noOverflowNoTrunc, _, _ := array.FromJSON(c.mem, &arrow.Decimal128Type{Precision: 38, Scale: 10},
							strings.NewReader(`["02.0000000000", "-11.0000000000", "22.0000000000", "-121.000000000", null]`))

						c.checkCastArr(noOverflowNoTrunc, arrow.PrimitiveTypes.Int64, `[2, -11, 22, -121, null]`, *opts)
						noOverflowNoTrunc.Release()
					})
				}
			})
		}
	})

	c.Run("truncate no overflow", func() {
		for _, allowIntOverflow := range []bool{false, true} {
			c.Run("allow overflow"+strconv.FormatBool(allowIntOverflow), func() {
				opts.AllowIntOverflow = allowIntOverflow
				truncNoOverflow, _, _ := array.FromJSON(c.mem, &arrow.Decimal128Type{Precision: 38, Scale: 10},
					strings.NewReader(`["02.1000000000", "-11.0000004500", "22.0000004500", "-121.1210000000", null]`))

				opts.AllowDecimalTruncate = true
				c.checkCastArr(truncNoOverflow, arrow.PrimitiveTypes.Int64, `[2, -11, 22, -121, null]`, *opts)

				opts.AllowDecimalTruncate = false
				checkCastFails(c.T(), truncNoOverflow, *opts)
				truncNoOverflow.Release()
			})
		}
	})

	c.Run("overflow no truncate", func() {
		for _, allowDecTruncate := range []bool{false, true} {
			c.Run("allow truncate "+strconv.FormatBool(allowDecTruncate), func() {
				opts.AllowDecimalTruncate = allowDecTruncate

				overflowNoTrunc, _, _ := array.FromJSON(c.mem, &arrow.Decimal128Type{Precision: 38, Scale: 10},
					strings.NewReader(`[
						"12345678901234567890000.0000000000", 
						"99999999999999999999999.0000000000",
						null]`), array.WithUseNumber())
				defer overflowNoTrunc.Release()
				opts.AllowIntOverflow = true
				c.checkCastArr(overflowNoTrunc, arrow.PrimitiveTypes.Int64,
					// 12345678901234567890000 % 2**64, 99999999999999999999999 % 2**64
					`[4807115922877858896, 200376420520689663, null]`, *opts)

				opts.AllowIntOverflow = false
				checkCastFails(c.T(), overflowNoTrunc, *opts)
			})
		}
	})

	c.Run("overflow and truncate", func() {
		for _, allowIntOverFlow := range []bool{false, true} {
			c.Run("allow overflow = "+strconv.FormatBool(allowIntOverFlow), func() {
				for _, allowDecTruncate := range []bool{false, true} {
					c.Run("allow truncate = "+strconv.FormatBool(allowDecTruncate), func() {
						opts.AllowIntOverflow = allowIntOverFlow
						opts.AllowDecimalTruncate = allowDecTruncate

						overflowAndTruncate, _, _ := array.FromJSON(c.mem, &arrow.Decimal128Type{Precision: 38, Scale: 10},
							strings.NewReader(`[
							"12345678901234567890000.0045345000",
							"99999999999999999999999.0000344300",
							null]`), array.WithUseNumber())
						defer overflowAndTruncate.Release()
						if opts.AllowIntOverflow && opts.AllowDecimalTruncate {
							c.checkCastArr(overflowAndTruncate, arrow.PrimitiveTypes.Int64,
								// 12345678901234567890000 % 2**64, 99999999999999999999999 % 2**64
								`[4807115922877858896, 200376420520689663, null]`, *opts)
						} else {
							checkCastFails(c.T(), overflowAndTruncate, *opts)
						}
					})
				}
			})
		}
	})

	c.Run("negative scale", func() {
		bldr := array.NewDecimal128Builder(c.mem, &arrow.Decimal128Type{Precision: 38, Scale: -4})
		defer bldr.Release()

		var err error
		for _, d := range []decimal128.Num{decimal128.FromU64(1234567890000), decimal128.FromI64(-120000)} {
			d, err = d.Rescale(0, -4)
			c.Require().NoError(err)
			bldr.Append(d)
		}
		negScale := bldr.NewArray()
		defer negScale.Release()

		opts.AllowIntOverflow = true
		opts.AllowDecimalTruncate = true
		c.checkCastArr(negScale, arrow.PrimitiveTypes.Int64, `[1234567890000, -120000]`, *opts)
	})
}

func (c *CastSuite) TestDecimal256ToInt() {
	opts := compute.SafeCastOptions(arrow.PrimitiveTypes.Int64)

	c.Run("no overflow no truncate", func() {
		for _, allowIntOverflow := range []bool{false, true} {
			c.Run(fmt.Sprintf("int_overflow=%t", allowIntOverflow), func() {
				for _, allowDecTruncate := range []bool{false, true} {
					c.Run(fmt.Sprintf("dec_truncate=%t", allowDecTruncate), func() {
						opts.AllowIntOverflow = allowIntOverflow
						opts.AllowDecimalTruncate = allowDecTruncate

						noOverflowNoTrunc, _, _ := array.FromJSON(c.mem, &arrow.Decimal256Type{Precision: 40, Scale: 10},
							strings.NewReader(`["02.0000000000", "-11.0000000000", "22.0000000000", "-121.000000000", null]`))

						c.checkCastArr(noOverflowNoTrunc, arrow.PrimitiveTypes.Int64, `[2, -11, 22, -121, null]`, *opts)
						noOverflowNoTrunc.Release()
					})
				}
			})
		}
	})

	c.Run("truncate no overflow", func() {
		for _, allowIntOverflow := range []bool{false, true} {
			c.Run("allow overflow"+strconv.FormatBool(allowIntOverflow), func() {
				opts.AllowIntOverflow = allowIntOverflow
				truncNoOverflow, _, _ := array.FromJSON(c.mem, &arrow.Decimal256Type{Precision: 40, Scale: 10},
					strings.NewReader(`["02.1000000000", "-11.0000004500", "22.0000004500", "-121.1210000000", null]`))

				opts.AllowDecimalTruncate = true
				c.checkCastArr(truncNoOverflow, arrow.PrimitiveTypes.Int64, `[2, -11, 22, -121, null]`, *opts)

				opts.AllowDecimalTruncate = false
				checkCastFails(c.T(), truncNoOverflow, *opts)
				truncNoOverflow.Release()
			})
		}
	})

	c.Run("overflow no truncate", func() {
		for _, allowDecTruncate := range []bool{false, true} {
			c.Run("allow truncate "+strconv.FormatBool(allowDecTruncate), func() {
				opts.AllowDecimalTruncate = allowDecTruncate

				overflowNoTrunc, _, _ := array.FromJSON(c.mem, &arrow.Decimal256Type{Precision: 40, Scale: 10},
					strings.NewReader(`[
						"1234567890123456789000000.0000000000",
						"9999999999999999999999999.0000000000",
						null]`), array.WithUseNumber())
				defer overflowNoTrunc.Release()
				opts.AllowIntOverflow = true
				c.checkCastArr(overflowNoTrunc, arrow.PrimitiveTypes.Int64,
					// 1234567890123456789000000 % 2**64, 9999999999999999999999999 % 2**64
					`[1096246371337547584, 1590897978359414783, null]`, *opts)

				opts.AllowIntOverflow = false
				checkCastFails(c.T(), overflowNoTrunc, *opts)
			})
		}
	})

	c.Run("overflow and truncate", func() {
		for _, allowIntOverFlow := range []bool{false, true} {
			c.Run("allow overflow = "+strconv.FormatBool(allowIntOverFlow), func() {
				for _, allowDecTruncate := range []bool{false, true} {
					c.Run("allow truncate = "+strconv.FormatBool(allowDecTruncate), func() {
						opts.AllowIntOverflow = allowIntOverFlow
						opts.AllowDecimalTruncate = allowDecTruncate

						overflowAndTruncate, _, _ := array.FromJSON(c.mem, &arrow.Decimal256Type{Precision: 40, Scale: 10},
							strings.NewReader(`[
							"1234567890123456789000000.0045345000",
							"9999999999999999999999999.0000344300",
							null]`), array.WithUseNumber())
						defer overflowAndTruncate.Release()
						if opts.AllowIntOverflow && opts.AllowDecimalTruncate {
							c.checkCastArr(overflowAndTruncate, arrow.PrimitiveTypes.Int64,
								// 1234567890123456789000000 % 2**64, 9999999999999999999999999 % 2**64
								`[1096246371337547584, 1590897978359414783, null]`, *opts)
						} else {
							checkCastFails(c.T(), overflowAndTruncate, *opts)
						}
					})
				}
			})
		}
	})

	c.Run("negative scale", func() {
		bldr := array.NewDecimal256Builder(c.mem, &arrow.Decimal256Type{Precision: 40, Scale: -4})
		defer bldr.Release()

		var err error
		for _, d := range []decimal256.Num{decimal256.FromU64(1234567890000), decimal256.FromI64(-120000)} {
			d, err = d.Rescale(0, -4)
			c.Require().NoError(err)
			bldr.Append(d)
		}
		negScale := bldr.NewArray()
		defer negScale.Release()

		opts.AllowIntOverflow = true
		opts.AllowDecimalTruncate = true
		c.checkCastArr(negScale, arrow.PrimitiveTypes.Int64, `[1234567890000, -120000]`, *opts)
	})
}

func (c *CastSuite) TestIntegerToDecimal() {
	for _, decType := range []arrow.DataType{&arrow.Decimal128Type{Precision: 22, Scale: 2}, &arrow.Decimal256Type{Precision: 22, Scale: 2}} {
		c.Run(decType.String(), func() {
			for _, intType := range integerTypes {
				c.Run(intType.String(), func() {
					c.checkCast(intType, decType, `[0, 7, null, 100, 99]`, `["0.00", "7.00", null, "100.00", "99.00"]`)
				})
			}
		})
	}

	c.Run("extreme value", func() {
		for _, dt := range []arrow.DataType{&arrow.Decimal128Type{Precision: 19, Scale: 0}, &arrow.Decimal256Type{Precision: 19, Scale: 0}} {
			c.Run(dt.String(), func() {
				c.checkCast(arrow.PrimitiveTypes.Int64, dt,
					`[-9223372036854775808, 9223372036854775807]`, `["-9223372036854775808", "9223372036854775807"]`)
			})
		}
		for _, dt := range []arrow.DataType{&arrow.Decimal128Type{Precision: 20, Scale: 0}, &arrow.Decimal256Type{Precision: 20, Scale: 0}} {
			c.Run(dt.String(), func() {
				c.checkCast(arrow.PrimitiveTypes.Uint64, dt,
					`[0, 18446744073709551615]`, `["0", "18446744073709551615"]`)
			})
		}
	})

	c.Run("insufficient output precision", func() {
		var opts compute.CastOptions
		opts.ToType = &arrow.Decimal128Type{Precision: 5, Scale: 3}
		c.checkCastFails(arrow.PrimitiveTypes.Int8, `[0]`, &opts)

		opts.ToType = &arrow.Decimal256Type{Precision: 76, Scale: 67}
		c.checkCastFails(arrow.PrimitiveTypes.Int32, `[0]`, &opts)
	})
}

func (c *CastSuite) TestDecimal128ToDecimal128() {
	var opts compute.CastOptions

	for _, allowDecTruncate := range []bool{false, true} {
		c.Run("decTruncate="+strconv.FormatBool(allowDecTruncate), func() {
			opts.AllowDecimalTruncate = allowDecTruncate

			noTruncate, _, _ := array.FromJSON(c.mem, &arrow.Decimal128Type{Precision: 38, Scale: 10},
				strings.NewReader(`["02.0000000000", "30.0000000000", "22.0000000000", "-121.0000000000", null]`))
			expected, _, _ := array.FromJSON(c.mem, &arrow.Decimal128Type{Precision: 28, Scale: 10},
				strings.NewReader(`["02.", "30.", "22.", "-121.", null]`))

			defer noTruncate.Release()
			defer expected.Release()

			checkCast(c.T(), noTruncate, expected, opts)
			checkCast(c.T(), expected, noTruncate, opts)
		})
	}

	c.Run("same scale diff precision", func() {
		for _, allowDecTruncate := range []bool{false, true} {
			c.Run("decTruncate="+strconv.FormatBool(allowDecTruncate), func() {
				opts.AllowDecimalTruncate = allowDecTruncate

				d52, _, _ := array.FromJSON(c.mem, &arrow.Decimal128Type{Precision: 5, Scale: 2},
					strings.NewReader(`["12.34", "0.56"]`))
				d42, _, _ := array.FromJSON(c.mem, &arrow.Decimal128Type{Precision: 4, Scale: 2},
					strings.NewReader(`["12.34", "0.56"]`))

				defer d52.Release()
				defer d42.Release()

				checkCast(c.T(), d52, d42, opts)
				checkCast(c.T(), d42, d52, opts)
			})
		}
	})

	c.Run("rescale leads to trunc", func() {
		dP38S10, _, _ := array.FromJSON(c.mem, &arrow.Decimal128Type{Precision: 38, Scale: 10},
			strings.NewReader(`["-02.1234567890", "30.1234567890", null]`))
		dP28S0, _, _ := array.FromJSON(c.mem, &arrow.Decimal128Type{Precision: 28, Scale: 0},
			strings.NewReader(`["-02.", "30.", null]`))
		dP38S10RoundTripped, _, _ := array.FromJSON(c.mem, &arrow.Decimal128Type{Precision: 38, Scale: 10},
			strings.NewReader(`["-02.0000000000", "30.0000000000", null]`))
		defer func() {
			dP38S10.Release()
			dP28S0.Release()
			dP38S10RoundTripped.Release()
		}()

		opts.AllowDecimalTruncate = true
		checkCast(c.T(), dP38S10, dP28S0, opts)
		checkCast(c.T(), dP28S0, dP38S10RoundTripped, opts)

		opts.AllowDecimalTruncate = false
		opts.ToType = dP28S0.DataType()
		checkCastFails(c.T(), dP38S10, opts)
		checkCast(c.T(), dP28S0, dP38S10RoundTripped, opts)
	})

	c.Run("precision loss without rescale = trunc", func() {
		d42, _, _ := array.FromJSON(c.mem, &arrow.Decimal128Type{Precision: 4, Scale: 2},
			strings.NewReader(`["12.34"]`))
		defer d42.Release()
		for _, dt := range []arrow.DataType{
			&arrow.Decimal128Type{Precision: 3, Scale: 2},
			&arrow.Decimal128Type{Precision: 4, Scale: 3},
			&arrow.Decimal128Type{Precision: 2, Scale: 1}} {

			opts.AllowDecimalTruncate = true
			opts.ToType = dt
			out, err := compute.CastArray(context.Background(), d42, &opts)
			out.Release()
			c.NoError(err)

			opts.AllowDecimalTruncate = false
			opts.ToType = dt
			checkCastFails(c.T(), d42, opts)
		}
	})
}

func (c *CastSuite) TestDecimal256ToDecimal256() {
	var opts compute.CastOptions

	for _, allowDecTruncate := range []bool{false, true} {
		c.Run("decTruncate="+strconv.FormatBool(allowDecTruncate), func() {
			opts.AllowDecimalTruncate = allowDecTruncate

			noTruncate, _, _ := array.FromJSON(c.mem, &arrow.Decimal256Type{Precision: 38, Scale: 10},
				strings.NewReader(`["02.0000000000", "30.0000000000", "22.0000000000", "-121.0000000000", null]`))
			expected, _, _ := array.FromJSON(c.mem, &arrow.Decimal256Type{Precision: 28, Scale: 10},
				strings.NewReader(`["02.", "30.", "22.", "-121.", null]`))

			defer noTruncate.Release()
			defer expected.Release()

			checkCast(c.T(), noTruncate, expected, opts)
			checkCast(c.T(), expected, noTruncate, opts)
		})
	}

	c.Run("same scale diff precision", func() {
		for _, allowDecTruncate := range []bool{false, true} {
			c.Run("decTruncate="+strconv.FormatBool(allowDecTruncate), func() {
				opts.AllowDecimalTruncate = allowDecTruncate

				d52, _, _ := array.FromJSON(c.mem, &arrow.Decimal256Type{Precision: 5, Scale: 2},
					strings.NewReader(`["12.34", "0.56"]`))
				d42, _, _ := array.FromJSON(c.mem, &arrow.Decimal256Type{Precision: 4, Scale: 2},
					strings.NewReader(`["12.34", "0.56"]`))

				defer d52.Release()
				defer d42.Release()

				checkCast(c.T(), d52, d42, opts)
				checkCast(c.T(), d42, d52, opts)
			})
		}
	})

	c.Run("rescale leads to trunc", func() {
		dP38S10, _, _ := array.FromJSON(c.mem, &arrow.Decimal256Type{Precision: 38, Scale: 10},
			strings.NewReader(`["-02.1234567890", "30.1234567890", null]`))
		dP28S0, _, _ := array.FromJSON(c.mem, &arrow.Decimal256Type{Precision: 28, Scale: 0},
			strings.NewReader(`["-02.", "30.", null]`))
		dP38S10RoundTripped, _, _ := array.FromJSON(c.mem, &arrow.Decimal256Type{Precision: 38, Scale: 10},
			strings.NewReader(`["-02.0000000000", "30.0000000000", null]`))
		defer func() {
			dP38S10.Release()
			dP28S0.Release()
			dP38S10RoundTripped.Release()
		}()

		opts.AllowDecimalTruncate = true
		checkCast(c.T(), dP38S10, dP28S0, opts)
		checkCast(c.T(), dP28S0, dP38S10RoundTripped, opts)

		opts.AllowDecimalTruncate = false
		opts.ToType = dP28S0.DataType()
		checkCastFails(c.T(), dP38S10, opts)
		checkCast(c.T(), dP28S0, dP38S10RoundTripped, opts)
	})

	c.Run("precision loss without rescale = trunc", func() {
		d42, _, _ := array.FromJSON(c.mem, &arrow.Decimal256Type{Precision: 4, Scale: 2},
			strings.NewReader(`["12.34"]`))
		defer d42.Release()
		for _, dt := range []arrow.DataType{
			&arrow.Decimal256Type{Precision: 3, Scale: 2},
			&arrow.Decimal256Type{Precision: 4, Scale: 3},
			&arrow.Decimal256Type{Precision: 2, Scale: 1}} {

			opts.AllowDecimalTruncate = true
			opts.ToType = dt
			out, err := compute.CastArray(context.Background(), d42, &opts)
			out.Release()
			c.NoError(err)

			opts.AllowDecimalTruncate = false
			opts.ToType = dt
			checkCastFails(c.T(), d42, opts)
		}
	})
}

func (c *CastSuite) TestDecimal128ToDecimal256() {
	var opts compute.CastOptions

	for _, allowDecTruncate := range []bool{false, true} {
		c.Run("decTruncate="+strconv.FormatBool(allowDecTruncate), func() {
			opts.AllowDecimalTruncate = allowDecTruncate

			noTruncate, _, _ := array.FromJSON(c.mem, &arrow.Decimal128Type{Precision: 38, Scale: 10},
				strings.NewReader(`["02.0000000000", "30.0000000000", "22.0000000000", "-121.0000000000", null]`))
			expected, _, _ := array.FromJSON(c.mem, &arrow.Decimal256Type{Precision: 28, Scale: 10},
				strings.NewReader(`["02.", "30.", "22.", "-121.", null]`))

			defer noTruncate.Release()
			defer expected.Release()

			checkCast(c.T(), noTruncate, expected, opts)
		})
	}

	c.Run("same scale diff precision", func() {
		for _, allowDecTruncate := range []bool{false, true} {
			c.Run("decTruncate="+strconv.FormatBool(allowDecTruncate), func() {
				opts.AllowDecimalTruncate = allowDecTruncate

				d52, _, _ := array.FromJSON(c.mem, &arrow.Decimal128Type{Precision: 5, Scale: 2},
					strings.NewReader(`["12.34", "0.56"]`))
				d42, _, _ := array.FromJSON(c.mem, &arrow.Decimal256Type{Precision: 4, Scale: 2},
					strings.NewReader(`["12.34", "0.56"]`))
				d402, _, _ := array.FromJSON(c.mem, &arrow.Decimal256Type{Precision: 40, Scale: 2},
					strings.NewReader(`["12.34", "0.56"]`))

				defer d52.Release()
				defer d42.Release()
				defer d402.Release()

				checkCast(c.T(), d52, d42, opts)
				checkCast(c.T(), d52, d402, opts)
			})
		}
	})

	c.Run("rescale leads to trunc", func() {
		d128P38S10, _, _ := array.FromJSON(c.mem, &arrow.Decimal128Type{Precision: 38, Scale: 10},
			strings.NewReader(`["-02.1234567890", "30.1234567890", null]`))
		d128P28S0, _, _ := array.FromJSON(c.mem, &arrow.Decimal128Type{Precision: 28, Scale: 0},
			strings.NewReader(`["-02.", "30.", null]`))
		d256P28S0, _, _ := array.FromJSON(c.mem, &arrow.Decimal256Type{Precision: 28, Scale: 0},
			strings.NewReader(`["-02.", "30.", null]`))
		d256P38S10RoundTripped, _, _ := array.FromJSON(c.mem, &arrow.Decimal256Type{Precision: 38, Scale: 10},
			strings.NewReader(`["-02.0000000000", "30.0000000000", null]`))
		defer func() {
			d128P38S10.Release()
			d128P28S0.Release()
			d256P28S0.Release()
			d256P38S10RoundTripped.Release()
		}()

		opts.AllowDecimalTruncate = true
		checkCast(c.T(), d128P38S10, d256P28S0, opts)
		checkCast(c.T(), d128P28S0, d256P38S10RoundTripped, opts)

		opts.AllowDecimalTruncate = false
		opts.ToType = d256P28S0.DataType()
		checkCastFails(c.T(), d128P38S10, opts)
		checkCast(c.T(), d128P28S0, d256P38S10RoundTripped, opts)
	})

	c.Run("precision loss without rescale = trunc", func() {
		d128P4S2, _, _ := array.FromJSON(c.mem, &arrow.Decimal128Type{Precision: 4, Scale: 2},
			strings.NewReader(`["12.34"]`))
		defer d128P4S2.Release()
		for _, dt := range []arrow.DataType{
			&arrow.Decimal256Type{Precision: 3, Scale: 2},
			&arrow.Decimal256Type{Precision: 4, Scale: 3},
			&arrow.Decimal256Type{Precision: 2, Scale: 1}} {

			opts.AllowDecimalTruncate = true
			opts.ToType = dt
			out, err := compute.CastArray(context.Background(), d128P4S2, &opts)
			out.Release()
			c.NoError(err)

			opts.AllowDecimalTruncate = false
			opts.ToType = dt
			checkCastFails(c.T(), d128P4S2, opts)
		}
	})
}

func (c *CastSuite) TestDecimal256ToDecimal128() {
	var opts compute.CastOptions

	for _, allowDecTruncate := range []bool{false, true} {
		c.Run("decTruncate="+strconv.FormatBool(allowDecTruncate), func() {
			opts.AllowDecimalTruncate = allowDecTruncate

			noTruncate, _, _ := array.FromJSON(c.mem, &arrow.Decimal256Type{Precision: 42, Scale: 10},
				strings.NewReader(`["02.0000000000", "30.0000000000", "22.0000000000", "-121.0000000000", null]`))
			expected, _, _ := array.FromJSON(c.mem, &arrow.Decimal128Type{Precision: 28, Scale: 0},
				strings.NewReader(`["02.", "30.", "22.", "-121.", null]`))

			defer noTruncate.Release()
			defer expected.Release()

			checkCast(c.T(), noTruncate, expected, opts)
			checkCast(c.T(), expected, noTruncate, opts)
		})
	}

	c.Run("same scale diff precision", func() {
		for _, allowDecTruncate := range []bool{false, true} {
			c.Run("decTruncate="+strconv.FormatBool(allowDecTruncate), func() {
				opts.AllowDecimalTruncate = allowDecTruncate

				dP42S2, _, _ := array.FromJSON(c.mem, &arrow.Decimal256Type{Precision: 42, Scale: 2},
					strings.NewReader(`["12.34", "0.56"]`))
				d42, _, _ := array.FromJSON(c.mem, &arrow.Decimal128Type{Precision: 4, Scale: 2},
					strings.NewReader(`["12.34", "0.56"]`))

				defer dP42S2.Release()
				defer d42.Release()

				checkCast(c.T(), dP42S2, d42, opts)
				checkCast(c.T(), d42, dP42S2, opts)
			})
		}
	})

	c.Run("rescale leads to trunc", func() {
		d256P52S10, _, _ := array.FromJSON(c.mem, &arrow.Decimal256Type{Precision: 52, Scale: 10},
			strings.NewReader(`["-02.1234567890", "30.1234567890", null]`))
		d256P42S0, _, _ := array.FromJSON(c.mem, &arrow.Decimal256Type{Precision: 42, Scale: 0},
			strings.NewReader(`["-02.", "30.", null]`))
		d128P28S0, _, _ := array.FromJSON(c.mem, &arrow.Decimal128Type{Precision: 28, Scale: 0},
			strings.NewReader(`["-02.", "30.", null]`))
		d128P38S10RoundTripped, _, _ := array.FromJSON(c.mem, &arrow.Decimal256Type{Precision: 38, Scale: 10},
			strings.NewReader(`["-02.0000000000", "30.0000000000", null]`))
		defer func() {
			d256P52S10.Release()
			d256P42S0.Release()
			d128P28S0.Release()
			d128P38S10RoundTripped.Release()
		}()

		opts.AllowDecimalTruncate = true
		checkCast(c.T(), d256P52S10, d128P28S0, opts)
		checkCast(c.T(), d256P42S0, d128P38S10RoundTripped, opts)

		opts.AllowDecimalTruncate = false
		opts.ToType = d128P28S0.DataType()
		checkCastFails(c.T(), d256P52S10, opts)
		checkCast(c.T(), d256P42S0, d128P38S10RoundTripped, opts)
	})

	c.Run("precision loss without rescale = trunc", func() {
		d42, _, _ := array.FromJSON(c.mem, &arrow.Decimal256Type{Precision: 4, Scale: 2},
			strings.NewReader(`["12.34"]`))
		defer d42.Release()
		for _, dt := range []arrow.DataType{
			&arrow.Decimal128Type{Precision: 3, Scale: 2},
			&arrow.Decimal128Type{Precision: 4, Scale: 3},
			&arrow.Decimal128Type{Precision: 2, Scale: 1}} {

			opts.AllowDecimalTruncate = true
			opts.ToType = dt
			out, err := compute.CastArray(context.Background(), d42, &opts)
			out.Release()
			c.NoError(err)

			opts.AllowDecimalTruncate = false
			opts.ToType = dt
			checkCastFails(c.T(), d42, opts)
		}
	})
}

func (c *CastSuite) TestFloatingToDecimal() {
	for _, fltType := range []arrow.DataType{arrow.PrimitiveTypes.Float32, arrow.PrimitiveTypes.Float64} {
		c.Run("from "+fltType.String(), func() {
			for _, decType := range []arrow.DataType{&arrow.Decimal128Type{Precision: 5, Scale: 2}, &arrow.Decimal256Type{Precision: 5, Scale: 2}} {
				c.Run("to "+decType.String(), func() {
					c.checkCast(fltType, decType,
						`[0.0, null, 123.45, 123.456, 999.994]`, `["0.00", null, "123.45", "123.46", "999.99"]`)

					c.Run("overflow", func() {
						opts := compute.CastOptions{ToType: decType}
						c.checkCastFails(fltType, `[999.996]`, &opts)

						opts.AllowDecimalTruncate = true
						c.checkCastOpts(fltType, decType, `[0.0, null, 999.996, 123.45, 999.994]`,
							`["0.00", null, "0.00", "123.45", "999.99"]`, opts)
					})
				})
			}
		})
	}

	dec128 := func(prec, scale int32) arrow.DataType {
		return &arrow.Decimal128Type{Precision: prec, Scale: scale}
	}
	dec256 := func(prec, scale int32) arrow.DataType {
		return &arrow.Decimal256Type{Precision: prec, Scale: scale}
	}

	type decFunc func(int32, int32) arrow.DataType

	for _, decType := range []decFunc{dec128, dec256} {
		// 2**64 + 2**41 (exactly representable as a float)
		c.checkCast(arrow.PrimitiveTypes.Float32, decType(20, 0),
			`[1.8446746e+19, -1.8446746e+19]`,
			`[18446746272732807168, -18446746272732807168]`)

		c.checkCast(arrow.PrimitiveTypes.Float64, decType(20, 0),
			`[1.8446744073709556e+19, -1.8446744073709556e+19]`,
			`[18446744073709555712, -18446744073709555712]`)

		c.checkCast(arrow.PrimitiveTypes.Float32, decType(20, 4),
			`[1.8446746e+15, -1.8446746e+15]`,
			`[1844674627273280.7168, -1844674627273280.7168]`)

		c.checkCast(arrow.PrimitiveTypes.Float64, decType(20, 4),
			`[1.8446744073709556e+15, -1.8446744073709556e+15]`,
			`[1844674407370955.5712, -1844674407370955.5712]`)
	}
}

func (c *CastSuite) TestDecimalToFloating() {
	for _, flt := range []arrow.DataType{arrow.PrimitiveTypes.Float32, arrow.PrimitiveTypes.Float64} {
		c.Run(flt.String(), func() {
			for _, dec := range []arrow.DataType{&arrow.Decimal128Type{Precision: 5, Scale: 2}, &arrow.Decimal256Type{Precision: 5, Scale: 2}} {
				c.Run(dec.String(), func() {
					c.checkCast(dec, flt, `["0.00", null, "123.45", "999.99"]`,
						`[0.0, null, 123.45, 999.99]`)
				})
			}
		})
	}
}

func (c *CastSuite) TestDateToString() {
	for _, stype := range []arrow.DataType{arrow.BinaryTypes.String, arrow.BinaryTypes.LargeString} {
		c.checkCast(arrow.FixedWidthTypes.Date32, stype,
			`[0, null]`, `["1970-01-01", null]`)
		c.checkCast(arrow.FixedWidthTypes.Date64, stype,
			`[86400000, null]`, `["1970-01-02", null]`)
	}
}

func (c *CastSuite) TestTimeToString() {
	for _, stype := range []arrow.DataType{arrow.BinaryTypes.String, arrow.BinaryTypes.LargeString} {
		c.checkCast(arrow.FixedWidthTypes.Time32s, stype, `[1, 62]`, `["00:00:01", "00:01:02"]`)
		c.checkCast(arrow.FixedWidthTypes.Time64ns, stype, `[0, 1]`, `["00:00:00.000000000", "00:00:00.000000001"]`)
	}
}

func (c *CastSuite) TestTimestampToString() {
	for _, stype := range []arrow.DataType{arrow.BinaryTypes.String, arrow.BinaryTypes.LargeString} {
		c.checkCast(&arrow.TimestampType{Unit: arrow.Second}, stype,
			`[-30610224000, -5364662400]`, `["1000-01-01 00:00:00", "1800-01-01 00:00:00"]`)

		c.checkCast(&arrow.TimestampType{Unit: arrow.Millisecond}, stype,
			`[-30610224000000, -5364662400000]`, `["1000-01-01 00:00:00.000", "1800-01-01 00:00:00.000"]`)

		c.checkCast(&arrow.TimestampType{Unit: arrow.Microsecond}, stype,
			`[-30610224000000000, -5364662400000000]`, `["1000-01-01 00:00:00.000000", "1800-01-01 00:00:00.000000"]`)

		c.checkCast(&arrow.TimestampType{Unit: arrow.Nanosecond}, stype,
			`[-596933876543210988, 349837323456789012]`, `["1951-02-01 01:02:03.456789012", "1981-02-01 01:02:03.456789012"]`)
	}
}

func (c *CastSuite) TestTimestampWithZoneToString() {
	for _, stype := range []arrow.DataType{arrow.BinaryTypes.String, arrow.BinaryTypes.LargeString} {
		c.checkCast(arrow.FixedWidthTypes.Timestamp_s, stype,
			`[-30610224000, -5364662400]`, `["1000-01-01 00:00:00Z", "1800-01-01 00:00:00Z"]`)

		c.checkCast(&arrow.TimestampType{Unit: arrow.Second, TimeZone: "America/Phoenix"}, stype,
			`[-34226955, 1456767743]`, `["1968-11-30 13:30:45-0700", "2016-02-29 10:42:23-0700"]`)

		c.checkCast(&arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: "America/Phoenix"}, stype,
			`[-34226955877, 1456767743456]`, `["1968-11-30 13:30:44.123-0700", "2016-02-29 10:42:23.456-0700"]`)

		c.checkCast(&arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "America/Phoenix"}, stype,
			`[-34226955877000, 1456767743456789]`, `["1968-11-30 13:30:44.123000-0700", "2016-02-29 10:42:23.456789-0700"]`)

		c.checkCast(&arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "America/Phoenix"}, stype,
			`[-34226955876543211, 1456767743456789246]`, `["1968-11-30 13:30:44.123456789-0700", "2016-02-29 10:42:23.456789246-0700"]`)
	}
}

func (c *CastSuite) assertBinaryZeroCopy(lhs, rhs arrow.Array) {
	// null bitmap and data buffers are always zero-copied
	assertBufferSame(c.T(), lhs, rhs, 0)
	assertBufferSame(c.T(), lhs, rhs, 2)

	lOffsetByteWidth := lhs.DataType().Layout().Buffers[1].ByteWidth
	rOffsetByteWidth := rhs.DataType().Layout().Buffers[1].ByteWidth
	if lOffsetByteWidth == rOffsetByteWidth {
		assertBufferSame(c.T(), lhs, rhs, 1)
		return
	}

	offsets := make([]arrow.Array, 0, 2)
	for _, arr := range []arrow.Array{lhs, rhs} {
		length := arr.Len()
		buffer := arr.Data().Buffers()[1]

		byteWidth := arr.DataType().Layout().Buffers[1].ByteWidth
		switch byteWidth {
		case 4:
			data := array.NewData(arrow.PrimitiveTypes.Int32, length, []*memory.Buffer{nil, buffer}, nil, 0, 0)
			defer data.Release()
			i32 := array.NewInt32Data(data)
			i64, err := compute.CastArray(context.Background(), i32, compute.SafeCastOptions(arrow.PrimitiveTypes.Int64))
			c.Require().NoError(err)
			i32.Release()
			defer i64.Release()
			offsets = append(offsets, i64)
		default:
			data := array.NewData(arrow.PrimitiveTypes.Int64, length, []*memory.Buffer{nil, buffer}, nil, 0, 0)
			defer data.Release()
			i64 := array.NewInt64Data(data)
			defer i64.Release()
			offsets = append(offsets, i64)
		}
	}
	c.Truef(array.Equal(offsets[0], offsets[1]), "lhs: %s\nrhs: %s", offsets[0], offsets[1])
}

func (c *CastSuite) TestBinaryToString() {
	for _, btype := range []arrow.DataType{arrow.BinaryTypes.Binary, arrow.BinaryTypes.LargeBinary} {
		c.Run(btype.String(), func() {
			for _, stype := range []arrow.DataType{arrow.BinaryTypes.String, arrow.BinaryTypes.LargeString} {
				c.Run(stype.String(), func() {
					// empty -> empty always works
					c.checkCast(btype, stype, `[]`, `[]`)

					invalidUtf8 := c.invalidUtf8Arr(btype)
					defer invalidUtf8.Release()

					invalidutf8Str := c.invalidUtf8Arr(stype)
					defer invalidutf8Str.Release()

					// invalid utf8 masked by a null bit is not an error
					masked := c.maskArrayWithNullsAt(invalidUtf8, []int{4})
					expMasked := c.maskArrayWithNullsAt(invalidutf8Str, []int{4})
					defer masked.Release()
					defer expMasked.Release()

					checkCast(c.T(), masked, expMasked, *compute.SafeCastOptions(stype))

					opts := compute.SafeCastOptions(stype)
					checkCastFails(c.T(), invalidUtf8, *opts)

					// override utf8 check
					opts.AllowInvalidUtf8 = true
					strs, err := compute.CastArray(context.Background(), invalidUtf8, opts)
					c.NoError(err)
					defer strs.Release()
					c.assertBinaryZeroCopy(invalidUtf8, strs)
				})
			}
		})
	}

	c.Run("fixed size binary", func() {
		fromType := &arrow.FixedSizeBinaryType{ByteWidth: 3}
		invalidUtf8Arr := c.fixedSizeInvalidUtf8(fromType)
		defer invalidUtf8Arr.Release()
		for _, stype := range []arrow.DataType{arrow.BinaryTypes.String, arrow.BinaryTypes.LargeString} {
			c.Run(stype.String(), func() {
				c.checkCast(fromType, stype, `[]`, `[]`)

				// invalid utf-8 masked by a null bit is not an error
				strInvalidUtf8 := c.fixedSizeInvalidUtf8(stype)
				defer strInvalidUtf8.Release()

				masked := c.maskArrayWithNullsAt(invalidUtf8Arr, []int{4})
				expMasked := c.maskArrayWithNullsAt(strInvalidUtf8, []int{4})
				defer masked.Release()
				defer expMasked.Release()

				checkCast(c.T(), masked, expMasked, *compute.SafeCastOptions(stype))

				opts := compute.SafeCastOptions(stype)
				checkCastFails(c.T(), invalidUtf8Arr, *opts)

				// override utf8 check
				opts.AllowInvalidUtf8 = true
				strs, err := compute.CastArray(context.Background(), invalidUtf8Arr, opts)
				c.NoError(err)
				defer strs.Release()

				// null buffer is not always the same if input is sliced
				assertBufferSame(c.T(), invalidUtf8Arr, strs, 0)

				c.Same(invalidUtf8Arr.Data().Buffers()[1], strs.Data().Buffers()[2])
			})
		}
	})
}

func (c *CastSuite) TestBinaryOrStringToBinary() {
	for _, fromType := range baseBinaryTypes {
		c.Run(fromType.String(), func() {
			for _, toType := range []arrow.DataType{arrow.BinaryTypes.Binary, arrow.BinaryTypes.LargeBinary} {
				c.Run(toType.String(), func() {
					// empty -> empty always works
					c.checkCast(fromType, toType, `[]`, `[]`)

					invalidUtf8 := c.invalidUtf8Arr(fromType)
					defer invalidUtf8.Release()

					// invalid utf-8 is not an error for binary
					out, err := compute.CastToType(context.Background(), invalidUtf8, toType)
					c.NoError(err)
					defer out.Release()
					c.assertBinaryZeroCopy(invalidUtf8, out)

					// invalid utf-8 masked by a null is also not an erro
					invalidutf8Bin := c.invalidUtf8Arr(toType)
					defer invalidutf8Bin.Release()

					// invalid utf8 masked by a null bit is not an error
					masked := c.maskArrayWithNullsAt(invalidUtf8, []int{4})
					expMasked := c.maskArrayWithNullsAt(invalidutf8Bin, []int{4})
					defer masked.Release()
					defer expMasked.Release()

					checkCast(c.T(), masked, expMasked, *compute.SafeCastOptions(toType))
				})
			}
		})
	}

	c.Run("fixed size binary", func() {
		fromType := &arrow.FixedSizeBinaryType{ByteWidth: 3}
		invalidUtf8Arr := c.fixedSizeInvalidUtf8(fromType)
		defer invalidUtf8Arr.Release()

		checkCast(c.T(), invalidUtf8Arr, invalidUtf8Arr, *compute.DefaultCastOptions(true))
		checkCastFails(c.T(), invalidUtf8Arr, *compute.SafeCastOptions(&arrow.FixedSizeBinaryType{ByteWidth: 5}))
		for _, toType := range []arrow.DataType{arrow.BinaryTypes.Binary, arrow.BinaryTypes.LargeBinary} {
			c.Run(toType.String(), func() {
				c.checkCast(fromType, toType, `[]`, `[]`)

				out, err := compute.CastToType(context.Background(), invalidUtf8Arr, toType)
				c.NoError(err)
				defer out.Release()
				assertBufferSame(c.T(), invalidUtf8Arr, out, 0)

				c.Same(invalidUtf8Arr.Data().Buffers()[1], out.Data().Buffers()[2])
			})
		}
	})
}

func (c *CastSuite) TestStringToString() {
	for _, fromType := range []arrow.DataType{arrow.BinaryTypes.String, arrow.BinaryTypes.LargeString} {
		c.Run("from "+fromType.String(), func() {
			for _, toType := range []arrow.DataType{arrow.BinaryTypes.String, arrow.BinaryTypes.LargeString} {
				c.Run("to "+toType.String(), func() {
					c.checkCast(fromType, toType, `[]`, `[]`)

					invalidUtf8 := c.invalidUtf8Arr(fromType)
					defer invalidUtf8.Release()

					invalidutf8Str := c.invalidUtf8Arr(toType)
					defer invalidutf8Str.Release()

					// invalid utf8 masked by a null bit is not an error
					masked := c.maskArrayWithNullsAt(invalidUtf8, []int{4})
					expMasked := c.maskArrayWithNullsAt(invalidutf8Str, []int{4})
					defer masked.Release()
					defer expMasked.Release()

					checkCast(c.T(), masked, expMasked, *compute.SafeCastOptions(toType))

					opts := compute.SafeCastOptions(toType)
					// override utf8 check
					opts.AllowInvalidUtf8 = true
					// utf-8 is not checked by cast when the origin (utf-8) guarantees utf-8
					strs, err := compute.CastArray(context.Background(), invalidUtf8, opts)
					c.NoError(err)
					defer strs.Release()
					c.assertBinaryZeroCopy(invalidUtf8, strs)
				})
			}
		})
	}
}

func (c *CastSuite) TestStringToInt() {
	for _, stype := range []arrow.DataType{arrow.BinaryTypes.String, arrow.BinaryTypes.LargeString} {
		for _, dt := range signedIntTypes {
			c.checkCast(stype, dt,
				`["0", null, "127", "-1", "0", "0x0", "0x7F"]`,
				`[0, null, 127, -1, 0, 0, 127]`)
		}

		c.checkCast(stype, arrow.PrimitiveTypes.Int32,
			`["2147483647", null, "-2147483648", "0", "0X0", "0x7FFFFFFF", "-0X1", "-0x10000000"]`,
			`[2147483647, null, -2147483648, 0, 0, 2147483647, -1, -268435456]`)

		c.checkCast(stype, arrow.PrimitiveTypes.Int64,
			`["9223372036854775807", null, "-9223372036854775808", "0", "0x0", "0x7FFFFFFFFFFFFFFf", "-0x0FFFFFFFFFFFFFFF"]`,
			`[9223372036854775807, null, -9223372036854775808, 0, 0, 9223372036854775807, -1152921504606846975]`)

		for _, dt := range unsignedIntTypes {
			c.checkCast(stype, dt, `["0", null, "127", "255", "0", "0x0", "0xff", "0X7f"]`,
				`[0, null, 127, 255, 0, 0, 255, 127]`)
		}

		c.checkCast(stype, arrow.PrimitiveTypes.Uint32,
			`["2147483647", null, "4294967295", "0", "0x0", "0x7FFFFFFf", "0xFFFFFFFF"]`,
			`[2147483647, null, 4294967295, 0, 0, 2147483647, 4294967295]`)

		c.checkCast(stype, arrow.PrimitiveTypes.Uint64,
			`["9223372036854775807", null, "18446744073709551615", "0", "0x0", "0x7FFFFFFFFFFFFFFf", "0xfFFFFFFFFFFFFFFf"]`,
			`[9223372036854775807, null, 18446744073709551615, 0, 0, 9223372036854775807, 18446744073709551615]`)

		for _, notInt8 := range []string{"z", "12 z", "128", "-129", "0.5", "0x", "0xfff", "-0xf0"} {
			c.checkCastFails(stype, `["`+notInt8+`"]`, compute.SafeCastOptions(arrow.PrimitiveTypes.Int8))
		}

		for _, notUint8 := range []string{"256", "-1", "0.5", "0x", "0x3wa", "0x123"} {
			c.checkCastFails(stype, `["`+notUint8+`"]`, compute.SafeCastOptions(arrow.PrimitiveTypes.Uint8))
		}
	}
}

func (c *CastSuite) TestStringToFloating() {
	for _, stype := range []arrow.DataType{arrow.BinaryTypes.String, arrow.BinaryTypes.LargeString} {
		for _, dt := range []arrow.DataType{arrow.PrimitiveTypes.Float32, arrow.PrimitiveTypes.Float64} {
			c.checkCast(stype, dt, `["0.1", null, "127.3", "1e3", "200.4", "0.5"]`,
				`[0.1, null, 127.3, 1000, 200.4, 0.5]`)

			for _, notFloat := range []string{"z"} {
				c.checkCastFails(stype, `["`+notFloat+`"]`, compute.SafeCastOptions(dt))
			}
		}
	}
}

func (c *CastSuite) TestUnsupportedInputType() {
	// casting to a supported target type, but with an unsupported
	// input for that target type.
	arr, _, _ := array.FromJSON(c.mem, arrow.PrimitiveTypes.Int32, strings.NewReader(`[1, 2, 3]`))
	defer arr.Release()

	toType := arrow.ListOf(arrow.BinaryTypes.String)
	_, err := compute.CastToType(context.Background(), arr, toType)
	c.ErrorIs(err, arrow.ErrNotImplemented)
	c.ErrorContains(err, "function 'cast_list' has no kernel matching input types (int32)")

	// test calling through the generic kernel API
	datum := compute.NewDatum(arr)
	defer datum.Release()
	_, err = compute.CallFunction(context.Background(), "cast", compute.SafeCastOptions(toType), datum)
	c.ErrorIs(err, arrow.ErrNotImplemented)
	c.ErrorContains(err, "function 'cast_list' has no kernel matching input types (int32)")
}

func (c *CastSuite) TestUnsupportedTargetType() {
	arr, _, _ := array.FromJSON(c.mem, arrow.PrimitiveTypes.Int32, strings.NewReader(`[1, 2, 3]`))
	defer arr.Release()

	toType := arrow.DenseUnionOf([]arrow.Field{{Name: "a", Type: arrow.PrimitiveTypes.Int32}}, []arrow.UnionTypeCode{0})
	_, err := compute.CastToType(context.Background(), arr, toType)
	c.ErrorIs(err, arrow.ErrNotImplemented)
	c.ErrorContains(err, "unsupported cast to dense_union<a: type=int32=0> from int32")

	// test calling through the generic kernel API
	datum := compute.NewDatum(arr)
	defer datum.Release()
	_, err = compute.CallFunction(context.Background(), "cast", compute.SafeCastOptions(toType), datum)
	c.ErrorIs(err, arrow.ErrNotImplemented)
	c.ErrorContains(err, "unsupported cast to dense_union<a: type=int32=0> from int32")
}

func (c *CastSuite) checkCastSelfZeroCopy(dt arrow.DataType, json string) {
	arr, _, _ := array.FromJSON(c.mem, dt, strings.NewReader(json))
	defer arr.Release()

	checkCastZeroCopy(c.T(), arr, dt, compute.NewCastOptions(dt, true))
}

func (c *CastSuite) checkCastZeroCopy(from arrow.DataType, json string, to arrow.DataType) {
	arr, _, _ := array.FromJSON(c.mem, from, strings.NewReader(json))
	defer arr.Release()
	checkCastZeroCopy(c.T(), arr, to, compute.NewCastOptions(to, true))
}

func (c *CastSuite) TestTimestampToTimestamp() {
	tests := []struct {
		coarse, fine arrow.DataType
	}{
		{arrow.FixedWidthTypes.Timestamp_s, arrow.FixedWidthTypes.Timestamp_ms},
		{arrow.FixedWidthTypes.Timestamp_ms, arrow.FixedWidthTypes.Timestamp_us},
		{arrow.FixedWidthTypes.Timestamp_us, arrow.FixedWidthTypes.Timestamp_ns},
	}

	var opts compute.CastOptions
	for _, tt := range tests {
		c.Run("coarse "+tt.coarse.String()+" fine "+tt.fine.String(), func() {
			c.checkCast(tt.coarse, tt.fine, `[0, null, 200, 1, 2]`, `[0, null, 200000, 1000, 2000]`)

			opts.AllowTimeTruncate = false
			opts.ToType = tt.coarse
			c.checkCastFails(tt.fine, `[0, null, 200456, 1123, 2456]`, &opts)

			// with truncation allowed, divide/truncate
			opts.AllowTimeTruncate = true
			c.checkCastOpts(tt.fine, tt.coarse, `[0, null, 200456, 1123, 2456]`, `[0, null, 200, 1, 2]`, opts)
		})
	}

	tests = []struct {
		coarse, fine arrow.DataType
	}{
		{arrow.FixedWidthTypes.Timestamp_s, arrow.FixedWidthTypes.Timestamp_ns},
	}

	for _, tt := range tests {
		c.Run("coarse "+tt.coarse.String()+" fine "+tt.fine.String(), func() {
			c.checkCast(tt.coarse, tt.fine, `[0, null, 200, 1, 2]`, `[0, null, 200000000000, 1000000000, 2000000000]`)

			opts.AllowTimeTruncate = false
			opts.ToType = tt.coarse
			c.checkCastFails(tt.fine, `[0, null, 200456000000, 1123000000, 2456000000]`, &opts)

			// with truncation allowed, divide/truncate
			opts.AllowTimeTruncate = true
			c.checkCastOpts(tt.fine, tt.coarse, `[0, null, 200456000000, 1123000000, 2456000000]`, `[0, null, 200, 1, 2]`, opts)
		})
	}
}

func (c *CastSuite) TestTimestampZeroCopy() {
	for _, dt := range []arrow.DataType{arrow.FixedWidthTypes.Timestamp_s /*,  arrow.PrimitiveTypes.Int64*/} {
		c.checkCastZeroCopy(arrow.FixedWidthTypes.Timestamp_s, `[0, null, 2000, 1000, 0]`, dt)
	}

	c.checkCastZeroCopy(arrow.PrimitiveTypes.Int64, `[0, null, 2000, 1000, 0]`, arrow.FixedWidthTypes.Timestamp_s)
}

func (c *CastSuite) TestTimestampToTimestampMultiplyOverflow() {
	opts := compute.CastOptions{ToType: arrow.FixedWidthTypes.Timestamp_ns}
	// 1000-01-01, 1800-01-01, 2000-01-01, 2300-01-01, 3000-01-01
	c.checkCastFails(arrow.FixedWidthTypes.Timestamp_s, `[-30610224000, -5364662400, 946684800, 10413792000, 32503680000]`, &opts)
}

var (
	timestampJSON = `["1970-01-01T00:00:59.123456789","2000-02-29T23:23:23.999999999",
		"1899-01-01T00:59:20.001001001","2033-05-18T03:33:20.000000000",
		"2020-01-01T01:05:05.001", "2019-12-31T02:10:10.002",
		"2019-12-30T03:15:15.003", "2009-12-31T04:20:20.004132",
		"2010-01-01T05:25:25.005321", "2010-01-03T06:30:30.006163",
		"2010-01-04T07:35:35", "2006-01-01T08:40:40", "2005-12-31T09:45:45",
		"2008-12-28", "2008-12-29", "2012-01-01 01:02:03", null]`
	timestampSecondsJSON = `["1970-01-01T00:00:59","2000-02-29T23:23:23",
		"1899-01-01T00:59:20","2033-05-18T03:33:20",
		"2020-01-01T01:05:05", "2019-12-31T02:10:10",
		"2019-12-30T03:15:15", "2009-12-31T04:20:20",
		"2010-01-01T05:25:25", "2010-01-03T06:30:30",
		"2010-01-04T07:35:35", "2006-01-01T08:40:40",
		"2005-12-31T09:45:45", "2008-12-28", "2008-12-29",
		"2012-01-01 01:02:03", null]`
	timestampExtremeJSON = `["1677-09-20T00:00:59.123456", "2262-04-13T23:23:23.999999"]`
)

func (c *CastSuite) TestTimestampToDate() {
	stamps, _, _ := array.FromJSON(c.mem, arrow.FixedWidthTypes.Timestamp_ns, strings.NewReader(timestampJSON))
	defer stamps.Release()
	date32, _, _ := array.FromJSON(c.mem, arrow.FixedWidthTypes.Date32,
		strings.NewReader(`[
			0, 11016, -25932, 23148,
			18262, 18261, 18260, 14609,
			14610, 14612, 14613, 13149,
			13148, 14241, 14242, 15340, null
		]`))
	defer date32.Release()
	date64, _, _ := array.FromJSON(c.mem, arrow.FixedWidthTypes.Date64,
		strings.NewReader(`[
		0, 951782400000, -2240524800000, 1999987200000,
		1577836800000, 1577750400000, 1577664000000, 1262217600000,
		1262304000000, 1262476800000, 1262563200000, 1136073600000,
		1135987200000, 1230422400000, 1230508800000, 1325376000000, null]`), array.WithUseNumber())
	defer date64.Release()

	checkCast(c.T(), stamps, date32, *compute.DefaultCastOptions(true))
	checkCast(c.T(), stamps, date64, *compute.DefaultCastOptions(true))
	c.checkCast(arrow.FixedWidthTypes.Timestamp_us, arrow.FixedWidthTypes.Date32,
		timestampExtremeJSON, `[-106753, 106753]`)
	c.checkCast(arrow.FixedWidthTypes.Timestamp_us, arrow.FixedWidthTypes.Date64,
		timestampExtremeJSON, `[-9223459200000, 9223459200000]`)
	for _, u := range []arrow.TimeUnit{arrow.Second, arrow.Microsecond, arrow.Millisecond, arrow.Nanosecond} {
		dt := &arrow.TimestampType{Unit: u}
		c.checkCastExp(dt, timestampSecondsJSON, date32)
		c.checkCastExp(dt, timestampSecondsJSON, date64)
	}
}

func (c *CastSuite) TestZonedTimestampToDate() {
	c.Run("Pacific/Marquesas", func() {
		dt := &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "Pacific/Marquesas"}
		c.checkCast(dt, arrow.FixedWidthTypes.Date32,
			timestampJSON, `[-1, 11016, -25933, 23147,
				18261, 18260, 18259, 14608,
				14609, 14611, 14612, 13148,
				13148, 14240, 14241, 15339, null]`)
		c.checkCast(dt, arrow.FixedWidthTypes.Date64, timestampJSON,
			`[-86400000, 951782400000, -2240611200000, 1999900800000,
			1577750400000, 1577664000000, 1577577600000, 1262131200000,
			1262217600000, 1262390400000, 1262476800000, 1135987200000,
			1135987200000, 1230336000000, 1230422400000, 1325289600000, null]`)
	})

	for _, u := range []arrow.TimeUnit{arrow.Second, arrow.Millisecond, arrow.Microsecond, arrow.Nanosecond} {
		dt := &arrow.TimestampType{Unit: u, TimeZone: "Australia/Broken_Hill"}
		c.checkCast(dt, arrow.FixedWidthTypes.Date32, timestampSecondsJSON, `[
			0, 11017, -25932, 23148,
			18262, 18261, 18260, 14609,
			14610, 14612, 14613, 13149,
			13148, 14241, 14242, 15340, null]`)
		c.checkCast(dt, arrow.FixedWidthTypes.Date64, timestampSecondsJSON, `[
			0, 951868800000, -2240524800000, 1999987200000, 1577836800000,
			1577750400000, 1577664000000, 1262217600000, 1262304000000,
			1262476800000, 1262563200000, 1136073600000, 1135987200000,
			1230422400000, 1230508800000, 1325376000000, null]`)
	}

	// invalid timezones
	for _, u := range []arrow.TimeUnit{arrow.Second, arrow.Millisecond, arrow.Microsecond, arrow.Nanosecond} {
		dt := &arrow.TimestampType{Unit: u, TimeZone: "Mars/Mariner_Valley"}
		c.checkCastFails(dt, timestampSecondsJSON, compute.NewCastOptions(arrow.FixedWidthTypes.Date32, false))
		c.checkCastFails(dt, timestampSecondsJSON, compute.NewCastOptions(arrow.FixedWidthTypes.Date64, false))
	}
}

func (c *CastSuite) TestTimestampToTime() {
	c.checkCast(arrow.FixedWidthTypes.Timestamp_ns, arrow.FixedWidthTypes.Time64ns,
		timestampJSON, `[
			59123456789, 84203999999999, 3560001001001, 12800000000000,
			3905001000000, 7810002000000, 11715003000000, 15620004132000,
			19525005321000, 23430006163000, 27335000000000, 31240000000000,
			35145000000000, 0, 0, 3723000000000, null]`)
	c.checkCastFails(arrow.FixedWidthTypes.Timestamp_ns, timestampJSON, compute.NewCastOptions(arrow.FixedWidthTypes.Time64us, true))
	c.checkCast(arrow.FixedWidthTypes.Timestamp_us, arrow.FixedWidthTypes.Time64us,
		timestampExtremeJSON, `[59123456, 84203999999]`)

	timesSec := `[59, 84203, 3560, 12800,
				3905, 7810, 11715, 15620,
				19525, 23430, 27335, 31240,
				35145, 0, 0, 3723, null]`
	timesMs := `[59000, 84203000, 3560000, 12800000,
				3905000, 7810000, 11715000, 15620000,
				19525000, 23430000, 27335000, 31240000,
				35145000, 0, 0, 3723000, null]`
	timesUs := `[59000000, 84203000000, 3560000000, 12800000000,
				3905000000, 7810000000, 11715000000, 15620000000,
				19525000000, 23430000000, 27335000000, 31240000000,
				35145000000, 0, 0, 3723000000, null]`
	timesNs := `[59000000000, 84203000000000, 3560000000000, 12800000000000,
				3905000000000, 7810000000000, 11715000000000, 15620000000000,
				19525000000000, 23430000000000, 27335000000000, 31240000000000,
				35145000000000, 0, 0, 3723000000000, null]`

	c.checkCast(arrow.FixedWidthTypes.Timestamp_s, arrow.FixedWidthTypes.Time32s,
		timestampSecondsJSON, timesSec)
	c.checkCast(arrow.FixedWidthTypes.Timestamp_s, arrow.FixedWidthTypes.Time32ms,
		timestampSecondsJSON, timesMs)
	c.checkCast(arrow.FixedWidthTypes.Timestamp_ms, arrow.FixedWidthTypes.Time32s,
		timestampSecondsJSON, timesSec)
	c.checkCast(arrow.FixedWidthTypes.Timestamp_ms, arrow.FixedWidthTypes.Time32ms,
		timestampSecondsJSON, timesMs)
	c.checkCast(arrow.FixedWidthTypes.Timestamp_us, arrow.FixedWidthTypes.Time64us,
		timestampSecondsJSON, timesUs)
	c.checkCast(arrow.FixedWidthTypes.Timestamp_us, arrow.FixedWidthTypes.Time64ns,
		timestampSecondsJSON, timesNs)
	c.checkCast(arrow.FixedWidthTypes.Timestamp_us, arrow.FixedWidthTypes.Time32ms,
		timestampSecondsJSON, timesMs)
	c.checkCast(arrow.FixedWidthTypes.Timestamp_us, arrow.FixedWidthTypes.Time32s,
		timestampSecondsJSON, timesSec)
	c.checkCast(arrow.FixedWidthTypes.Timestamp_ns, arrow.FixedWidthTypes.Time64us,
		timestampSecondsJSON, timesUs)
	c.checkCast(arrow.FixedWidthTypes.Timestamp_ns, arrow.FixedWidthTypes.Time64ns,
		timestampSecondsJSON, timesNs)
	c.checkCast(arrow.FixedWidthTypes.Timestamp_ns, arrow.FixedWidthTypes.Time32ms,
		timestampSecondsJSON, timesMs)
	c.checkCast(arrow.FixedWidthTypes.Timestamp_ns, arrow.FixedWidthTypes.Time32s,
		timestampSecondsJSON, timesSec)

	trunc := compute.CastOptions{AllowTimeTruncate: true}

	timestampsUS := `["1970-01-01T00:00:59.123456","2000-02-29T23:23:23.999999",
					"1899-01-01T00:59:20.001001","2033-05-18T03:33:20.000000",
					"2020-01-01T01:05:05.001", "2019-12-31T02:10:10.002",
					"2019-12-30T03:15:15.003", "2009-12-31T04:20:20.004132",
					"2010-01-01T05:25:25.005321", "2010-01-03T06:30:30.006163",
					"2010-01-04T07:35:35", "2006-01-01T08:40:40", "2005-12-31T09:45:45",
					"2008-12-28", "2008-12-29", "2012-01-01 01:02:03", null]`
	timestampsMS := `["1970-01-01T00:00:59.123","2000-02-29T23:23:23.999",
					"1899-01-01T00:59:20.001","2033-05-18T03:33:20.000",
					"2020-01-01T01:05:05.001", "2019-12-31T02:10:10.002",
					"2019-12-30T03:15:15.003", "2009-12-31T04:20:20.004",
					"2010-01-01T05:25:25.005", "2010-01-03T06:30:30.006",
					"2010-01-04T07:35:35", "2006-01-01T08:40:40", "2005-12-31T09:45:45",
					"2008-12-28", "2008-12-29", "2012-01-01 01:02:03", null]`

	c.checkCastFails(arrow.FixedWidthTypes.Timestamp_ns, timestampJSON, compute.NewCastOptions(arrow.FixedWidthTypes.Time64us, true))
	c.checkCastFails(arrow.FixedWidthTypes.Timestamp_ns, timestampJSON, compute.NewCastOptions(arrow.FixedWidthTypes.Time32ms, true))
	c.checkCastFails(arrow.FixedWidthTypes.Timestamp_ns, timestampJSON, compute.NewCastOptions(arrow.FixedWidthTypes.Time32s, true))
	c.checkCastFails(arrow.FixedWidthTypes.Timestamp_us, timestampsUS, compute.NewCastOptions(arrow.FixedWidthTypes.Time32ms, true))
	c.checkCastFails(arrow.FixedWidthTypes.Timestamp_us, timestampsUS, compute.NewCastOptions(arrow.FixedWidthTypes.Time32s, true))
	c.checkCastFails(arrow.FixedWidthTypes.Timestamp_ms, timestampsMS, compute.NewCastOptions(arrow.FixedWidthTypes.Time32s, true))

	timesNsUs := `[59123456, 84203999999, 3560001001, 12800000000,
				3905001000, 7810002000, 11715003000, 15620004132,
				19525005321, 23430006163, 27335000000, 31240000000,
				35145000000, 0, 0, 3723000000, null]`
	timesNsMs := `[59123, 84203999, 3560001, 12800000,
				3905001, 7810002, 11715003, 15620004,
				19525005, 23430006, 27335000, 31240000,
				35145000, 0, 0, 3723000, null]`
	timesUsNs := `[59123456000, 84203999999000, 3560001001000, 12800000000000,
				3905001000000, 7810002000000, 11715003000000, 15620004132000,
				19525005321000, 23430006163000, 27335000000000, 31240000000000,
				35145000000000, 0, 0, 3723000000000, null]`
	timesMsNs := `[59123000000, 84203999000000, 3560001000000, 12800000000000,
				3905001000000, 7810002000000, 11715003000000, 15620004000000,
				19525005000000, 23430006000000, 27335000000000, 31240000000000,
				35145000000000, 0, 0, 3723000000000, null]`
	timesMsUs := `[59123000, 84203999000, 3560001000, 12800000000,
				3905001000, 7810002000, 11715003000, 15620004000,
				19525005000, 23430006000, 27335000000, 31240000000,
				35145000000, 0, 0, 3723000000, null]`

	c.checkCastOpts(arrow.FixedWidthTypes.Timestamp_ns, arrow.FixedWidthTypes.Time64us, timestampJSON, timesNsUs, trunc)
	c.checkCastOpts(arrow.FixedWidthTypes.Timestamp_ns, arrow.FixedWidthTypes.Time32ms, timestampJSON, timesNsMs, trunc)
	c.checkCastOpts(arrow.FixedWidthTypes.Timestamp_ns, arrow.FixedWidthTypes.Time32s, timestampJSON, timesSec, trunc)
	c.checkCastOpts(arrow.FixedWidthTypes.Timestamp_us, arrow.FixedWidthTypes.Time32ms, timestampsUS, timesNsMs, trunc)
	c.checkCastOpts(arrow.FixedWidthTypes.Timestamp_us, arrow.FixedWidthTypes.Time32s, timestampsUS, timesSec, trunc)
	c.checkCastOpts(arrow.FixedWidthTypes.Timestamp_ms, arrow.FixedWidthTypes.Time32s, timestampsMS, timesSec, trunc)

	// upscaling tests
	c.checkCast(arrow.FixedWidthTypes.Timestamp_us, arrow.FixedWidthTypes.Time64ns, timestampsUS, timesUsNs)
	c.checkCast(arrow.FixedWidthTypes.Timestamp_ms, arrow.FixedWidthTypes.Time64ns, timestampsMS, timesMsNs)
	c.checkCast(arrow.FixedWidthTypes.Timestamp_ms, arrow.FixedWidthTypes.Time64us, timestampsMS, timesMsUs)
	c.checkCast(arrow.FixedWidthTypes.Timestamp_s, arrow.FixedWidthTypes.Time64ns, timestampSecondsJSON, timesNs)
	c.checkCast(arrow.FixedWidthTypes.Timestamp_s, arrow.FixedWidthTypes.Time64us, timestampSecondsJSON, timesUs)
	c.checkCast(arrow.FixedWidthTypes.Timestamp_s, arrow.FixedWidthTypes.Time32ms, timestampSecondsJSON, timesMs)

	// invalid timezones
	for _, u := range []arrow.TimeUnit{arrow.Second, arrow.Millisecond, arrow.Microsecond, arrow.Nanosecond} {
		dt := &arrow.TimestampType{Unit: u, TimeZone: "Mars/Mariner_Valley"}
		switch u {
		case arrow.Second, arrow.Millisecond:
			c.checkCastFails(dt, timestampSecondsJSON, compute.NewCastOptions(&arrow.Time32Type{Unit: u}, false))
		default:
			c.checkCastFails(dt, timestampSecondsJSON, compute.NewCastOptions(&arrow.Time64Type{Unit: u}, false))
		}
	}
}

func (c *CastSuite) TestZonedTimestampToTime() {
	c.checkCast(&arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "Pacific/Marquesas"},
		arrow.FixedWidthTypes.Time64ns, timestampJSON, `[52259123456789, 50003999999999, 56480001001001, 65000000000000,
			56105001000000, 60010002000000, 63915003000000, 67820004132000,
			71725005321000, 75630006163000, 79535000000000, 83440000000000,
			945000000000, 52200000000000, 52200000000000, 55923000000000, null]`)

	timesSec := `[
		34259, 35603, 35960, 47000,
		41705, 45610, 49515, 53420,
		57325, 61230, 65135, 69040,
		72945, 37800, 37800, 41523, null
	]`
	timesMs := `[
		34259000, 35603000, 35960000, 47000000,
		41705000, 45610000, 49515000, 53420000,
		57325000, 61230000, 65135000, 69040000,
		72945000, 37800000, 37800000, 41523000, null
	]`
	timesUs := `[
		34259000000, 35603000000, 35960000000, 47000000000,
		41705000000, 45610000000, 49515000000, 53420000000,
		57325000000, 61230000000, 65135000000, 69040000000,
		72945000000, 37800000000, 37800000000, 41523000000, null
	]`
	timesNs := `[
		34259000000000, 35603000000000, 35960000000000, 47000000000000,
		41705000000000, 45610000000000, 49515000000000, 53420000000000,
		57325000000000, 61230000000000, 65135000000000, 69040000000000,
		72945000000000, 37800000000000, 37800000000000, 41523000000000, null
	]`

	c.checkCast(&arrow.TimestampType{Unit: arrow.Second, TimeZone: "Australia/Broken_Hill"},
		arrow.FixedWidthTypes.Time32s, timestampSecondsJSON, timesSec)
	c.checkCast(&arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: "Australia/Broken_Hill"},
		arrow.FixedWidthTypes.Time32ms, timestampSecondsJSON, timesMs)
	c.checkCast(&arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "Australia/Broken_Hill"},
		arrow.FixedWidthTypes.Time64us, timestampSecondsJSON, timesUs)
	c.checkCast(&arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "Australia/Broken_Hill"},
		arrow.FixedWidthTypes.Time64ns, timestampSecondsJSON, timesNs)
}

func (c *CastSuite) TestTimeToTime() {
	var opts compute.CastOptions

	tests := []struct {
		coarse, fine arrow.DataType
	}{
		{arrow.FixedWidthTypes.Time32s, arrow.FixedWidthTypes.Time32ms},
		{arrow.FixedWidthTypes.Time32ms, arrow.FixedWidthTypes.Time64us},
		{arrow.FixedWidthTypes.Time64us, arrow.FixedWidthTypes.Time64ns},
	}

	for _, tt := range tests {
		c.Run("coarse "+tt.coarse.String()+" fine "+tt.fine.String(), func() {
			coarse := `[0, null, 200, 1, 2]`
			promoted := `[0, null, 200000, 1000, 2000]`
			willBeTruncated := `[0, null, 200456, 1123, 2456]`

			c.checkCast(tt.coarse, tt.fine, coarse, promoted)

			opts.AllowTimeTruncate = false
			opts.ToType = tt.coarse
			c.checkCastFails(tt.fine, willBeTruncated, &opts)

			opts.AllowTimeTruncate = true
			c.checkCastOpts(tt.fine, tt.coarse, willBeTruncated, coarse, opts)
		})
	}

	tests = []struct {
		coarse, fine arrow.DataType
	}{
		{arrow.FixedWidthTypes.Time32s, arrow.FixedWidthTypes.Time64us},
		{arrow.FixedWidthTypes.Time32ms, arrow.FixedWidthTypes.Time64ns},
	}

	for _, tt := range tests {
		c.Run("coarse "+tt.coarse.String()+" fine "+tt.fine.String(), func() {
			coarse := `[0, null, 200, 1, 2]`
			promoted := `[0, null, 200000000, 1000000, 2000000]`
			willBeTruncated := `[0, null, 200456000, 1123000, 2456000]`

			c.checkCast(tt.coarse, tt.fine, coarse, promoted)

			opts.AllowTimeTruncate = false
			opts.ToType = tt.coarse
			c.checkCastFails(tt.fine, willBeTruncated, &opts)

			opts.AllowTimeTruncate = true
			c.checkCastOpts(tt.fine, tt.coarse, willBeTruncated, coarse, opts)
		})
	}

	tests = []struct {
		coarse, fine arrow.DataType
	}{
		{arrow.FixedWidthTypes.Time32s, arrow.FixedWidthTypes.Time64ns},
	}

	for _, tt := range tests {
		c.Run("coarse "+tt.coarse.String()+" fine "+tt.fine.String(), func() {
			coarse := `[0, null, 200, 1, 2]`
			promoted := `[0, null, 200000000000, 1000000000, 2000000000]`
			willBeTruncated := `[0, null, 200456000000, 1123000000, 2456000000]`

			c.checkCast(tt.coarse, tt.fine, coarse, promoted)

			opts.AllowTimeTruncate = false
			opts.ToType = tt.coarse
			c.checkCastFails(tt.fine, willBeTruncated, &opts)

			opts.AllowTimeTruncate = true
			c.checkCastOpts(tt.fine, tt.coarse, willBeTruncated, coarse, opts)
		})
	}
}

func (c *CastSuite) TestTimeZeroCopy() {
	for _, dt := range []arrow.DataType{arrow.FixedWidthTypes.Time32s /*, arrow.PrimitiveTypes.Int32*/} {
		c.checkCastZeroCopy(arrow.FixedWidthTypes.Time32s, `[0, null, 2000, 1000, 0]`, dt)
	}
	c.checkCastZeroCopy(arrow.PrimitiveTypes.Int32, `[0, null, 2000, 1000, 0]`, arrow.FixedWidthTypes.Time32s)

	for _, dt := range []arrow.DataType{arrow.FixedWidthTypes.Time64us /*, arrow.PrimitiveTypes.Int64*/} {
		c.checkCastZeroCopy(arrow.FixedWidthTypes.Time64us, `[0, null, 2000, 1000, 0]`, dt)
	}
	c.checkCastZeroCopy(arrow.PrimitiveTypes.Int64, `[0, null, 2000, 1000, 0]`, arrow.FixedWidthTypes.Time64us)
}

func (c *CastSuite) TestDateToDate() {
	day32 := `[0, null, 100, 1, 10]`
	day64 := `[0, null,  8640000000, 86400000, 864000000]`

	// multiply promotion
	c.checkCast(arrow.FixedWidthTypes.Date32, arrow.FixedWidthTypes.Date64, day32, day64)
	// no truncation
	c.checkCast(arrow.FixedWidthTypes.Date64, arrow.FixedWidthTypes.Date32, day64, day32)

	day64WillBeTruncated := `[0, null, 8640000123, 86400456, 864000789]`

	opts := compute.CastOptions{ToType: arrow.FixedWidthTypes.Date32}
	c.checkCastFails(arrow.FixedWidthTypes.Date64, day64WillBeTruncated, &opts)

	opts.AllowTimeTruncate = true
	c.checkCastOpts(arrow.FixedWidthTypes.Date64, arrow.FixedWidthTypes.Date32,
		day64WillBeTruncated, day32, opts)
}

func (c *CastSuite) TestDateZeroCopy() {
	for _, dt := range []arrow.DataType{arrow.FixedWidthTypes.Date32 /*, arrow.PrimitiveTypes.Int32*/} {
		c.checkCastZeroCopy(arrow.FixedWidthTypes.Date32, `[0, null, 2000, 1000, 0]`, dt)
	}
	c.checkCastZeroCopy(arrow.PrimitiveTypes.Int32, `[0, null, 2000, 1000, 0]`, arrow.FixedWidthTypes.Date32)

	for _, dt := range []arrow.DataType{arrow.FixedWidthTypes.Date64 /*, arrow.PrimitiveTypes.Int64*/} {
		c.checkCastZeroCopy(arrow.FixedWidthTypes.Date64, `[0, null, 172800000, 86400000, 0]`, dt)
	}
	c.checkCastZeroCopy(arrow.PrimitiveTypes.Int64, `[0, null, 172800000, 86400000, 0]`, arrow.FixedWidthTypes.Date64)
}

func (c *CastSuite) TestDurationToDuration() {
	var opts compute.CastOptions

	tests := []struct {
		coarse, fine arrow.DataType
	}{
		{arrow.FixedWidthTypes.Duration_s, arrow.FixedWidthTypes.Duration_ms},
		{arrow.FixedWidthTypes.Duration_ms, arrow.FixedWidthTypes.Duration_us},
		{arrow.FixedWidthTypes.Duration_us, arrow.FixedWidthTypes.Duration_ns},
	}

	for _, tt := range tests {
		c.Run("coarse "+tt.coarse.String()+" fine "+tt.fine.String(), func() {
			coarse := `[0, null, 200, 1, 2]`
			promoted := `[0, null, 200000, 1000, 2000]`
			willBeTruncated := `[0, null, 200456, 1123, 2456]`

			c.checkCast(tt.coarse, tt.fine, coarse, promoted)

			opts.AllowTimeTruncate = false
			opts.ToType = tt.coarse
			c.checkCastFails(tt.fine, willBeTruncated, &opts)

			opts.AllowTimeTruncate = true
			c.checkCastOpts(tt.fine, tt.coarse, willBeTruncated, coarse, opts)
		})
	}

	tests = []struct {
		coarse, fine arrow.DataType
	}{
		{arrow.FixedWidthTypes.Duration_s, arrow.FixedWidthTypes.Duration_us},
		{arrow.FixedWidthTypes.Duration_ms, arrow.FixedWidthTypes.Duration_ns},
	}

	for _, tt := range tests {
		c.Run("coarse "+tt.coarse.String()+" fine "+tt.fine.String(), func() {
			coarse := `[0, null, 200, 1, 2]`
			promoted := `[0, null, 200000000, 1000000, 2000000]`
			willBeTruncated := `[0, null, 200456000, 1123000, 2456000]`

			c.checkCast(tt.coarse, tt.fine, coarse, promoted)

			opts.AllowTimeTruncate = false
			opts.ToType = tt.coarse
			c.checkCastFails(tt.fine, willBeTruncated, &opts)

			opts.AllowTimeTruncate = true
			c.checkCastOpts(tt.fine, tt.coarse, willBeTruncated, coarse, opts)
		})
	}

	tests = []struct {
		coarse, fine arrow.DataType
	}{
		{arrow.FixedWidthTypes.Duration_s, arrow.FixedWidthTypes.Duration_ns},
	}

	for _, tt := range tests {
		c.Run("coarse "+tt.coarse.String()+" fine "+tt.fine.String(), func() {
			coarse := `[0, null, 200, 1, 2]`
			promoted := `[0, null, 200000000000, 1000000000, 2000000000]`
			willBeTruncated := `[0, null, 200456000000, 1123000000, 2456000000]`

			c.checkCast(tt.coarse, tt.fine, coarse, promoted)

			opts.AllowTimeTruncate = false
			opts.ToType = tt.coarse
			c.checkCastFails(tt.fine, willBeTruncated, &opts)

			opts.AllowTimeTruncate = true
			c.checkCastOpts(tt.fine, tt.coarse, willBeTruncated, coarse, opts)
		})
	}
}

func (c *CastSuite) TestDurationZeroCopy() {
	for _, dt := range []arrow.DataType{arrow.FixedWidthTypes.Duration_s /*, arrow.PrimitiveTypes.Int64*/} {
		c.checkCastZeroCopy(arrow.FixedWidthTypes.Duration_s, `[0, null, 2000, 1000, 0]`, dt)
	}
	c.checkCastZeroCopy(arrow.PrimitiveTypes.Int64, `[0, null, 2000, 1000, 0]`, arrow.FixedWidthTypes.Duration_s)
}

func (c *CastSuite) TestDurationToDurationMultiplyOverflow() {
	opts := compute.CastOptions{ToType: arrow.FixedWidthTypes.Duration_ns}
	c.checkCastFails(arrow.FixedWidthTypes.Duration_s, `[10000000000, 1, 2, 3, 10000000000]`, &opts)
}

func (c *CastSuite) TestStringToTimestamp() {
	for _, dt := range []arrow.DataType{arrow.BinaryTypes.String, arrow.BinaryTypes.LargeString} {
		c.checkCast(dt, &arrow.TimestampType{Unit: arrow.Second}, `["1970-01-01", null, "2000-02-29"]`, `[0, null, 951782400]`)
		c.checkCast(dt, &arrow.TimestampType{Unit: arrow.Microsecond}, `["1970-01-01", null, "2000-02-29"]`, `[0, null, 951782400000000]`)

		for _, u := range []arrow.TimeUnit{arrow.Second, arrow.Millisecond, arrow.Microsecond, arrow.Nanosecond} {
			for _, notTS := range []string{"", "xxx"} {
				opts := compute.NewCastOptions(&arrow.TimestampType{Unit: u}, true)
				c.checkCastFails(dt, `["`+notTS+`"]`, opts)
			}
		}

		zoned, _, _ := array.FromJSON(c.mem, dt, strings.NewReader(`["2020-02-29T00:00:00Z", "2020-03-02T10:11:12+0102"]`))
		defer zoned.Release()
		mixed, _, _ := array.FromJSON(c.mem, dt, strings.NewReader(`["2020-03-02T10:11:12+0102", "2020-02-29T00:00:00"]`))
		defer mixed.Release()

		c.checkCastArr(zoned, &arrow.TimestampType{Unit: arrow.Second, TimeZone: "UTC"}, `[1582934400, 1583140152]`, *compute.DefaultCastOptions(true))

		// timestamp with zone offset should not parse as naive
		checkCastFails(c.T(), zoned, *compute.NewCastOptions(&arrow.TimestampType{Unit: arrow.Second}, true))

		// mixed zoned/unzoned should not parse as naive
		checkCastFails(c.T(), mixed, *compute.NewCastOptions(&arrow.TimestampType{Unit: arrow.Second}, true))

		// timestamp with zone offset can parse as any time zone (since they're unambiguous)
		c.checkCastArr(zoned, arrow.FixedWidthTypes.Timestamp_s, `[1582934400, 1583140152]`, *compute.DefaultCastOptions(true))
		c.checkCastArr(zoned, &arrow.TimestampType{Unit: arrow.Second, TimeZone: "America/Phoenix"}, `[1582934400, 1583140152]`, *compute.DefaultCastOptions(true))
	}
}

func (c *CastSuite) TestIntToString() {
	for _, stype := range []arrow.DataType{arrow.BinaryTypes.String, arrow.BinaryTypes.LargeString} {
		c.Run(stype.String(), func() {
			c.checkCast(arrow.PrimitiveTypes.Int8, stype,
				`[0, 1, 127, -128, null]`, `["0", "1", "127", "-128", null]`)

			c.checkCast(arrow.PrimitiveTypes.Uint8, stype,
				`[0, 1, 255, null]`, `["0", "1", "255", null]`)

			c.checkCast(arrow.PrimitiveTypes.Int16, stype,
				`[0, 1, 32767, -32768, null]`, `["0", "1", "32767", "-32768", null]`)

			c.checkCast(arrow.PrimitiveTypes.Uint16, stype,
				`[0, 1, 65535, null]`, `["0", "1", "65535", null]`)

			c.checkCast(arrow.PrimitiveTypes.Int32, stype,
				`[0, 1, 2147483647, -2147483648, null]`,
				`["0", "1", "2147483647", "-2147483648", null]`)

			c.checkCast(arrow.PrimitiveTypes.Uint32, stype,
				`[0, 1, 4294967295, null]`, `["0", "1", "4294967295", null]`)

			c.checkCast(arrow.PrimitiveTypes.Int64, stype,
				`[0, 1, 9223372036854775807, -9223372036854775808, null]`,
				`["0", "1", "9223372036854775807", "-9223372036854775808", null]`)

			c.checkCast(arrow.PrimitiveTypes.Uint64, stype,
				`[0, 1, 18446744073709551615, null]`, `["0", "1", "18446744073709551615", null]`)
		})
	}
}

func (c *CastSuite) TestFloatingToString() {
	for _, stype := range []arrow.DataType{arrow.BinaryTypes.String, arrow.BinaryTypes.LargeString} {
		c.Run(stype.String(), func() {
			bldr := array.NewFloat32Builder(c.mem)
			defer bldr.Release()
			bldr.AppendValues([]float32{
				0, float32(math.Copysign(0, -1)), 1.5, float32(math.Inf(-1)),
				float32(math.Inf(0)), float32(math.NaN())}, nil)
			bldr.AppendNull()
			arr := bldr.NewArray()
			defer arr.Release()

			bldr64 := array.NewFloat64Builder(c.mem)
			defer bldr64.Release()
			bldr64.AppendValues([]float64{
				0, math.Copysign(0, -1), 1.5, math.Inf(-1), math.Inf(0), math.NaN()}, nil)
			bldr64.AppendNull()
			arr64 := bldr64.NewArray()
			defer arr64.Release()

			c.checkCastArr(arr, stype, `["0", "-0", "1.5", "-Inf", "+Inf", "NaN", null]`, *compute.DefaultCastOptions(true))

			c.checkCastArr(arr64, stype, `["0", "-0", "1.5", "-Inf", "+Inf", "NaN", null]`, *compute.DefaultCastOptions(true))
		})
	}
}

func (c *CastSuite) TestBooleanToString() {
	for _, stype := range []arrow.DataType{arrow.BinaryTypes.String, arrow.BinaryTypes.LargeString} {
		c.Run(stype.String(), func() {
			c.checkCast(arrow.FixedWidthTypes.Boolean, stype,
				`[true, true, false, null]`, `["true", "true", "false", null]`)
		})
	}
}

func (c *CastSuite) TestIdentityCasts() {
	c.checkCastSelfZeroCopy(arrow.FixedWidthTypes.Boolean, `[false, true, null, false]`)

	c.checkCastSelfZeroCopy(arrow.Null, `[null, null, null]`)
	for _, typ := range numericTypes {
		c.checkCastSelfZeroCopy(typ, `[1, 2, null, 4]`)
	}

	// ["foo", "bar"] base64 encoded for binary
	c.checkCastSelfZeroCopy(arrow.BinaryTypes.Binary, `["Zm9v", "YmFy"]`)
	c.checkCastSelfZeroCopy(arrow.BinaryTypes.String, `["foo", "bar"]`)
	c.checkCastSelfZeroCopy(&arrow.FixedSizeBinaryType{ByteWidth: 3}, `["Zm9v", "YmFy"]`)

	c.checkCastSelfZeroCopy(arrow.FixedWidthTypes.Time32ms, `[1, 2, 3, 4]`)
	c.checkCastSelfZeroCopy(arrow.FixedWidthTypes.Time64us, `[1, 2, 3, 4]`)
	c.checkCastSelfZeroCopy(arrow.FixedWidthTypes.Date32, `[1, 2, 3, 4]`)
	c.checkCastSelfZeroCopy(arrow.FixedWidthTypes.Date64, `[86400000, 0]`)
	c.checkCastSelfZeroCopy(arrow.FixedWidthTypes.Timestamp_s, `[1, 2, 3, 4]`)

	c.checkCastSelfZeroCopy(&arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.PrimitiveTypes.Int8},
		`[1, 2, 3, 1, null, 3]`)
}

func (c *CastSuite) TestListToPrimitive() {
	arr, _, _ := array.FromJSON(c.mem, arrow.ListOf(arrow.PrimitiveTypes.Int8), strings.NewReader(`[[1, 2], [3, 4]]`))
	defer arr.Release()

	_, err := compute.CastToType(context.Background(), arr, arrow.PrimitiveTypes.Uint8)
	c.ErrorIs(err, arrow.ErrNotImplemented)
}

type makeList func(arrow.DataType) arrow.DataType

var listFactories = []makeList{
	func(dt arrow.DataType) arrow.DataType { return arrow.ListOf(dt) },
	func(dt arrow.DataType) arrow.DataType { return arrow.LargeListOf(dt) },
}

func (c *CastSuite) checkListToList(valTypes []arrow.DataType, jsonData string) {
	for _, makeSrc := range listFactories {
		for _, makeDest := range listFactories {
			for _, srcValueType := range valTypes {
				for _, dstValueType := range valTypes {
					srcType := makeSrc(srcValueType)
					dstType := makeDest(dstValueType)
					c.Run(fmt.Sprintf("from %s to %s", srcType, dstType), func() {
						c.checkCast(srcType, dstType, jsonData, jsonData)
					})
				}
			}
		}
	}
}

func (c *CastSuite) TestListToList() {
	c.checkListToList([]arrow.DataType{arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Float32, arrow.PrimitiveTypes.Int64},
		`[[0], [1], null, [2, 3, 4], [5, 6], null, [], [7], [8, 9]]`)
}

func (c *CastSuite) TestListToListNoNulls() {
	c.checkListToList([]arrow.DataType{arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Float32, arrow.PrimitiveTypes.Int64},
		`[[0], [1], [2, 3, 4], [5, 6], [], [7], [8, 9]]`)
}

func (c *CastSuite) TestListToListOptionsPassthru() {
	for _, makeSrc := range listFactories {
		for _, makeDest := range listFactories {
			opts := compute.SafeCastOptions(makeDest(arrow.PrimitiveTypes.Int16))
			c.checkCastFails(makeSrc(arrow.PrimitiveTypes.Int32), `[[87654321]]`, opts)

			opts.AllowIntOverflow = true
			c.checkCastOpts(makeSrc(arrow.PrimitiveTypes.Int32), makeDest(arrow.PrimitiveTypes.Int16),
				`[[87654321]]`, `[[32689]]`, *opts)
		}
	}
}

func (c *CastSuite) checkStructToStruct(types []arrow.DataType) {
	for _, srcType := range types {
		c.Run(srcType.String(), func() {
			for _, destType := range types {
				c.Run(destType.String(), func() {
					fieldNames := []string{"a", "b"}
					a1, _, _ := array.FromJSON(c.mem, srcType, strings.NewReader(`[1, 2, 3, 4, null]`))
					b1, _, _ := array.FromJSON(c.mem, srcType, strings.NewReader(`[null, 7, 8, 9, 0]`))
					a2, _, _ := array.FromJSON(c.mem, destType, strings.NewReader(`[1, 2, 3, 4, null]`))
					b2, _, _ := array.FromJSON(c.mem, destType, strings.NewReader(`[null, 7, 8, 9, 0]`))
					src, _ := array.NewStructArray([]arrow.Array{a1, b1}, fieldNames)
					dest, _ := array.NewStructArray([]arrow.Array{a2, b2}, fieldNames)
					defer func() {
						a1.Release()
						b1.Release()
						a2.Release()
						b2.Release()
						src.Release()
						dest.Release()
					}()

					checkCast(c.T(), src, dest, *compute.DefaultCastOptions(true))
					c.Run("with nulls", func() {
						nullBitmap := memory.NewBufferBytes([]byte{10})
						srcNullData := src.Data().(*array.Data).Copy()
						srcNullData.Buffers()[0] = nullBitmap
						srcNullData.SetNullN(3)
						defer srcNullData.Release()
						destNullData := dest.Data().(*array.Data).Copy()
						destNullData.Buffers()[0] = nullBitmap
						destNullData.SetNullN(3)
						defer destNullData.Release()

						srcNulls := array.NewStructData(srcNullData)
						destNulls := array.NewStructData(destNullData)
						defer srcNulls.Release()
						defer destNulls.Release()

						checkCast(c.T(), srcNulls, destNulls, *compute.DefaultCastOptions(true))
					})
				})
			}
		})
	}
}

func (c *CastSuite) checkStructToStructSubset(types []arrow.DataType) {
	for _, srcType := range types {
		c.Run(srcType.String(), func() {
			for _, destType := range types {
				c.Run(destType.String(), func() {
					fieldNames := []string{"a", "b", "c", "d", "e"}

					a1, _, _ := array.FromJSON(c.mem, srcType, strings.NewReader(`[1, 2, 5]`))
					defer a1.Release()
					b1, _, _ := array.FromJSON(c.mem, srcType, strings.NewReader(`[3, 4, 7]`))
					defer b1.Release()
					c1, _, _ := array.FromJSON(c.mem, srcType, strings.NewReader(`[9, 11, 44]`))
					defer c1.Release()
					d1, _, _ := array.FromJSON(c.mem, srcType, strings.NewReader(`[6, 51, 49]`))
					defer d1.Release()
					e1, _, _ := array.FromJSON(c.mem, srcType, strings.NewReader(`[19, 17, 74]`))
					defer e1.Release()

					a2, _, _ := array.FromJSON(c.mem, destType, strings.NewReader(`[1, 2, 5]`))
					defer a2.Release()
					b2, _, _ := array.FromJSON(c.mem, destType, strings.NewReader(`[3, 4, 7]`))
					defer b2.Release()
					c2, _, _ := array.FromJSON(c.mem, destType, strings.NewReader(`[9, 11, 44]`))
					defer c2.Release()
					d2, _, _ := array.FromJSON(c.mem, destType, strings.NewReader(`[6, 51, 49]`))
					defer d2.Release()
					e2, _, _ := array.FromJSON(c.mem, destType, strings.NewReader(`[19, 17, 74]`))
					defer e2.Release()

					src, _ := array.NewStructArray([]arrow.Array{a1, b1, c1, d1, e1}, fieldNames)
					defer src.Release()
					dest1, _ := array.NewStructArray([]arrow.Array{a2}, []string{"a"})
					defer dest1.Release()

					opts := *compute.DefaultCastOptions(true)
					checkCast(c.T(), src, dest1, opts)

					dest2, _ := array.NewStructArray([]arrow.Array{b2, c2}, []string{"b", "c"})
					defer dest2.Release()
					checkCast(c.T(), src, dest2, opts)

					dest3, _ := array.NewStructArray([]arrow.Array{c2, d2, e2}, []string{"c", "d", "e"})
					defer dest3.Release()
					checkCast(c.T(), src, dest3, opts)

					dest4, _ := array.NewStructArray([]arrow.Array{a2, b2, c2, e2}, []string{"a", "b", "c", "e"})
					defer dest4.Release()
					checkCast(c.T(), src, dest4, opts)

					dest5, _ := array.NewStructArray([]arrow.Array{a2, b2, c2, d2, e2}, []string{"a", "b", "c", "d", "e"})
					defer dest5.Release()
					checkCast(c.T(), src, dest5, opts)

					// field does not exist
					dest6 := arrow.StructOf(
						arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int8, Nullable: true},
						arrow.Field{Name: "d", Type: arrow.PrimitiveTypes.Int16, Nullable: true},
						arrow.Field{Name: "f", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
					)
					options6 := compute.SafeCastOptions(dest6)
					_, err := compute.CastArray(context.TODO(), src, options6)
					c.ErrorIs(err, arrow.ErrType)
					c.ErrorContains(err, "struct fields don't match or are in the wrong order")

					// fields in wrong order
					dest7 := arrow.StructOf(
						arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int8, Nullable: true},
						arrow.Field{Name: "c", Type: arrow.PrimitiveTypes.Int16, Nullable: true},
						arrow.Field{Name: "b", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
					)
					options7 := compute.SafeCastOptions(dest7)
					_, err = compute.CastArray(context.TODO(), src, options7)
					c.ErrorIs(err, arrow.ErrType)
					c.ErrorContains(err, "struct fields don't match or are in the wrong order")
				})
			}
		})
	}
}

func (c *CastSuite) checkStructToStructSubsetWithNulls(types []arrow.DataType) {
	for _, srcType := range types {
		c.Run(srcType.String(), func() {
			for _, destType := range types {
				c.Run(destType.String(), func() {
					fieldNames := []string{"a", "b", "c", "d", "e"}

					a1, _, _ := array.FromJSON(c.mem, srcType, strings.NewReader(`[1, 2, 5]`))
					defer a1.Release()
					b1, _, _ := array.FromJSON(c.mem, srcType, strings.NewReader(`[3, null, 7]`))
					defer b1.Release()
					c1, _, _ := array.FromJSON(c.mem, srcType, strings.NewReader(`[9, 11, 44]`))
					defer c1.Release()
					d1, _, _ := array.FromJSON(c.mem, srcType, strings.NewReader(`[6, 51, null]`))
					defer d1.Release()
					e1, _, _ := array.FromJSON(c.mem, srcType, strings.NewReader(`[null, 17, 74]`))
					defer e1.Release()

					a2, _, _ := array.FromJSON(c.mem, destType, strings.NewReader(`[1, 2, 5]`))
					defer a2.Release()
					b2, _, _ := array.FromJSON(c.mem, destType, strings.NewReader(`[3, null, 7]`))
					defer b2.Release()
					c2, _, _ := array.FromJSON(c.mem, destType, strings.NewReader(`[9, 11, 44]`))
					defer c2.Release()
					d2, _, _ := array.FromJSON(c.mem, destType, strings.NewReader(`[6, 51, null]`))
					defer d2.Release()
					e2, _, _ := array.FromJSON(c.mem, destType, strings.NewReader(`[null, 17, 74]`))
					defer e2.Release()

					// 0, 1, 0
					nullBitmap := memory.NewBufferBytes([]byte{2})
					srcNull, _ := array.NewStructArrayWithNulls([]arrow.Array{a1, b1, c1, d1, e1}, fieldNames, nullBitmap, 2, 0)
					defer srcNull.Release()

					dest1Null, _ := array.NewStructArrayWithNulls([]arrow.Array{a2}, []string{"a"}, nullBitmap, -1, 0)
					defer dest1Null.Release()
					opts := compute.DefaultCastOptions(true)
					checkCast(c.T(), srcNull, dest1Null, *opts)

					dest2Null, _ := array.NewStructArrayWithNulls([]arrow.Array{b2, c2}, []string{"b", "c"}, nullBitmap, -1, 0)
					defer dest2Null.Release()
					checkCast(c.T(), srcNull, dest2Null, *opts)

					dest3Null, _ := array.NewStructArrayWithNulls([]arrow.Array{a2, d2, e2}, []string{"a", "d", "e"}, nullBitmap, -1, 0)
					defer dest3Null.Release()
					checkCast(c.T(), srcNull, dest3Null, *opts)

					dest4Null, _ := array.NewStructArrayWithNulls([]arrow.Array{a2, b2, c2, e2}, []string{"a", "b", "c", "e"}, nullBitmap, -1, 0)
					defer dest4Null.Release()
					checkCast(c.T(), srcNull, dest4Null, *opts)

					dest5Null, _ := array.NewStructArrayWithNulls([]arrow.Array{a2, b2, c2, d2, e2}, []string{"a", "b", "c", "d", "e"}, nullBitmap, -1, 0)
					defer dest5Null.Release()
					checkCast(c.T(), srcNull, dest5Null, *opts)

					// field does not exist
					dest6Null := arrow.StructOf(
						arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int8, Nullable: true},
						arrow.Field{Name: "d", Type: arrow.PrimitiveTypes.Int16, Nullable: true},
						arrow.Field{Name: "f", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
					)
					options6Null := compute.SafeCastOptions(dest6Null)
					_, err := compute.CastArray(context.TODO(), srcNull, options6Null)
					c.ErrorIs(err, arrow.ErrType)
					c.ErrorContains(err, "struct fields don't match or are in the wrong order")

					// fields in wrong order
					dest7Null := arrow.StructOf(
						arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int8, Nullable: true},
						arrow.Field{Name: "c", Type: arrow.PrimitiveTypes.Int16, Nullable: true},
						arrow.Field{Name: "b", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
					)
					options7Null := compute.SafeCastOptions(dest7Null)
					_, err = compute.CastArray(context.TODO(), srcNull, options7Null)
					c.ErrorIs(err, arrow.ErrType)
					c.ErrorContains(err, "struct fields don't match or are in the wrong order")
				})
			}
		})
	}
}

func (c *CastSuite) TestStructToSameSizedAndNamedStruct() {
	c.checkStructToStruct(numericTypes)
}

func (c *CastSuite) TestStructToStructSubset() {
	c.checkStructToStructSubset(numericTypes)
}

func (c *CastSuite) TestStructToStructSubsetWithNulls() {
	c.checkStructToStructSubsetWithNulls(numericTypes)
}

func (c *CastSuite) TestStructToSameSizedButDifferentNamedStruct() {
	fieldNames := []string{"a", "b"}
	a, _, _ := array.FromJSON(c.mem, arrow.PrimitiveTypes.Int8, strings.NewReader(`[1, 2]`))
	defer a.Release()
	b, _, _ := array.FromJSON(c.mem, arrow.PrimitiveTypes.Int8, strings.NewReader(`[3, 4]`))
	defer b.Release()

	src, _ := array.NewStructArray([]arrow.Array{a, b}, fieldNames)
	defer src.Release()

	dest := arrow.StructOf(
		arrow.Field{Name: "c", Type: arrow.PrimitiveTypes.Int8, Nullable: true},
		arrow.Field{Name: "d", Type: arrow.PrimitiveTypes.Int8, Nullable: true},
	)
	opts := compute.SafeCastOptions(dest)
	_, err := compute.CastArray(context.TODO(), src, opts)
	c.ErrorIs(err, arrow.ErrType)
	c.ErrorContains(err, "struct fields don't match or are in the wrong order")
}

func (c *CastSuite) TestStructToBiggerStruct() {
	fieldNames := []string{"a", "b"}
	a, _, _ := array.FromJSON(c.mem, arrow.PrimitiveTypes.Int8, strings.NewReader(`[1, 2]`))
	defer a.Release()
	b, _, _ := array.FromJSON(c.mem, arrow.PrimitiveTypes.Int8, strings.NewReader(`[3, 4]`))
	defer b.Release()

	src, _ := array.NewStructArray([]arrow.Array{a, b}, fieldNames)
	defer src.Release()

	dest := arrow.StructOf(
		arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int8, Nullable: true},
		arrow.Field{Name: "b", Type: arrow.PrimitiveTypes.Int8, Nullable: true},
		arrow.Field{Name: "c", Type: arrow.PrimitiveTypes.Int8, Nullable: true},
	)
	opts := compute.SafeCastOptions(dest)
	_, err := compute.CastArray(context.TODO(), src, opts)
	c.ErrorIs(err, arrow.ErrType)
	c.ErrorContains(err, "struct fields don't match or are in the wrong order")
}

func (c *CastSuite) TestStructToDifferentNullabilityStruct() {
	c.Run("non-nullable to nullable", func() {
		fieldsSrcNonNullable := []arrow.Field{
			{Name: "a", Type: arrow.PrimitiveTypes.Int8},
			{Name: "b", Type: arrow.PrimitiveTypes.Int8},
			{Name: "c", Type: arrow.PrimitiveTypes.Int8},
		}
		srcNonNull, _, err := array.FromJSON(c.mem, arrow.StructOf(fieldsSrcNonNullable...),
			strings.NewReader(`[
				{"a": 11, "b": 32, "c", 95},
				{"a": 23, "b": 46, "c": 11},
				{"a": 56, "b": 37, "c": 44}
			]`))
		c.Require().NoError(err)
		defer srcNonNull.Release()

		fieldsDest1Nullable := []arrow.Field{
			{Name: "a", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: "b", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: "c", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		}
		destNullable, _, err := array.FromJSON(c.mem, arrow.StructOf(fieldsDest1Nullable...),
			strings.NewReader(`[
				{"a": 11, "b": 32, "c", 95},
				{"a": 23, "b": 46, "c": 11},
				{"a": 56, "b": 37, "c": 44}
			]`))
		c.Require().NoError(err)
		defer destNullable.Release()

		checkCast(c.T(), srcNonNull, destNullable, *compute.DefaultCastOptions(true))

		fieldsDest2Nullable := []arrow.Field{
			{Name: "a", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: "c", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		}

		data := array.NewData(arrow.StructOf(fieldsDest2Nullable...), destNullable.Len(), destNullable.Data().Buffers(),
			[]arrow.ArrayData{destNullable.Data().Children()[0], destNullable.Data().Children()[2]},
			destNullable.NullN(), 0)
		defer data.Release()
		dest2Nullable := array.NewStructData(data)
		defer dest2Nullable.Release()
		checkCast(c.T(), srcNonNull, dest2Nullable, *compute.DefaultCastOptions(true))

		fieldsDest3Nullable := []arrow.Field{
			{Name: "b", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		}

		data = array.NewData(arrow.StructOf(fieldsDest3Nullable...), destNullable.Len(), destNullable.Data().Buffers(),
			[]arrow.ArrayData{destNullable.Data().Children()[1]}, destNullable.NullN(), 0)
		defer data.Release()
		dest3Nullable := array.NewStructData(data)
		defer dest3Nullable.Release()
		checkCast(c.T(), srcNonNull, dest3Nullable, *compute.DefaultCastOptions(true))
	})
	c.Run("non-nullable to nullable", func() {
		fieldsSrcNullable := []arrow.Field{
			{Name: "a", Type: arrow.PrimitiveTypes.Int8, Nullable: true},
			{Name: "b", Type: arrow.PrimitiveTypes.Int8, Nullable: true},
			{Name: "c", Type: arrow.PrimitiveTypes.Int8, Nullable: true},
		}
		srcNullable, _, err := array.FromJSON(c.mem, arrow.StructOf(fieldsSrcNullable...),
			strings.NewReader(`[
				{"a": 1, "b": 3, "c", 9},
				{"a": null, "b": 4, "c": 11},
				{"a": 5, "b": null, "c": 44}
			]`))
		c.Require().NoError(err)
		defer srcNullable.Release()

		fieldsDest1NonNullable := []arrow.Field{
			{Name: "a", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "b", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "c", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		}
		dest1NonNullable := arrow.StructOf(fieldsDest1NonNullable...)
		options1NoNullable := compute.SafeCastOptions(dest1NonNullable)
		_, err = compute.CastArray(context.TODO(), srcNullable, options1NoNullable)
		c.ErrorIs(err, arrow.ErrType)
		c.ErrorContains(err, "cannot cast nullable field to non-nullable field")

		fieldsDest2NonNullable := []arrow.Field{
			{Name: "a", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "c", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		}
		dest2NonNullable := arrow.StructOf(fieldsDest2NonNullable...)
		options2NoNullable := compute.SafeCastOptions(dest2NonNullable)
		_, err = compute.CastArray(context.TODO(), srcNullable, options2NoNullable)
		c.ErrorIs(err, arrow.ErrType)
		c.ErrorContains(err, "cannot cast nullable field to non-nullable field")

		fieldsDest3NonNullable := []arrow.Field{
			{Name: "c", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		}
		dest3NonNullable := arrow.StructOf(fieldsDest3NonNullable...)
		options3NoNullable := compute.SafeCastOptions(dest3NonNullable)
		_, err = compute.CastArray(context.TODO(), srcNullable, options3NoNullable)
		c.ErrorIs(err, arrow.ErrType)
		c.ErrorContains(err, "cannot cast nullable field to non-nullable field")
	})
}

func (c *CastSuite) smallIntArrayFromJSON(data string) arrow.Array {
	arr, _, _ := array.FromJSON(c.mem, types.NewSmallintType(), strings.NewReader(data))
	return arr
}

func (c *CastSuite) TestExtensionTypeToIntDowncast() {
	smallint := types.NewSmallintType()
	arrow.RegisterExtensionType(smallint)
	defer arrow.UnregisterExtensionType("smallint")

	c.Run("smallint(int16) to int16", func() {
		arr := c.smallIntArrayFromJSON(`[0, 100, 200, 1, 2]`)
		defer arr.Release()

		checkCastZeroCopy(c.T(), arr, arrow.PrimitiveTypes.Int16, compute.DefaultCastOptions(true))

		c.checkCast(smallint, arrow.PrimitiveTypes.Uint8,
			`[0, 100, 200, 1, 2]`, `[0, 100, 200, 1, 2]`)
	})

	c.Run("smallint(int16) to uint8 with overflow", func() {
		opts := compute.SafeCastOptions(arrow.PrimitiveTypes.Uint8)
		c.checkCastFails(smallint, `[0, null, 256, 1, 3]`, opts)

		opts.AllowIntOverflow = true
		c.checkCastOpts(smallint, arrow.PrimitiveTypes.Uint8,
			`[0, null, 256, 1, 3]`, `[0, null, 0, 1, 3]`, *opts)
	})

	c.Run("smallint(int16) to uint8 with underflow", func() {
		opts := compute.SafeCastOptions(arrow.PrimitiveTypes.Uint8)
		c.checkCastFails(smallint, `[0, null, -1, 1, 3]`, opts)

		opts.AllowIntOverflow = true
		c.checkCastOpts(smallint, arrow.PrimitiveTypes.Uint8,
			`[0, null, -1, 1, 3]`, `[0, null, 255, 1, 3]`, *opts)
	})
}

func (c *CastSuite) TestNoOutBitmapIfIsAllValid() {
	a, _, _ := array.FromJSON(c.mem, arrow.PrimitiveTypes.Int8, strings.NewReader(`[1]`))
	defer a.Release()

	opts := compute.SafeCastOptions(arrow.PrimitiveTypes.Int32)
	result, err := compute.CastArray(context.Background(), a, opts)
	c.NoError(err)
	c.NotNil(a.Data().Buffers()[0])
	c.Nil(result.Data().Buffers()[0])
}

func (c *CastSuite) TestFromDictionary() {
	ctx := compute.WithAllocator(context.Background(), c.mem)

	dictionaries := []arrow.Array{}

	for _, ty := range numericTypes {
		a, _, _ := array.FromJSON(c.mem, ty, strings.NewReader(`[23, 12, 45, 12, null]`))
		defer a.Release()
		dictionaries = append(dictionaries, a)
	}

	for _, ty := range []arrow.DataType{arrow.BinaryTypes.String, arrow.BinaryTypes.LargeString} {
		a, _, _ := array.FromJSON(c.mem, ty, strings.NewReader(`["foo", "bar", "baz", "foo", null]`))
		defer a.Release()
		dictionaries = append(dictionaries, a)
	}

	for _, d := range dictionaries {
		for _, ty := range dictIndexTypes {
			indices, _, _ := array.FromJSON(c.mem, ty, strings.NewReader(`[4, 0, 1, 2, 0, 4, null, 2]`))

			expected, err := compute.Take(ctx, compute.TakeOptions{}, &compute.ArrayDatum{d.Data()}, &compute.ArrayDatum{indices.Data()})
			c.Require().NoError(err)
			exp := expected.(*compute.ArrayDatum).MakeArray()

			dictArr := array.NewDictionaryArray(&arrow.DictionaryType{IndexType: ty, ValueType: d.DataType()}, indices, d)
			checkCast(c.T(), dictArr, exp, *compute.SafeCastOptions(d.DataType()))

			indices.Release()
			expected.Release()
			exp.Release()
			dictArr.Release()
			return
		}
	}
}

func TestCasts(t *testing.T) {
	suite.Run(t, new(CastSuite))
}

const rngseed = 0x94378165

func benchmarkNumericCast(b *testing.B, fromType, toType arrow.DataType, opts compute.CastOptions, size, min, max int64, nullprob float64) {
	rng := gen.NewRandomArrayGenerator(rngseed, memory.DefaultAllocator)
	arr := rng.Numeric(fromType.ID(), size, min, max, nullprob)
	var (
		err   error
		out   compute.Datum
		ctx   = context.Background()
		input = compute.NewDatum(arr.Data())
	)

	b.Cleanup(func() {
		arr.Release()
		input.Release()
	})

	opts.ToType = toType
	b.ResetTimer()
	b.SetBytes(size * int64(fromType.(arrow.FixedWidthDataType).Bytes()))
	for i := 0; i < b.N; i++ {
		out, err = compute.CastDatum(ctx, input, &opts)
		if err != nil {
			b.Fatal(err)
		}
		out.Release()
	}
}

func benchmarkFloatingToIntegerCast(b *testing.B, fromType, toType arrow.DataType, opts compute.CastOptions, size, min, max int64, nullprob float64) {
	rng := gen.NewRandomArrayGenerator(rngseed, memory.DefaultAllocator)
	arr := rng.Numeric(toType.ID(), size, min, max, nullprob)
	asFloat, err := compute.CastToType(context.Background(), arr, fromType)
	if err != nil {
		b.Fatal(err)
	}
	arr.Release()

	var (
		out   compute.Datum
		ctx   = context.Background()
		input = compute.NewDatum(asFloat.Data())
	)

	b.Cleanup(func() {
		asFloat.Release()
		input.Release()
	})

	opts.ToType = toType
	b.ResetTimer()
	b.SetBytes(size * int64(fromType.(arrow.FixedWidthDataType).Bytes()))
	for i := 0; i < b.N; i++ {
		out, err = compute.CastDatum(ctx, input, &opts)
		if err != nil {
			b.Fatal(err)
		}
		out.Release()
	}
}

func BenchmarkCasting(b *testing.B) {
	type benchfn func(b *testing.B, fromType, toType arrow.DataType, opts compute.CastOptions, size, min, max int64, nullprob float64)

	tests := []struct {
		from, to arrow.DataType
		min, max int64
		safe     bool
		fn       benchfn
	}{
		{arrow.PrimitiveTypes.Int64, arrow.PrimitiveTypes.Int32, math.MinInt32, math.MaxInt32, true, benchmarkNumericCast},
		{arrow.PrimitiveTypes.Int64, arrow.PrimitiveTypes.Int32, math.MinInt32, math.MaxInt32, false, benchmarkNumericCast},
		{arrow.PrimitiveTypes.Uint32, arrow.PrimitiveTypes.Int32, 0, math.MaxInt32, true, benchmarkNumericCast},
		{arrow.PrimitiveTypes.Int64, arrow.PrimitiveTypes.Float64, 0, 1000, true, benchmarkNumericCast},
		{arrow.PrimitiveTypes.Int64, arrow.PrimitiveTypes.Float64, 0, 1000, false, benchmarkNumericCast},
		{arrow.PrimitiveTypes.Float64, arrow.PrimitiveTypes.Int32, -1000, 1000, true, benchmarkFloatingToIntegerCast},
		{arrow.PrimitiveTypes.Float64, arrow.PrimitiveTypes.Int32, -1000, 1000, false, benchmarkFloatingToIntegerCast},
	}

	for _, tt := range tests {
		for _, sz := range []int64{int64(CpuCacheSizes[1]) /* L2 Cache Size */} {
			for _, nullProb := range []float64{0, 0.1, 0.5, 0.9, 1} {
				arraySize := sz / int64(tt.from.(arrow.FixedWidthDataType).Bytes())
				opts := compute.DefaultCastOptions(tt.safe)
				b.Run(fmt.Sprintf("sz=%d/nullprob=%.2f/from=%s/to=%s/safe=%t", arraySize, nullProb, tt.from, tt.to, tt.safe), func(b *testing.B) {
					tt.fn(b, tt.from, tt.to, *opts, arraySize, tt.min, tt.max, nullProb)
				})
			}
		}
	}
}
