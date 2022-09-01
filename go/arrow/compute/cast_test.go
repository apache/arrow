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

package compute_test

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/bitutil"
	"github.com/apache/arrow/go/v10/arrow/compute"
	"github.com/apache/arrow/go/v10/arrow/decimal128"
	"github.com/apache/arrow/go/v10/arrow/decimal256"
	"github.com/apache/arrow/go/v10/arrow/internal/testing/types"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"github.com/apache/arrow/go/v10/arrow/scalar"
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

func assertDatumsEqual(t *testing.T, expected, actual compute.Datum) {
	require.Equal(t, expected.Kind(), actual.Kind())

	switch expected.Kind() {
	case compute.KindScalar:
		want := expected.(*compute.ScalarDatum).Value
		got := actual.(*compute.ScalarDatum).Value
		assert.Truef(t, scalar.Equals(want, got), "expected: %s\ngot: %s", want, got)
	case compute.KindArray:
		want := expected.(*compute.ArrayDatum).MakeArray()
		got := actual.(*compute.ArrayDatum).MakeArray()
		assert.Truef(t, array.Equal(want, got), "expected: %s\ngot: %s", want, got)
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
	assertDatumsEqual(t, expected, out)
}

func checkScalarWithScalars(t *testing.T, funcName string, inputs []scalar.Scalar, expected scalar.Scalar, opts compute.FunctionOptions) {
	datums := getDatums(inputs)
	defer func() {
		for _, d := range datums {
			d.Release()
		}
	}()
	out, err := compute.CallFunction(context.Background(), funcName, opts, datums...)
	assert.NoError(t, err)
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
	integerTypes = []arrow.DataType{
		arrow.PrimitiveTypes.Uint8,
		arrow.PrimitiveTypes.Int8,
		arrow.PrimitiveTypes.Uint16,
		arrow.PrimitiveTypes.Int16,
		arrow.PrimitiveTypes.Uint32,
		arrow.PrimitiveTypes.Int32,
		arrow.PrimitiveTypes.Uint64,
		arrow.PrimitiveTypes.Int64,
	}
	numericTypes = append(integerTypes,
		arrow.PrimitiveTypes.Float32,
		arrow.PrimitiveTypes.Float64)
	baseBinaryTypes = []arrow.DataType{
		arrow.BinaryTypes.Binary,
		arrow.BinaryTypes.LargeBinary,
		arrow.BinaryTypes.String,
		arrow.BinaryTypes.LargeString,
	}
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
	arr, _, err := array.FromJSON(c.mem, dt, strings.NewReader(`["Hi", "olá mundo", "你好世界", "", "`+"\xa0\xa1"+`"]`))
	c.Require().NoError(err)
	return arr
}

func (c *CastSuite) fixedSizeInvalidUtf8(dt arrow.DataType) arrow.Array {
	if dt.ID() == arrow.FIXED_SIZE_BINARY {
		c.Require().Equal(3, dt.(*arrow.FixedSizeBinaryType).ByteWidth)
	}
	arr, _, err := array.FromJSON(c.mem, dt, strings.NewReader(`["Hi!", "lá", "你", "   ", "`+"\xa0\xa1\xa2"+`"]`))
	c.Require().NoError(err)
	return arr
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

	// will uncomment lines as support for those casts is added

	canCast(arrow.Null, []arrow.DataType{arrow.FixedWidthTypes.Boolean})
	canCast(arrow.Null, numericTypes)
	cannotCast(arrow.Null, baseBinaryTypes)
	cannotCast(arrow.Null, []arrow.DataType{
		arrow.FixedWidthTypes.Date32, arrow.FixedWidthTypes.Date64, arrow.FixedWidthTypes.Time32ms, arrow.FixedWidthTypes.Timestamp_s,
	})
	// canCast(&arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Uint16, ValueType: arrow.Null}, []arrow.DataType{arrow.Null})

	canCast(arrow.FixedWidthTypes.Boolean, []arrow.DataType{arrow.FixedWidthTypes.Boolean})
	canCast(arrow.FixedWidthTypes.Boolean, numericTypes)
	// canCast(arrow.FixedWidthTypes.Boolean, []arrow.DataType{arrow.BinaryTypes.String, arrow.BinaryTypes.LargeString})
	// canCast(&arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: arrow.FixedWidthTypes.Boolean}, []arrow.DataType{arrow.FixedWidthTypes.Boolean})

	cannotCast(arrow.FixedWidthTypes.Boolean, []arrow.DataType{arrow.Null})
	cannotCast(arrow.FixedWidthTypes.Boolean, []arrow.DataType{arrow.BinaryTypes.Binary, arrow.BinaryTypes.LargeBinary})
	cannotCast(arrow.FixedWidthTypes.Boolean, []arrow.DataType{
		arrow.FixedWidthTypes.Date32, arrow.FixedWidthTypes.Date64, arrow.FixedWidthTypes.Time32ms, arrow.FixedWidthTypes.Timestamp_s})

	for _, from := range numericTypes {
		canCast(from, []arrow.DataType{arrow.FixedWidthTypes.Boolean})
		canCast(from, numericTypes)
		// canCast(from, []arrow.DataType{arrow.BinaryTypes.String, arrow.BinaryTypes.LargeString})
		// canCast(&arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: from}, []arrow.DataType{from})

		cannotCast(from, []arrow.DataType{arrow.Null})
	}

	for _, from := range baseBinaryTypes {
		// canCast(from, []arrow.DataType{arrow.FixedWidthTypes.Boolean})
		// canCast(from, numericTypes)
		// canCast(from, baseBinaryTypes)
		// canCast(&arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int64, ValueType: from}, []arrow.DataType{from})

		// any cast which is valid for the dictionary is valid for the dictionary array
		// canCast(&arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Uint32, ValueType: from}, baseBinaryTypes)
		// canCast(&arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int16, ValueType: from}, baseBinaryTypes)

		cannotCast(from, []arrow.DataType{arrow.Null})
	}

	// canCast(arrow.BinaryTypes.String, []arrow.DataType{arrow.FixedWidthTypes.Timestamp_ms})
	// canCast(arrow.BinaryTypes.LargeString, []arrow.DataType{arrow.FixedWidthTypes.Timestamp_ns})
	// no formatting supported
	cannotCast(arrow.FixedWidthTypes.Timestamp_us, []arrow.DataType{arrow.BinaryTypes.Binary, arrow.BinaryTypes.LargeBinary})

	// canCast(&arrow.FixedSizeBinaryType{ByteWidth: 3}, []arrow.DataType{
	// 	arrow.BinaryTypes.Binary, arrow.BinaryTypes.LargeBinary, arrow.BinaryTypes.String, arrow.BinaryTypes.LargeString,
	// 	&arrow.FixedSizeBinaryType{ByteWidth: 3}})

	arrow.RegisterExtensionType(types.NewSmallintType())
	defer arrow.UnregisterExtensionType("smallint")
	// canCast(types.NewSmallintType(), []arrow.DataType{arrow.PrimitiveTypes.Int16})
	// canCast(types.NewSmallintType(), numericTypes) // any cast which is valid for storage is supported
	// canCast(arrow.Null, []arrow.DataType{types.NewSmallintType()})

	// canCast(arrow.FixedWidthTypes.Date32, []arrow.DataType{arrow.BinaryTypes.String, arrow.BinaryTypes.LargeString})
	// canCast(arrow.FixedWidthTypes.Date64, []arrow.DataType{arrow.BinaryTypes.String, arrow.BinaryTypes.LargeString})
	// canCast(arrow.FixedWidthTypes.Timestamp_ns, []arrow.DataType{arrow.BinaryTypes.String, arrow.BinaryTypes.LargeString})
	// canCast(arrow.FixedWidthTypes.Timestamp_us, []arrow.DataType{arrow.BinaryTypes.String, arrow.BinaryTypes.LargeString})
	// canCast(arrow.FixedWidthTypes.Time32ms, []arrow.DataType{arrow.BinaryTypes.String, arrow.BinaryTypes.LargeString})
	// canCast(arrow.FixedWidthTypes.Time64ns, []arrow.DataType{arrow.BinaryTypes.String, arrow.BinaryTypes.LargeString})
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

func (c *CastSuite) checkCastZeroCopy(dt arrow.DataType, json string) {
	arr, _, _ := array.FromJSON(c.mem, dt, strings.NewReader(json))
	defer arr.Release()

	checkCastZeroCopy(c.T(), arr, dt, compute.NewCastOptions(dt, true))
}

func (c *CastSuite) TestIdentityCasts() {
	c.checkCastZeroCopy(arrow.FixedWidthTypes.Boolean, `[false, true, null, false]`)
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

func TestCasts(t *testing.T) {
	suite.Run(t, new(CastSuite))
}
