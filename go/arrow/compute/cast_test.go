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
	"strings"
	"testing"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/compute"
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

	canCast(arrow.Null, []arrow.DataType{arrow.FixedWidthTypes.Boolean})
	cannotCast(arrow.Null, numericTypes)
	cannotCast(arrow.Null, baseBinaryTypes)
	cannotCast(arrow.Null, []arrow.DataType{
		arrow.FixedWidthTypes.Date32, arrow.FixedWidthTypes.Date64, arrow.FixedWidthTypes.Time32ms, arrow.FixedWidthTypes.Timestamp_s,
	})

	canCast(arrow.FixedWidthTypes.Boolean, []arrow.DataType{arrow.FixedWidthTypes.Boolean})
	cannotCast(arrow.FixedWidthTypes.Boolean, []arrow.DataType{arrow.Null})
}

func (c *CastSuite) checkCastFails(dt arrow.DataType, input string, opts *compute.CastOptions) {
	inArr, _, _ := array.FromJSON(c.mem, dt, strings.NewReader(input))
	defer inArr.Release()

	checkCastFails(c.T(), inArr, *opts)
}

func (c *CastSuite) checkCast(dtIn, dtOut arrow.DataType, inJSON, outJSON string) {
	inArr, _, _ := array.FromJSON(c.mem, dtIn, strings.NewReader(inJSON))
	outArr, _, _ := array.FromJSON(c.mem, dtOut, strings.NewReader(outJSON))
	defer inArr.Release()
	defer outArr.Release()

	checkCast(c.T(), inArr, outArr, *compute.DefaultCastOptions(true))
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

func (c *CastSuite) checkCastZeroCopy(dt arrow.DataType, json string) {
	arr, _, _ := array.FromJSON(c.mem, dt, strings.NewReader(json))
	defer arr.Release()

	checkCastZeroCopy(c.T(), arr, dt, compute.NewCastOptions(dt, true))
}

func (c *CastSuite) TestIdentityCasts() {
	c.checkCastZeroCopy(arrow.FixedWidthTypes.Boolean, `[false, true, null, false]`)
}

func TestCasts(t *testing.T) {
	suite.Run(t, new(CastSuite))
}
