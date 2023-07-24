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
	"strings"
	"testing"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/bitutil"
	"github.com/apache/arrow/go/v13/arrow/compute"
	"github.com/apache/arrow/go/v13/arrow/compute/internal/exec"
	"github.com/apache/arrow/go/v13/arrow/internal/testing/gen"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/stretchr/testify/suite"
)

var runEndTypes = []arrow.DataType{
	arrow.PrimitiveTypes.Int16,
	arrow.PrimitiveTypes.Int32,
	arrow.PrimitiveTypes.Int64,
}

type RunEndEncodeDecodeSuite struct {
	suite.Suite
	mem *memory.CheckedAllocator

	runEndType arrow.DataType
	valueType  arrow.DataType
	jsonData   []string

	expected compute.Datum
	input    compute.Datum

	ctx context.Context
}

func (suite *RunEndEncodeDecodeSuite) SetupTest() {
	suite.mem = memory.NewCheckedAllocator(memory.DefaultAllocator)
	suite.ctx = compute.WithAllocator(context.Background(), suite.mem)

	switch len(suite.jsonData) {
	case 1:
		expected, _, err := array.FromJSON(suite.mem,
			arrow.RunEndEncodedOf(suite.runEndType, suite.valueType),
			strings.NewReader(suite.jsonData[0]))
		suite.Require().NoError(err)
		defer expected.Release()

		input, _, err := array.FromJSON(suite.mem, suite.valueType, strings.NewReader(suite.jsonData[0]))
		suite.Require().NoError(err)
		defer input.Release()

		suite.expected = compute.NewDatum(expected)
		suite.input = compute.NewDatum(input)
	default:
		var err error
		exChunks := make([]arrow.Array, len(suite.jsonData))
		inputChunks := make([]arrow.Array, len(suite.jsonData))
		for i, data := range suite.jsonData {
			exChunks[i], _, err = array.FromJSON(suite.mem,
				arrow.RunEndEncodedOf(suite.runEndType, suite.valueType),
				strings.NewReader(data))
			suite.Require().NoError(err)
			defer exChunks[i].Release()

			inputChunks[i], _, err = array.FromJSON(suite.mem,
				suite.valueType, strings.NewReader(data))
			suite.Require().NoError(err)
			defer inputChunks[i].Release()
		}

		chunked := arrow.NewChunked(exChunks[0].DataType(), exChunks)
		suite.expected = &compute.ChunkedDatum{Value: chunked}
		chunked = arrow.NewChunked(inputChunks[0].DataType(), inputChunks)
		suite.input = &compute.ChunkedDatum{Value: chunked}
	}
}

func (suite *RunEndEncodeDecodeSuite) TearDownTest() {
	suite.expected.Release()
	suite.input.Release()
	suite.mem.AssertSize(suite.T(), 0)
}

func (suite *RunEndEncodeDecodeSuite) TestEncodeArray() {
	result, err := compute.RunEndEncode(suite.ctx,
		compute.RunEndEncodeOptions{RunEndType: suite.runEndType}, suite.input)
	suite.Require().NoError(err)
	defer result.Release()

	assertDatumsEqual(suite.T(), suite.expected, result, nil, nil)
}

func (suite *RunEndEncodeDecodeSuite) TestDecodeArray() {
	result, err := compute.RunEndDecode(suite.ctx, suite.expected)
	suite.Require().NoError(err)
	defer result.Release()

	assertDatumsEqual(suite.T(), suite.input, result, nil, nil)
}

func (suite *RunEndEncodeDecodeSuite) TestEncodeWithOffset() {
	// skip chunked examples for ease of testing
	expected, ok := suite.expected.(*compute.ArrayDatum)
	if !ok {
		suite.T().SkipNow()
	}

	input := suite.input.(*compute.ArrayDatum)

	if input.Len() == 0 {
		// skip 0 len arrays for this test
		suite.T().SkipNow()
	}

	expectedOffset := array.NewSliceData(expected.Value, 1, expected.Len())
	defer expectedOffset.Release()
	inputOffset := array.NewSliceData(input.Value, 1, input.Len())
	defer inputOffset.Release()

	result, err := compute.RunEndEncode(suite.ctx,
		compute.RunEndEncodeOptions{RunEndType: suite.runEndType},
		&compute.ArrayDatum{Value: inputOffset})
	suite.Require().NoError(err)
	defer result.Release()

	assertDatumsEqual(suite.T(), &compute.ArrayDatum{Value: expectedOffset}, result, nil, nil)
}

func (suite *RunEndEncodeDecodeSuite) TestDecodeWithOffset() {
	// skip chunked examples for ease of testing
	expected, ok := suite.expected.(*compute.ArrayDatum)
	if !ok {
		suite.T().SkipNow()
	}

	input := suite.input.(*compute.ArrayDatum)

	if input.Len() == 0 {
		// skip 0 len arrays for this test
		suite.T().SkipNow()
	}

	expectedOffset := array.NewSliceData(expected.Value, 1, expected.Len())
	defer expectedOffset.Release()
	inputOffset := array.NewSliceData(input.Value, 1, input.Len())
	defer inputOffset.Release()

	result, err := compute.RunEndDecode(suite.ctx, &compute.ArrayDatum{Value: expectedOffset})
	suite.Require().NoError(err)
	defer result.Release()

	assertDatumsEqual(suite.T(), &compute.ArrayDatum{Value: inputOffset}, result, nil, nil)
}

func (suite *RunEndEncodeDecodeSuite) TestDecodeWithChildOffset() {
	// artificially add a bunch of nulls to the values child of the
	// run-end encoded array both before and after the data and then
	// replace it with a slice. Then make sure it still decodes
	// correctly.

	// skip chunked
	expected, ok := suite.expected.(*compute.ArrayDatum)
	if !ok {
		suite.T().SkipNow()
	}

	const offset = 100

	var newValuesData arrow.ArrayData
	valuesData := expected.Value.Children()[1]
	newLength := offset + int64(valuesData.Len()) + offset
	byteLen := bitutil.BytesForBits(newLength)

	validity, values := memory.NewResizableBuffer(suite.mem), memory.NewResizableBuffer(suite.mem)
	defer validity.Release()
	defer values.Release()

	validity.Resize(int(byteLen))
	if valuesData.Len() > 0 {
		bitutil.CopyBitmap(valuesData.Buffers()[0].Buf(), valuesData.Offset(), valuesData.Len(),
			validity.Buf(), offset)
	}

	switch dt := valuesData.DataType().(type) {
	case *arrow.BooleanType:
		values.Resize(int(byteLen))

		if valuesData.Len() > 0 {
			bitutil.CopyBitmap(valuesData.Buffers()[1].Buf(), valuesData.Offset(), valuesData.Len(),
				values.Buf(), offset)
		}

		newValuesData = array.NewData(valuesData.DataType(), valuesData.Len(),
			[]*memory.Buffer{validity, values}, nil, valuesData.NullN(), offset)
	case *arrow.StringType, *arrow.BinaryType:
		values.Resize(int(newLength+1) * int(arrow.Int32SizeBytes))
		copy(values.Bytes()[offset*arrow.Int32SizeBytes:], valuesData.Buffers()[1].Bytes())
		tail := values.Bytes()[(offset+valuesData.Len())*arrow.Int32SizeBytes:]
		for j := arrow.Int32SizeBytes; j < len(tail); j *= 2 {
			copy(tail[j:], tail[:j])
		}

		newValuesData = array.NewData(valuesData.DataType(), valuesData.Len(),
			[]*memory.Buffer{validity, values, valuesData.Buffers()[2]}, nil, valuesData.NullN(), offset)
	case *arrow.LargeStringType, *arrow.LargeBinaryType:
		values.Resize(int(newLength+1) * int(arrow.Int64SizeBytes))
		copy(values.Bytes()[offset*arrow.Int64SizeBytes:], valuesData.Buffers()[1].Bytes())
		tail := values.Bytes()[(offset+valuesData.Len())*arrow.Int64SizeBytes:]
		for j := arrow.Int64SizeBytes; j < len(tail); j *= 2 {
			copy(tail[j:], tail[:j])
		}

		newValuesData = array.NewData(valuesData.DataType(), valuesData.Len(),
			[]*memory.Buffer{validity, values, valuesData.Buffers()[2]}, nil, valuesData.NullN(), offset)
	case arrow.FixedWidthDataType:
		width := dt.Bytes()
		values.Resize(int(newLength) * width)
		if valuesData.Len() > 0 {
			copy(values.Bytes()[offset*width:], valuesData.Buffers()[1].Bytes())
		}
		newValuesData = array.NewData(valuesData.DataType(), valuesData.Len(),
			[]*memory.Buffer{validity, values}, nil, valuesData.NullN(), offset)
	}

	withOffset := expected.Value.(*array.Data).Copy()
	withOffset.Children()[1].Release()
	withOffset.Children()[1] = newValuesData
	defer withOffset.Release()

	result, err := compute.RunEndDecode(suite.ctx, &compute.ArrayDatum{Value: withOffset})
	suite.Require().NoError(err)
	defer result.Release()

	assertDatumsEqual(suite.T(), suite.input, result, nil, nil)
}

func TestRunEndFunctions(t *testing.T) {
	// base64 encoded for testing fixed size binary
	const (
		valAba = `YWJh`
		valAbc = `YWJj`
		valAbd = `YWJk`
	)

	tests := []struct {
		name      string
		data      []string
		valueType arrow.DataType
	}{
		{"simple int32", []string{`[1, 1, 0, -5, -5, -5, 255, 255]`}, arrow.PrimitiveTypes.Int32},
		{"uint32 with nulls", []string{`[null, 1, 1, null, null, 5]`}, arrow.PrimitiveTypes.Uint32},
		{"boolean", []string{`[true, true, true, false, false]`}, arrow.FixedWidthTypes.Boolean},
		{"boolean no runs", []string{`[true, false, true, false, true, false, true, false, true]`}, arrow.FixedWidthTypes.Boolean},
		{"float64 len=1", []string{`[1.0]`}, arrow.PrimitiveTypes.Float64},
		{"bool chunks", []string{`[true, true]`, `[true, false, null, null, false]`, `[null, null]`}, arrow.FixedWidthTypes.Boolean},
		{"float32 chunked", []string{`[1, 1, 0, -5, -5]`, `[-5, 255, 255]`}, arrow.PrimitiveTypes.Float32},
		{"str", []string{`["foo", "foo", "foo", "bar", "bar", "baz", "bar", "bar", "foo", "foo"]`}, arrow.BinaryTypes.String},
		{"large str", []string{`["foo", "foo", "foo", "bar", "bar", "baz", "bar", "bar", "foo", "foo"]`}, arrow.BinaryTypes.LargeString},
		{"str chunked", []string{`["foo", "foo", null]`, `["foo", "bar", "bar"]`, `[null, null, "baz"]`, `[null]`}, arrow.BinaryTypes.String},
		{"empty arrs", []string{`[]`}, arrow.PrimitiveTypes.Float32},
		{"empty str array", []string{`[]`}, arrow.BinaryTypes.String},
		{"empty chunked", []string{`[]`, `[]`, `[]`}, arrow.FixedWidthTypes.Boolean},
		{"fsb", []string{`["` + valAba + `", "` + valAba + `", null, "` + valAbc + `", "` + valAbd + `", "` + valAbd + `", "` + valAbd + `"]`}, &arrow.FixedSizeBinaryType{ByteWidth: 3}},
		{"fsb chunked", []string{`["` + valAba + `", "` + valAba + `", null]`, `["` + valAbc + `", "` + valAbd + `", "` + valAbd + `", "` + valAbd + `"]`, `[]`}, &arrow.FixedSizeBinaryType{ByteWidth: 3}}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, runEndType := range runEndTypes {
				t.Run("run_ends="+runEndType.String(), func(t *testing.T) {
					suite.Run(t, &RunEndEncodeDecodeSuite{
						runEndType: runEndType,
						valueType:  tt.valueType,
						jsonData:   tt.data,
					})
				})
			}
		})
	}
}

func benchRunEndEncode(b *testing.B, sz int, nullProb float64, runEndType, valueType arrow.DataType) {
	b.Run("encode", func(b *testing.B) {
		var (
			mem = memory.NewCheckedAllocator(memory.DefaultAllocator)
			rng = gen.NewRandomArrayGenerator(seed, mem)
		)

		values := rng.ArrayOf(valueType.ID(), int64(sz), nullProb)
		b.Cleanup(func() {
			values.Release()
		})

		var (
			res   compute.Datum
			err   error
			ctx   = compute.WithAllocator(context.Background(), mem)
			input = &compute.ArrayDatum{Value: values.Data()}
			opts  = compute.RunEndEncodeOptions{RunEndType: runEndType}

			byts int64
		)

		for _, buf := range values.Data().Buffers() {
			if buf != nil {
				byts += int64(buf.Len())
			}
		}

		b.SetBytes(byts)
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			res, err = compute.RunEndEncode(ctx, opts, input)
			b.StopTimer()
			if err != nil {
				b.Fatal(err)
			}
			res.Release()
			b.StartTimer()
		}
	})
}

func benchRunEndDecode(b *testing.B, sz int, nullProb float64, runEndType, valueType arrow.DataType) {
	b.Run("decode", func(b *testing.B) {
		var (
			mem = memory.NewCheckedAllocator(memory.DefaultAllocator)
			rng = gen.NewRandomArrayGenerator(seed, mem)
		)

		values := rng.ArrayOf(valueType.ID(), int64(sz), nullProb)
		b.Cleanup(func() {
			values.Release()
		})

		var (
			res        compute.Datum
			ctx        = compute.WithAllocator(context.Background(), mem)
			opts       = compute.RunEndEncodeOptions{RunEndType: runEndType}
			input, err = compute.RunEndEncode(ctx, opts, &compute.ArrayDatum{Value: values.Data()})
			byts       int64
		)

		if err != nil {
			b.Fatal(err)
		}

		for _, buf := range values.Data().Buffers() {
			if buf != nil {
				byts += int64(buf.Len())
			}
		}

		b.SetBytes(byts)
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			res, err = compute.RunEndDecode(ctx, input)
			b.StopTimer()
			if err != nil {
				b.Fatal(err)
			}
			res.Release()
			b.StartTimer()
		}
	})
}

func BenchmarkRunEndKernels(b *testing.B) {
	args := []struct {
		sz       int
		nullProb float64
	}{
		{CpuCacheSizes[2], 0},
		{CpuCacheSizes[2], 0.5},
		{CpuCacheSizes[2], 1},
	}

	runEnds := []struct {
		dt     arrow.DataType
		maxLen int
	}{
		{arrow.PrimitiveTypes.Int16, math.MaxInt16},
		{arrow.PrimitiveTypes.Int32, math.MaxInt32},
		{arrow.PrimitiveTypes.Int64, math.MaxInt64},
	}

	for _, a := range args {
		b.Run(fmt.Sprintf("nullprob=%.1f", a.nullProb), func(b *testing.B) {
			for _, runEndType := range runEnds {
				sz := exec.Min(a.sz, runEndType.maxLen)
				b.Run("run_ends_type="+runEndType.dt.String(), func(b *testing.B) {
					for _, valType := range append(numericTypes, arrow.BinaryTypes.String, arrow.FixedWidthTypes.Boolean) {
						b.Run("value_type="+valType.String(), func(b *testing.B) {
							benchRunEndEncode(b, sz, a.nullProb, runEndType.dt, valType)
							benchRunEndDecode(b, sz, a.nullProb, runEndType.dt, valType)
						})
					}
				})
			}
		})
	}
}
