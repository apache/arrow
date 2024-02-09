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

package metadata_test

import (
	"math"
	"reflect"
	"testing"

	"github.com/apache/arrow/go/v16/arrow/bitutil"
	"github.com/apache/arrow/go/v16/arrow/float16"
	"github.com/apache/arrow/go/v16/arrow/memory"
	"github.com/apache/arrow/go/v16/parquet"
	"github.com/apache/arrow/go/v16/parquet/metadata"
	"github.com/apache/arrow/go/v16/parquet/schema"
	"github.com/stretchr/testify/assert"
)

// NOTE(zeroshade): tests will be added and updated after merging the "file" package
// since the tests that I wrote relied on the file writer/reader for ease of use.

func newFloat16Node(name string, rep parquet.Repetition, fieldID int32) *schema.PrimitiveNode {
	return schema.MustPrimitive(schema.NewPrimitiveNodeLogical(name, rep, schema.Float16LogicalType{}, parquet.Types.FixedLenByteArray, 2, fieldID))
}

func TestCheckNaNs(t *testing.T) {
	const (
		numvals = 8
		min     = -4.0
		max     = 3.0
	)
	var (
		nan                              = math.NaN()
		f16Min parquet.FixedLenByteArray = float16.New(float32(min)).ToLEBytes()
		f16Max parquet.FixedLenByteArray = float16.New(float32(max)).ToLEBytes()
	)

	allNans := []float64{nan, nan, nan, nan, nan, nan, nan, nan}
	allNansf32 := make([]float32, numvals)
	allNansf16 := make([]parquet.FixedLenByteArray, numvals)
	for idx, v := range allNans {
		allNansf32[idx] = float32(v)
		allNansf16[idx] = float16.New(float32(v)).ToLEBytes()
	}

	someNans := []float64{nan, max, -3.0, -1.0, nan, 2.0, min, nan}
	someNansf32 := make([]float32, numvals)
	someNansf16 := make([]parquet.FixedLenByteArray, numvals)
	for idx, v := range someNans {
		someNansf32[idx] = float32(v)
		someNansf16[idx] = float16.New(float32(v)).ToLEBytes()
	}

	validBitmap := []byte{0x7F}       // 0b01111111
	validBitmapNoNaNs := []byte{0x6E} // 0b01101110

	assertUnsetMinMax := func(stats metadata.TypedStatistics, values interface{}, bitmap []byte) {
		if bitmap == nil {
			switch s := stats.(type) {
			case *metadata.Float32Statistics:
				s.Update(values.([]float32), 0)
			case *metadata.Float64Statistics:
				s.Update(values.([]float64), 0)
			case *metadata.Float16Statistics:
				s.Update(values.([]parquet.FixedLenByteArray), 0)
			}
			assert.False(t, stats.HasMinMax())
		} else {
			nvalues := reflect.ValueOf(values).Len()
			nullCount := bitutil.CountSetBits(bitmap, 0, nvalues)
			switch s := stats.(type) {
			case *metadata.Float32Statistics:
				s.UpdateSpaced(values.([]float32), bitmap, 0, int64(nullCount))
			case *metadata.Float64Statistics:
				s.UpdateSpaced(values.([]float64), bitmap, 0, int64(nullCount))
			case *metadata.Float16Statistics:
				s.UpdateSpaced(values.([]parquet.FixedLenByteArray), bitmap, 0, int64(nullCount))
			}
			assert.False(t, stats.HasMinMax())
		}
	}

	assertMinMaxAre := func(stats metadata.TypedStatistics, values interface{}, expectedMin, expectedMax interface{}) {
		switch s := stats.(type) {
		case *metadata.Float32Statistics:
			s.Update(values.([]float32), 0)
			assert.True(t, stats.HasMinMax())
			assert.Equal(t, expectedMin, s.Min())
			assert.Equal(t, expectedMax, s.Max())
		case *metadata.Float64Statistics:
			s.Update(values.([]float64), 0)
			assert.True(t, stats.HasMinMax())
			assert.Equal(t, expectedMin, s.Min())
			assert.Equal(t, expectedMax, s.Max())
		case *metadata.Float16Statistics:
			s.Update(values.([]parquet.FixedLenByteArray), 0)
			assert.True(t, stats.HasMinMax())
			assert.Equal(t, expectedMin, s.Min())
			assert.Equal(t, expectedMax, s.Max())
		}
	}

	assertMinMaxAreSpaced := func(stats metadata.TypedStatistics, values interface{}, bitmap []byte, expectedMin, expectedMax interface{}) {
		nvalues := reflect.ValueOf(values).Len()
		nullCount := bitutil.CountSetBits(bitmap, 0, nvalues)
		switch s := stats.(type) {
		case *metadata.Float32Statistics:
			s.UpdateSpaced(values.([]float32), bitmap, 0, int64(nullCount))
			assert.True(t, s.HasMinMax())
			assert.Equal(t, expectedMin, s.Min())
			assert.Equal(t, expectedMax, s.Max())
		case *metadata.Float64Statistics:
			s.UpdateSpaced(values.([]float64), bitmap, 0, int64(nullCount))
			assert.True(t, s.HasMinMax())
			assert.Equal(t, expectedMin, s.Min())
			assert.Equal(t, expectedMax, s.Max())
		case *metadata.Float16Statistics:
			s.UpdateSpaced(values.([]parquet.FixedLenByteArray), bitmap, 0, int64(nullCount))
			assert.True(t, s.HasMinMax())
			assert.Equal(t, expectedMin, s.Min())
			assert.Equal(t, expectedMax, s.Max())
		}
	}

	f32Col := schema.NewColumn(schema.NewFloat32Node("f", parquet.Repetitions.Optional, -1), 1, 1)
	f64Col := schema.NewColumn(schema.NewFloat64Node("f", parquet.Repetitions.Optional, -1), 1, 1)
	f16Col := schema.NewColumn(newFloat16Node("f", parquet.Repetitions.Required, -1), 1, 1)
	// test values
	someNanStats := metadata.NewStatistics(f64Col, memory.DefaultAllocator)
	someNanStatsf32 := metadata.NewStatistics(f32Col, memory.DefaultAllocator)
	someNanStatsf16 := metadata.NewStatistics(f16Col, memory.DefaultAllocator)
	// ingesting only nans should not yield a min or max
	assertUnsetMinMax(someNanStats, allNans, nil)
	assertUnsetMinMax(someNanStatsf32, allNansf32, nil)
	assertUnsetMinMax(someNanStatsf16, allNansf16, nil)
	// ingesting a mix should yield a valid min/max
	assertMinMaxAre(someNanStats, someNans, min, max)
	assertMinMaxAre(someNanStatsf32, someNansf32, float32(min), float32(max))
	assertMinMaxAre(someNanStatsf16, someNansf16, f16Min, f16Max)
	// ingesting only nans after a valid min/max should have no effect
	assertMinMaxAre(someNanStats, allNans, min, max)
	assertMinMaxAre(someNanStatsf32, allNansf32, float32(min), float32(max))
	assertMinMaxAre(someNanStatsf16, allNansf16, f16Min, f16Max)

	someNanStats = metadata.NewStatistics(f64Col, memory.DefaultAllocator)
	someNanStatsf32 = metadata.NewStatistics(f32Col, memory.DefaultAllocator)
	someNanStatsf16 = metadata.NewStatistics(f16Col, memory.DefaultAllocator)
	assertUnsetMinMax(someNanStats, allNans, validBitmap)
	assertUnsetMinMax(someNanStatsf32, allNansf32, validBitmap)
	assertUnsetMinMax(someNanStatsf16, allNansf16, validBitmap)
	// nans should not pollute min/max when excluded via null bitmap
	assertMinMaxAreSpaced(someNanStats, someNans, validBitmapNoNaNs, min, max)
	assertMinMaxAreSpaced(someNanStatsf32, someNansf32, validBitmapNoNaNs, float32(min), float32(max))
	assertMinMaxAreSpaced(someNanStatsf16, someNansf16, validBitmapNoNaNs, f16Min, f16Max)
	// ingesting nans with a null bitmap should not change the result
	assertMinMaxAreSpaced(someNanStats, someNans, validBitmap, min, max)
	assertMinMaxAreSpaced(someNanStatsf32, someNansf32, validBitmap, float32(min), float32(max))
	assertMinMaxAreSpaced(someNanStatsf16, someNansf16, validBitmap, f16Min, f16Max)
}

func TestCheckNegativeZeroStats(t *testing.T) {
	assertMinMaxZeroesSign := func(stats metadata.TypedStatistics, values interface{}) {
		switch s := stats.(type) {
		case *metadata.Float32Statistics:
			s.Update(values.([]float32), 0)
			assert.True(t, s.HasMinMax())
			var zero float32
			assert.Equal(t, zero, s.Min())
			assert.True(t, math.Signbit(float64(s.Min())))
			assert.Equal(t, zero, s.Max())
			assert.False(t, math.Signbit(float64(s.Max())))
		case *metadata.Float64Statistics:
			s.Update(values.([]float64), 0)
			assert.True(t, s.HasMinMax())
			var zero float64
			assert.Equal(t, zero, s.Min())
			assert.True(t, math.Signbit(s.Min()))
			assert.Equal(t, zero, s.Max())
			assert.False(t, math.Signbit(s.Max()))
		case *metadata.Float16Statistics:
			s.Update(values.([]parquet.FixedLenByteArray), 0)
			assert.True(t, s.HasMinMax())
			var zero float64
			min := float64(float16.FromLEBytes(s.Min()).Float32())
			max := float64(float16.FromLEBytes(s.Max()).Float32())
			assert.Equal(t, zero, min)
			assert.True(t, math.Signbit(min))
			assert.Equal(t, zero, max)
			assert.False(t, math.Signbit(max))
		}
	}

	fcol := schema.NewColumn(schema.NewFloat32Node("f", parquet.Repetitions.Optional, -1), 1, 1)
	dcol := schema.NewColumn(schema.NewFloat64Node("d", parquet.Repetitions.Optional, -1), 1, 1)
	hcol := schema.NewColumn(newFloat16Node("h", parquet.Repetitions.Optional, -1), 1, 1)

	var f32zero float32
	var f64zero float64
	var f16PosZero parquet.FixedLenByteArray = float16.New(+f32zero).ToLEBytes()
	var f16NegZero parquet.FixedLenByteArray = float16.New(-f32zero).ToLEBytes()

	assert.False(t, float16.FromLEBytes(f16PosZero).Signbit())
	assert.True(t, float16.FromLEBytes(f16NegZero).Signbit())
	{
		fstats := metadata.NewStatistics(fcol, memory.DefaultAllocator)
		dstats := metadata.NewStatistics(dcol, memory.DefaultAllocator)
		hstats := metadata.NewStatistics(hcol, memory.DefaultAllocator)
		assertMinMaxZeroesSign(fstats, []float32{-f32zero, f32zero})
		assertMinMaxZeroesSign(dstats, []float64{-f64zero, f64zero})
		assertMinMaxZeroesSign(hstats, []parquet.FixedLenByteArray{f16NegZero, f16PosZero})
	}
	{
		fstats := metadata.NewStatistics(fcol, memory.DefaultAllocator)
		dstats := metadata.NewStatistics(dcol, memory.DefaultAllocator)
		hstats := metadata.NewStatistics(hcol, memory.DefaultAllocator)
		assertMinMaxZeroesSign(fstats, []float32{f32zero, -f32zero})
		assertMinMaxZeroesSign(dstats, []float64{f64zero, -f64zero})
		assertMinMaxZeroesSign(hstats, []parquet.FixedLenByteArray{f16PosZero, f16NegZero})
	}
	{
		fstats := metadata.NewStatistics(fcol, memory.DefaultAllocator)
		dstats := metadata.NewStatistics(dcol, memory.DefaultAllocator)
		hstats := metadata.NewStatistics(hcol, memory.DefaultAllocator)
		assertMinMaxZeroesSign(fstats, []float32{-f32zero, -f32zero})
		assertMinMaxZeroesSign(dstats, []float64{-f64zero, -f64zero})
		assertMinMaxZeroesSign(hstats, []parquet.FixedLenByteArray{f16NegZero, f16NegZero})
	}
	{
		fstats := metadata.NewStatistics(fcol, memory.DefaultAllocator)
		dstats := metadata.NewStatistics(dcol, memory.DefaultAllocator)
		hstats := metadata.NewStatistics(hcol, memory.DefaultAllocator)
		assertMinMaxZeroesSign(fstats, []float32{f32zero, f32zero})
		assertMinMaxZeroesSign(dstats, []float64{f64zero, f64zero})
		assertMinMaxZeroesSign(hstats, []parquet.FixedLenByteArray{f16PosZero, f16PosZero})
	}
}

func TestBooleanStatisticsEncoding(t *testing.T) {
	n := schema.NewBooleanNode("boolean", parquet.Repetitions.Required, -1)
	descr := schema.NewColumn(n, 0, 0)
	s := metadata.NewStatistics(descr, nil)
	bs := s.(*metadata.BooleanStatistics)
	bs.SetMinMax(false, true)
	maxEnc := bs.EncodeMax()
	minEnc := bs.EncodeMin()
	assert.Equal(t, []byte{1}, maxEnc)
	assert.Equal(t, []byte{0}, minEnc)
}
