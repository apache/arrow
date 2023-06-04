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

	"github.com/apache/arrow/go/v13/arrow/bitutil"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/apache/arrow/go/v13/parquet"
	"github.com/apache/arrow/go/v13/parquet/metadata"
	"github.com/apache/arrow/go/v13/parquet/schema"
	"github.com/stretchr/testify/assert"
)

// NOTE(zeroshade): tests will be added and updated after merging the "file" package
// since the tests that I wrote relied on the file writer/reader for ease of use.

func TestCheckNaNs(t *testing.T) {
	const (
		numvals = 8
		min     = -4.0
		max     = 3.0
	)
	nan := math.NaN()

	allNans := []float64{nan, nan, nan, nan, nan, nan, nan, nan}
	allNansf32 := make([]float32, numvals)
	for idx, v := range allNans {
		allNansf32[idx] = float32(v)
	}

	someNans := []float64{nan, max, -3.0, -1.0, nan, 2.0, min, nan}
	someNansf32 := make([]float32, numvals)
	for idx, v := range someNans {
		someNansf32[idx] = float32(v)
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
		}
	}

	f32Col := schema.NewColumn(schema.NewFloat32Node("f", parquet.Repetitions.Optional, -1), 1, 1)
	f64Col := schema.NewColumn(schema.NewFloat64Node("f", parquet.Repetitions.Optional, -1), 1, 1)
	// test values
	someNanStats := metadata.NewStatistics(f64Col, memory.DefaultAllocator)
	someNanStatsf32 := metadata.NewStatistics(f32Col, memory.DefaultAllocator)
	// ingesting only nans should not yield a min or max
	assertUnsetMinMax(someNanStats, allNans, nil)
	assertUnsetMinMax(someNanStatsf32, allNansf32, nil)
	// ingesting a mix should yield a valid min/max
	assertMinMaxAre(someNanStats, someNans, min, max)
	assertMinMaxAre(someNanStatsf32, someNansf32, float32(min), float32(max))
	// ingesting only nans after a valid min/max should have no effect
	assertMinMaxAre(someNanStats, allNans, min, max)
	assertMinMaxAre(someNanStatsf32, allNansf32, float32(min), float32(max))

	someNanStats = metadata.NewStatistics(f64Col, memory.DefaultAllocator)
	someNanStatsf32 = metadata.NewStatistics(f32Col, memory.DefaultAllocator)
	assertUnsetMinMax(someNanStats, allNans, validBitmap)
	assertUnsetMinMax(someNanStatsf32, allNansf32, validBitmap)
	// nans should not pollute min/max when excluded via null bitmap
	assertMinMaxAreSpaced(someNanStats, someNans, validBitmapNoNaNs, min, max)
	assertMinMaxAreSpaced(someNanStatsf32, someNansf32, validBitmapNoNaNs, float32(min), float32(max))
	// ingesting nans with a null bitmap should not change the result
	assertMinMaxAreSpaced(someNanStats, someNans, validBitmap, min, max)
	assertMinMaxAreSpaced(someNanStatsf32, someNansf32, validBitmap, float32(min), float32(max))
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
		}
	}

	fcol := schema.NewColumn(schema.NewFloat32Node("f", parquet.Repetitions.Optional, -1), 1, 1)
	dcol := schema.NewColumn(schema.NewFloat64Node("d", parquet.Repetitions.Optional, -1), 1, 1)

	var f32zero float32
	var f64zero float64
	{
		fstats := metadata.NewStatistics(fcol, memory.DefaultAllocator)
		dstats := metadata.NewStatistics(dcol, memory.DefaultAllocator)
		assertMinMaxZeroesSign(fstats, []float32{-f32zero, f32zero})
		assertMinMaxZeroesSign(dstats, []float64{-f64zero, f64zero})
	}
	{
		fstats := metadata.NewStatistics(fcol, memory.DefaultAllocator)
		dstats := metadata.NewStatistics(dcol, memory.DefaultAllocator)
		assertMinMaxZeroesSign(fstats, []float32{f32zero, -f32zero})
		assertMinMaxZeroesSign(dstats, []float64{f64zero, -f64zero})
	}
	{
		fstats := metadata.NewStatistics(fcol, memory.DefaultAllocator)
		dstats := metadata.NewStatistics(dcol, memory.DefaultAllocator)
		assertMinMaxZeroesSign(fstats, []float32{-f32zero, -f32zero})
		assertMinMaxZeroesSign(dstats, []float64{-f64zero, -f64zero})
	}
	{
		fstats := metadata.NewStatistics(fcol, memory.DefaultAllocator)
		dstats := metadata.NewStatistics(dcol, memory.DefaultAllocator)
		assertMinMaxZeroesSign(fstats, []float32{f32zero, f32zero})
		assertMinMaxZeroesSign(dstats, []float64{f64zero, f64zero})
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
