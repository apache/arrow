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

package scalar_test

import (
	"math"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/internal/testing/tools"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/apache/arrow/go/v12/arrow/scalar"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"golang.org/x/exp/constraints"
	"golang.org/x/exp/rand"
	"gonum.org/v1/gonum/stat/distuv"
)

type primitiveTypes interface {
	constraints.Integer | constraints.Float
}

func draw[T constraints.Integer](n int64, min, max T) []T {
	const seed = 1337
	gen := rand.New(rand.NewSource(seed))

	normalizedMin := uint64(math.Abs(float64(min)))
	normalizedMax := uint64(max) + normalizedMin

	out := make([]T, n)
	for i := range out {
		out[i] = T(gen.Uint64n(normalizedMax) - normalizedMin)
	}
	return out
}

func drawFloat[T float32 | float64](n int64) []T {
	const seed = 0xdeadbeef
	d := distuv.Uniform{
		Min: -1000.0, Max: 1000.0,
		Src: rand.NewSource(seed),
	}

	out := make([]T, n)
	for i := range out {
		out[i] = T(d.Rand())
	}
	return out
}

func drawBytes[T string | []byte](n int64, minLen, maxLen int) []T {
	const seed = 1337
	gen := rand.New(rand.NewSource(seed))

	out := make([]T, n)
	for i := range out {
		l := gen.Intn(maxLen-minLen+1) + minLen
		buf := make([]byte, l)
		for j := range buf {
			buf[j] = uint8(gen.Intn(int('z')-int('A')+1) + int('A'))
		}
		out[i] = T(buf)
	}
	return out
}

func randomBools(n int64, pctFalse float64) []bool {
	const seed = 0
	d := distuv.Uniform{
		Min: 0.0, Max: 1.0,
		Src: rand.NewSource(seed),
	}

	out := make([]bool, n)
	for i := range out {
		out[i] = d.Rand() > pctFalse
	}
	return out
}

type builder[T primitiveTypes | string | []byte] interface {
	array.Builder
	Append(T)
}

type PrimitiveAppendTestSuite[T primitiveTypes | string | []byte] struct {
	suite.Suite

	mem     *memory.CheckedAllocator
	dt      arrow.DataType
	bldr    builder[T]
	bldrNN  builder[T]
	scalars []scalar.Scalar

	getRand func(n int64) []T

	draws      []T
	validBytes []bool
}

func (pt *PrimitiveAppendTestSuite[T]) SetupTest() {
	pt.mem = memory.NewCheckedAllocator(memory.DefaultAllocator)
	pt.dt = tools.GetDataType[T]()
	pt.bldr = array.NewBuilder(pt.mem, pt.dt).(builder[T])
	pt.bldrNN = array.NewBuilder(pt.mem, pt.dt).(builder[T])
}

func (pt *PrimitiveAppendTestSuite[T]) TearDownTest() {
	pt.bldr.Release()
	pt.bldrNN.Release()

	pt.mem.AssertSize(pt.T(), 0)
}

func (pt *PrimitiveAppendTestSuite[T]) randomData(n int64, pctNull float64) {
	pt.draws = pt.getRand(n)
	pt.validBytes = randomBools(n, pctNull)
}

func (pt *PrimitiveAppendTestSuite[T]) TestAppendScalar() {
	const size int = 1000

	pt.randomData(int64(size), 0.1)

	pt.bldr.Reserve(size)
	pt.scalars = make([]scalar.Scalar, size)

	var nullCount int
	for i := 0; i < 1000; i++ {
		if pt.validBytes[i] {
			pt.bldr.Append(pt.draws[i])
			pt.scalars[i] = scalar.MakeScalar(pt.draws[i])
		} else {
			pt.bldr.AppendNull()
			nullCount++
			pt.scalars[i] = scalar.MakeNullScalar(pt.dt)
		}
	}

	pt.Require().NoError(scalar.AppendSlice(pt.bldrNN, pt.scalars))

	pt.Equal(nullCount, pt.bldr.NullN())
	pt.Equal(nullCount, pt.bldrNN.NullN())
	pt.Equal(1000, pt.bldr.Len())
	pt.Equal(1024, pt.bldr.Cap())
	pt.Equal(1000, pt.bldrNN.Len())
	pt.Equal(1024, pt.bldrNN.Cap())

	expected := pt.bldr.NewArray()
	defer expected.Release()
	out := pt.bldrNN.NewArray()
	defer out.Release()
	pt.Truef(array.Equal(expected, out), "expected: %s, got: %s", expected, out)
}

type PrimitiveIntegralAppendTestSuite[T constraints.Integer] struct {
	PrimitiveAppendTestSuite[T]
	min, max T
}

func (pt *PrimitiveIntegralAppendTestSuite[T]) SetupSuite() {
	pt.getRand = func(n int64) []T {
		return draw(n, pt.min, pt.max)
	}
}

type PrimitiveFloatingAppendTestSuite[T float32 | float64] struct {
	PrimitiveAppendTestSuite[T]
}

func (pt *PrimitiveFloatingAppendTestSuite[T]) SetupSuite() {
	pt.getRand = drawFloat[T]
}

type PrimitiveStringAppendTestSuite[T string | []byte] struct {
	PrimitiveAppendTestSuite[T]
}

func (pt *PrimitiveStringAppendTestSuite[T]) SetupSuite() {
	pt.getRand = func(n int64) []T {
		return drawBytes[T](n, 3, 9)
	}
}

func TestPrimitiveAppendScalar(t *testing.T) {
	suite.Run(t, &PrimitiveIntegralAppendTestSuite[int8]{min: math.MinInt8, max: math.MaxInt8})
	suite.Run(t, &PrimitiveIntegralAppendTestSuite[int16]{min: math.MinInt16, max: math.MaxInt16})
	suite.Run(t, &PrimitiveIntegralAppendTestSuite[int32]{min: math.MinInt32, max: math.MaxInt32})
	suite.Run(t, &PrimitiveIntegralAppendTestSuite[arrow.Date32]{min: math.MinInt32, max: math.MaxInt32})
	suite.Run(t, &PrimitiveIntegralAppendTestSuite[arrow.Date64]{min: math.MinInt64, max: math.MaxInt64})
	suite.Run(t, &PrimitiveIntegralAppendTestSuite[int64]{min: math.MinInt64, max: math.MaxInt64})
	suite.Run(t, &PrimitiveIntegralAppendTestSuite[uint8]{min: 0, max: math.MaxUint8})
	suite.Run(t, &PrimitiveIntegralAppendTestSuite[uint16]{min: 0, max: math.MaxUint16})
	suite.Run(t, &PrimitiveIntegralAppendTestSuite[uint32]{min: 0, max: math.MaxUint32})
	suite.Run(t, &PrimitiveIntegralAppendTestSuite[uint64]{min: 0, max: math.MaxUint64})
	suite.Run(t, new(PrimitiveFloatingAppendTestSuite[float32]))
	suite.Run(t, new(PrimitiveFloatingAppendTestSuite[float64]))
	suite.Run(t, new(PrimitiveStringAppendTestSuite[string]))
	suite.Run(t, new(PrimitiveStringAppendTestSuite[[]byte]))
}

func TestAppendMapScalar(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	expected, _, err := array.FromJSON(mem, arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int8),
		strings.NewReader(`[[{"key": "a", "value": 1}, {"key": "b", "value": 2}]]`))
	require.NoError(t, err)
	defer expected.Release()

	mapScalar, err := scalar.GetScalar(expected, 0)
	require.NoError(t, err)
	defer mapScalar.(scalar.Releasable).Release()

	bldr := array.NewBuilder(mem, mapScalar.DataType())
	defer bldr.Release()

	require.NoError(t, scalar.Append(bldr, mapScalar))

	result := bldr.NewArray()
	defer result.Release()

	assert.Truef(t, array.Equal(expected, result), "expected: %s, got: %s", expected, result)
}
