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
	"math"
	"math/rand"
	"reflect"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/bitutil"
	"github.com/apache/arrow/go/v14/arrow/decimal128"
	"github.com/apache/arrow/go/v14/arrow/decimal256"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type PrimitiveDictionaryTestSuite struct {
	suite.Suite

	mem    *memory.CheckedAllocator
	typ    arrow.DataType
	reftyp reflect.Type
}

func (p *PrimitiveDictionaryTestSuite) SetupTest() {
	p.mem = memory.NewCheckedAllocator(memory.DefaultAllocator)
}

func (p *PrimitiveDictionaryTestSuite) TearDownTest() {
	p.mem.AssertSize(p.T(), 0)
}

func TestPrimitiveDictionaryBuilders(t *testing.T) {
	tests := []struct {
		name   string
		typ    arrow.DataType
		reftyp reflect.Type
	}{
		{"int8", arrow.PrimitiveTypes.Int8, reflect.TypeOf(int8(0))},
		{"uint8", arrow.PrimitiveTypes.Uint8, reflect.TypeOf(uint8(0))},
		{"int16", arrow.PrimitiveTypes.Int16, reflect.TypeOf(int16(0))},
		{"uint16", arrow.PrimitiveTypes.Uint16, reflect.TypeOf(uint16(0))},
		{"int32", arrow.PrimitiveTypes.Int32, reflect.TypeOf(int32(0))},
		{"uint32", arrow.PrimitiveTypes.Uint32, reflect.TypeOf(uint32(0))},
		{"int64", arrow.PrimitiveTypes.Int64, reflect.TypeOf(int64(0))},
		{"uint64", arrow.PrimitiveTypes.Uint64, reflect.TypeOf(uint64(0))},
		{"float32", arrow.PrimitiveTypes.Float32, reflect.TypeOf(float32(0))},
		{"float64", arrow.PrimitiveTypes.Float64, reflect.TypeOf(float64(0))},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			suite.Run(t, &PrimitiveDictionaryTestSuite{typ: tt.typ, reftyp: tt.reftyp})
		})
	}
}

func (p *PrimitiveDictionaryTestSuite) TestDictionaryBuilderBasic() {
	expectedType := &arrow.DictionaryType{IndexType: &arrow.Int8Type{}, ValueType: p.typ}
	bldr := array.NewDictionaryBuilder(p.mem, expectedType)
	defer bldr.Release()

	builder := reflect.ValueOf(bldr)
	appfn := builder.MethodByName("Append")
	p.Nil(appfn.Call([]reflect.Value{reflect.ValueOf(1).Convert(p.reftyp)})[0].Interface())
	p.Nil(appfn.Call([]reflect.Value{reflect.ValueOf(2).Convert(p.reftyp)})[0].Interface())
	p.Nil(appfn.Call([]reflect.Value{reflect.ValueOf(1).Convert(p.reftyp)})[0].Interface())
	bldr.AppendNull()

	p.EqualValues(4, bldr.Len())
	p.EqualValues(1, bldr.NullN())

	arr := bldr.NewArray().(*array.Dictionary)
	defer arr.Release()

	p.True(arrow.TypeEqual(expectedType, arr.DataType()))
	expectedDict, _, err := array.FromJSON(p.mem, expectedType.ValueType, strings.NewReader("[1, 2]"))
	p.NoError(err)
	defer expectedDict.Release()

	expectedIndices, _, err := array.FromJSON(p.mem, expectedType.IndexType, strings.NewReader("[0, 1, 0, null]"))
	p.NoError(err)
	defer expectedIndices.Release()

	expected := array.NewDictionaryArray(expectedType, expectedIndices, expectedDict)
	defer expected.Release()

	p.True(array.Equal(expected, arr))
}

func (p *PrimitiveDictionaryTestSuite) TestDictionaryBuilderInit() {
	valueType := p.typ
	dictArr, _, err := array.FromJSON(p.mem, valueType, strings.NewReader("[1, 2]"))
	p.NoError(err)
	defer dictArr.Release()

	dictType := &arrow.DictionaryType{IndexType: &arrow.Int8Type{}, ValueType: valueType}
	bldr := array.NewDictionaryBuilderWithDict(p.mem, dictType, dictArr)
	defer bldr.Release()

	builder := reflect.ValueOf(bldr)
	appfn := builder.MethodByName("Append")
	p.Nil(appfn.Call([]reflect.Value{reflect.ValueOf(1).Convert(p.reftyp)})[0].Interface())
	p.Nil(appfn.Call([]reflect.Value{reflect.ValueOf(2).Convert(p.reftyp)})[0].Interface())
	p.Nil(appfn.Call([]reflect.Value{reflect.ValueOf(1).Convert(p.reftyp)})[0].Interface())
	bldr.AppendNull()

	p.EqualValues(4, bldr.Len())
	p.EqualValues(1, bldr.NullN())

	arr := bldr.NewDictionaryArray()
	defer arr.Release()

	expectedIndices, _, err := array.FromJSON(p.mem, dictType.IndexType, strings.NewReader("[0, 1, 0, null]"))
	p.NoError(err)
	defer expectedIndices.Release()

	expected := array.NewDictionaryArray(dictType, expectedIndices, dictArr)
	defer expected.Release()

	p.True(array.Equal(expected, arr))
}

func (p *PrimitiveDictionaryTestSuite) TestDictionaryNewBuilder() {
	valueType := p.typ
	dictArr, _, err := array.FromJSON(p.mem, valueType, strings.NewReader("[1, 2]"))
	p.NoError(err)
	defer dictArr.Release()

	dictType := &arrow.DictionaryType{IndexType: &arrow.Int8Type{}, ValueType: valueType}
	bldr := array.NewBuilder(p.mem, dictType)
	defer bldr.Release()

	builder := reflect.ValueOf(bldr)
	appfn := builder.MethodByName("Append")
	p.Nil(appfn.Call([]reflect.Value{reflect.ValueOf(1).Convert(p.reftyp)})[0].Interface())
	p.Nil(appfn.Call([]reflect.Value{reflect.ValueOf(2).Convert(p.reftyp)})[0].Interface())
	p.Nil(appfn.Call([]reflect.Value{reflect.ValueOf(1).Convert(p.reftyp)})[0].Interface())
	bldr.AppendNull()

	p.EqualValues(4, bldr.Len())
	p.EqualValues(1, bldr.NullN())

	arr := bldr.NewArray().(*array.Dictionary)
	defer arr.Release()

	expectedIndices, _, err := array.FromJSON(p.mem, dictType.IndexType, strings.NewReader("[0, 1, 0, null]"))
	p.NoError(err)
	defer expectedIndices.Release()

	expected := array.NewDictionaryArray(dictType, expectedIndices, dictArr)
	defer expected.Release()

	p.True(array.Equal(expected, arr))
}

func (p *PrimitiveDictionaryTestSuite) TestDictionaryBuilderAppendArr() {
	valueType := p.typ
	intermediate, _, err := array.FromJSON(p.mem, valueType, strings.NewReader("[1, 2, 1]"))
	p.NoError(err)
	defer intermediate.Release()

	expectedType := &arrow.DictionaryType{IndexType: &arrow.Int8Type{}, ValueType: p.typ}
	bldr := array.NewDictionaryBuilder(p.mem, expectedType)
	defer bldr.Release()

	bldr.AppendArray(intermediate)
	result := bldr.NewArray()
	defer result.Release()

	expectedDict, _, err := array.FromJSON(p.mem, expectedType.ValueType, strings.NewReader("[1, 2]"))
	p.NoError(err)
	defer expectedDict.Release()

	expectedIndices, _, err := array.FromJSON(p.mem, expectedType.IndexType, strings.NewReader("[0, 1, 0]"))
	p.NoError(err)
	defer expectedIndices.Release()

	expected := array.NewDictionaryArray(expectedType, expectedIndices, expectedDict)
	defer expected.Release()

	p.True(array.Equal(expected, result))
}

func (p *PrimitiveDictionaryTestSuite) TestDictionaryBuilderDeltaDictionary() {
	expectedType := &arrow.DictionaryType{IndexType: &arrow.Int8Type{}, ValueType: p.typ}
	bldr := array.NewDictionaryBuilder(p.mem, expectedType)
	defer bldr.Release()

	builder := reflect.ValueOf(bldr)
	appfn := builder.MethodByName("Append")
	p.Nil(appfn.Call([]reflect.Value{reflect.ValueOf(1).Convert(p.reftyp)})[0].Interface())
	p.Nil(appfn.Call([]reflect.Value{reflect.ValueOf(2).Convert(p.reftyp)})[0].Interface())
	p.Nil(appfn.Call([]reflect.Value{reflect.ValueOf(1).Convert(p.reftyp)})[0].Interface())
	p.Nil(appfn.Call([]reflect.Value{reflect.ValueOf(2).Convert(p.reftyp)})[0].Interface())

	result := bldr.NewArray()
	defer result.Release()

	exdict, _, err := array.FromJSON(p.mem, p.typ, strings.NewReader("[1, 2]"))
	p.NoError(err)
	defer exdict.Release()
	exindices, _, err := array.FromJSON(p.mem, arrow.PrimitiveTypes.Int8, strings.NewReader("[0, 1, 0, 1]"))
	p.NoError(err)
	defer exindices.Release()
	expected := array.NewDictionaryArray(result.DataType().(*arrow.DictionaryType), exindices, exdict)
	defer expected.Release()
	p.True(array.Equal(expected, result))

	p.Nil(appfn.Call([]reflect.Value{reflect.ValueOf(2).Convert(p.reftyp)})[0].Interface())
	p.Nil(appfn.Call([]reflect.Value{reflect.ValueOf(3).Convert(p.reftyp)})[0].Interface())
	p.Nil(appfn.Call([]reflect.Value{reflect.ValueOf(3).Convert(p.reftyp)})[0].Interface())
	p.Nil(appfn.Call([]reflect.Value{reflect.ValueOf(1).Convert(p.reftyp)})[0].Interface())
	p.Nil(appfn.Call([]reflect.Value{reflect.ValueOf(3).Convert(p.reftyp)})[0].Interface())

	indices, delta, err := bldr.NewDelta()
	p.NoError(err)
	defer indices.Release()
	defer delta.Release()

	exindices, _, _ = array.FromJSON(p.mem, arrow.PrimitiveTypes.Int8, strings.NewReader("[1, 2, 2, 0, 2]"))
	defer exindices.Release()
	exdelta, _, _ := array.FromJSON(p.mem, p.typ, strings.NewReader("[3]"))
	defer exdelta.Release()

	p.True(array.Equal(exindices, indices))
	p.True(array.Equal(exdelta, delta))
}

func (p *PrimitiveDictionaryTestSuite) TestDictionaryBuilderDoubleDeltaDictionary() {
	expectedType := &arrow.DictionaryType{IndexType: &arrow.Int8Type{}, ValueType: p.typ}
	bldr := array.NewDictionaryBuilder(p.mem, expectedType)
	defer bldr.Release()

	builder := reflect.ValueOf(bldr)
	appfn := builder.MethodByName("Append")
	p.Nil(appfn.Call([]reflect.Value{reflect.ValueOf(1).Convert(p.reftyp)})[0].Interface())
	p.Nil(appfn.Call([]reflect.Value{reflect.ValueOf(2).Convert(p.reftyp)})[0].Interface())
	p.Nil(appfn.Call([]reflect.Value{reflect.ValueOf(1).Convert(p.reftyp)})[0].Interface())
	p.Nil(appfn.Call([]reflect.Value{reflect.ValueOf(2).Convert(p.reftyp)})[0].Interface())

	result := bldr.NewArray()
	defer result.Release()

	exdict, _, err := array.FromJSON(p.mem, p.typ, strings.NewReader("[1, 2]"))
	p.NoError(err)
	defer exdict.Release()
	exindices, _, err := array.FromJSON(p.mem, arrow.PrimitiveTypes.Int8, strings.NewReader("[0, 1, 0, 1]"))
	p.NoError(err)
	defer exindices.Release()
	expected := array.NewDictionaryArray(result.DataType().(*arrow.DictionaryType), exindices, exdict)
	defer expected.Release()
	p.True(array.Equal(expected, result))

	p.Nil(appfn.Call([]reflect.Value{reflect.ValueOf(2).Convert(p.reftyp)})[0].Interface())
	p.Nil(appfn.Call([]reflect.Value{reflect.ValueOf(3).Convert(p.reftyp)})[0].Interface())
	p.Nil(appfn.Call([]reflect.Value{reflect.ValueOf(3).Convert(p.reftyp)})[0].Interface())
	p.Nil(appfn.Call([]reflect.Value{reflect.ValueOf(1).Convert(p.reftyp)})[0].Interface())
	p.Nil(appfn.Call([]reflect.Value{reflect.ValueOf(3).Convert(p.reftyp)})[0].Interface())

	indices, delta, err := bldr.NewDelta()
	p.NoError(err)
	defer indices.Release()
	defer delta.Release()

	exindices, _, _ = array.FromJSON(p.mem, arrow.PrimitiveTypes.Int8, strings.NewReader("[1, 2, 2, 0, 2]"))
	defer exindices.Release()
	exdelta, _, _ := array.FromJSON(p.mem, p.typ, strings.NewReader("[3]"))
	defer exdelta.Release()

	p.True(array.Equal(exindices, indices))
	p.True(array.Equal(exdelta, delta))

	p.Nil(appfn.Call([]reflect.Value{reflect.ValueOf(1).Convert(p.reftyp)})[0].Interface())
	p.Nil(appfn.Call([]reflect.Value{reflect.ValueOf(2).Convert(p.reftyp)})[0].Interface())
	p.Nil(appfn.Call([]reflect.Value{reflect.ValueOf(3).Convert(p.reftyp)})[0].Interface())
	p.Nil(appfn.Call([]reflect.Value{reflect.ValueOf(4).Convert(p.reftyp)})[0].Interface())
	p.Nil(appfn.Call([]reflect.Value{reflect.ValueOf(5).Convert(p.reftyp)})[0].Interface())

	indices, delta, err = bldr.NewDelta()
	p.NoError(err)
	defer indices.Release()
	defer delta.Release()

	exindices, _, _ = array.FromJSON(p.mem, arrow.PrimitiveTypes.Int8, strings.NewReader("[0, 1, 2, 3, 4]"))
	defer exindices.Release()
	exdelta, _, _ = array.FromJSON(p.mem, p.typ, strings.NewReader("[4, 5]"))
	defer exdelta.Release()

	p.True(array.Equal(exindices, indices))
	p.True(array.Equal(exdelta, delta))
}

func (p *PrimitiveDictionaryTestSuite) TestNewResetBehavior() {
	expectedType := &arrow.DictionaryType{IndexType: &arrow.Int8Type{}, ValueType: p.typ}
	bldr := array.NewDictionaryBuilder(p.mem, expectedType)
	defer bldr.Release()

	builder := reflect.ValueOf(bldr)
	appfn := builder.MethodByName("Append")
	p.Nil(appfn.Call([]reflect.Value{reflect.ValueOf(1).Convert(p.reftyp)})[0].Interface())
	bldr.AppendNull()
	p.Nil(appfn.Call([]reflect.Value{reflect.ValueOf(1).Convert(p.reftyp)})[0].Interface())
	p.Nil(appfn.Call([]reflect.Value{reflect.ValueOf(2).Convert(p.reftyp)})[0].Interface())

	p.Less(0, bldr.Cap())
	p.Less(0, bldr.NullN())
	p.Equal(4, bldr.Len())

	result := bldr.NewDictionaryArray()
	defer result.Release()

	p.Zero(bldr.Cap())
	p.Zero(bldr.Len())
	p.Zero(bldr.NullN())

	p.Nil(appfn.Call([]reflect.Value{reflect.ValueOf(3).Convert(p.reftyp)})[0].Interface())
	bldr.AppendNull()
	p.Nil(appfn.Call([]reflect.Value{reflect.ValueOf(4).Convert(p.reftyp)})[0].Interface())

	result = bldr.NewDictionaryArray()
	defer result.Release()

	p.Equal(4, result.Dictionary().Len())
}

func (p *PrimitiveDictionaryTestSuite) TestResetFull() {
	expectedType := &arrow.DictionaryType{IndexType: &arrow.Int32Type{}, ValueType: p.typ}
	bldr := array.NewDictionaryBuilder(p.mem, expectedType)
	defer bldr.Release()

	builder := reflect.ValueOf(bldr)
	appfn := builder.MethodByName("Append")
	p.Nil(appfn.Call([]reflect.Value{reflect.ValueOf(1).Convert(p.reftyp)})[0].Interface())
	bldr.AppendNull()
	p.Nil(appfn.Call([]reflect.Value{reflect.ValueOf(1).Convert(p.reftyp)})[0].Interface())
	p.Nil(appfn.Call([]reflect.Value{reflect.ValueOf(2).Convert(p.reftyp)})[0].Interface())

	result := bldr.NewDictionaryArray()
	defer result.Release()

	p.Nil(appfn.Call([]reflect.Value{reflect.ValueOf(3).Convert(p.reftyp)})[0].Interface())
	result = bldr.NewDictionaryArray()
	defer result.Release()

	exindices, _, _ := array.FromJSON(p.mem, arrow.PrimitiveTypes.Int32, strings.NewReader("[2]"))
	exdict, _, _ := array.FromJSON(p.mem, p.typ, strings.NewReader("[1, 2, 3]"))
	defer exindices.Release()
	defer exdict.Release()

	p.True(array.Equal(exindices, result.Indices()))
	p.True(array.Equal(exdict, result.Dictionary()))

	bldr.ResetFull()
	p.Nil(appfn.Call([]reflect.Value{reflect.ValueOf(4).Convert(p.reftyp)})[0].Interface())
	result = bldr.NewDictionaryArray()
	defer result.Release()

	exindices, _, _ = array.FromJSON(p.mem, arrow.PrimitiveTypes.Int32, strings.NewReader("[0]"))
	exdict, _, _ = array.FromJSON(p.mem, p.typ, strings.NewReader("[4]"))
	defer exindices.Release()
	defer exdict.Release()

	p.True(array.Equal(exindices, result.Indices()))
	p.True(array.Equal(exdict, result.Dictionary()))
}

func (p *PrimitiveDictionaryTestSuite) TestStringRoundTrip() {
	dt := &arrow.DictionaryType{IndexType: &arrow.Int8Type{}, ValueType: p.typ}
	b := array.NewDictionaryBuilder(p.mem, dt)
	defer b.Release()

	builder := reflect.ValueOf(b)
	fn := builder.MethodByName("Append")
	p.Nil(fn.Call([]reflect.Value{reflect.ValueOf(1).Convert(p.reftyp)})[0].Interface())
	p.Nil(fn.Call([]reflect.Value{reflect.ValueOf(2).Convert(p.reftyp)})[0].Interface())
	p.Nil(fn.Call([]reflect.Value{reflect.ValueOf(1).Convert(p.reftyp)})[0].Interface())
	b.AppendNull()

	p.EqualValues(4, b.Len())
	p.EqualValues(1, b.NullN())

	arr := b.NewArray().(*array.Dictionary)
	defer arr.Release()
	p.True(arrow.TypeEqual(dt, arr.DataType()))

	b1 := array.NewDictionaryBuilder(p.mem, dt)
	defer b1.Release()

	for i := 0; i < arr.Len(); i++ {
		p.NoError(b1.AppendValueFromString(arr.ValueStr(i)))
	}

	arr1 := b1.NewArray().(*array.Dictionary)
	defer arr1.Release()

	p.Equal(arr.Len(), arr1.Len())
	p.True(array.Equal(arr, arr1))
}

func TestBasicStringDictionaryBuilder(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	dictType := &arrow.DictionaryType{IndexType: &arrow.Int8Type{}, ValueType: arrow.BinaryTypes.String}
	bldr := array.NewDictionaryBuilder(mem, dictType)
	defer bldr.Release()

	builder := bldr.(*array.BinaryDictionaryBuilder)
	assert.NoError(t, builder.Append([]byte("test")))
	assert.NoError(t, builder.AppendString("test2"))
	assert.NoError(t, builder.AppendString("test"))

	assert.Equal(t, "test", builder.ValueStr(builder.GetValueIndex(0)))
	assert.Equal(t, "test2", builder.ValueStr(builder.GetValueIndex(1)))
	assert.Equal(t, "test", builder.ValueStr(builder.GetValueIndex(2)))

	result := bldr.NewDictionaryArray()
	defer result.Release()

	exdict, _, _ := array.FromJSON(mem, arrow.BinaryTypes.String, strings.NewReader(`["test", "test2"]`))
	defer exdict.Release()
	exint, _, _ := array.FromJSON(mem, arrow.PrimitiveTypes.Int8, strings.NewReader("[0, 1, 0]"))
	defer exint.Release()

	assert.True(t, arrow.TypeEqual(dictType, result.DataType()))
	expected := array.NewDictionaryArray(dictType, exint, exdict)
	defer expected.Release()

	assert.True(t, array.Equal(expected, result))
}

func TestStringDictionaryInsertValues(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	exdict, _, _ := array.FromJSON(mem, arrow.BinaryTypes.String, strings.NewReader(`["c", "a", "b", "d"]`))
	defer exdict.Release()

	invalidDict, _, err := array.FromJSON(mem, arrow.BinaryTypes.Binary, strings.NewReader(`["ZQ==", "Zg=="]`))
	assert.NoError(t, err)
	defer invalidDict.Release()

	dictType := &arrow.DictionaryType{IndexType: &arrow.Int16Type{}, ValueType: arrow.BinaryTypes.String}
	bldr := array.NewDictionaryBuilder(mem, dictType)
	defer bldr.Release()

	builder := bldr.(*array.BinaryDictionaryBuilder)
	assert.NoError(t, builder.InsertStringDictValues(exdict.(*array.String)))
	// inserting again should have no effect
	assert.NoError(t, builder.InsertStringDictValues(exdict.(*array.String)))

	assert.Error(t, builder.InsertDictValues(invalidDict.(*array.Binary)))

	for i := 0; i < 2; i++ {
		builder.AppendString("c")
		builder.AppendString("a")
		builder.AppendString("b")
		builder.AppendNull()
		builder.AppendString("d")
	}

	assert.Equal(t, 10, bldr.Len())

	result := bldr.NewDictionaryArray()
	defer result.Release()

	exindices, _, _ := array.FromJSON(mem, arrow.PrimitiveTypes.Int16, strings.NewReader("[0, 1, 2, null, 3, 0, 1, 2, null, 3]"))
	defer exindices.Release()
	expected := array.NewDictionaryArray(dictType, exindices, exdict)
	defer expected.Release()
	assert.True(t, array.Equal(expected, result))
}

func TestStringDictionaryBuilderInit(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	dictArr, _, _ := array.FromJSON(mem, arrow.BinaryTypes.String, strings.NewReader(`["test", "test2"]`))
	defer dictArr.Release()
	intarr, _, _ := array.FromJSON(mem, arrow.PrimitiveTypes.Int8, strings.NewReader("[0, 1, 0]"))
	defer intarr.Release()

	dictType := &arrow.DictionaryType{IndexType: intarr.DataType().(arrow.FixedWidthDataType), ValueType: arrow.BinaryTypes.String}
	bldr := array.NewDictionaryBuilderWithDict(mem, dictType, dictArr)
	defer bldr.Release()

	builder := bldr.(*array.BinaryDictionaryBuilder)
	assert.NoError(t, builder.AppendString("test"))
	assert.NoError(t, builder.AppendString("test2"))
	assert.NoError(t, builder.AppendString("test"))

	result := bldr.NewDictionaryArray()
	defer result.Release()

	expected := array.NewDictionaryArray(dictType, intarr, dictArr)
	defer expected.Release()

	assert.True(t, array.Equal(expected, result))
}

func TestStringDictionaryBuilderOnlyNull(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	dictType := &arrow.DictionaryType{IndexType: &arrow.Int8Type{}, ValueType: arrow.BinaryTypes.String}
	bldr := array.NewDictionaryBuilder(mem, dictType)
	defer bldr.Release()

	bldr.AppendNull()
	result := bldr.NewDictionaryArray()
	defer result.Release()

	dict, _, _ := array.FromJSON(mem, arrow.BinaryTypes.String, strings.NewReader("[]"))
	defer dict.Release()
	intarr, _, _ := array.FromJSON(mem, arrow.PrimitiveTypes.Int8, strings.NewReader("[null]"))
	defer intarr.Release()

	expected := array.NewDictionaryArray(dictType, intarr, dict)
	defer expected.Release()

	assert.True(t, array.Equal(expected, result))
}

func TestStringDictionaryBuilderDelta(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	dictType := &arrow.DictionaryType{IndexType: &arrow.Int8Type{}, ValueType: arrow.BinaryTypes.String}
	bldr := array.NewDictionaryBuilder(mem, dictType)
	defer bldr.Release()

	builder := bldr.(*array.BinaryDictionaryBuilder)
	assert.NoError(t, builder.AppendString("test"))
	assert.NoError(t, builder.AppendString("test2"))
	assert.NoError(t, builder.AppendString("test"))

	result := bldr.NewDictionaryArray()
	defer result.Release()

	exdict, _, _ := array.FromJSON(mem, arrow.BinaryTypes.String, strings.NewReader(`["test", "test2"]`))
	defer exdict.Release()
	exint, _, _ := array.FromJSON(mem, arrow.PrimitiveTypes.Int8, strings.NewReader("[0, 1, 0]"))
	defer exint.Release()

	assert.True(t, arrow.TypeEqual(dictType, result.DataType()))
	expected := array.NewDictionaryArray(dictType, exint, exdict)
	defer expected.Release()

	assert.True(t, array.Equal(expected, result))

	assert.NoError(t, builder.AppendString("test2"))
	assert.NoError(t, builder.AppendString("test3"))
	assert.NoError(t, builder.AppendString("test2"))

	indices, delta, err := builder.NewDelta()
	assert.NoError(t, err)
	defer indices.Release()
	defer delta.Release()

	exdelta, _, _ := array.FromJSON(mem, arrow.BinaryTypes.String, strings.NewReader(`["test3"]`))
	defer exdelta.Release()
	exint, _, _ = array.FromJSON(mem, arrow.PrimitiveTypes.Int8, strings.NewReader("[1, 2, 1]"))
	defer exint.Release()

	assert.True(t, array.Equal(exdelta, delta))
	assert.True(t, array.Equal(exint, indices))
}

func TestStringDictionaryBuilderBigDelta(t *testing.T) {
	const testlen = 2048

	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	dictType := &arrow.DictionaryType{IndexType: &arrow.Int16Type{}, ValueType: arrow.BinaryTypes.String}
	bldr := array.NewDictionaryBuilder(mem, dictType)
	defer bldr.Release()
	builder := bldr.(*array.BinaryDictionaryBuilder)

	strbldr := array.NewStringBuilder(mem)
	defer strbldr.Release()

	intbldr := array.NewInt16Builder(mem)
	defer intbldr.Release()

	for idx := int16(0); idx < testlen; idx++ {
		var b strings.Builder
		b.WriteString("test")
		fmt.Fprint(&b, idx)

		val := b.String()
		assert.NoError(t, builder.AppendString(val))
		strbldr.Append(val)
		intbldr.Append(idx)
	}

	result := bldr.NewDictionaryArray()
	defer result.Release()
	strarr := strbldr.NewStringArray()
	defer strarr.Release()
	intarr := intbldr.NewInt16Array()
	defer intarr.Release()

	expected := array.NewDictionaryArray(dictType, intarr, strarr)
	defer expected.Release()

	assert.True(t, array.Equal(expected, result))

	strbldr2 := array.NewStringBuilder(mem)
	defer strbldr2.Release()
	intbldr2 := array.NewInt16Builder(mem)
	defer intbldr2.Release()

	for idx := int16(0); idx < testlen; idx++ {
		builder.AppendString("test1")
		intbldr2.Append(1)
	}
	for idx := int16(0); idx < testlen; idx++ {
		builder.AppendString("test_new_value1")
		intbldr2.Append(testlen)
	}
	strbldr2.Append("test_new_value1")

	indices2, delta2, err := bldr.NewDelta()
	assert.NoError(t, err)
	defer indices2.Release()
	defer delta2.Release()
	strarr2 := strbldr2.NewStringArray()
	defer strarr2.Release()
	intarr2 := intbldr2.NewInt16Array()
	defer intarr2.Release()

	assert.True(t, array.Equal(intarr2, indices2))
	assert.True(t, array.Equal(strarr2, delta2))

	strbldr3 := array.NewStringBuilder(mem)
	defer strbldr3.Release()
	intbldr3 := array.NewInt16Builder(mem)
	defer intbldr3.Release()

	for idx := int16(0); idx < testlen; idx++ {
		assert.NoError(t, builder.AppendString("test2"))
		intbldr3.Append(2)
	}
	for idx := int16(0); idx < testlen; idx++ {
		assert.NoError(t, builder.AppendString("test_new_value2"))
		intbldr3.Append(testlen + 1)
	}
	strbldr3.Append("test_new_value2")

	indices3, delta3, err := bldr.NewDelta()
	assert.NoError(t, err)
	defer indices3.Release()
	defer delta3.Release()
	strarr3 := strbldr3.NewStringArray()
	defer strarr3.Release()
	intarr3 := intbldr3.NewInt16Array()
	defer intarr3.Release()

	assert.True(t, array.Equal(intarr3, indices3))
	assert.True(t, array.Equal(strarr3, delta3))
}

func TestStringDictionaryBuilderIsNull(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	dictType := &arrow.DictionaryType{IndexType: &arrow.Int8Type{}, ValueType: arrow.BinaryTypes.String}
	bldr := array.NewDictionaryBuilder(mem, dictType)
	defer bldr.Release()

	builder := bldr.(*array.BinaryDictionaryBuilder)
	assert.NoError(t, builder.AppendString("test"))
	builder.AppendNull()
	assert.NoError(t, builder.AppendString("test2"))
	assert.NoError(t, builder.AppendString("test"))

	assert.False(t, bldr.IsNull(0))
	assert.True(t, bldr.IsNull(1))
	assert.False(t, bldr.IsNull(2))
	assert.False(t, bldr.IsNull(3))
}

func TestFixedSizeBinaryDictionaryBuilder(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	dictType := &arrow.DictionaryType{IndexType: &arrow.Int8Type{}, ValueType: &arrow.FixedSizeBinaryType{ByteWidth: 4}}
	bldr := array.NewDictionaryBuilder(mem, dictType)
	defer bldr.Release()

	builder := bldr.(*array.FixedSizeBinaryDictionaryBuilder)
	test := []byte{12, 12, 11, 12}
	test2 := []byte{12, 12, 11, 11}
	assert.NoError(t, builder.Append(test))
	assert.NoError(t, builder.Append(test2))
	assert.NoError(t, builder.Append(test))

	result := builder.NewDictionaryArray()
	defer result.Release()

	fsbBldr := array.NewFixedSizeBinaryBuilder(mem, dictType.ValueType.(*arrow.FixedSizeBinaryType))
	defer fsbBldr.Release()

	fsbBldr.Append(test)
	fsbBldr.Append(test2)
	fsbArr := fsbBldr.NewFixedSizeBinaryArray()
	defer fsbArr.Release()

	intbldr := array.NewInt8Builder(mem)
	defer intbldr.Release()

	intbldr.AppendValues([]int8{0, 1, 0}, nil)
	intArr := intbldr.NewInt8Array()
	defer intArr.Release()

	expected := array.NewDictionaryArray(dictType, intArr, fsbArr)
	defer expected.Release()

	assert.True(t, array.Equal(expected, result))
}

func TestFixedSizeBinaryDictionaryBuilderInit(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	fsbBldr := array.NewFixedSizeBinaryBuilder(mem, &arrow.FixedSizeBinaryType{ByteWidth: 4})
	defer fsbBldr.Release()

	test, test2 := []byte("abcd"), []byte("wxyz")
	fsbBldr.AppendValues([][]byte{test, test2}, nil)
	dictArr := fsbBldr.NewFixedSizeBinaryArray()
	defer dictArr.Release()

	dictType := &arrow.DictionaryType{IndexType: &arrow.Int8Type{}, ValueType: dictArr.DataType()}
	bldr := array.NewDictionaryBuilderWithDict(mem, dictType, dictArr)
	defer bldr.Release()

	builder := bldr.(*array.FixedSizeBinaryDictionaryBuilder)
	assert.NoError(t, builder.Append(test))
	assert.NoError(t, builder.Append(test2))
	assert.NoError(t, builder.Append(test))

	result := builder.NewDictionaryArray()
	defer result.Release()

	indices, _, _ := array.FromJSON(mem, arrow.PrimitiveTypes.Int8, strings.NewReader("[0, 1, 0]"))
	defer indices.Release()

	expected := array.NewDictionaryArray(dictType, indices, dictArr)
	defer expected.Release()

	assert.True(t, array.Equal(expected, result))
}

func TestFixedSizeBinaryDictionaryBuilderMakeBuilder(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	fsbBldr := array.NewFixedSizeBinaryBuilder(mem, &arrow.FixedSizeBinaryType{ByteWidth: 4})
	defer fsbBldr.Release()

	test, test2 := []byte("abcd"), []byte("wxyz")
	fsbBldr.AppendValues([][]byte{test, test2}, nil)
	dictArr := fsbBldr.NewFixedSizeBinaryArray()
	defer dictArr.Release()

	dictType := &arrow.DictionaryType{IndexType: &arrow.Int8Type{}, ValueType: dictArr.DataType()}
	bldr := array.NewBuilder(mem, dictType)
	defer bldr.Release()

	builder := bldr.(*array.FixedSizeBinaryDictionaryBuilder)
	assert.NoError(t, builder.Append(test))
	assert.NoError(t, builder.Append(test2))
	assert.NoError(t, builder.Append(test))

	result := builder.NewDictionaryArray()
	defer result.Release()

	indices, _, _ := array.FromJSON(mem, arrow.PrimitiveTypes.Int8, strings.NewReader("[0, 1, 0]"))
	defer indices.Release()

	expected := array.NewDictionaryArray(dictType, indices, dictArr)
	defer expected.Release()

	assert.True(t, array.Equal(expected, result))
}

func TestFixedSizeBinaryDictionaryBuilderDeltaDictionary(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	dictType := &arrow.DictionaryType{IndexType: &arrow.Int8Type{}, ValueType: &arrow.FixedSizeBinaryType{ByteWidth: 4}}
	bldr := array.NewDictionaryBuilder(mem, dictType)
	defer bldr.Release()

	builder := bldr.(*array.FixedSizeBinaryDictionaryBuilder)
	test := []byte{12, 12, 11, 12}
	test2 := []byte{12, 12, 11, 11}
	test3 := []byte{12, 12, 11, 10}

	assert.NoError(t, builder.Append(test))
	assert.NoError(t, builder.Append(test2))
	assert.NoError(t, builder.Append(test))

	result1 := bldr.NewDictionaryArray()
	defer result1.Release()

	fsbBuilder := array.NewFixedSizeBinaryBuilder(mem, dictType.ValueType.(*arrow.FixedSizeBinaryType))
	defer fsbBuilder.Release()

	fsbBuilder.AppendValues([][]byte{test, test2}, nil)
	fsbArr1 := fsbBuilder.NewFixedSizeBinaryArray()
	defer fsbArr1.Release()

	intBuilder := array.NewInt8Builder(mem)
	defer intBuilder.Release()
	intBuilder.AppendValues([]int8{0, 1, 0}, nil)
	intArr1 := intBuilder.NewInt8Array()
	defer intArr1.Release()

	expected := array.NewDictionaryArray(dictType, intArr1, fsbArr1)
	defer expected.Release()
	assert.True(t, array.Equal(expected, result1))

	assert.NoError(t, builder.Append(test))
	assert.NoError(t, builder.Append(test2))
	assert.NoError(t, builder.Append(test3))

	indices2, delta2, err := builder.NewDelta()
	assert.NoError(t, err)
	defer indices2.Release()
	defer delta2.Release()

	fsbBuilder.Append(test3)
	fsbArr2 := fsbBuilder.NewFixedSizeBinaryArray()
	defer fsbArr2.Release()

	intBuilder.AppendValues([]int8{0, 1, 2}, nil)
	intArr2 := intBuilder.NewInt8Array()
	defer intArr2.Release()

	assert.True(t, array.Equal(intArr2, indices2))
	assert.True(t, array.Equal(fsbArr2, delta2))
}

func TestFixedSizeBinaryDictionaryStringRoundTrip(t *testing.T) {
	// 1. create array
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	dictType := &arrow.DictionaryType{IndexType: &arrow.Int8Type{}, ValueType: &arrow.FixedSizeBinaryType{ByteWidth: 4}}
	b := array.NewDictionaryBuilder(mem, dictType)
	defer b.Release()

	builder := b.(*array.FixedSizeBinaryDictionaryBuilder)
	test := []byte{12, 12, 11, 12}
	test2 := []byte{12, 12, 11, 11}
	assert.NoError(t, builder.Append(test))
	assert.NoError(t, builder.Append(test2))
	assert.NoError(t, builder.Append(test))

	arr := builder.NewDictionaryArray()
	defer arr.Release()

	// 2. create array via AppendValueFromString
	b1 := array.NewDictionaryBuilder(mem, dictType)
	defer b1.Release()

	for i := 0; i < arr.Len(); i++ {
		assert.NoError(t, b1.AppendValueFromString(arr.ValueStr(i)))
	}

	arr1 := b1.NewArray().(*array.Dictionary)
	defer arr1.Release()

	assert.True(t, array.Equal(arr, arr1))
}

func TestDecimal128DictionaryBuilderBasic(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	test := []decimal128.Num{decimal128.FromI64(12), decimal128.FromI64(12), decimal128.FromI64(11), decimal128.FromI64(12)}
	dictType := &arrow.DictionaryType{IndexType: &arrow.Int8Type{}, ValueType: &arrow.Decimal128Type{Precision: 2, Scale: 0}}
	bldr := array.NewDictionaryBuilder(mem, dictType)
	defer bldr.Release()

	builder := bldr.(*array.Decimal128DictionaryBuilder)
	for _, v := range test {
		assert.NoError(t, builder.Append(v))
	}

	result := bldr.NewDictionaryArray()
	defer result.Release()

	indices, _, _ := array.FromJSON(mem, dictType.IndexType, strings.NewReader("[0, 0, 1, 0]"))
	defer indices.Release()
	dict, _, _ := array.FromJSON(mem, dictType.ValueType, strings.NewReader("[12, 11]"))
	defer dict.Release()

	expected := array.NewDictionaryArray(dictType, indices, dict)
	defer expected.Release()

	assert.True(t, array.ApproxEqual(expected, result))
}

func TestDecimal256DictionaryBuilderBasic(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	test := []decimal256.Num{decimal256.FromI64(12), decimal256.FromI64(12), decimal256.FromI64(11), decimal256.FromI64(12)}
	dictType := &arrow.DictionaryType{IndexType: &arrow.Int8Type{}, ValueType: &arrow.Decimal256Type{Precision: 2, Scale: 0}}
	bldr := array.NewDictionaryBuilder(mem, dictType)
	defer bldr.Release()

	builder := bldr.(*array.Decimal256DictionaryBuilder)
	for _, v := range test {
		assert.NoError(t, builder.Append(v))
	}

	result := bldr.NewDictionaryArray()
	defer result.Release()

	indices, _, _ := array.FromJSON(mem, dictType.IndexType, strings.NewReader("[0, 0, 1, 0]"))
	defer indices.Release()
	dict, _, _ := array.FromJSON(mem, dictType.ValueType, strings.NewReader("[12, 11]"))
	defer dict.Release()

	expected := array.NewDictionaryArray(dictType, indices, dict)
	defer expected.Release()

	assert.True(t, array.ApproxEqual(expected, result))
}

func TestNullDictionaryBuilderBasic(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	dictType := &arrow.DictionaryType{IndexType: &arrow.Int8Type{}, ValueType: arrow.Null}
	bldr := array.NewBuilder(mem, dictType)
	defer bldr.Release()

	builder := bldr.(*array.NullDictionaryBuilder)
	builder.AppendNulls(3)
	assert.Equal(t, 3, builder.Len())
	assert.Equal(t, 3, builder.NullN())

	nullarr, _, _ := array.FromJSON(mem, arrow.Null, strings.NewReader("[null, null, null]"))
	defer nullarr.Release()

	assert.NoError(t, builder.AppendArray(nullarr))
	assert.Equal(t, 6, bldr.Len())
	assert.Equal(t, 6, bldr.NullN())

	result := builder.NewDictionaryArray()
	defer result.Release()
	assert.Equal(t, 6, result.Len())
	assert.Equal(t, 6, result.NullN())
}

func TestDictionaryEquals(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	var (
		isValid                     = []bool{true, true, false, true, true, true}
		dict, dict2                 arrow.Array
		indices, indices2, indices3 arrow.Array
	)

	dict, _, _ = array.FromJSON(mem, arrow.BinaryTypes.String, strings.NewReader(`["foo", "bar", "baz"]`))
	defer dict.Release()
	dictType := &arrow.DictionaryType{IndexType: &arrow.Uint16Type{}, ValueType: arrow.BinaryTypes.String}

	dict2, _, _ = array.FromJSON(mem, arrow.BinaryTypes.String, strings.NewReader(`["foo", "bar", "baz", "qux"]`))
	defer dict2.Release()
	dictType2 := &arrow.DictionaryType{IndexType: &arrow.Uint16Type{}, ValueType: arrow.BinaryTypes.String}

	idxbuilder := array.NewUint16Builder(mem)
	defer idxbuilder.Release()

	idxbuilder.AppendValues([]uint16{1, 2, math.MaxUint16, 0, 2, 0}, isValid)
	indices = idxbuilder.NewArray()
	defer indices.Release()

	idxbuilder.AppendValues([]uint16{1, 2, 0, 0, 2, 0}, isValid)
	indices2 = idxbuilder.NewArray()
	defer indices2.Release()

	idxbuilder.AppendValues([]uint16{1, 1, 0, 0, 2, 0}, isValid)
	indices3 = idxbuilder.NewArray()
	defer indices3.Release()

	var (
		arr  = array.NewDictionaryArray(dictType, indices, dict)
		arr2 = array.NewDictionaryArray(dictType, indices2, dict)
		arr3 = array.NewDictionaryArray(dictType2, indices, dict2)
		arr4 = array.NewDictionaryArray(dictType, indices3, dict)
	)
	defer func() {
		arr.Release()
		arr2.Release()
		arr3.Release()
		arr4.Release()
	}()

	assert.True(t, array.Equal(arr, arr))
	// equal because the unequal index is masked by null
	assert.True(t, array.Equal(arr, arr2))
	// unequal dictionaries
	assert.False(t, array.Equal(arr, arr3))
	// unequal indices
	assert.False(t, array.Equal(arr, arr4))
	assert.True(t, array.SliceEqual(arr, 3, 6, arr4, 3, 6))
	assert.False(t, array.SliceEqual(arr, 1, 3, arr4, 1, 3))

	sz := arr.Len()
	slice := array.NewSlice(arr, 2, int64(sz))
	defer slice.Release()
	slice2 := array.NewSlice(arr, 2, int64(sz))
	defer slice2.Release()

	assert.Equal(t, sz-2, slice.Len())
	assert.True(t, array.Equal(slice, slice2))
	assert.True(t, array.SliceEqual(arr, 2, int64(arr.Len()), slice, 0, int64(slice.Len())))

	// chained slice
	slice2 = array.NewSlice(arr, 1, int64(arr.Len()))
	defer slice2.Release()
	slice2 = array.NewSlice(slice2, 1, int64(slice2.Len()))
	defer slice2.Release()

	assert.True(t, array.Equal(slice, slice2))
	slice = array.NewSlice(arr, 1, 4)
	defer slice.Release()
	slice2 = array.NewSlice(arr, 1, 4)
	defer slice2.Release()

	assert.Equal(t, 3, slice.Len())
	assert.True(t, array.Equal(slice, slice2))
	assert.True(t, array.SliceEqual(arr, 1, 4, slice, 0, int64(slice.Len())))
}

func TestDictionaryIndexTypes(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	dictIndexTypes := []arrow.DataType{
		arrow.PrimitiveTypes.Int8, arrow.PrimitiveTypes.Uint8,
		arrow.PrimitiveTypes.Int16, arrow.PrimitiveTypes.Uint16,
		arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Uint32,
		arrow.PrimitiveTypes.Int64, arrow.PrimitiveTypes.Uint64,
	}

	for _, indextyp := range dictIndexTypes {
		t.Run(indextyp.Name(), func(t *testing.T) {
			scope := memory.NewCheckedAllocatorScope(mem)
			defer scope.CheckSize(t)

			dictType := &arrow.DictionaryType{IndexType: indextyp, ValueType: arrow.BinaryTypes.String}
			bldr := array.NewDictionaryBuilder(mem, dictType)
			defer bldr.Release()

			builder := bldr.(*array.BinaryDictionaryBuilder)
			builder.AppendString("foo")
			builder.AppendString("bar")
			builder.AppendString("foo")
			builder.AppendString("baz")
			builder.Append(nil)

			assert.Equal(t, 5, builder.Len())
			assert.Equal(t, 1, builder.NullN())

			result := builder.NewDictionaryArray()
			defer result.Release()

			expectedIndices, _, _ := array.FromJSON(mem, indextyp, strings.NewReader("[0, 1, 0, 2, null]"))
			defer expectedIndices.Release()

			assert.True(t, array.Equal(expectedIndices, result.Indices()))
		})
	}
}

func TestDictionaryFromArrays(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	dict, _, _ := array.FromJSON(mem, arrow.BinaryTypes.String, strings.NewReader(`["foo", "bar", "baz"]`))
	defer dict.Release()

	dictIndexTypes := []arrow.DataType{
		arrow.PrimitiveTypes.Int8, arrow.PrimitiveTypes.Uint8,
		arrow.PrimitiveTypes.Int16, arrow.PrimitiveTypes.Uint16,
		arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Uint32,
		arrow.PrimitiveTypes.Int64, arrow.PrimitiveTypes.Uint64,
	}

	for _, indextyp := range dictIndexTypes {
		t.Run(indextyp.Name(), func(t *testing.T) {
			scope := memory.NewCheckedAllocatorScope(mem)
			defer scope.CheckSize(t)

			dictType := &arrow.DictionaryType{IndexType: indextyp, ValueType: arrow.BinaryTypes.String}
			indices1, _, _ := array.FromJSON(mem, indextyp, strings.NewReader("[1, 2, 0, 0, 2, 0]"))
			defer indices1.Release()

			indices2, _, _ := array.FromJSON(mem, indextyp, strings.NewReader("[1, 2, 0, 3, 2, 0]"))
			defer indices2.Release()

			arr1, err := array.NewValidatedDictionaryArray(dictType, indices1, dict)
			assert.NoError(t, err)
			defer arr1.Release()

			_, err = array.NewValidatedDictionaryArray(dictType, indices2, dict)
			assert.Error(t, err)

			switch indextyp.ID() {
			case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64:
				indices3, _, _ := array.FromJSON(mem, indextyp, strings.NewReader("[1, 2, 0, null, 2, 0]"))
				defer indices3.Release()
				bitutil.ClearBit(indices3.Data().Buffers()[0].Bytes(), 2)
				arr3, err := array.NewValidatedDictionaryArray(dictType, indices3, dict)
				assert.NoError(t, err)
				defer arr3.Release()
			}

			indices4, _, _ := array.FromJSON(mem, indextyp, strings.NewReader("[1, 2, null, 3, 2, 0]"))
			defer indices4.Release()
			_, err = array.NewValidatedDictionaryArray(dictType, indices4, dict)
			assert.Error(t, err)

			diffIndexType := arrow.PrimitiveTypes.Int8
			if indextyp.ID() == arrow.INT8 {
				diffIndexType = arrow.PrimitiveTypes.Uint8
			}
			_, err = array.NewValidatedDictionaryArray(&arrow.DictionaryType{IndexType: diffIndexType, ValueType: arrow.BinaryTypes.String}, indices4, dict)
			assert.Error(t, err)
		})
	}
}

func TestListOfDictionary(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	rootBuilder := array.NewBuilder(mem, arrow.ListOf(&arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int16, ValueType: arrow.BinaryTypes.String}))
	defer rootBuilder.Release()

	listBldr := rootBuilder.(*array.ListBuilder)
	dictBldr := listBldr.ValueBuilder().(*array.BinaryDictionaryBuilder)

	listBldr.Append(true)
	expected := []string{}
	for _, a := range []byte("abc") {
		for _, d := range []byte("def") {
			for _, g := range []byte("ghi") {
				for _, j := range []byte("jkl") {
					for _, m := range []byte("mno") {
						for _, p := range []byte("pqr") {
							if a+d+g+j+m+p%16 == 0 {
								listBldr.Append(true)
							}

							str := string([]byte{a, d, g, j, m, p})
							dictBldr.AppendString(str)
							expected = append(expected, str)
						}
					}
				}
			}
		}
	}

	strbldr := array.NewStringBuilder(mem)
	defer strbldr.Release()
	strbldr.AppendValues(expected, nil)

	expectedDict := strbldr.NewStringArray()
	defer expectedDict.Release()

	arr := rootBuilder.NewArray()
	defer arr.Release()

	actualDict := arr.(*array.List).ListValues().(*array.Dictionary)
	assert.True(t, array.Equal(expectedDict, actualDict.Dictionary()))
}

func TestDictionaryCanCompareIndices(t *testing.T) {
	makeDict := func(mem memory.Allocator, idxType, valueType arrow.DataType, dictJSON string) *array.Dictionary {
		indices, _, _ := array.FromJSON(mem, idxType, strings.NewReader("[]"))
		defer indices.Release()
		dict, _, _ := array.FromJSON(mem, valueType, strings.NewReader(dictJSON))
		defer dict.Release()

		out, _ := array.NewValidatedDictionaryArray(&arrow.DictionaryType{IndexType: idxType, ValueType: valueType}, indices, dict)
		return out
	}

	compareSwap := func(t *testing.T, l, r *array.Dictionary, expected bool) {
		assert.Equalf(t, expected, l.CanCompareIndices(r), "left: %s\nright: %s\n", l, r)
		assert.Equalf(t, expected, r.CanCompareIndices(l), "left: %s\nright: %s\n", r, l)
	}

	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	t.Run("same", func(t *testing.T) {
		arr := makeDict(mem, arrow.PrimitiveTypes.Int16, arrow.BinaryTypes.String, `["foo", "bar"]`)
		defer arr.Release()
		same := makeDict(mem, arrow.PrimitiveTypes.Int16, arrow.BinaryTypes.String, `["foo", "bar"]`)
		defer same.Release()
		compareSwap(t, arr, same, true)
	})

	t.Run("prefix dict", func(t *testing.T) {
		arr := makeDict(mem, arrow.PrimitiveTypes.Int16, arrow.BinaryTypes.String, `["foo", "bar", "quux"]`)
		defer arr.Release()
		prefixDict := makeDict(mem, arrow.PrimitiveTypes.Int16, arrow.BinaryTypes.String, `["foo", "bar"]`)
		defer prefixDict.Release()
		compareSwap(t, arr, prefixDict, true)
	})

	t.Run("indices need cast", func(t *testing.T) {
		arr := makeDict(mem, arrow.PrimitiveTypes.Int16, arrow.BinaryTypes.String, `["foo", "bar"]`)
		defer arr.Release()
		needcast := makeDict(mem, arrow.PrimitiveTypes.Int8, arrow.BinaryTypes.String, `["foo", "bar"]`)
		defer needcast.Release()
		compareSwap(t, arr, needcast, false)
	})

	t.Run("non prefix", func(t *testing.T) {
		arr := makeDict(mem, arrow.PrimitiveTypes.Int16, arrow.BinaryTypes.String, `["foo", "bar", "quux"]`)
		defer arr.Release()
		nonPrefix := makeDict(mem, arrow.PrimitiveTypes.Int16, arrow.BinaryTypes.String, `["foo", "blink"]`)
		defer nonPrefix.Release()
		compareSwap(t, arr, nonPrefix, false)
	})
}

func TestDictionaryGetValueIndex(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	indicesJson := "[5, 0, 1, 3, 2, 4]"
	indices64, _, _ := array.FromJSON(mem, arrow.PrimitiveTypes.Int64, strings.NewReader(indicesJson))
	defer indices64.Release()
	dict, _, _ := array.FromJSON(mem, arrow.PrimitiveTypes.Int32, strings.NewReader("[10, 20, 30, 40, 50, 60]"))
	defer dict.Release()

	dictIndexTypes := []arrow.DataType{
		arrow.PrimitiveTypes.Int8, arrow.PrimitiveTypes.Uint8,
		arrow.PrimitiveTypes.Int16, arrow.PrimitiveTypes.Uint16,
		arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Uint32,
		arrow.PrimitiveTypes.Int64, arrow.PrimitiveTypes.Uint64,
	}
	i64Index := indices64.(*array.Int64)
	for _, idxt := range dictIndexTypes {
		t.Run(idxt.Name(), func(t *testing.T) {
			indices, _, _ := array.FromJSON(mem, idxt, strings.NewReader(indicesJson))
			defer indices.Release()
			dictType := &arrow.DictionaryType{IndexType: idxt, ValueType: arrow.PrimitiveTypes.Int32}

			dictArr := array.NewDictionaryArray(dictType, indices, dict)
			defer dictArr.Release()

			const offset = 1
			slicedDictArr := array.NewSlice(dictArr, offset, int64(dictArr.Len()))
			defer slicedDictArr.Release()
			assert.EqualValues(t, "10", slicedDictArr.(*array.Dictionary).ValueStr(0))
			for i := 0; i < indices.Len(); i++ {
				assert.EqualValues(t, i64Index.Value(i), dictArr.GetValueIndex(i))
				if i < slicedDictArr.Len() {
					assert.EqualValues(t, i64Index.Value(i+offset), slicedDictArr.(*array.Dictionary).GetValueIndex(i))
				}
			}
		})
	}
}

func checkTransposeMap(t *testing.T, b *memory.Buffer, exp []int32) bool {
	got := arrow.Int32Traits.CastFromBytes(b.Bytes())
	return assert.Equal(t, exp, got)
}

func TestDictionaryUnifierNumeric(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	dictType := arrow.PrimitiveTypes.Int64

	d1, _, err := array.FromJSON(mem, dictType, strings.NewReader(`[3, 4, 7]`))
	require.NoError(t, err)
	d2, _, err := array.FromJSON(mem, dictType, strings.NewReader(`[1, 7, 4, 8]`))
	require.NoError(t, err)
	d3, _, err := array.FromJSON(mem, dictType, strings.NewReader(`[1, -200]`))
	require.NoError(t, err)

	expected := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: dictType}
	expectedDict, _, err := array.FromJSON(mem, dictType, strings.NewReader(`[3, 4, 7, 1, 8, -200]`))
	require.NoError(t, err)
	defer func() {
		d1.Release()
		d2.Release()
		d3.Release()
		expectedDict.Release()
	}()

	unifier, err := array.NewDictionaryUnifier(mem, dictType)
	assert.NoError(t, err)
	defer unifier.Release()

	assert.NoError(t, unifier.Unify(d1))
	assert.NoError(t, unifier.Unify(d2))
	assert.NoError(t, unifier.Unify(d3))

	invalid, _, _ := array.FromJSON(mem, arrow.PrimitiveTypes.Int32, strings.NewReader(`[1, -200]`))
	defer invalid.Release()
	assert.EqualError(t, unifier.Unify(invalid), "dictionary type different from unifier: int32, expected: int64")

	outType, outDict, err := unifier.GetResult()
	assert.NoError(t, err)
	defer outDict.Release()
	assert.Truef(t, arrow.TypeEqual(expected, outType), "got: %s, expected: %s", outType, expected)
	assert.Truef(t, array.Equal(expectedDict, outDict), "got: %s, expected: %s", outDict, expectedDict)

	b1, err := unifier.UnifyAndTranspose(d1)
	assert.NoError(t, err)
	b2, err := unifier.UnifyAndTranspose(d2)
	assert.NoError(t, err)
	b3, err := unifier.UnifyAndTranspose(d3)
	assert.NoError(t, err)

	outType, outDict, err = unifier.GetResult()
	assert.NoError(t, err)
	defer func() {
		outDict.Release()
		b1.Release()
		b2.Release()
		b3.Release()
	}()

	assert.Truef(t, arrow.TypeEqual(expected, outType), "got: %s, expected: %s", outType, expected)
	assert.Truef(t, array.Equal(expectedDict, outDict), "got: %s, expected: %s", outDict, expectedDict)

	checkTransposeMap(t, b1, []int32{0, 1, 2})
	checkTransposeMap(t, b2, []int32{3, 2, 1, 4})
	checkTransposeMap(t, b3, []int32{3, 5})
}

func TestDictionaryUnifierString(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	dictType := arrow.BinaryTypes.String
	d1, _, err := array.FromJSON(mem, dictType, strings.NewReader(`["foo", "bar"]`))
	require.NoError(t, err)
	defer d1.Release()

	d2, _, err := array.FromJSON(mem, dictType, strings.NewReader(`["quux", "foo"]`))
	require.NoError(t, err)
	defer d2.Release()

	expected := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: dictType}
	expectedDict, _, _ := array.FromJSON(mem, dictType, strings.NewReader(`["foo", "bar", "quux"]`))
	defer expectedDict.Release()

	unifier, err := array.NewDictionaryUnifier(mem, dictType)
	assert.NoError(t, err)
	defer unifier.Release()

	assert.NoError(t, unifier.Unify(d1))
	assert.NoError(t, unifier.Unify(d2))
	outType, outDict, err := unifier.GetResult()
	assert.NoError(t, err)
	defer outDict.Release()

	assert.Truef(t, arrow.TypeEqual(expected, outType), "got: %s, expected: %s", outType, expected)
	assert.Truef(t, array.Equal(expectedDict, outDict), "got: %s, expected: %s", outDict, expectedDict)

	b1, err := unifier.UnifyAndTranspose(d1)
	assert.NoError(t, err)
	b2, err := unifier.UnifyAndTranspose(d2)
	assert.NoError(t, err)

	outType, outDict, err = unifier.GetResult()
	assert.NoError(t, err)
	defer func() {
		outDict.Release()
		b1.Release()
		b2.Release()
	}()

	assert.Truef(t, arrow.TypeEqual(expected, outType), "got: %s, expected: %s", outType, expected)
	assert.Truef(t, array.Equal(expectedDict, outDict), "got: %s, expected: %s", outDict, expectedDict)

	checkTransposeMap(t, b1, []int32{0, 1})
	checkTransposeMap(t, b2, []int32{2, 0})
}

func TestDictionaryUnifierBinary(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	dictType := arrow.BinaryTypes.Binary
	d1, _, err := array.FromJSON(mem, dictType, strings.NewReader(`["Zm9vCg==", "YmFyCg=="]`)) // base64("foo\n"), base64("bar\n")
	require.NoError(t, err)
	defer d1.Release()

	d2, _, err := array.FromJSON(mem, dictType, strings.NewReader(`["cXV1eAo=", "Zm9vCg=="]`)) // base64("quux\n"), base64("foo\n")
	require.NoError(t, err)
	defer d2.Release()

	expected := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: dictType}
	expectedDict, _, _ := array.FromJSON(mem, dictType, strings.NewReader(`["Zm9vCg==", "YmFyCg==", "cXV1eAo="]`))
	defer expectedDict.Release()

	unifier := array.NewBinaryDictionaryUnifier(mem)
	defer unifier.Release()

	assert.NoError(t, unifier.Unify(d1))
	assert.NoError(t, unifier.Unify(d2))
	outType, outDict, err := unifier.GetResult()
	assert.NoError(t, err)
	defer outDict.Release()

	assert.Truef(t, arrow.TypeEqual(expected, outType), "got: %s, expected: %s", outType, expected)
	assert.Truef(t, array.Equal(expectedDict, outDict), "got: %s, expected: %s", outDict, expectedDict)

	b1, err := unifier.UnifyAndTranspose(d1)
	assert.NoError(t, err)
	b2, err := unifier.UnifyAndTranspose(d2)
	assert.NoError(t, err)

	outType, outDict, err = unifier.GetResult()
	assert.NoError(t, err)
	defer func() {
		outDict.Release()
		b1.Release()
		b2.Release()
	}()

	assert.Truef(t, arrow.TypeEqual(expected, outType), "got: %s, expected: %s", outType, expected)
	assert.Truef(t, array.Equal(expectedDict, outDict), "got: %s, expected: %s", outDict, expectedDict)

	checkTransposeMap(t, b1, []int32{0, 1})
	checkTransposeMap(t, b2, []int32{2, 0})
}

func TestDictionaryUnifierFixedSizeBinary(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	dictType := &arrow.FixedSizeBinaryType{ByteWidth: 3}
	data := memory.NewBufferBytes([]byte(`foobarbazqux`))

	fsbData := array.NewData(dictType, 2, []*memory.Buffer{nil, memory.SliceBuffer(data, 0, 6)}, nil, 0, 0)
	defer fsbData.Release()
	d1 := array.NewFixedSizeBinaryData(fsbData)
	fsbData = array.NewData(dictType, 3, []*memory.Buffer{nil, memory.SliceBuffer(data, 3, 9)}, nil, 0, 0)
	defer fsbData.Release()
	d2 := array.NewFixedSizeBinaryData(fsbData)

	fsbData = array.NewData(dictType, 4, []*memory.Buffer{nil, data}, nil, 0, 0)
	defer fsbData.Release()
	expectedDict := array.NewFixedSizeBinaryData(fsbData)
	expected := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: dictType}

	unifier, err := array.NewDictionaryUnifier(mem, dictType)
	assert.NoError(t, err)

	defer func() {
		d1.Release()
		d2.Release()
		expectedDict.Release()
		unifier.Release()
	}()

	assert.NoError(t, unifier.Unify(d1))
	assert.NoError(t, unifier.Unify(d2))
	outType, outDict, err := unifier.GetResult()
	assert.NoError(t, err)
	defer outDict.Release()

	assert.Truef(t, arrow.TypeEqual(expected, outType), "got: %s, expected: %s", outType, expected)
	assert.Truef(t, array.Equal(expectedDict, outDict), "got: %s, expected: %s", outDict, expectedDict)

	b1, err := unifier.UnifyAndTranspose(d1)
	assert.NoError(t, err)
	b2, err := unifier.UnifyAndTranspose(d2)
	assert.NoError(t, err)

	outType, outDict, err = unifier.GetResult()
	assert.NoError(t, err)
	defer func() {
		outDict.Release()
		b1.Release()
		b2.Release()
	}()

	assert.Truef(t, arrow.TypeEqual(expected, outType), "got: %s, expected: %s", outType, expected)
	assert.Truef(t, array.Equal(expectedDict, outDict), "got: %s, expected: %s", outDict, expectedDict)

	checkTransposeMap(t, b1, []int32{0, 1})
	checkTransposeMap(t, b2, []int32{1, 2, 3})
}

func TestDictionaryUnifierLarge(t *testing.T) {
	// unifying larger dictionaries should choose the right index type
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	bldr := array.NewInt32Builder(mem)
	defer bldr.Release()
	bldr.Reserve(120)
	for i := int32(0); i < 120; i++ {
		bldr.UnsafeAppend(i)
	}

	d1 := bldr.NewInt32Array()
	defer d1.Release()
	assert.EqualValues(t, 120, d1.Len())

	bldr.Reserve(30)
	for i := int32(110); i < 140; i++ {
		bldr.UnsafeAppend(i)
	}

	d2 := bldr.NewInt32Array()
	defer d2.Release()
	assert.EqualValues(t, 30, d2.Len())

	bldr.Reserve(140)
	for i := int32(0); i < 140; i++ {
		bldr.UnsafeAppend(i)
	}

	expectedDict := bldr.NewInt32Array()
	defer expectedDict.Release()
	assert.EqualValues(t, 140, expectedDict.Len())

	// int8 would be too narrow to hold all the values
	expected := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int16, ValueType: arrow.PrimitiveTypes.Int32}

	unifier, err := array.NewDictionaryUnifier(mem, arrow.PrimitiveTypes.Int32)
	assert.NoError(t, err)
	defer unifier.Release()

	assert.NoError(t, unifier.Unify(d1))
	assert.NoError(t, unifier.Unify(d2))
	outType, outDict, err := unifier.GetResult()
	assert.NoError(t, err)
	defer outDict.Release()

	assert.Truef(t, arrow.TypeEqual(expected, outType), "got: %s, expected: %s", outType, expected)
	assert.Truef(t, array.Equal(expectedDict, outDict), "got: %s, expected: %s", outDict, expectedDict)
}

func checkDictionaryArray(t *testing.T, arr, expectedVals, expectedIndices arrow.Array) bool {
	require.IsType(t, (*array.Dictionary)(nil), arr)
	dictArr := arr.(*array.Dictionary)
	ret := true
	ret = ret && assert.Truef(t, array.Equal(expectedVals, dictArr.Dictionary()), "got: %s, expected: %s", dictArr.Dictionary(), expectedVals)
	return ret && assert.Truef(t, array.Equal(expectedIndices, dictArr.Indices()), "got: %s, expected: %s", dictArr.Indices(), expectedIndices)
}

func TestDictionaryUnifierSimpleChunkedArray(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	dictType := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.BinaryTypes.String}
	chunk1, _, _ := array.FromJSON(mem, dictType, strings.NewReader(`["ab", "cd", null, "cd"]`))
	chunk2, _, _ := array.FromJSON(mem, dictType, strings.NewReader(`["ef", "cd", "ef"]`))
	chunk3, _, _ := array.FromJSON(mem, dictType, strings.NewReader(`["ef", "ab", null, "ab"]`))
	chunk4, _, _ := array.FromJSON(mem, dictType, strings.NewReader(`[]`))
	chunked := arrow.NewChunked(dictType, []arrow.Array{chunk1, chunk2, chunk3, chunk4})
	defer func() {
		chunk1.Release()
		chunk2.Release()
		chunk3.Release()
		chunk4.Release()
		chunked.Release()
	}()

	unified, err := array.UnifyChunkedDicts(mem, chunked)
	assert.NoError(t, err)
	defer unified.Release()

	assert.Len(t, unified.Chunks(), 4)
	expectedDict, _, _ := array.FromJSON(mem, dictType.ValueType, strings.NewReader(`["ab", "cd", "ef"]`))
	defer expectedDict.Release()

	c1Indices, _, _ := array.FromJSON(mem, dictType.IndexType, strings.NewReader(`[0, 1, null, 1]`))
	defer c1Indices.Release()
	c2Indices, _, _ := array.FromJSON(mem, dictType.IndexType, strings.NewReader(`[2, 1, 2]`))
	defer c2Indices.Release()
	c3Indices, _, _ := array.FromJSON(mem, dictType.IndexType, strings.NewReader(`[2, 0, null, 0]`))
	defer c3Indices.Release()
	c4Indices, _, _ := array.FromJSON(mem, dictType.IndexType, strings.NewReader(`[]`))
	defer c4Indices.Release()
	checkDictionaryArray(t, unified.Chunk(0), expectedDict, c1Indices)
	checkDictionaryArray(t, unified.Chunk(1), expectedDict, c2Indices)
	checkDictionaryArray(t, unified.Chunk(2), expectedDict, c3Indices)
	checkDictionaryArray(t, unified.Chunk(3), expectedDict, c4Indices)
}

func TestDictionaryUnifierChunkedArrayZeroChunks(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	dictType := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.BinaryTypes.String}
	chunked := arrow.NewChunked(dictType, []arrow.Array{})
	unified, err := array.UnifyChunkedDicts(mem, chunked)
	assert.NoError(t, err)
	assert.True(t, array.ChunkedEqual(unified, chunked))
}

func TestDictionaryUnifierChunkedArrayOneChunk(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	dictType := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.BinaryTypes.String}
	chunk1, _, _ := array.FromJSON(mem, dictType, strings.NewReader(`["ab", "cd", null, "cd"]`))
	defer chunk1.Release()

	chunked := arrow.NewChunked(dictType, []arrow.Array{chunk1})
	defer chunked.Release()

	unified, err := array.UnifyChunkedDicts(mem, chunked)
	assert.NoError(t, err)
	defer unified.Release()

	assert.True(t, array.ChunkedEqual(unified, chunked))
	assert.Same(t, unified, chunked)
}

func TestDictionaryUnifierChunkedArrayNoDict(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	typ := arrow.PrimitiveTypes.Int8
	chunk1, _, _ := array.FromJSON(mem, typ, strings.NewReader(`[1, 1, 2, 3]`))
	defer chunk1.Release()

	chunk2, _, _ := array.FromJSON(mem, typ, strings.NewReader(`[5, 8, 13]`))
	defer chunk2.Release()

	chunked := arrow.NewChunked(typ, []arrow.Array{chunk1, chunk2})
	defer chunked.Release()

	unified, err := array.UnifyChunkedDicts(mem, chunked)
	assert.NoError(t, err)
	defer unified.Release()

	assert.True(t, array.ChunkedEqual(unified, chunked))
	assert.Same(t, unified, chunked)
}

func TestDictionaryUnifierChunkedArrayNested(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	typ := arrow.ListOf(&arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int16, ValueType: arrow.BinaryTypes.String})
	chunk1, _, err := array.FromJSON(mem, typ, strings.NewReader(`[["ab", "cd"], ["cd"]]`))
	assert.NoError(t, err)
	// defer chunk1.Release()
	chunk2, _, err := array.FromJSON(mem, typ, strings.NewReader(`[[], ["ef", "cd", "ef"]]`))
	assert.NoError(t, err)
	// defer chunk2.Release()
	chunked := arrow.NewChunked(typ, []arrow.Array{chunk1, chunk2})
	// defer chunked.Release()

	unified, err := array.UnifyChunkedDicts(mem, chunked)
	assert.NoError(t, err)
	// defer unified.Release()
	assert.Len(t, unified.Chunks(), 2)

	expectedDict, _, _ := array.FromJSON(mem, arrow.BinaryTypes.String, strings.NewReader(`["ab", "cd", "ef"]`))
	// defer expectedDict.Release()

	unified1 := unified.Chunk(0).(*array.List)
	assert.Equal(t, []int32{0, 2, 3}, unified1.Offsets())
	expectedIndices1, _, _ := array.FromJSON(mem, arrow.PrimitiveTypes.Int16, strings.NewReader(`[0, 1, 1]`))
	// defer expectedIndices1.Release()
	checkDictionaryArray(t, unified1.ListValues(), expectedDict, expectedIndices1)

	unified2 := unified.Chunk(1).(*array.List)
	assert.Equal(t, []int32{0, 0, 3}, unified2.Offsets())
	expectedIndices2, _, _ := array.FromJSON(mem, arrow.PrimitiveTypes.Int16, strings.NewReader(`[2, 1, 2]`))
	// defer expectedIndices2.Release()
	checkDictionaryArray(t, unified2.ListValues(), expectedDict, expectedIndices2)
	defer func() {
		expectedIndices1.Release()
		expectedIndices2.Release()
		expectedDict.Release()
		unified.Release()
		chunked.Release()
		chunk2.Release()
		chunk1.Release()
	}()
}

func TestDictionaryUnifierChunkedArrayExtension(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	dt := types.NewDictExtensionType()
	chunk1, _, err := array.FromJSON(mem, dt, strings.NewReader(`["ab", null, "cd", "ab"]`))
	assert.NoError(t, err)
	defer chunk1.Release()

	chunk2, _, err := array.FromJSON(mem, dt, strings.NewReader(`["ef", "ab", "ab"]`))
	assert.NoError(t, err)
	defer chunk2.Release()

	chunked := arrow.NewChunked(dt, []arrow.Array{chunk1, chunk2})
	defer chunked.Release()
	unified, err := array.UnifyChunkedDicts(mem, chunked)
	assert.NoError(t, err)
	defer unified.Release()
	assert.Len(t, unified.Chunks(), 2)

	expectedDict, _, _ := array.FromJSON(mem, arrow.BinaryTypes.String, strings.NewReader(`["ab", "cd", "ef"]`))
	defer expectedDict.Release()

	unified1 := unified.Chunk(0).(array.ExtensionArray)
	assert.Truef(t, arrow.TypeEqual(dt, unified1.DataType()), "expected: %s, got: %s", dt, unified1.DataType())
	indices, _, _ := array.FromJSON(mem, arrow.PrimitiveTypes.Int8, strings.NewReader(`[0, null, 1, 0]`))
	defer indices.Release()
	checkDictionaryArray(t, unified1.Storage(), expectedDict, indices)

	unified2 := unified.Chunk(1).(array.ExtensionArray)
	assert.Truef(t, arrow.TypeEqual(dt, unified2.DataType()), "expected: %s, got: %s", dt, unified1.DataType())
	indices, _, _ = array.FromJSON(mem, arrow.PrimitiveTypes.Int8, strings.NewReader(`[2, 0, 0]`))
	defer indices.Release()
	checkDictionaryArray(t, unified2.Storage(), expectedDict, indices)
}

func TestDictionaryUnifierChunkedArrayNestedDict(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	innerType := arrow.ListOf(&arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Uint32, ValueType: arrow.BinaryTypes.String})
	innerDict1, _, err := array.FromJSON(mem, innerType, strings.NewReader(`[["ab", "cd"], [], ["cd", null]]`))
	assert.NoError(t, err)
	defer innerDict1.Release()
	indices1, _, _ := array.FromJSON(mem, arrow.PrimitiveTypes.Int32, strings.NewReader(`[2, 1, 0, 1, 2]`))
	defer indices1.Release()

	chunk1 := array.NewDictionaryArray(&arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: innerType}, indices1, innerDict1)
	defer chunk1.Release()

	innerDict2, _, err := array.FromJSON(mem, innerType, strings.NewReader(`[["cd", "ef"], ["cd", null], []]`))
	assert.NoError(t, err)
	defer innerDict2.Release()
	indices2, _, _ := array.FromJSON(mem, arrow.PrimitiveTypes.Int32, strings.NewReader(`[1, 2, 2, 0]`))
	defer indices2.Release()

	chunk2 := array.NewDictionaryArray(&arrow.DictionaryType{IndexType: indices2.DataType(), ValueType: innerType}, indices2, innerDict2)
	defer chunk2.Release()

	chunked := arrow.NewChunked(chunk1.DataType(), []arrow.Array{chunk1, chunk2})
	defer chunked.Release()

	unified, err := array.UnifyChunkedDicts(mem, chunked)
	assert.Nil(t, unified)
	assert.EqualError(t, err, "unimplemented dictionary value type, list<item: dictionary<values=utf8, indices=uint32, ordered=false>, nullable>")
}

func TestDictionaryUnifierTableZeroColumns(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	schema := arrow.NewSchema([]arrow.Field{}, nil)
	table := array.NewTable(schema, []arrow.Column{}, 42)
	defer table.Release()

	unified, err := array.UnifyTableDicts(mem, table)
	assert.NoError(t, err)
	assert.True(t, schema.Equal(unified.Schema()))
	assert.EqualValues(t, 42, unified.NumRows())
	assert.True(t, array.TableEqual(table, unified))
}

func TestDictionaryAppendIndices(t *testing.T) {
	indexTypes := []arrow.DataType{
		arrow.PrimitiveTypes.Int8,
		arrow.PrimitiveTypes.Uint8,
		arrow.PrimitiveTypes.Int16,
		arrow.PrimitiveTypes.Uint16,
		arrow.PrimitiveTypes.Int32,
		arrow.PrimitiveTypes.Uint32,
		arrow.PrimitiveTypes.Int64,
		arrow.PrimitiveTypes.Uint64,
	}

	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	dict, _, err := array.FromJSON(mem, arrow.BinaryTypes.String, strings.NewReader(`["a", "b", "c", "d", "e", "f"]`))
	require.NoError(t, err)
	defer dict.Release()

	indices := []int{3, 4, 0, 3, 1, 4, 4, 5}

	for _, typ := range indexTypes {
		t.Run(typ.String(), func(t *testing.T) {
			scoped := memory.NewCheckedAllocatorScope(mem)
			defer scoped.CheckSize(t)

			dictType := &arrow.DictionaryType{
				IndexType: typ, ValueType: dict.DataType()}
			bldr := array.NewDictionaryBuilderWithDict(mem, dictType, dict)
			defer bldr.Release()

			bldr.AppendIndices(indices, nil)

			arr := bldr.NewDictionaryArray()
			defer arr.Release()

			arrIndices := arr.Indices()
			assert.EqualValues(t, len(indices), arr.Len())
			assert.EqualValues(t, len(indices), arrIndices.Len())

			assert.Equal(t, fmt.Sprint(indices), arrIndices.String())
		})
	}
}

type panicAllocator struct {
	n       int
	paniced bool
	memory.Allocator
}

func (p *panicAllocator) Allocate(size int) []byte {
	if size > p.n {
		p.paniced = true
		panic("panic allocator")
	}
	return p.Allocator.Allocate(size)
}

func (p *panicAllocator) Reallocate(size int, b []byte) []byte {
	return p.Allocator.Reallocate(size, b)
}

func (p *panicAllocator) Free(b []byte) {
	p.Allocator.Free(b)
}

func TestBinaryDictionaryPanic(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	allocator := &panicAllocator{
		n:         400,
		Allocator: mem,
	}

	expectedType := &arrow.DictionaryType{IndexType: &arrow.Int8Type{}, ValueType: arrow.BinaryTypes.String}
	bldr := array.NewDictionaryBuilder(allocator, expectedType)
	defer bldr.Release()

	bldr.AppendNull()
	allocator.n = 0 // force panic
	func() {
		defer func() {
			recover()
		}()
		bldr.NewArray()
	}()
	assert.True(t, allocator.paniced)
}

func BenchmarkBinaryDictionaryBuilder(b *testing.B) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(b, 0)

	dictType := &arrow.DictionaryType{IndexType: &arrow.Int32Type{}, ValueType: arrow.BinaryTypes.String}
	bldr := array.NewDictionaryBuilder(mem, dictType)
	defer bldr.Release()

	randString := func() string {
		return fmt.Sprintf("test-%d", rand.Intn(30))
	}

	builder := bldr.(*array.BinaryDictionaryBuilder)
	for i := 0; i < b.N; i++ {
		assert.NoError(b, builder.AppendString(randString()))
	}
}
