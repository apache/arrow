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
	"reflect"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v9/arrow"
	"github.com/apache/arrow/go/v9/arrow/array"
	"github.com/apache/arrow/go/v9/arrow/bitutil"
	"github.com/apache/arrow/go/v9/arrow/decimal128"
	"github.com/apache/arrow/go/v9/arrow/memory"
	"github.com/stretchr/testify/assert"
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

	p.True(array.ArrayEqual(expected, arr))
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

	p.True(array.ArrayEqual(expected, arr))
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

	p.True(array.ArrayEqual(expected, arr))
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

	p.True(array.ArrayEqual(expected, result))
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
	p.True(array.ArrayEqual(expected, result))

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

	p.True(array.ArrayEqual(exindices, indices))
	p.True(array.ArrayEqual(exdelta, delta))
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
	p.True(array.ArrayEqual(expected, result))

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

	p.True(array.ArrayEqual(exindices, indices))
	p.True(array.ArrayEqual(exdelta, delta))

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

	p.True(array.ArrayEqual(exindices, indices))
	p.True(array.ArrayEqual(exdelta, delta))
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

	p.True(array.ArrayEqual(exindices, result.Indices()))
	p.True(array.ArrayEqual(exdict, result.Dictionary()))

	bldr.ResetFull()
	p.Nil(appfn.Call([]reflect.Value{reflect.ValueOf(4).Convert(p.reftyp)})[0].Interface())
	result = bldr.NewDictionaryArray()
	defer result.Release()

	exindices, _, _ = array.FromJSON(p.mem, arrow.PrimitiveTypes.Int32, strings.NewReader("[0]"))
	exdict, _, _ = array.FromJSON(p.mem, p.typ, strings.NewReader("[4]"))
	defer exindices.Release()
	defer exdict.Release()

	p.True(array.ArrayEqual(exindices, result.Indices()))
	p.True(array.ArrayEqual(exdict, result.Dictionary()))
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

	result := bldr.NewDictionaryArray()
	defer result.Release()

	exdict, _, _ := array.FromJSON(mem, arrow.BinaryTypes.String, strings.NewReader(`["test", "test2"]`))
	defer exdict.Release()
	exint, _, _ := array.FromJSON(mem, arrow.PrimitiveTypes.Int8, strings.NewReader("[0, 1, 0]"))
	defer exint.Release()

	assert.True(t, arrow.TypeEqual(dictType, result.DataType()))
	expected := array.NewDictionaryArray(dictType, exint, exdict)
	defer expected.Release()

	assert.True(t, array.ArrayEqual(expected, result))
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
	assert.True(t, array.ArrayEqual(expected, result))
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

	assert.True(t, array.ArrayEqual(expected, result))
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

	assert.True(t, array.ArrayEqual(expected, result))
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

	assert.True(t, array.ArrayEqual(expected, result))

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

	assert.True(t, array.ArrayEqual(exdelta, delta))
	assert.True(t, array.ArrayEqual(exint, indices))
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

	assert.True(t, array.ArrayEqual(expected, result))

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

	assert.True(t, array.ArrayEqual(intarr2, indices2))
	assert.True(t, array.ArrayEqual(strarr2, delta2))

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

	assert.True(t, array.ArrayEqual(intarr3, indices3))
	assert.True(t, array.ArrayEqual(strarr3, delta3))
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

	assert.True(t, array.ArrayEqual(expected, result))
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

	assert.True(t, array.ArrayEqual(expected, result))
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

	assert.True(t, array.ArrayEqual(expected, result))
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
	assert.True(t, array.ArrayEqual(expected, result1))

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

	assert.True(t, array.ArrayEqual(intArr2, indices2))
	assert.True(t, array.ArrayEqual(fsbArr2, delta2))
}

func TestDecimalDictionaryBuilderBasic(t *testing.T) {
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

	assert.True(t, array.ArrayApproxEqual(expected, result))
}

func TestNullDictionaryBuilderBasic(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	dictType := &arrow.DictionaryType{IndexType: &arrow.Int8Type{}, ValueType: arrow.Null}
	bldr := array.NewBuilder(mem, dictType)
	defer bldr.Release()

	builder := bldr.(*array.NullDictionaryBuilder)
	builder.AppendNull()
	builder.AppendNull()
	builder.AppendNull()
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

	assert.True(t, array.ArrayEqual(arr, arr))
	// equal because the unequal index is masked by null
	assert.True(t, array.ArrayEqual(arr, arr2))
	// unequal dictionaries
	assert.False(t, array.ArrayEqual(arr, arr3))
	// unequal indices
	assert.False(t, array.ArrayEqual(arr, arr4))
	assert.True(t, array.ArraySliceEqual(arr, 3, 6, arr4, 3, 6))
	assert.False(t, array.ArraySliceEqual(arr, 1, 3, arr4, 1, 3))

	sz := arr.Len()
	slice := array.NewSlice(arr, 2, int64(sz))
	defer slice.Release()
	slice2 := array.NewSlice(arr, 2, int64(sz))
	defer slice2.Release()

	assert.Equal(t, sz-2, slice.Len())
	assert.True(t, array.ArrayEqual(slice, slice2))
	assert.True(t, array.ArraySliceEqual(arr, 2, int64(arr.Len()), slice, 0, int64(slice.Len())))

	// chained slice
	slice2 = array.NewSlice(arr, 1, int64(arr.Len()))
	defer slice2.Release()
	slice2 = array.NewSlice(slice2, 1, int64(slice2.Len()))
	defer slice2.Release()

	assert.True(t, array.ArrayEqual(slice, slice2))
	slice = array.NewSlice(arr, 1, 4)
	defer slice.Release()
	slice2 = array.NewSlice(arr, 1, 4)
	defer slice2.Release()

	assert.Equal(t, 3, slice.Len())
	assert.True(t, array.ArrayEqual(slice, slice2))
	assert.True(t, array.ArraySliceEqual(arr, 1, 4, slice, 0, int64(slice.Len())))
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

			assert.True(t, array.ArrayEqual(expectedIndices, result.Indices()))
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
	assert.True(t, array.ArrayEqual(expectedDict, actualDict.Dictionary()))
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

			for i := 0; i < indices.Len(); i++ {
				assert.EqualValues(t, i64Index.Value(i), dictArr.GetValueIndex(i))
				if i < slicedDictArr.Len() {
					assert.EqualValues(t, i64Index.Value(i+offset), slicedDictArr.(*array.Dictionary).GetValueIndex(i))
				}
			}
		})
	}
}
