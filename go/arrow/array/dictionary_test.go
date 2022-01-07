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
	"reflect"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v7/arrow"
	"github.com/apache/arrow/go/v7/arrow/array"
	"github.com/apache/arrow/go/v7/arrow/memory"
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
