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
	"strings"
	"testing"

	"github.com/apache/arrow/go/v7/arrow"
	"github.com/apache/arrow/go/v7/arrow/array"
	"github.com/apache/arrow/go/v7/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestDictionaryBuilderBasic(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	expectedType := &arrow.DictionaryType{IndexType: &arrow.Int8Type{}, ValueType: arrow.PrimitiveTypes.Int8}
	bldr := array.NewDictionaryBuilder(mem, expectedType)
	defer bldr.Release()

	builder := bldr.(*array.Int8DictionaryBuilder)
	assert.NoError(t, builder.Append(1))
	assert.NoError(t, builder.Append(2))
	assert.NoError(t, builder.Append(1))
	builder.AppendNull()

	assert.EqualValues(t, 4, builder.Len())
	assert.EqualValues(t, 1, builder.NullN())

	arr := builder.NewArray().(*array.Dictionary)
	defer arr.Release()

	assert.True(t, arrow.TypeEqual(expectedType, arr.DataType()))
	expectedDict, _, err := array.FromJSON(mem, expectedType.ValueType, strings.NewReader("[1, 2]"))
	assert.NoError(t, err)
	defer expectedDict.Release()

	expectedIndices, _, err := array.FromJSON(mem, expectedType.IndexType, strings.NewReader("[0, 1, 0, null]"))
	assert.NoError(t, err)
	defer expectedIndices.Release()

	expected := array.NewDictionaryArray(expectedType, expectedIndices, expectedDict)
	defer expected.Release()

	assert.True(t, array.ArrayEqual(expected, arr))
}
