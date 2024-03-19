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

package array

import (
	"testing"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestDataReset(t *testing.T) {
	var (
		buffers1 = make([]*memory.Buffer, 0, 3)
		buffers2 = make([]*memory.Buffer, 0, 3)
	)
	for i := 0; i < cap(buffers1); i++ {
		buffers1 = append(buffers1, memory.NewBufferBytes([]byte("some-bytes1")))
		buffers2 = append(buffers2, memory.NewBufferBytes([]byte("some-bytes2")))
	}

	data := NewData(&arrow.StringType{}, 10, buffers1, nil, 0, 0)
	data.Reset(&arrow.Int64Type{}, 5, buffers2, nil, 1, 2)

	for i := 0; i < 2; i++ {
		assert.Equal(t, buffers2, data.Buffers())
		assert.Equal(t, &arrow.Int64Type{}, data.DataType())
		assert.Equal(t, 1, data.NullN())
		assert.Equal(t, 2, data.Offset())
		assert.Equal(t, 5, data.Len())

		// Make sure it works when resetting the data with its own buffers (new buffers are retained
		// before old ones are released.)
		data.Reset(&arrow.Int64Type{}, 5, data.Buffers(), nil, 1, 2)
	}
}

func TestSizeInBytes(t *testing.T) {
	var buffers1 = make([]*memory.Buffer, 0, 3)

	for i := 0; i < cap(buffers1); i++ {
		buffers1 = append(buffers1, memory.NewBufferBytes([]byte("15-bytes-buffer")))
	}
	data := NewData(&arrow.StringType{}, 10, buffers1, nil, 0, 0)
	var arrayData arrow.ArrayData = data
	dataWithChild := NewData(&arrow.StringType{}, 10, buffers1, []arrow.ArrayData{arrayData}, 0, 0)

	t.Run("buffers only", func(t *testing.T) {
		expectedSize := uint64(45)
		if actualSize := data.SizeInBytes(); actualSize != expectedSize {
			t.Errorf("expected size %d, got %d", expectedSize, actualSize)
		}
	})

	t.Run("buffers and child data", func(t *testing.T) {
		// 45 bytes in buffers, 45 bytes in child data
		expectedSize := uint64(90)
		if actualSize := dataWithChild.SizeInBytes(); actualSize != expectedSize {
			t.Errorf("expected size %d, got %d", expectedSize, actualSize)
		}
	})

	t.Run("buffers and nested child data", func(t *testing.T) {
		var dataWithChildArrayData arrow.ArrayData = dataWithChild
		var dataWithNestedChild arrow.ArrayData = NewData(&arrow.StringType{}, 10, buffers1, []arrow.ArrayData{dataWithChildArrayData}, 0, 0)
		// 45 bytes in buffers, 90 bytes in nested child data
		expectedSize := uint64(135)
		if actualSize := dataWithNestedChild.SizeInBytes(); actualSize != expectedSize {
			t.Errorf("expected size %d, got %d", expectedSize, actualSize)
		}
	})

	t.Run("buffers and dictionary", func(t *testing.T) {
		dictData := data
		dataWithDict := NewDataWithDictionary(&arrow.StringType{}, 10, buffers1, 0, 0, dictData)
		// 45 bytes in buffers, 45 bytes in dictionary
		expectedSize := uint64(90)
		if actualSize := dataWithDict.SizeInBytes(); actualSize != expectedSize {
			t.Errorf("expected size %d, got %d", expectedSize, actualSize)
		}
	})

	t.Run("sliced data", func(t *testing.T) {
		sliceData := NewSliceData(arrayData, 3, 5)
		// offset is not taken into account in SizeInBytes()
		expectedSize := uint64(45)
		if actualSize := sliceData.SizeInBytes(); actualSize != expectedSize {
			t.Errorf("expected size %d, got %d", expectedSize, actualSize)
		}
	})

	t.Run("sliced data with children", func(t *testing.T) {
		var dataWithChildArrayData arrow.ArrayData = dataWithChild
		sliceData := NewSliceData(dataWithChildArrayData, 3, 5)
		// offset is not taken into account in SizeInBytes()
		expectedSize := uint64(90)
		if actualSize := sliceData.SizeInBytes(); actualSize != expectedSize {
			t.Errorf("expected size %d, got %d", expectedSize, actualSize)
		}
	})

	t.Run("buffers with children which are sliced data", func(t *testing.T) {
		sliceData := NewSliceData(arrayData, 3, 5)
		dataWithSlicedChildren := NewData(&arrow.StringType{}, 10, buffers1, []arrow.ArrayData{sliceData}, 0, 0)
		// offset is not taken into account in SizeInBytes()
		expectedSize := uint64(90)
		if actualSize := dataWithSlicedChildren.SizeInBytes(); actualSize != expectedSize {
			t.Errorf("expected size %d, got %d", expectedSize, actualSize)
		}
	})
}
