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

package utils_test

import (
	"testing"

	"github.com/apache/arrow/go/parquet/internal/utils"
	"github.com/stretchr/testify/assert"
)

func assertReaderVals(t *testing.T, reader *utils.BitmapReader, vals []bool) {
	for _, v := range vals {
		if v {
			assert.True(t, reader.Set())
			assert.False(t, reader.NotSet())
		} else {
			assert.True(t, reader.NotSet())
			assert.False(t, reader.Set())
		}
		reader.Next()
	}
}

func TestNormalOperation(t *testing.T) {
	for _, offset := range []int64{0, 1, 3, 5, 7, 8, 12, 13, 21, 38, 75, 120} {
		buf := bitmapFromSlice([]int{0, 1, 1, 1, 0, 0, 0, 1, 0, 1, 0, 1, 0, 1}, offset)

		reader := utils.NewBitmapReader(buf, offset, 14)
		assertReaderVals(t, reader, []bool{false, true, true, true, false, false, false, true, false, true, false, true, false, true})
	}
}

func TestDoesNotReadOutOfBounds(t *testing.T) {
	var bitmap [16]byte
	const length = 128

	reader := utils.NewBitmapReader(bitmap[:], 0, length)
	assert.EqualValues(t, length, reader.Len())
	assert.NotPanics(t, func() {
		for i := 0; i < length; i++ {
			assert.True(t, reader.NotSet())
			reader.Next()
		}
	})
	assert.EqualValues(t, length, reader.Pos())

	reader = utils.NewBitmapReader(bitmap[:], 5, length-5)
	assert.EqualValues(t, length-5, reader.Len())
	assert.NotPanics(t, func() {
		for i := 0; i < length-5; i++ {
			assert.True(t, reader.NotSet())
			reader.Next()
		}
	})
	assert.EqualValues(t, length-5, reader.Pos())

	assert.NotPanics(t, func() {
		reader = utils.NewBitmapReader(nil, 0, 0)
	})
}
