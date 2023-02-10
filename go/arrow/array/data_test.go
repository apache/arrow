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

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/memory"
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
