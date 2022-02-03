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

package ipc

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/arrow/bitutil"
	"github.com/apache/arrow/go/v8/arrow/memory"
	"github.com/stretchr/testify/assert"
)

// reproducer from ARROW-13529
func TestSliceAndWrite(t *testing.T) {
	alloc := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "s", Type: arrow.BinaryTypes.String},
	}, nil)

	b := array.NewRecordBuilder(alloc, schema)
	defer b.Release()

	b.Field(0).(*array.StringBuilder).AppendValues([]string{"foo", "bar", "baz"}, nil)
	rec := b.NewRecord()
	defer rec.Release()

	sliceAndWrite := func(rec arrow.Record, schema *arrow.Schema) {
		slice := rec.NewSlice(1, 2)
		defer slice.Release()

		fmt.Println(slice.Columns()[0].(*array.String).Value(0))

		var buf bytes.Buffer
		w := NewWriter(&buf, WithSchema(schema))
		w.Write(slice)
		w.Close()
	}

	assert.NotPanics(t, func() {
		for i := 0; i < 2; i++ {
			sliceAndWrite(rec, schema)
		}
	})
}

func TestNewTruncatedBitmap(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	assert.Nil(t, newTruncatedBitmap(alloc, 0, 0, nil), "input bitmap is null")

	buf := memory.NewBufferBytes(make([]byte, bitutil.BytesForBits(8)))
	defer buf.Release()

	bitutil.SetBit(buf.Bytes(), 0)
	bitutil.SetBit(buf.Bytes(), 2)
	bitutil.SetBit(buf.Bytes(), 4)
	bitutil.SetBit(buf.Bytes(), 6)

	assert.Same(t, buf, newTruncatedBitmap(alloc, 0, 8, buf), "no truncation necessary")

	result := newTruncatedBitmap(alloc, 1, 7, buf)
	defer result.Release()
	for i, exp := range []bool{false, true, false, true, false, true, false} {
		assert.Equal(t, exp, bitutil.BitIsSet(result.Bytes(), i), "truncate for offset")
	}

	buf = memory.NewBufferBytes(make([]byte, 128))
	defer buf.Release()
	bitutil.SetBitsTo(buf.Bytes(), 0, 128*8, true)

	result = newTruncatedBitmap(alloc, 0, 8, buf)
	defer result.Release()
	assert.Equal(t, 64, result.Len(), "truncate to smaller buffer")
	assert.Equal(t, 8, bitutil.CountSetBits(result.Bytes(), 0, 8))
}
