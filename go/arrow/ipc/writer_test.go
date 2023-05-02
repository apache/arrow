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
	"encoding/binary"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/bitutil"
	"github.com/apache/arrow/go/v13/arrow/internal/flatbuf"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestGetZeroBasedValueOffsets(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	vals := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}
	b := array.NewStringBuilder(alloc)
	defer b.Release()
	b.AppendValues(vals, nil)

	arr := b.NewArray()
	defer arr.Release()

	env := &recordEncoder{mem: alloc}

	offsets, err := env.getZeroBasedValueOffsets(arr)
	require.NoError(t, err)
	defer offsets.Release()
	assert.Equal(t, 44, offsets.Len(), "include all offsets if array is not sliced")

	sl := array.NewSlice(arr, 0, 4)
	defer sl.Release()

	offsets, err = env.getZeroBasedValueOffsets(sl)
	require.NoError(t, err)
	defer offsets.Release()
	assert.Equal(t, 20, offsets.Len(), "trim trailing offsets after slice")
}

func TestWriterCatchPanic(t *testing.T) {
	alloc := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "s", Type: arrow.BinaryTypes.String},
	}, nil)

	b := array.NewRecordBuilder(alloc, schema)
	defer b.Release()

	b.Field(0).(*array.StringBuilder).AppendValues([]string{"foo", "bar", "baz"}, nil)
	rec := b.NewRecord()
	defer rec.Release()

	// mess up the first offset for the string column
	offsetBuf := rec.Column(0).Data().Buffers()[1]
	bitutil.SetBitsTo(offsetBuf.Bytes(), 0, 32, true)

	buf := new(bytes.Buffer)

	writer := NewWriter(buf, WithSchema(schema))
	assert.EqualError(t, writer.Write(rec), "arrow/ipc: unknown error while writing: runtime error: slice bounds out of range [-1:]")
}

func TestWriterMemCompression(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "s", Type: arrow.BinaryTypes.String},
	}, nil)

	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	b.Field(0).(*array.StringBuilder).AppendValues([]string{"foo", "bar", "baz"}, nil)
	rec := b.NewRecord()
	defer rec.Release()

	var buf bytes.Buffer
	w := NewWriter(&buf, WithAllocator(mem), WithSchema(schema), WithZstd())
	defer w.Close()

	require.NoError(t, w.Write(rec))
}

func TestWriteWithCompressionAndMinSavings(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	// a small batch that is known to be compressible
	batch, _, err := array.RecordFromJSON(mem, arrow.NewSchema([]arrow.Field{
		{Name: "n", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil),
		strings.NewReader(`[
			{"n": 0}, {"n": 1}, {"n": 2}, {"n": 3}, {"n": 4},
			{"n": 5}, {"n": 6}, {"n": 7}, {"n": 8}, {"n": 9}]`))
	require.NoError(t, err)
	defer batch.Release()

	prefixedSize := func(buf *memory.Buffer) int64 {
		if buf.Len() < arrow.Int64SizeBytes {
			return 0
		}
		return int64(binary.LittleEndian.Uint64(buf.Bytes()))
	}
	contentSize := func(buf *memory.Buffer) int64 {
		return int64(buf.Len()) - int64(arrow.Int64SizeBytes)
	}

	for _, codec := range []flatbuf.CompressionType{flatbuf.CompressionTypeLZ4_FRAME, flatbuf.CompressionTypeZSTD} {
		enc := newRecordEncoder(mem, 0, 5, true, codec, 1, nil)
		var payload Payload
		require.NoError(t, enc.encode(&payload, batch))
		assert.Len(t, payload.body, 2)

		// compute the savings when body buffers are compressed unconditionally.
		// We also validate that our test batch is indeed compressible.
		uncompressedSize, compressedSize := prefixedSize(payload.body[1]), contentSize(payload.body[1])
		assert.Less(t, compressedSize, uncompressedSize)
		assert.Greater(t, compressedSize, int64(0))
		expectedSavings := 1.0 - float64(compressedSize)/float64(uncompressedSize)

		compressEncoder := newRecordEncoder(mem, 0, 5, true, codec, 1, &expectedSavings)
		payload.Release()
		payload.body = payload.body[:0]
		require.NoError(t, compressEncoder.encode(&payload, batch))
		assert.Len(t, payload.body, 2)
		assert.Equal(t, uncompressedSize, prefixedSize(payload.body[1]))
		assert.Equal(t, compressedSize, contentSize(payload.body[1]))

		payload.Release()
		payload.body = payload.body[:0]
		// slightly bump the threshold. the body buffer should now be prefixed
		// with -1 and its content left uncompressed
		minSavings := math.Nextafter(expectedSavings, 1.0)
		compressEncoder.minSpaceSavings = &minSavings
		require.NoError(t, compressEncoder.encode(&payload, batch))
		assert.Len(t, payload.body, 2)
		assert.EqualValues(t, -1, prefixedSize(payload.body[1]))
		assert.Equal(t, uncompressedSize, contentSize(payload.body[1]))
		payload.Release()
		payload.body = payload.body[:0]

		for _, outOfRange := range []float64{math.Nextafter(1.0, 2.0), math.Nextafter(0, -1)} {
			compressEncoder.minSpaceSavings = &outOfRange
			err := compressEncoder.encode(&payload, batch)
			assert.ErrorIs(t, err, arrow.ErrInvalid)
			assert.ErrorContains(t, err, "minSpaceSavings not in range [0,1]")
		}
	}
}
