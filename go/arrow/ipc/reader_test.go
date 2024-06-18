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
	"io"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReaderCatchPanic(t *testing.T) {
	alloc := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "s", Type: arrow.BinaryTypes.String},
	}, nil)

	b := array.NewRecordBuilder(alloc, schema)
	defer b.Release()

	b.Field(0).(*array.StringBuilder).AppendValues([]string{"foo", "bar", "baz"}, nil)
	rec := b.NewRecord()
	defer rec.Release()

	buf := new(bytes.Buffer)
	writer := NewWriter(buf, WithSchema(schema))
	require.NoError(t, writer.Write(rec))

	for i := buf.Len() - 100; i < buf.Len(); i++ {
		buf.Bytes()[i] = 0
	}

	reader, err := NewReader(buf)
	require.NoError(t, err)

	_, err = reader.Read()
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "arrow/ipc: unknown error while reading")
	}
}

func TestReaderCheckedAllocator(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)
	schema := arrow.NewSchema([]arrow.Field{
		{
			Name: "s",
			Type: &arrow.DictionaryType{
				ValueType: arrow.BinaryTypes.String,
				IndexType: arrow.PrimitiveTypes.Int32,
			},
		},
	}, nil)

	b := array.NewRecordBuilder(alloc, schema)
	defer b.Release()

	bldr := b.Field(0).(*array.BinaryDictionaryBuilder)
	bldr.Append([]byte("foo"))
	bldr.Append([]byte("bar"))
	bldr.Append([]byte("baz"))

	rec := b.NewRecord()
	defer rec.Release()

	buf := new(bytes.Buffer)
	writer := NewWriter(buf, WithSchema(schema), WithAllocator(alloc))
	defer writer.Close()
	require.NoError(t, writer.Write(rec))

	reader, err := NewReader(buf, WithAllocator(alloc))
	require.NoError(t, err)
	defer reader.Release()

	_, err = reader.Read()
	require.NoError(t, err)
}

func BenchmarkIPC(b *testing.B) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(b, 0)

	schema := arrow.NewSchema([]arrow.Field{
		{
			Name: "s",
			Type: &arrow.DictionaryType{
				ValueType: arrow.BinaryTypes.String,
				IndexType: arrow.PrimitiveTypes.Int32,
			},
		},
	}, nil)

	rb := array.NewRecordBuilder(alloc, schema)
	defer rb.Release()

	bldr := rb.Field(0).(*array.BinaryDictionaryBuilder)
	bldr.Append([]byte("foo"))
	bldr.Append([]byte("bar"))
	bldr.Append([]byte("baz"))

	rec := rb.NewRecord()
	defer rec.Release()

	for _, codec := range []struct {
		name        string
		codecOption Option
	}{
		{
			name: "plain",
		},
		{
			name:        "zstd",
			codecOption: WithZstd(),
		},
		{
			name:        "lz4",
			codecOption: WithLZ4(),
		},
	} {
		options := []Option{WithSchema(schema), WithAllocator(alloc)}
		if codec.codecOption != nil {
			options = append(options, codec.codecOption)
		}
		b.Run(fmt.Sprintf("Writer/codec=%s", codec.name), func(b *testing.B) {
			buf := new(bytes.Buffer)
			for i := 0; i < b.N; i++ {
				func() {
					buf.Reset()
					writer := NewWriter(buf, options...)
					defer writer.Close()
					if err := writer.Write(rec); err != nil {
						b.Fatal(err)
					}
				}()
			}
		})

		b.Run(fmt.Sprintf("Reader/codec=%s", codec.name), func(b *testing.B) {
			buf := new(bytes.Buffer)
			writer := NewWriter(buf, options...)
			defer writer.Close()
			require.NoError(b, writer.Write(rec))
			bufBytes := buf.Bytes()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				func() {
					reader, err := NewReader(bytes.NewReader(bufBytes), WithAllocator(alloc))
					if err != nil {
						b.Fatal(err)
					}
					defer reader.Release()
					for {
						if _, err := reader.Read(); err != nil {
							if err == io.EOF {
								break
							}
							b.Fatal(err)
						}
					}
				}()
			}
		})
	}
}
