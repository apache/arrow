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

//go:build go1.18

package exec_test

import (
	"reflect"
	"strings"
	"testing"
	"unsafe"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/compute/internal/exec"
	"github.com/apache/arrow/go/v13/arrow/decimal128"
	"github.com/apache/arrow/go/v13/arrow/endian"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/apache/arrow/go/v13/arrow/scalar"
	"github.com/apache/arrow/go/v13/internal/types"
	"github.com/stretchr/testify/assert"
)

func TestBufferSpan_SetBuffer(t *testing.T) {
	type fields struct {
		Buf       []byte
		Owner     *memory.Buffer
		SelfAlloc bool
	}
	type args struct {
		buf *memory.Buffer
	}
	foo := []byte{0xde, 0xad, 0xbe, 0xef}
	own := memory.NewBufferBytes(foo)
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{"simple", fields{SelfAlloc: true}, args{own}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &exec.BufferSpan{
				Buf:       tt.fields.Buf,
				Owner:     tt.fields.Owner,
				SelfAlloc: tt.fields.SelfAlloc,
			}
			b.SetBuffer(tt.args.buf)
			assert.Same(t, &foo[0], &b.Buf[0])
			assert.Same(t, own, b.Owner)
			assert.False(t, b.SelfAlloc)
		})
	}
}

func TestBufferSpan_WrapBuffer(t *testing.T) {
	type fields struct {
		Buf       []byte
		Owner     *memory.Buffer
		SelfAlloc bool
	}
	type args struct {
		buf *memory.Buffer
	}
	foo := []byte{0xde, 0xad, 0xbe, 0xef}
	own := memory.NewBufferBytes(foo)
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{"simple", fields{SelfAlloc: false}, args{own}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &exec.BufferSpan{
				Buf:       tt.fields.Buf,
				Owner:     tt.fields.Owner,
				SelfAlloc: tt.fields.SelfAlloc,
			}
			b.WrapBuffer(tt.args.buf)
			assert.Same(t, &foo[0], &b.Buf[0])
			assert.Same(t, own, b.Owner)
			assert.True(t, b.SelfAlloc)
		})
	}
}

func TestArraySpan_UpdateNullCount(t *testing.T) {
	type fields struct {
		Type     arrow.DataType
		Len      int64
		Nulls    int64
		Offset   int64
		Buffers  [3]exec.BufferSpan
		Scratch  [2]uint64
		Children []exec.ArraySpan
	}
	tests := []struct {
		name   string
		fields fields
		want   int64
	}{
		{"known", fields{Nulls: 25}, 25},
		{"unknown", fields{
			Nulls:   array.UnknownNullCount,
			Len:     8, // 0b01101101
			Buffers: [3]exec.BufferSpan{{Buf: []byte{109}}, {}, {}}}, 3},
		{"unknown with offset", fields{
			Nulls:   array.UnknownNullCount,
			Len:     4,
			Offset:  2, // 0b01101101
			Buffers: [3]exec.BufferSpan{{Buf: []byte{109}}, {}, {}}}, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &exec.ArraySpan{
				Type:     tt.fields.Type,
				Len:      tt.fields.Len,
				Nulls:    tt.fields.Nulls,
				Offset:   tt.fields.Offset,
				Buffers:  tt.fields.Buffers,
				Scratch:  tt.fields.Scratch,
				Children: tt.fields.Children,
			}
			if got := a.UpdateNullCount(); got != tt.want {
				t.Errorf("ArraySpan.UpdateNullCount() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestArraySpan_Dictionary(t *testing.T) {
	type fields struct {
		Type     arrow.DataType
		Len      int64
		Nulls    int64
		Offset   int64
		Buffers  [3]exec.BufferSpan
		Scratch  [2]uint64
		Children []exec.ArraySpan
	}
	children := []exec.ArraySpan{{}}
	tests := []struct {
		name   string
		fields fields
		want   *exec.ArraySpan
	}{
		{"basic", fields{Children: children}, &children[0]},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &exec.ArraySpan{
				Type:     tt.fields.Type,
				Len:      tt.fields.Len,
				Nulls:    tt.fields.Nulls,
				Offset:   tt.fields.Offset,
				Buffers:  tt.fields.Buffers,
				Scratch:  tt.fields.Scratch,
				Children: tt.fields.Children,
			}
			if got := a.Dictionary(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ArraySpan.Dictionary() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestArraySpan_NumBuffers(t *testing.T) {
	type fields struct {
		Type     arrow.DataType
		Len      int64
		Nulls    int64
		Offset   int64
		Buffers  [3]exec.BufferSpan
		Scratch  [2]uint64
		Children []exec.ArraySpan
	}

	arrow.RegisterExtensionType(types.NewUUIDType())
	defer arrow.UnregisterExtensionType("uuid")

	tests := []struct {
		name   string
		fields fields
		want   int
	}{
		{"null", fields{Type: arrow.Null}, 1},
		{"struct", fields{Type: arrow.StructOf()}, 1},
		{"fixed size list", fields{Type: arrow.FixedSizeListOf(4, arrow.PrimitiveTypes.Int32)}, 1},
		{"binary", fields{Type: arrow.BinaryTypes.Binary}, 3},
		{"large binary", fields{Type: arrow.BinaryTypes.LargeBinary}, 3},
		{"string", fields{Type: arrow.BinaryTypes.String}, 3},
		{"large string", fields{Type: arrow.BinaryTypes.LargeString}, 3},
		{"extension", fields{Type: types.NewUUIDType()}, 2},
		{"int32", fields{Type: arrow.PrimitiveTypes.Int32}, 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &exec.ArraySpan{
				Type:     tt.fields.Type,
				Len:      tt.fields.Len,
				Nulls:    tt.fields.Nulls,
				Offset:   tt.fields.Offset,
				Buffers:  tt.fields.Buffers,
				Scratch:  tt.fields.Scratch,
				Children: tt.fields.Children,
			}
			if got := a.NumBuffers(); got != tt.want {
				t.Errorf("ArraySpan.NumBuffers() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestArraySpan_MakeData(t *testing.T) {
	type fields struct {
		Type     arrow.DataType
		Len      int64
		Nulls    int64
		Offset   int64
		Buffers  [3]exec.BufferSpan
		Scratch  [2]uint64
		Children []exec.ArraySpan
	}

	var (
		buf1 *memory.Buffer
	)
	arrow.RegisterExtensionType(types.NewDictExtensionType())
	defer arrow.UnregisterExtensionType("dict-extension")

	tests := []struct {
		name   string
		fields func(mem memory.Allocator) fields
		want   func(mem memory.Allocator) arrow.ArrayData
	}{
		{"null type", func(mem memory.Allocator) fields {
			return fields{
				Type:  arrow.Null,
				Len:   5,
				Nulls: array.UnknownNullCount,
			}
		}, func(mem memory.Allocator) arrow.ArrayData {
			return array.NewData(arrow.Null, 5, []*memory.Buffer{nil}, nil, 5, 0)
		}},
		{"zero len", func(mem memory.Allocator) fields {
			return fields{Type: arrow.PrimitiveTypes.Int32}
		}, func(mem memory.Allocator) arrow.ArrayData {
			return array.NewData(arrow.PrimitiveTypes.Int32, 0, []*memory.Buffer{nil, nil}, nil, 0, 0)
		}},
		{"non-owning offset", func(mem memory.Allocator) fields {
			ret := fields{
				Type:   arrow.PrimitiveTypes.Int8,
				Len:    4,
				Nulls:  1,
				Offset: 1,
			}
			buf1 = memory.NewResizableBuffer(mem)
			buf1.Resize(1)
			buf1.Bytes()[0] = 109
			ret.Buffers[0].SetBuffer(buf1)
			ret.Buffers[1].SetBuffer(memory.NewBufferBytes([]byte{5, 5, 5, 5, 5}))
			return ret
		}, func(mem memory.Allocator) arrow.ArrayData {
			// created in the above func, we release after constructing
			// the NewData so the refcount is as expected
			defer buf1.Release()
			return array.NewData(arrow.PrimitiveTypes.Int8, 4,
				[]*memory.Buffer{buf1, memory.NewBufferBytes([]byte{5, 5, 5, 5, 5})}, nil, 1, 1)
		}},
		{"self-alloc", func(mem memory.Allocator) fields {
			ret := fields{
				Type: arrow.PrimitiveTypes.Int8,
				Len:  4,
			}
			buf := memory.NewResizableBuffer(mem)
			buf.Resize(1)
			ret.Buffers[0].WrapBuffer(buf)
			buf2 := memory.NewResizableBuffer(mem)
			buf2.Resize(4)
			ret.Buffers[1].WrapBuffer(buf2)
			return ret
		}, func(mem memory.Allocator) arrow.ArrayData {
			buf := memory.NewResizableBuffer(mem)
			buf.Resize(1)
			defer buf.Release()
			buf2 := memory.NewResizableBuffer(mem)
			buf2.Resize(4)
			defer buf2.Release()
			return array.NewData(arrow.PrimitiveTypes.Int8, 4, []*memory.Buffer{buf, buf2}, nil, 0, 0)
		}},
		{"with children", func(mem memory.Allocator) fields {
			ret := fields{
				Type: arrow.ListOf(arrow.PrimitiveTypes.Int8),
				Len:  1,
				Children: []exec.ArraySpan{{
					Type: arrow.PrimitiveTypes.Int8,
					Len:  4,
				}},
			}
			var offsets [8]byte
			endian.Native.PutUint32(offsets[4:], 4)
			ret.Buffers[1].SetBuffer(memory.NewBufferBytes(offsets[:]))
			buf := memory.NewResizableBuffer(mem)
			buf.Resize(4)
			buf.Bytes()[0] = 1
			buf.Bytes()[1] = 2
			buf.Bytes()[2] = 3
			buf.Bytes()[3] = 4

			ret.Children[0].Buffers[1].WrapBuffer(buf)
			return ret
		}, func(mem memory.Allocator) arrow.ArrayData {
			buf := memory.NewResizableBuffer(mem)
			buf.Resize(4)
			buf.Bytes()[0] = 1
			buf.Bytes()[1] = 2
			buf.Bytes()[2] = 3
			buf.Bytes()[3] = 4
			defer buf.Release()
			child := array.NewData(arrow.PrimitiveTypes.Int8, 4, []*memory.Buffer{nil, buf}, nil, 0, 0)
			defer child.Release()

			var offsets [8]byte
			endian.Native.PutUint32(offsets[4:], 4)

			return array.NewData(arrow.ListOf(arrow.PrimitiveTypes.Int8), 1,
				[]*memory.Buffer{nil, memory.NewBufferBytes(offsets[:])},
				[]arrow.ArrayData{child}, 0, 0)
		}},
		{"dict-extension-type", func(mem memory.Allocator) fields {
			// dict-extension-type is dict(Index: int8, Value: string)
			// so there should be an int8 in the arrayspan and
			// a child of a string arrayspan in the first index of
			// Children
			ret := fields{
				Type: types.NewDictExtensionType(),
				Len:  1,
				Children: []exec.ArraySpan{{
					Type: arrow.BinaryTypes.String,
					Len:  2,
				}},
			}

			indices := memory.NewResizableBuffer(mem)
			indices.Resize(1)
			indices.Bytes()[0] = 1
			ret.Buffers[1].WrapBuffer(indices)

			offsets := memory.NewResizableBuffer(mem)
			offsets.Resize(3 * arrow.Int32SizeBytes)
			copy(offsets.Bytes(), arrow.Int32Traits.CastToBytes([]int32{0, 5, 10}))

			values := memory.NewResizableBuffer(mem)
			values.Resize(len("HelloWorld"))
			copy(values.Bytes(), []byte("HelloWorld"))

			nulls := memory.NewResizableBuffer(mem)
			nulls.Resize(1)
			nulls.Bytes()[0] = 3
			ret.Children[0].Buffers[0].WrapBuffer(nulls)
			ret.Children[0].Buffers[1].WrapBuffer(offsets)
			ret.Children[0].Buffers[2].WrapBuffer(values)

			return ret
		}, func(mem memory.Allocator) arrow.ArrayData {
			dict, _, _ := array.FromJSON(mem, arrow.BinaryTypes.String, strings.NewReader(`["Hello", "World"]`))
			defer dict.Release()
			index, _, _ := array.FromJSON(mem, arrow.PrimitiveTypes.Int8, strings.NewReader(`[1]`))
			defer index.Release()

			out := array.NewData(types.NewDictExtensionType(), 1, []*memory.Buffer{nil, index.Data().Buffers()[1]}, nil, 0, 0)
			out.SetDictionary(dict.Data())
			return out
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)

			t.Run("MakeData", func(t *testing.T) {
				f := tt.fields(mem)
				a := &exec.ArraySpan{
					Type:     f.Type,
					Len:      f.Len,
					Nulls:    f.Nulls,
					Offset:   f.Offset,
					Buffers:  f.Buffers,
					Scratch:  f.Scratch,
					Children: f.Children,
				}
				got := a.MakeData()
				want := tt.want(mem)
				if !reflect.DeepEqual(got, want) {
					t.Errorf("ArraySpan.MakeData() = %v, want %v", got, want)
				}
				want.Release()
				got.Release()
			})

			t.Run("MakeArray", func(t *testing.T) {
				f := tt.fields(mem)
				a := &exec.ArraySpan{
					Type:     f.Type,
					Len:      f.Len,
					Nulls:    f.Nulls,
					Offset:   f.Offset,
					Buffers:  f.Buffers,
					Scratch:  f.Scratch,
					Children: f.Children,
				}
				arr := a.MakeArray()
				want := tt.want(mem)
				defer want.Release()
				exp := array.MakeFromData(want)

				assert.Truef(t, array.Equal(arr, exp), "expected: %s\ngot: %s", exp, arr)

				exp.Release()
				arr.Release()
			})
		})
	}
}

func TestArraySpan_SetSlice(t *testing.T) {
	type fields struct {
		Type     arrow.DataType
		Len      int64
		Nulls    int64
		Offset   int64
		Buffers  [3]exec.BufferSpan
		Scratch  [2]uint64
		Children []exec.ArraySpan
	}
	type args struct {
		off    int64
		length int64
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		wantNulls int64
	}{
		{"null type", fields{Type: arrow.Null}, args{5, 10}, 10},
		{"not-null type", fields{Type: arrow.PrimitiveTypes.Int8}, args{5, 10}, 0},
		{"not-null type with nulls", fields{Type: arrow.PrimitiveTypes.Int8, Nulls: -1}, args{5, 10}, array.UnknownNullCount},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &exec.ArraySpan{
				Type:     tt.fields.Type,
				Len:      tt.fields.Len,
				Nulls:    tt.fields.Nulls,
				Offset:   tt.fields.Offset,
				Buffers:  tt.fields.Buffers,
				Scratch:  tt.fields.Scratch,
				Children: tt.fields.Children,
			}
			a.SetSlice(tt.args.off, tt.args.length)
			assert.Equal(t, tt.args.off, a.Offset)
			assert.Equal(t, tt.args.length, a.Len)
			assert.Equal(t, tt.wantNulls, a.Nulls)
		})
	}
}

func TestArraySpan_FillFromScalar(t *testing.T) {
	var (
		expDecimalBuf [arrow.Decimal128SizeBytes]byte
		expScratch    [2]uint64
	)

	endian.Native.PutUint64(expDecimalBuf[:], 1234)
	endian.Native.PutUint32(arrow.Uint64Traits.CastToBytes(expScratch[:])[4:], 10)

	dict, _, _ := array.FromJSON(memory.DefaultAllocator, arrow.BinaryTypes.String, strings.NewReader(`["Hello", "World"]`))
	defer dict.Release()

	tests := []struct {
		name string
		args scalar.Scalar
		exp  exec.ArraySpan
	}{
		{"null-type",
			scalar.MakeNullScalar(arrow.Null),
			exec.ArraySpan{Type: arrow.Null, Len: 1, Nulls: 1}},
		{"bool valid",
			scalar.MakeScalar(true),
			exec.ArraySpan{
				Type:    arrow.FixedWidthTypes.Boolean,
				Len:     1,
				Nulls:   0,
				Buffers: [3]exec.BufferSpan{{Buf: []byte{0x01}}, {Buf: []byte{0x01}}, {}},
			}},
		{"bool valid false",
			scalar.MakeScalar(false),
			exec.ArraySpan{
				Type:    arrow.FixedWidthTypes.Boolean,
				Len:     1,
				Nulls:   0,
				Buffers: [3]exec.BufferSpan{{Buf: []byte{0x01}}, {Buf: []byte{0x00}}, {}},
			}},
		{"primitive null",
			scalar.MakeNullScalar(arrow.PrimitiveTypes.Int32),
			exec.ArraySpan{
				Type:    arrow.PrimitiveTypes.Int32,
				Len:     1,
				Nulls:   1,
				Buffers: [3]exec.BufferSpan{{Buf: []byte{0x00}}, {Buf: []byte{0, 0, 0, 0}}, {}},
			}},
		{"decimal valid",
			scalar.NewDecimal128Scalar(decimal128.FromU64(1234), &arrow.Decimal128Type{Precision: 12, Scale: 2}),
			exec.ArraySpan{
				Type:    &arrow.Decimal128Type{Precision: 12, Scale: 2},
				Len:     1,
				Nulls:   0,
				Buffers: [3]exec.BufferSpan{{Buf: []byte{0x01}}, {Buf: expDecimalBuf[:]}, {}},
			}},
		{"dictionary scalar",
			scalar.NewDictScalar(scalar.NewInt8Scalar(1), dict),
			exec.ArraySpan{
				Type:  &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.BinaryTypes.String},
				Len:   1,
				Nulls: 0,
				Buffers: [3]exec.BufferSpan{{Buf: []byte{0x01}},
					{Buf: []byte{1}}, {},
				},
				Children: []exec.ArraySpan{{
					Type: arrow.BinaryTypes.String,
					Len:  2,
					Buffers: [3]exec.BufferSpan{
						{Buf: dict.NullBitmapBytes(), Owner: dict.Data().Buffers()[0]},
						{Buf: dict.Data().Buffers()[1].Bytes(), Owner: dict.Data().Buffers()[1]},
						{Buf: dict.Data().Buffers()[2].Bytes(), Owner: dict.Data().Buffers()[2]},
					},
				}},
			},
		},
		{"binary scalar",
			scalar.NewBinaryScalar(dict.Data().Buffers()[2], arrow.BinaryTypes.String),
			exec.ArraySpan{
				Type:    arrow.BinaryTypes.String,
				Len:     1,
				Nulls:   0,
				Scratch: expScratch,
				Buffers: [3]exec.BufferSpan{
					{Buf: []byte{0x01}},
					{Buf: arrow.Uint64Traits.CastToBytes(expScratch[:1])},
					{Buf: dict.Data().Buffers()[2].Bytes(), Owner: dict.Data().Buffers()[2]}},
			},
		},
		{"large binary",
			scalar.NewLargeStringScalarFromBuffer(dict.Data().Buffers()[2]),
			exec.ArraySpan{
				Type:    arrow.BinaryTypes.LargeString,
				Len:     1,
				Nulls:   0,
				Scratch: [2]uint64{0, 10},
				Buffers: [3]exec.BufferSpan{
					{Buf: []byte{0x01}},
					{Buf: arrow.Uint64Traits.CastToBytes([]uint64{0, 10})},
					{Buf: dict.Data().Buffers()[2].Bytes(), Owner: dict.Data().Buffers()[2]}},
			}},
		{"fixed size binary",
			scalar.NewFixedSizeBinaryScalar(dict.Data().Buffers()[2], &arrow.FixedSizeBinaryType{ByteWidth: 10}),
			exec.ArraySpan{
				Type: &arrow.FixedSizeBinaryType{ByteWidth: 10},
				Len:  1,
				Buffers: [3]exec.BufferSpan{
					{Buf: []byte{0x01}},
					{Buf: dict.Data().Buffers()[2].Bytes(), Owner: dict.Data().Buffers()[2]}, {},
				},
			}},
		{"map scalar null value",
			scalar.MakeNullScalar(arrow.MapOf(arrow.PrimitiveTypes.Int8, arrow.BinaryTypes.String)),
			exec.ArraySpan{
				Type:  arrow.MapOf(arrow.PrimitiveTypes.Int8, arrow.BinaryTypes.String),
				Len:   1,
				Nulls: 1,
				Buffers: [3]exec.BufferSpan{
					{Buf: []byte{0}},
					{Buf: []byte{0, 0, 0, 0, 0, 0, 0, 0}},
					{},
				},
				Children: []exec.ArraySpan{{
					Type: arrow.StructOf(arrow.Field{Name: "key", Type: arrow.PrimitiveTypes.Int8},
						arrow.Field{Name: "value", Type: arrow.BinaryTypes.String, Nullable: true}),
					Len:   0,
					Nulls: 0,
					Buffers: [3]exec.BufferSpan{
						{Buf: []byte{}}, {}, {},
					},
					Children: []exec.ArraySpan{
						{
							Type: arrow.PrimitiveTypes.Int8,
							Buffers: [3]exec.BufferSpan{
								{Buf: []byte{}}, {Buf: []byte{}}, {},
							},
						},
						{
							Type: arrow.BinaryTypes.String,
							Buffers: [3]exec.BufferSpan{
								{Buf: []byte{}}, {Buf: []byte{}}, {Buf: []byte{}},
							},
						},
					},
				}},
			}},
		{"list scalar",
			scalar.NewListScalarData(dict.Data()),
			exec.ArraySpan{
				Type: arrow.ListOf(arrow.BinaryTypes.String),
				Len:  1,
				Scratch: [2]uint64{
					*(*uint64)(unsafe.Pointer(&[]int32{0, 2}[0])),
					0,
				},
				Buffers: [3]exec.BufferSpan{
					{Buf: []byte{0x1}},
					{Buf: arrow.Int32Traits.CastToBytes([]int32{0, 2})},
				},
				Children: []exec.ArraySpan{{
					Type: arrow.BinaryTypes.String,
					Len:  2,
					Buffers: [3]exec.BufferSpan{
						{Buf: dict.NullBitmapBytes(), Owner: dict.Data().Buffers()[0]},
						{Buf: dict.Data().Buffers()[1].Bytes(), Owner: dict.Data().Buffers()[1]},
						{Buf: dict.Data().Buffers()[2].Bytes(), Owner: dict.Data().Buffers()[2]},
					},
				}},
			},
		},
		{"large list scalar",
			scalar.NewLargeListScalarData(dict.Data()),
			exec.ArraySpan{
				Type:    arrow.LargeListOf(arrow.BinaryTypes.String),
				Len:     1,
				Scratch: [2]uint64{0, 2},
				Buffers: [3]exec.BufferSpan{
					{Buf: []byte{0x1}},
					{Buf: arrow.Int64Traits.CastToBytes([]int64{0, 2})},
				},
				Children: []exec.ArraySpan{{
					Type: arrow.BinaryTypes.String,
					Len:  2,
					Buffers: [3]exec.BufferSpan{
						{Buf: dict.NullBitmapBytes(), Owner: dict.Data().Buffers()[0]},
						{Buf: dict.Data().Buffers()[1].Bytes(), Owner: dict.Data().Buffers()[1]},
						{Buf: dict.Data().Buffers()[2].Bytes(), Owner: dict.Data().Buffers()[2]},
					},
				}},
			},
		},
		{"fixed size list",
			scalar.NewFixedSizeListScalar(dict),
			exec.ArraySpan{
				Type: arrow.FixedSizeListOf(2, arrow.BinaryTypes.String),
				Len:  1,
				Buffers: [3]exec.BufferSpan{
					{Buf: []byte{0x1}},
					{}, {},
				},
				Children: []exec.ArraySpan{{
					Type: arrow.BinaryTypes.String,
					Len:  2,
					Buffers: [3]exec.BufferSpan{
						{Buf: dict.NullBitmapBytes(), Owner: dict.Data().Buffers()[0]},
						{Buf: dict.Data().Buffers()[1].Bytes(), Owner: dict.Data().Buffers()[1]},
						{Buf: dict.Data().Buffers()[2].Bytes(), Owner: dict.Data().Buffers()[2]},
					},
				}},
			},
		},
		{"struct scalar",
			func() scalar.Scalar {
				s, _ := scalar.NewStructScalarWithNames([]scalar.Scalar{
					scalar.MakeScalar(int32(5)), scalar.MakeScalar(uint8(10)),
				}, []string{"int32", "uint8"})
				return s
			}(),
			exec.ArraySpan{
				Type: arrow.StructOf(
					arrow.Field{Name: "int32", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
					arrow.Field{Name: "uint8", Type: arrow.PrimitiveTypes.Uint8, Nullable: true}),
				Buffers: [3]exec.BufferSpan{
					{Buf: []byte{0x1}}, {}, {},
				},
				Len: 1,
				Children: []exec.ArraySpan{
					{
						Type: arrow.PrimitiveTypes.Int32,
						Len:  1,
						Buffers: [3]exec.BufferSpan{
							{Buf: []byte{0x1}},
							{Buf: arrow.Int32Traits.CastToBytes([]int32{5})},
							{},
						},
					},
					{
						Type: arrow.PrimitiveTypes.Uint8,
						Len:  1,
						Buffers: [3]exec.BufferSpan{
							{Buf: []byte{0x1}},
							{Buf: []byte{10}},
							{},
						},
					},
				},
			},
		},
		{"dense union scalar",
			func() scalar.Scalar {
				dt := arrow.UnionOf(arrow.DenseMode, []arrow.Field{
					{Name: "string", Type: arrow.BinaryTypes.String, Nullable: true},
					{Name: "number", Type: arrow.PrimitiveTypes.Uint64, Nullable: true},
					{Name: "other_number", Type: arrow.PrimitiveTypes.Uint64, Nullable: true},
				}, []arrow.UnionTypeCode{3, 42, 43})
				return scalar.NewDenseUnionScalar(scalar.MakeScalar(uint64(25)), 42, dt.(*arrow.DenseUnionType))
			}(),
			exec.ArraySpan{
				Type: arrow.UnionOf(arrow.DenseMode, []arrow.Field{
					{Name: "string", Type: arrow.BinaryTypes.String, Nullable: true},
					{Name: "number", Type: arrow.PrimitiveTypes.Uint64, Nullable: true},
					{Name: "other_number", Type: arrow.PrimitiveTypes.Uint64, Nullable: true},
				}, []arrow.UnionTypeCode{3, 42, 43}),
				Len:     1,
				Scratch: [2]uint64{42, 1},
				Buffers: [3]exec.BufferSpan{{},
					{Buf: []byte{42}}, {Buf: arrow.Int32Traits.CastToBytes([]int32{0, 1})},
				},
				Children: []exec.ArraySpan{
					{
						Type: arrow.BinaryTypes.String,
						Buffers: [3]exec.BufferSpan{
							{Buf: []byte{}}, {Buf: []byte{}}, {Buf: []byte{}},
						},
					},
					{
						Type: arrow.PrimitiveTypes.Uint64,
						Len:  1,
						Buffers: [3]exec.BufferSpan{
							{Buf: []byte{0x1}},
							{Buf: arrow.Uint64Traits.CastToBytes([]uint64{25})},
							{},
						},
					},
					{
						Type: arrow.PrimitiveTypes.Uint64,
						Buffers: [3]exec.BufferSpan{
							{Buf: []byte{}}, {Buf: []byte{}}, {},
						},
					},
				},
			},
		},
		{"sparse union",
			func() scalar.Scalar {
				dt := arrow.UnionOf(arrow.SparseMode, []arrow.Field{
					{Name: "string", Type: arrow.BinaryTypes.String, Nullable: true},
					{Name: "number", Type: arrow.PrimitiveTypes.Uint64, Nullable: true},
					{Name: "other_number", Type: arrow.PrimitiveTypes.Uint64, Nullable: true},
				}, []arrow.UnionTypeCode{3, 42, 43})
				return scalar.NewSparseUnionScalarFromValue(scalar.MakeScalar(uint64(25)), 1, dt.(*arrow.SparseUnionType))
			}(),
			exec.ArraySpan{
				Type: arrow.UnionOf(arrow.SparseMode, []arrow.Field{
					{Name: "string", Type: arrow.BinaryTypes.String, Nullable: true},
					{Name: "number", Type: arrow.PrimitiveTypes.Uint64, Nullable: true},
					{Name: "other_number", Type: arrow.PrimitiveTypes.Uint64, Nullable: true},
				}, []arrow.UnionTypeCode{3, 42, 43}),
				Len:     1,
				Scratch: [2]uint64{42, 0},
				Buffers: [3]exec.BufferSpan{{},
					{Buf: []byte{42}}, {},
				},
				Children: []exec.ArraySpan{
					{
						Type:  arrow.BinaryTypes.String,
						Len:   1,
						Nulls: 1,
						Buffers: [3]exec.BufferSpan{
							{Buf: []byte{0x0}},
							{Buf: []byte{0, 0, 0, 0, 0, 0, 0, 0}},
							{},
						},
					},
					{
						Type: arrow.PrimitiveTypes.Uint64,
						Len:  1,
						Buffers: [3]exec.BufferSpan{
							{Buf: []byte{0x1}},
							{Buf: arrow.Uint64Traits.CastToBytes([]uint64{25})},
							{},
						},
					},
					{
						Type:  arrow.PrimitiveTypes.Uint64,
						Len:   1,
						Nulls: 1,
						Buffers: [3]exec.BufferSpan{
							{Buf: []byte{0x0}}, {Buf: []byte{0, 0, 0, 0, 0, 0, 0, 0}}, {},
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &exec.ArraySpan{
				Nulls:   array.UnknownNullCount,
				Buffers: [3]exec.BufferSpan{{SelfAlloc: true, Owner: &memory.Buffer{}}, {SelfAlloc: true, Owner: &memory.Buffer{}}, {}},
			}
			a.FillFromScalar(tt.args)
			assert.Equal(t, tt.exp, *a)
		})
	}
}
