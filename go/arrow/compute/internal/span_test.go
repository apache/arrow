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

package internal_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/compute/internal"
	"github.com/apache/arrow/go/v10/arrow/endian"
	"github.com/apache/arrow/go/v10/arrow/internal/testing/types"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"github.com/apache/arrow/go/v10/arrow/scalar"
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
			b := &internal.BufferSpan{
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
			b := &internal.BufferSpan{
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
		Buffers  [3]internal.BufferSpan
		Scratch  [2]uint64
		Children []internal.ArraySpan
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
			Buffers: [3]internal.BufferSpan{{Buf: []byte{109}}, {}, {}}}, 3},
		{"unknown with offset", fields{
			Nulls:   array.UnknownNullCount,
			Len:     4,
			Offset:  2, // 0b01101101
			Buffers: [3]internal.BufferSpan{{Buf: []byte{109}}, {}, {}}}, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &internal.ArraySpan{
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
		Buffers  [3]internal.BufferSpan
		Scratch  [2]uint64
		Children []internal.ArraySpan
	}
	children := []internal.ArraySpan{{}}
	tests := []struct {
		name   string
		fields fields
		want   *internal.ArraySpan
	}{
		{"basic", fields{Children: children}, &children[0]},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &internal.ArraySpan{
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
		Buffers  [3]internal.BufferSpan
		Scratch  [2]uint64
		Children []internal.ArraySpan
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
			a := &internal.ArraySpan{
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
		Buffers  [3]internal.BufferSpan
		Scratch  [2]uint64
		Children []internal.ArraySpan
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
				Children: []internal.ArraySpan{{
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
				Children: []internal.ArraySpan{{
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
				a := &internal.ArraySpan{
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
				a := &internal.ArraySpan{
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
		Buffers  [3]internal.BufferSpan
		Scratch  [2]uint64
		Children []internal.ArraySpan
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
		{"not-null type", fields{Type: arrow.PrimitiveTypes.Int8}, args{5, 10}, array.UnknownNullCount},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &internal.ArraySpan{
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

func TestArraySpan_GetBuffer(t *testing.T) {
	type fields struct {
		Type     arrow.DataType
		Len      int64
		Nulls    int64
		Offset   int64
		Buffers  [3]internal.BufferSpan
		Scratch  [2]uint64
		Children []internal.ArraySpan
	}
	type args struct {
		idx int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *memory.Buffer
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &internal.ArraySpan{
				Type:     tt.fields.Type,
				Len:      tt.fields.Len,
				Nulls:    tt.fields.Nulls,
				Offset:   tt.fields.Offset,
				Buffers:  tt.fields.Buffers,
				Scratch:  tt.fields.Scratch,
				Children: tt.fields.Children,
			}
			if got := a.GetBuffer(tt.args.idx); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ArraySpan.GetBuffer() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestArraySpan_FillFromScalar(t *testing.T) {
	type fields struct {
		Type     arrow.DataType
		Len      int64
		Nulls    int64
		Offset   int64
		Buffers  [3]internal.BufferSpan
		Scratch  [2]uint64
		Children []internal.ArraySpan
	}
	type args struct {
		val scalar.Scalar
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &internal.ArraySpan{
				Type:     tt.fields.Type,
				Len:      tt.fields.Len,
				Nulls:    tt.fields.Nulls,
				Offset:   tt.fields.Offset,
				Buffers:  tt.fields.Buffers,
				Scratch:  tt.fields.Scratch,
				Children: tt.fields.Children,
			}
			a.FillFromScalar(tt.args.val)
		})
	}
}

func TestArraySpan_SetMembers(t *testing.T) {
	type fields struct {
		Type     arrow.DataType
		Len      int64
		Nulls    int64
		Offset   int64
		Buffers  [3]internal.BufferSpan
		Scratch  [2]uint64
		Children []internal.ArraySpan
	}
	type args struct {
		data arrow.ArrayData
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &internal.ArraySpan{
				Type:     tt.fields.Type,
				Len:      tt.fields.Len,
				Nulls:    tt.fields.Nulls,
				Offset:   tt.fields.Offset,
				Buffers:  tt.fields.Buffers,
				Scratch:  tt.fields.Scratch,
				Children: tt.fields.Children,
			}
			a.SetMembers(tt.args.data)
		})
	}
}

func TestExecValue_IsArray(t *testing.T) {
	type fields struct {
		Array  internal.ArraySpan
		Scalar scalar.Scalar
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &internal.ExecValue{
				Array:  tt.fields.Array,
				Scalar: tt.fields.Scalar,
			}
			if got := e.IsArray(); got != tt.want {
				t.Errorf("ExecValue.IsArray() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExecValue_IsScalar(t *testing.T) {
	type fields struct {
		Array  internal.ArraySpan
		Scalar scalar.Scalar
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &internal.ExecValue{
				Array:  tt.fields.Array,
				Scalar: tt.fields.Scalar,
			}
			if got := e.IsScalar(); got != tt.want {
				t.Errorf("ExecValue.IsScalar() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExecValue_Type(t *testing.T) {
	type fields struct {
		Array  internal.ArraySpan
		Scalar scalar.Scalar
	}
	tests := []struct {
		name   string
		fields fields
		want   arrow.DataType
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &internal.ExecValue{
				Array:  tt.fields.Array,
				Scalar: tt.fields.Scalar,
			}
			if got := e.Type(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ExecValue.Type() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPromoteExecSpanScalars(t *testing.T) {
	type args struct {
		span internal.ExecSpan
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			internal.PromoteExecSpanScalars(tt.args.span)
		})
	}
}
