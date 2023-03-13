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
	"bytes"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/bitutil"
	"github.com/apache/arrow/go/v12/arrow/internal/debug"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/goccy/go-json"
)

type ListLike interface {
	arrow.Array
	ListValues() arrow.Array
	ValueOffsets(i int) (start, end int64)
}

// List represents an immutable sequence of array values.
type List struct {
	array
	values  arrow.Array
	offsets []int32
}

// NewListData returns a new List array value, from data.
func NewListData(data arrow.ArrayData) *List {
	a := &List{}
	a.refCount = 1
	a.setData(data.(*Data))
	return a
}

func (a *List) ListValues() arrow.Array { return a.values }

func (a *List) String() string {
	o := new(strings.Builder)
	o.WriteString("[")
	for i := 0; i < a.Len(); i++ {
		if i > 0 {
			o.WriteString(" ")
		}
		if !a.IsValid(i) {
			o.WriteString("(null)")
			continue
		}
		sub := a.newListValue(i)
		fmt.Fprintf(o, "%v", sub)
		sub.Release()
	}
	o.WriteString("]")
	return o.String()
}

func (a *List) newListValue(i int) arrow.Array {
	j := i + a.array.data.offset
	beg := int64(a.offsets[j])
	end := int64(a.offsets[j+1])
	return NewSlice(a.values, beg, end)
}

func (a *List) setData(data *Data) {
	a.array.setData(data)
	vals := data.buffers[1]
	if vals != nil {
		a.offsets = arrow.Int32Traits.CastFromBytes(vals.Bytes())
	}
	a.values = MakeFromData(data.childData[0])
}

func (a *List) GetOneForMarshal(i int) interface{} {
	if a.IsNull(i) {
		return nil
	}

	slice := a.newListValue(i)
	defer slice.Release()
	v, err := json.Marshal(slice)
	if err != nil {
		panic(err)
	}
	return json.RawMessage(v)
}

func (a *List) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)

	buf.WriteByte('[')
	for i := 0; i < a.Len(); i++ {
		if i != 0 {
			buf.WriteByte(',')
		}
		if err := enc.Encode(a.GetOneForMarshal(i)); err != nil {
			return nil, err
		}
	}
	buf.WriteByte(']')
	return buf.Bytes(), nil
}

func arrayEqualList(left, right *List) bool {
	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) {
			continue
		}
		o := func() bool {
			l := left.newListValue(i)
			defer l.Release()
			r := right.newListValue(i)
			defer r.Release()
			return Equal(l, r)
		}()
		if !o {
			return false
		}
	}
	return true
}

// Len returns the number of elements in the array.
func (a *List) Len() int { return a.array.Len() }

func (a *List) Offsets() []int32 { return a.offsets }

func (a *List) Retain() {
	a.array.Retain()
	a.values.Retain()
}

func (a *List) Release() {
	a.array.Release()
	a.values.Release()
}

func (a *List) ValueOffsets(i int) (start, end int64) {
	debug.Assert(i >= 0 && i < a.array.data.length, "index out of range")
	start, end = int64(a.offsets[i+a.data.offset]), int64(a.offsets[i+a.data.offset+1])
	return
}

// LargeList represents an immutable sequence of array values.
type LargeList struct {
	array
	values  arrow.Array
	offsets []int64
}

// NewLargeListData returns a new LargeList array value, from data.
func NewLargeListData(data arrow.ArrayData) *LargeList {
	a := new(LargeList)
	a.refCount = 1
	a.setData(data.(*Data))
	return a
}

func (a *LargeList) ListValues() arrow.Array { return a.values }

func (a *LargeList) String() string {
	o := new(strings.Builder)
	o.WriteString("[")
	for i := 0; i < a.Len(); i++ {
		if i > 0 {
			o.WriteString(" ")
		}
		if !a.IsValid(i) {
			o.WriteString("(null)")
			continue
		}
		sub := a.newListValue(i)
		fmt.Fprintf(o, "%v", sub)
		sub.Release()
	}
	o.WriteString("]")
	return o.String()
}

func (a *LargeList) newListValue(i int) arrow.Array {
	j := i + a.array.data.offset
	beg := int64(a.offsets[j])
	end := int64(a.offsets[j+1])
	return NewSlice(a.values, beg, end)
}

func (a *LargeList) setData(data *Data) {
	a.array.setData(data)
	vals := data.buffers[1]
	if vals != nil {
		a.offsets = arrow.Int64Traits.CastFromBytes(vals.Bytes())
	}
	a.values = MakeFromData(data.childData[0])
}

func (a *LargeList) GetOneForMarshal(i int) interface{} {
	if a.IsNull(i) {
		return nil
	}

	slice := a.newListValue(i)
	defer slice.Release()
	v, err := json.Marshal(slice)
	if err != nil {
		panic(err)
	}
	return json.RawMessage(v)
}

func (a *LargeList) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)

	buf.WriteByte('[')
	for i := 0; i < a.Len(); i++ {
		if i != 0 {
			buf.WriteByte(',')
		}
		if err := enc.Encode(a.GetOneForMarshal(i)); err != nil {
			return nil, err
		}
	}
	buf.WriteByte(']')
	return buf.Bytes(), nil
}

func arrayEqualLargeList(left, right *LargeList) bool {
	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) {
			continue
		}
		o := func() bool {
			l := left.newListValue(i)
			defer l.Release()
			r := right.newListValue(i)
			defer r.Release()
			return Equal(l, r)
		}()
		if !o {
			return false
		}
	}
	return true
}

// Len returns the number of elements in the array.
func (a *LargeList) Len() int { return a.array.Len() }

func (a *LargeList) Offsets() []int64 { return a.offsets }

func (a *LargeList) ValueOffsets(i int) (start, end int64) {
	debug.Assert(i >= 0 && i < a.array.data.length, "index out of range")
	start, end = a.offsets[i], a.offsets[i+1]
	return
}

func (a *LargeList) Retain() {
	a.array.Retain()
	a.values.Retain()
}

func (a *LargeList) Release() {
	a.array.Release()
	a.values.Release()
}

type baseListBuilder struct {
	builder

	values  Builder // value builder for the list's elements.
	offsets Builder

	// actual list type
	dt              arrow.DataType
	appendOffsetVal func(int)
}

type ListLikeBuilder interface {
	Builder
	ValueBuilder() Builder
	Append(bool)
}

type ListBuilder struct {
	baseListBuilder
}

type LargeListBuilder struct {
	baseListBuilder
}

// NewListBuilder returns a builder, using the provided memory allocator.
// The created list builder will create a list whose elements will be of type etype.
func NewListBuilder(mem memory.Allocator, etype arrow.DataType) *ListBuilder {
	offsetBldr := NewInt32Builder(mem)
	return &ListBuilder{
		baseListBuilder{
			builder:         builder{refCount: 1, mem: mem},
			values:          NewBuilder(mem, etype),
			offsets:         offsetBldr,
			dt:              arrow.ListOf(etype),
			appendOffsetVal: func(o int) { offsetBldr.Append(int32(o)) },
		},
	}
}

// NewListBuilderWithField takes a field to use for the child rather than just
// a datatype to allow for more customization.
func NewListBuilderWithField(mem memory.Allocator, field arrow.Field) *ListBuilder {
	offsetBldr := NewInt32Builder(mem)
	return &ListBuilder{
		baseListBuilder{
			builder:         builder{refCount: 1, mem: mem},
			values:          NewBuilder(mem, field.Type),
			offsets:         offsetBldr,
			dt:              arrow.ListOfField(field),
			appendOffsetVal: func(o int) { offsetBldr.Append(int32(o)) },
		},
	}
}

func (b *baseListBuilder) Type() arrow.DataType {
	switch dt := b.dt.(type) {
	case *arrow.ListType:
		f := dt.ElemField()
		f.Type = b.values.Type()
		return arrow.ListOfField(f)
	case *arrow.LargeListType:
		f := dt.ElemField()
		f.Type = b.values.Type()
		return arrow.LargeListOfField(f)
	}
	return nil
}

// NewLargeListBuilder returns a builder, using the provided memory allocator.
// The created list builder will create a list whose elements will be of type etype.
func NewLargeListBuilder(mem memory.Allocator, etype arrow.DataType) *LargeListBuilder {
	offsetBldr := NewInt64Builder(mem)
	return &LargeListBuilder{
		baseListBuilder{
			builder:         builder{refCount: 1, mem: mem},
			values:          NewBuilder(mem, etype),
			offsets:         offsetBldr,
			dt:              arrow.LargeListOf(etype),
			appendOffsetVal: func(o int) { offsetBldr.Append(int64(o)) },
		},
	}
}

// NewLargeListBuilderWithField takes a field rather than just an element type
// to allow for more customization of the final type of the LargeList Array
func NewLargeListBuilderWithField(mem memory.Allocator, field arrow.Field) *LargeListBuilder {
	offsetBldr := NewInt64Builder(mem)
	return &LargeListBuilder{
		baseListBuilder{
			builder:         builder{refCount: 1, mem: mem},
			values:          NewBuilder(mem, field.Type),
			offsets:         offsetBldr,
			dt:              arrow.LargeListOfField(field),
			appendOffsetVal: func(o int) { offsetBldr.Append(int64(o)) },
		},
	}
}

// Release decreases the reference count by 1.
// When the reference count goes to zero, the memory is freed.
func (b *baseListBuilder) Release() {
	debug.Assert(atomic.LoadInt64(&b.refCount) > 0, "too many releases")

	if atomic.AddInt64(&b.refCount, -1) == 0 {
		if b.nullBitmap != nil {
			b.nullBitmap.Release()
			b.nullBitmap = nil
		}
		b.values.Release()
		b.offsets.Release()
	}

}

func (b *baseListBuilder) appendNextOffset() {
	b.appendOffsetVal(b.values.Len())
}

func (b *baseListBuilder) Append(v bool) {
	b.Reserve(1)
	b.unsafeAppendBoolToBitmap(v)
	b.appendNextOffset()
}

func (b *baseListBuilder) AppendNull() {
	b.Reserve(1)
	b.unsafeAppendBoolToBitmap(false)
	b.appendNextOffset()
}

func (b *baseListBuilder) AppendEmptyValue() {
	b.Append(true)
}

func (b *ListBuilder) AppendValues(offsets []int32, valid []bool) {
	b.Reserve(len(valid))
	b.offsets.(*Int32Builder).AppendValues(offsets, nil)
	b.builder.unsafeAppendBoolsToBitmap(valid, len(valid))
}

func (b *LargeListBuilder) AppendValues(offsets []int64, valid []bool) {
	b.Reserve(len(valid))
	b.offsets.(*Int64Builder).AppendValues(offsets, nil)
	b.builder.unsafeAppendBoolsToBitmap(valid, len(valid))
}

func (b *baseListBuilder) unsafeAppendBoolToBitmap(isValid bool) {
	if isValid {
		bitutil.SetBit(b.nullBitmap.Bytes(), b.length)
	} else {
		b.nulls++
	}
	b.length++
}

func (b *baseListBuilder) init(capacity int) {
	b.builder.init(capacity)
	b.offsets.init(capacity + 1)
}

// Reserve ensures there is enough space for appending n elements
// by checking the capacity and calling Resize if necessary.
func (b *baseListBuilder) Reserve(n int) {
	b.builder.reserve(n, b.resizeHelper)
	b.offsets.Reserve(n)
}

// Resize adjusts the space allocated by b to n elements. If n is greater than b.Cap(),
// additional memory will be allocated. If n is smaller, the allocated memory may reduced.
func (b *baseListBuilder) Resize(n int) {
	b.resizeHelper(n)
	b.offsets.Resize(n)
}

func (b *baseListBuilder) resizeHelper(n int) {
	if n < minBuilderCapacity {
		n = minBuilderCapacity
	}

	if b.capacity == 0 {
		b.init(n)
	} else {
		b.builder.resize(n, b.builder.init)
	}
}

func (b *baseListBuilder) ValueBuilder() Builder {
	return b.values
}

// NewArray creates a List array from the memory buffers used by the builder and resets the ListBuilder
// so it can be used to build a new array.
func (b *ListBuilder) NewArray() arrow.Array {
	return b.NewListArray()
}

// NewArray creates a LargeList array from the memory buffers used by the builder and resets the LargeListBuilder
// so it can be used to build a new array.
func (b *LargeListBuilder) NewArray() arrow.Array {
	return b.NewLargeListArray()
}

// NewListArray creates a List array from the memory buffers used by the builder and resets the ListBuilder
// so it can be used to build a new array.
func (b *ListBuilder) NewListArray() (a *List) {
	data := b.newData()
	a = NewListData(data)
	data.Release()
	return
}

// NewLargeListArray creates a List array from the memory buffers used by the builder and resets the LargeListBuilder
// so it can be used to build a new array.
func (b *LargeListBuilder) NewLargeListArray() (a *LargeList) {
	data := b.newData()
	a = NewLargeListData(data)
	data.Release()
	return
}

func (b *baseListBuilder) newData() (data *Data) {
	if b.offsets.Len() != b.length+1 {
		b.appendNextOffset()
	}
	values := b.values.NewArray()
	defer values.Release()

	var offsets *memory.Buffer
	if b.offsets != nil {
		arr := b.offsets.NewArray()
		defer arr.Release()
		offsets = arr.Data().Buffers()[1]
	}

	data = NewData(
		b.Type(), b.length,
		[]*memory.Buffer{
			b.nullBitmap,
			offsets,
		},
		[]arrow.ArrayData{values.Data()},
		b.nulls,
		0,
	)
	b.reset()

	return
}

func (b *baseListBuilder) UnmarshalOne(dec *json.Decoder) error {
	t, err := dec.Token()
	if err != nil {
		return err
	}

	switch t {
	case json.Delim('['):
		b.Append(true)
		if err := b.values.Unmarshal(dec); err != nil {
			return err
		}
		// consume ']'
		_, err := dec.Token()
		return err
	case nil:
		b.AppendNull()
	default:
		return &json.UnmarshalTypeError{
			Value:  fmt.Sprint(t),
			Struct: b.dt.String(),
		}
	}

	return nil
}

func (b *baseListBuilder) Unmarshal(dec *json.Decoder) error {
	for dec.More() {
		if err := b.UnmarshalOne(dec); err != nil {
			return err
		}
	}
	return nil
}

func (b *baseListBuilder) UnmarshalJSON(data []byte) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	t, err := dec.Token()
	if err != nil {
		return err
	}

	if delim, ok := t.(json.Delim); !ok || delim != '[' {
		return fmt.Errorf("list builder must unpack from json array, found %s", delim)
	}

	return b.Unmarshal(dec)
}

var (
	_ arrow.Array = (*List)(nil)
	_ arrow.Array = (*LargeList)(nil)
	_ Builder     = (*ListBuilder)(nil)
	_ Builder     = (*LargeListBuilder)(nil)
)
