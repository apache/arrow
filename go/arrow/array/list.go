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

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/bitutil"
	"github.com/apache/arrow/go/v10/arrow/internal/debug"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"github.com/goccy/go-json"
)

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

func (a *List) getOneForMarshal(i int) interface{} {
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
		if err := enc.Encode(a.getOneForMarshal(i)); err != nil {
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

// LargeList represents an immutable sequence of array values.
type LargeList struct {
	array
	values  arrow.Array
	offsets []int64
}

// NewLargeListData returns a new LargeList array value, from data.
func NewLargeListData(data arrow.ArrayData) *LargeList {
	a := &LargeList{}
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

func (a *LargeList) getOneForMarshal(i int) interface{} {
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
		if err := enc.Encode(a.getOneForMarshal(i)); err != nil {
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

func (a *LargeList) Retain() {
	a.array.Retain()
	a.values.Retain()
}

func (a *LargeList) Release() {
	a.array.Release()
	a.values.Release()
}

type listBuilder struct {
	builder

	etype   arrow.DataType // data type of the list's elements.
	values  Builder        // value builder for the list's elements.
	offsets Builder

	dt              arrow.DataType
	appendOffsetVal func(int)
}

type ListBuilder struct {
	listBuilder
}

type LargeListBuilder struct {
	listBuilder
}

// NewListBuilder returns a builder, using the provided memory allocator.
// The created list builder will create a list whose elements will be of type etype.
func NewListBuilder(mem memory.Allocator, etype arrow.DataType) *ListBuilder {
	offsetBldr := NewInt32Builder(mem)
	return &ListBuilder{
		listBuilder{
			builder:         builder{refCount: 1, mem: mem},
			etype:           etype,
			values:          NewBuilder(mem, etype),
			offsets:         offsetBldr,
			dt:              arrow.ListOf(etype),
			appendOffsetVal: func(o int) { offsetBldr.Append(int32(o)) },
		},
	}
}

// NewLargeListBuilder returns a builder, using the provided memory allocator.
// The created list builder will create a list whose elements will be of type etype.
func NewLargeListBuilder(mem memory.Allocator, etype arrow.DataType) *LargeListBuilder {
	offsetBldr := NewInt64Builder(mem)
	return &LargeListBuilder{
		listBuilder{
			builder:         builder{refCount: 1, mem: mem},
			etype:           etype,
			values:          NewBuilder(mem, etype),
			offsets:         offsetBldr,
			dt:              arrow.LargeListOf(etype),
			appendOffsetVal: func(o int) { offsetBldr.Append(int64(o)) },
		},
	}
}

// Release decreases the reference count by 1.
// When the reference count goes to zero, the memory is freed.
func (b *listBuilder) Release() {
	debug.Assert(atomic.LoadInt64(&b.refCount) > 0, "too many releases")

	if atomic.AddInt64(&b.refCount, -1) == 0 {
		if b.nullBitmap != nil {
			b.nullBitmap.Release()
			b.nullBitmap = nil
		}
	}

	b.values.Release()
	b.offsets.Release()
}

func (b *listBuilder) appendNextOffset() {
	b.appendOffsetVal(b.values.Len())
	// b.offsets.Append(int32(b.values.Len()))
}

func (b *listBuilder) Append(v bool) {
	b.Reserve(1)
	b.unsafeAppendBoolToBitmap(v)
	b.appendNextOffset()
}

func (b *listBuilder) AppendNull() {
	b.Reserve(1)
	b.unsafeAppendBoolToBitmap(false)
	b.appendNextOffset()
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

func (b *listBuilder) unsafeAppendBoolToBitmap(isValid bool) {
	if isValid {
		bitutil.SetBit(b.nullBitmap.Bytes(), b.length)
	} else {
		b.nulls++
	}
	b.length++
}

func (b *listBuilder) init(capacity int) {
	b.builder.init(capacity)
	b.offsets.init(capacity + 1)
}

// Reserve ensures there is enough space for appending n elements
// by checking the capacity and calling Resize if necessary.
func (b *listBuilder) Reserve(n int) {
	b.builder.reserve(n, b.resizeHelper)
	b.offsets.Reserve(n)
}

// Resize adjusts the space allocated by b to n elements. If n is greater than b.Cap(),
// additional memory will be allocated. If n is smaller, the allocated memory may reduced.
func (b *listBuilder) Resize(n int) {
	b.resizeHelper(n)
	b.offsets.Resize(n)
}

func (b *listBuilder) resizeHelper(n int) {
	if n < minBuilderCapacity {
		n = minBuilderCapacity
	}

	if b.capacity == 0 {
		b.init(n)
	} else {
		b.builder.resize(n, b.builder.init)
	}
}

func (b *listBuilder) ValueBuilder() Builder {
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
	if b.offsets.Len() != b.length+1 {
		b.appendNextOffset()
	}
	data := b.newData()
	a = NewListData(data)
	data.Release()
	return
}

// NewListArray creates a List array from the memory buffers used by the builder and resets the ListBuilder
// so it can be used to build a new array.
func (b *LargeListBuilder) NewLargeListArray() (a *LargeList) {
	if b.offsets.Len() != b.length+1 {
		b.appendNextOffset()
	}
	data := b.newData()
	a = NewLargeListData(data)
	data.Release()
	return
}

func (b *listBuilder) newData() (data *Data) {
	values := b.values.NewArray()
	defer values.Release()

	var offsets *memory.Buffer
	if b.offsets != nil {
		arr := b.offsets.NewArray()
		defer arr.Release()
		offsets = arr.Data().Buffers()[1]
	}

	data = NewData(
		b.dt, b.length,
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

func (b *listBuilder) unmarshalOne(dec *json.Decoder) error {
	t, err := dec.Token()
	if err != nil {
		return err
	}

	switch t {
	case json.Delim('['):
		b.Append(true)
		if err := b.values.unmarshal(dec); err != nil {
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
			Struct: arrow.ListOf(b.etype).String(),
		}
	}

	return nil
}

func (b *listBuilder) unmarshal(dec *json.Decoder) error {
	for dec.More() {
		if err := b.unmarshalOne(dec); err != nil {
			return err
		}
	}
	return nil
}

func (b *listBuilder) UnmarshalJSON(data []byte) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	t, err := dec.Token()
	if err != nil {
		return err
	}

	if delim, ok := t.(json.Delim); !ok || delim != '[' {
		return fmt.Errorf("list builder must unpack from json array, found %s", delim)
	}

	return b.unmarshal(dec)
}

var (
	_ arrow.Array = (*List)(nil)
	_ arrow.Array = (*LargeList)(nil)
	_ Builder     = (*ListBuilder)(nil)
	_ Builder     = (*LargeListBuilder)(nil)
)
