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
	"math"
	"sync/atomic"

	"github.com/apache/arrow/go/v11/arrow"
	"github.com/apache/arrow/go/v11/arrow/encoded"
	"github.com/apache/arrow/go/v11/arrow/internal/debug"
	"github.com/apache/arrow/go/v11/arrow/memory"
	"github.com/goccy/go-json"
)

// RunEndEncoded represents an array containing two children:
// an array of int32 values defining the ends of each run of values
// and an array of values
type RunEndEncoded struct {
	array

	ends   arrow.Array
	values arrow.Array
}

func NewRunEndEncodedArray(runEnds, values arrow.Array, logicalLength, offset int) *RunEndEncoded {
	data := NewData(arrow.RunEndEncodedOf(runEnds.DataType(), values.DataType()), logicalLength,
		[]*memory.Buffer{nil}, []arrow.ArrayData{runEnds.Data(), values.Data()}, 0, offset)
	defer data.Release()
	return NewRunEndEncodedData(data)
}

func NewRunEndEncodedData(data arrow.ArrayData) *RunEndEncoded {
	r := &RunEndEncoded{}
	r.refCount = 1
	r.setData(data.(*Data))
	return r
}

func (r *RunEndEncoded) Values() arrow.Array     { return r.values }
func (r *RunEndEncoded) RunEndsArr() arrow.Array { return r.ends }

func (r *RunEndEncoded) Retain() {
	r.array.Retain()
	r.values.Retain()
	r.ends.Retain()
}

func (r *RunEndEncoded) Release() {
	r.array.Release()
	r.values.Release()
	r.ends.Release()
}

func (r *RunEndEncoded) setData(data *Data) {
	if len(data.childData) != 2 {
		panic(fmt.Errorf("%w: arrow/array: RLE array must have exactly 2 children", arrow.ErrInvalid))
	}
	debug.Assert(data.dtype.ID() == arrow.RUN_END_ENCODED, "invalid type for RunLengthEncoded")
	if !data.dtype.(*arrow.RunEndEncodedType).ValidRunEndsType(data.childData[0].DataType()) {
		panic(fmt.Errorf("%w: arrow/array: run ends array must be int16, int32, or int64", arrow.ErrInvalid))
	}
	if data.childData[0].NullN() > 0 {
		panic(fmt.Errorf("%w: arrow/array: run ends array cannot contain nulls", arrow.ErrInvalid))
	}

	r.array.setData(data)

	r.ends = MakeFromData(r.data.childData[0])
	r.values = MakeFromData(r.data.childData[1])
}

func (r *RunEndEncoded) GetPhysicalOffset() int {
	return encoded.FindPhysicalOffset(r.data)
}

func (r *RunEndEncoded) GetPhysicalLength() int {
	return encoded.GetPhysicalLength(r.data)
}

func (r *RunEndEncoded) String() string {
	var buf bytes.Buffer
	buf.WriteByte('[')
	for i := 0; i < r.ends.Len(); i++ {
		if i != 0 {
			buf.WriteByte(',')
		}
		fmt.Fprintf(&buf, "{%v -> %v}",
			r.ends.(arraymarshal).getOneForMarshal(i),
			r.values.(arraymarshal).getOneForMarshal(i))
	}

	buf.WriteByte(']')
	return buf.String()
}

func (r *RunEndEncoded) getOneForMarshal(i int) interface{} {
	return [2]interface{}{r.ends.(arraymarshal).getOneForMarshal(i),
		r.values.(arraymarshal).getOneForMarshal(i)}
}

func (r *RunEndEncoded) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	buf.WriteByte('[')
	for i := 0; i < r.ends.Len(); i++ {
		if i != 0 {
			buf.WriteByte(',')
		}
		if err := enc.Encode(r.getOneForMarshal(i)); err != nil {
			return nil, err
		}
	}
	buf.WriteByte(']')
	return buf.Bytes(), nil
}

func arrayRunEndEncodedEqual(l, r *RunEndEncoded) bool {
	// types were already checked before getting here, so we know
	// the encoded types are equal
	mr := encoded.NewMergedRuns([2]arrow.Array{l, r})
	for mr.Next() {
		lIndex := mr.IndexIntoArray(0)
		rIndex := mr.IndexIntoArray(1)
		if !SliceEqual(l.values, lIndex, lIndex+1, r.values, rIndex, rIndex+1) {
			return false
		}
	}
	return true
}

func arrayRunEndEncodedApproxEqual(l, r *RunEndEncoded, opt equalOption) bool {
	// types were already checked before getting here, so we know
	// the encoded types are equal
	mr := encoded.NewMergedRuns([2]arrow.Array{l, r})
	for mr.Next() {
		lIndex := mr.IndexIntoArray(0)
		rIndex := mr.IndexIntoArray(1)
		if !sliceApproxEqual(l.values, lIndex, lIndex+1, r.values, rIndex, rIndex+1, opt) {
			return false
		}
	}
	return true
}

type RunEndEncodedBuilder struct {
	builder

	dt        arrow.DataType
	runEnds   Builder
	values    Builder
	maxRunEnd uint64
}

func NewRunEndEncodedBuilder(mem memory.Allocator, runEnds, encoded arrow.DataType) *RunEndEncodedBuilder {
	dt := arrow.RunEndEncodedOf(runEnds, encoded)
	if !dt.ValidRunEndsType(runEnds) {
		panic("arrow/ree: invalid runEnds type for run length encoded array")
	}

	var maxEnd uint64
	switch runEnds.ID() {
	case arrow.INT16:
		maxEnd = math.MaxInt16
	case arrow.INT32:
		maxEnd = math.MaxInt32
	case arrow.INT64:
		maxEnd = math.MaxInt64
	}
	return &RunEndEncodedBuilder{
		builder:   builder{refCount: 1, mem: mem},
		dt:        dt,
		runEnds:   NewBuilder(mem, runEnds),
		values:    NewBuilder(mem, encoded),
		maxRunEnd: maxEnd,
	}
}

func (b *RunEndEncodedBuilder) Type() arrow.DataType {
	return b.dt
}

func (b *RunEndEncodedBuilder) Release() {
	debug.Assert(atomic.LoadInt64(&b.refCount) > 0, "too many releases")

	if atomic.AddInt64(&b.refCount, -1) == 0 {
		b.values.Release()
		b.runEnds.Release()
	}
}

func (b *RunEndEncodedBuilder) addLength(n uint64) {
	if uint64(b.length)+n > b.maxRunEnd {
		panic(fmt.Errorf("%w: %s array length must fit be less than %d", arrow.ErrInvalid, b.dt, b.maxRunEnd))
	}

	b.length += int(n)
}

func (b *RunEndEncodedBuilder) finishRun() {
	if b.length == 0 {
		return
	}

	switch bldr := b.runEnds.(type) {
	case *Int16Builder:
		bldr.Append(int16(b.length))
	case *Int32Builder:
		bldr.Append(int32(b.length))
	case *Int64Builder:
		bldr.Append(int64(b.length))
	}
}

func (b *RunEndEncodedBuilder) ValueBuilder() Builder { return b.values }
func (b *RunEndEncodedBuilder) Append(n uint64) {
	b.finishRun()
	b.addLength(n)
}
func (b *RunEndEncodedBuilder) ContinueRun(n uint64) {
	b.addLength(n)
}
func (b *RunEndEncodedBuilder) AppendNull() {
	b.finishRun()
	b.values.AppendNull()
	b.addLength(1)
}

func (b *RunEndEncodedBuilder) NullN() int {
	return UnknownNullCount
}

func (b *RunEndEncodedBuilder) AppendEmptyValue() {
	b.AppendNull()
}

func (b *RunEndEncodedBuilder) Reserve(n int) {
	b.values.Reserve(n)
	b.runEnds.Reserve(n)
}

func (b *RunEndEncodedBuilder) Resize(n int) {
	b.values.Resize(n)
	b.runEnds.Resize(n)
}

func (b *RunEndEncodedBuilder) NewRunEndEncodedArray() *RunEndEncoded {
	data := b.newData()
	defer data.Release()
	return NewRunEndEncodedData(data)
}

func (b *RunEndEncodedBuilder) NewArray() arrow.Array {
	return b.NewRunEndEncodedArray()
}

func (b *RunEndEncodedBuilder) newData() (data *Data) {
	b.finishRun()
	values := b.values.NewArray()
	defer values.Release()
	runEnds := b.runEnds.NewArray()
	defer runEnds.Release()

	data = NewData(
		b.dt, b.length, []*memory.Buffer{nil},
		[]arrow.ArrayData{runEnds.Data(), values.Data()}, 0, 0)
	b.reset()
	return
}

func (b *RunEndEncodedBuilder) unmarshalOne(dec *json.Decoder) error {
	return arrow.ErrNotImplemented
}

func (b *RunEndEncodedBuilder) unmarshal(dec *json.Decoder) error {
	for dec.More() {
		if err := b.unmarshalOne(dec); err != nil {
			return err
		}
	}
	return nil
}

func (b *RunEndEncodedBuilder) UnmarshalJSON(data []byte) error {
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
	_ arrow.Array = (*RunEndEncoded)(nil)
	_ Builder     = (*RunEndEncodedBuilder)(nil)
)
