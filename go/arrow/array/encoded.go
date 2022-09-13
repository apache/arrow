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

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/internal/debug"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"github.com/apache/arrow/go/v10/arrow/rle"
	"github.com/goccy/go-json"
)

// RunLengthEncoded represents an array containing two children:
// an array of int32 values defining the ends of each run of values
// and an array of values
type RunLengthEncoded struct {
	array

	runEnds []int32
	ends    arrow.Array
	values  arrow.Array
}

func NewRunLengthEncodedArray(runEnds, values arrow.Array, logicalLength, offset int) *RunLengthEncoded {
	data := NewData(arrow.RunLengthEncodedOf(values.DataType()), logicalLength,
		[]*memory.Buffer{nil}, []arrow.ArrayData{runEnds.Data(), values.Data()}, 0, offset)
	defer data.Release()
	return NewRunLengthEncodedData(data)
}

func NewRunLengthEncodedData(data arrow.ArrayData) *RunLengthEncoded {
	r := &RunLengthEncoded{}
	r.refCount = 1
	r.setData(data.(*Data))
	return r
}

func (r *RunLengthEncoded) RunEnds() []int32        { return r.runEnds }
func (r *RunLengthEncoded) Values() arrow.Array     { return r.values }
func (r *RunLengthEncoded) RunEndsArr() arrow.Array { return r.ends }

func (r *RunLengthEncoded) Retain() {
	r.array.Retain()
	r.values.Retain()
	r.ends.Retain()
}

func (r *RunLengthEncoded) Release() {
	r.array.Release()
	r.values.Release()
	r.ends.Release()
}

func (r *RunLengthEncoded) setData(data *Data) {
	if len(data.childData) != 2 {
		panic(fmt.Errorf("%w: arrow/array: RLE array must have exactly 2 children", arrow.ErrInvalid))
	}
	if data.childData[0].DataType().ID() != arrow.INT32 {
		panic(fmt.Errorf("%w: arrow/array: run ends array must be int32", arrow.ErrInvalid))
	}
	if data.childData[0].NullN() > 0 {
		panic(fmt.Errorf("%w: arrow/array: run ends array cannot contain nulls", arrow.ErrInvalid))
	}

	debug.Assert(data.dtype.ID() == arrow.RUN_LENGTH_ENCODED, "invalid type for RunLengthEncoded")
	r.array.setData(data)

	if r.data.childData[0].Buffers()[1] != nil {
		r.runEnds = arrow.Int32Traits.CastFromBytes(r.data.childData[0].Buffers()[1].Bytes())
	}
	r.ends = MakeFromData(r.data.childData[0])
	r.values = MakeFromData(r.data.childData[1])
}

func (r *RunLengthEncoded) GetPhysicalOffset() int {
	return rle.FindPhysicalOffset(r.runEnds, r.data.offset)
}

func (r *RunLengthEncoded) GetPhysicalLength() int {
	if r.data.length == 0 {
		return 0
	}

	physicalOffset := r.GetPhysicalOffset()
	return rle.FindPhysicalOffset(r.runEnds[physicalOffset:],
		r.data.offset+r.data.length-1) + 1
}

func (r *RunLengthEncoded) String() string {
	var buf bytes.Buffer
	buf.WriteByte('[')
	for i, runEnd := range r.runEnds {
		if i != 0 {
			buf.WriteByte(',')
		}

		fmt.Fprintf(&buf, "{%d -> %v}", runEnd, r.values.(arraymarshal).getOneForMarshal(i))
	}
	buf.WriteByte(']')
	return buf.String()
}

func (r *RunLengthEncoded) getOneForMarshal(i int) interface{} {
	return [2]interface{}{r.runEnds[i], r.values.(arraymarshal).getOneForMarshal(i)}
}

func (r *RunLengthEncoded) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	buf.WriteByte('[')

	for i := range r.runEnds {
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

func arrayRunLengthEncodedEqual(l, r *RunLengthEncoded) bool {
	// types were already checked before getting here, so we know
	// the encoded types are equal
	mr := rle.NewMergedRuns([2]arrow.Array{l, r})
	for mr.Next() {
		lIndex := mr.IndexIntoArray(0)
		rIndex := mr.IndexIntoArray(1)
		if !SliceEqual(l.values, lIndex, lIndex+1, r.values, rIndex, rIndex+1) {
			return false
		}
	}
	return true
}

func arrayRunLengthEncodedApproxEqual(l, r *RunLengthEncoded, opt equalOption) bool {
	// types were already checked before getting here, so we know
	// the encoded types are equal
	mr := rle.NewMergedRuns([2]arrow.Array{l, r})
	for mr.Next() {
		lIndex := mr.IndexIntoArray(0)
		rIndex := mr.IndexIntoArray(1)
		if !sliceApproxEqual(l.values, lIndex, lIndex+1, r.values, rIndex, rIndex+1, opt) {
			return false
		}
	}
	return true

}

type RunLengthEncodedBuilder struct {
	builder

	dt      arrow.DataType
	runEnds *Int32Builder
	values  Builder
}

func NewRunLengthEncodedBuilder(mem memory.Allocator, typ arrow.DataType) *RunLengthEncodedBuilder {
	return &RunLengthEncodedBuilder{
		builder: builder{refCount: 1, mem: mem},
		dt:      arrow.RunLengthEncodedOf(typ),
		runEnds: NewInt32Builder(mem),
		values:  NewBuilder(mem, typ),
	}
}

func (b *RunLengthEncodedBuilder) Type() arrow.DataType {
	return b.dt
}

func (b *RunLengthEncodedBuilder) Release() {
	debug.Assert(atomic.LoadInt64(&b.refCount) > 0, "too many releases")

	if atomic.AddInt64(&b.refCount, -1) == 0 {
		b.values.Release()
		b.runEnds.Release()
	}
}

func (b *RunLengthEncodedBuilder) addLength(n uint32) {
	if b.length+int(n) > math.MaxInt32 {
		panic(fmt.Errorf("%w: run-length encoded array length must fit in a 32-bit signed integer", arrow.ErrInvalid))
	}

	b.length += int(n)
}

func (b *RunLengthEncodedBuilder) finishRun() {
	if b.length == 0 {
		return
	}

	b.runEnds.Append(int32(b.length))
}

func (b *RunLengthEncodedBuilder) ValueBuilder() Builder { return b.values }
func (b *RunLengthEncodedBuilder) Append(n uint32) {
	b.finishRun()
	b.addLength(n)
}
func (b *RunLengthEncodedBuilder) ContinueRun(n uint32) {
	b.addLength(n)
}
func (b *RunLengthEncodedBuilder) AppendNull() {
	b.finishRun()
	b.values.AppendNull()
	b.addLength(1)
}

func (b *RunLengthEncodedBuilder) NullN() int {
	return UnknownNullCount
}

func (b *RunLengthEncodedBuilder) AppendEmptyValue() {
	b.AppendNull()
}

func (b *RunLengthEncodedBuilder) Reserve(n int) {
	b.values.Reserve(n)
	b.runEnds.Reserve(n)
}

func (b *RunLengthEncodedBuilder) Resize(n int) {
	b.values.Resize(n)
	b.runEnds.Resize(n)
}

func (b *RunLengthEncodedBuilder) NewRunLengthEncodedArray() *RunLengthEncoded {
	data := b.newData()
	defer data.Release()
	return NewRunLengthEncodedData(data)
}

func (b *RunLengthEncodedBuilder) NewArray() arrow.Array {
	return b.NewRunLengthEncodedArray()
}

func (b *RunLengthEncodedBuilder) newData() (data *Data) {
	b.finishRun()
	values := b.values.NewArray()
	defer values.Release()
	runEnds := b.runEnds.NewInt32Array()
	defer runEnds.Release()

	data = NewData(
		b.dt, b.length, []*memory.Buffer{nil},
		[]arrow.ArrayData{runEnds.data, values.Data()}, 0, 0)
	b.reset()
	return
}

func (b *RunLengthEncodedBuilder) unmarshalOne(dec *json.Decoder) error {
	return arrow.ErrNotImplemented
}

func (b *RunLengthEncodedBuilder) unmarshal(dec *json.Decoder) error {
	for dec.More() {
		if err := b.unmarshalOne(dec); err != nil {
			return err
		}
	}
	return nil
}

func (b *RunLengthEncodedBuilder) UnmarshalJSON(data []byte) error {
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
	_ arrow.Array = (*RunLengthEncoded)(nil)
	_ Builder     = (*RunLengthEncodedBuilder)(nil)
)
