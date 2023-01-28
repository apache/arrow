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
