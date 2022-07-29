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
	"errors"
	"fmt"
	"strings"

	"github.com/apache/arrow/go/v9/arrow"
	"github.com/apache/arrow/go/v9/arrow/bitutil"
	"github.com/apache/arrow/go/v9/arrow/internal/debug"
	"github.com/apache/arrow/go/v9/arrow/memory"
	"github.com/apache/arrow/go/v9/internal/bitutils"
	"github.com/goccy/go-json"
)

type Union interface {
	arrow.Array
	Validate() error
	ValidateFull() error
	TypeCodes() *memory.Buffer
	RawTypeCodes() []arrow.UnionTypeCode
	TypeCode(i int) arrow.UnionTypeCode
	ChildID(i int) int
	UnionType() arrow.UnionType
	Mode() arrow.UnionMode
	Field(pos int) arrow.Array
}

type union struct {
	array

	unionType arrow.UnionType
	typecodes []arrow.UnionTypeCode

	children []arrow.Array
}

func (a *union) Retain() {
	a.array.Retain()
	for _, c := range a.children {
		c.Retain()
	}
}

func (a *union) Release() {
	a.array.Release()
	for _, c := range a.children {
		c.Release()
	}
	a.children = nil
}

func (a *union) NumFields() int { return len(a.unionType.Fields()) }

func (a *union) Mode() arrow.UnionMode { return a.unionType.Mode() }

func (a *union) UnionType() arrow.UnionType { return a.unionType }

func (a *union) TypeCodes() *memory.Buffer {
	return a.data.buffers[1]
}

func (a *union) RawTypeCodes() []arrow.UnionTypeCode {
	return a.typecodes[a.data.offset:]
}

func (a *union) TypeCode(i int) arrow.UnionTypeCode {
	return a.typecodes[i+a.data.offset]
}

func (a *union) ChildID(i int) int {
	return a.unionType.ChildIDs()[a.typecodes[i+a.data.offset]]
}

func (a *union) setData(data *Data) {
	a.array.setData(data)
	a.unionType = data.dtype.(arrow.UnionType)
	debug.Assert(len(data.buffers) >= 2, "arrow/array: invalid number of union array buffers")

	a.typecodes = arrow.Int8Traits.CastFromBytes(a.data.buffers[1].Bytes())
	a.children = make([]arrow.Array, len(a.data.childData))
	for i, child := range data.childData {
		if a.unionType.Mode() == arrow.SparseMode && (data.offset != 0 || child.Len() != data.length) {
			child = NewSliceData(child, int64(data.offset), int64(data.offset+data.length))
			defer child.Release()
		}
		a.children[i] = MakeFromData(child)
	}
}

func (a *union) Field(pos int) (result arrow.Array) {
	if pos < 0 || pos >= len(a.children) {
		return nil
	}

	return a.children[pos]
}

func (a *union) Validate() error {
	fields := a.unionType.Fields()
	for i, f := range fields {
		fieldData := a.data.childData[i]
		if a.unionType.Mode() == arrow.SparseMode && fieldData.Len() < a.data.length+a.data.offset {
			return fmt.Errorf("arrow/array: sparse union child array #%d has length smaller than expected for union array (%d < %d)",
				i, fieldData.Len(), a.data.length+a.data.offset)
		}

		if !arrow.TypeEqual(f.Type, fieldData.DataType()) {
			return fmt.Errorf("arrow/array: union child array #%d does not match type field %s vs %s",
				i, fieldData.DataType(), f.Type)
		}
	}
	return nil
}

func (a *union) ValidateFull() error {
	if err := a.Validate(); err != nil {
		return err
	}

	childIDs := a.unionType.ChildIDs()
	codesMap := a.unionType.TypeCodes()
	codes := a.RawTypeCodes()

	for i := 0; i < a.data.length; i++ {
		code := codes[i]
		if code < 0 || childIDs[code] == arrow.InvalidUnionChildID {
			return fmt.Errorf("arrow/array: union value at position %d has invalid type id %d", i, code)
		}
	}

	if a.unionType.Mode() == arrow.DenseMode {
		// validate offsets

		// map logical typeid to child length
		var childLengths [256]int64
		for i := range a.unionType.Fields() {
			childLengths[codesMap[i]] = int64(a.data.childData[i].Len())
		}

		// check offsets are in bounds
		var lastOffsets [256]int64
		offsets := arrow.Int32Traits.CastFromBytes(a.data.buffers[2].Bytes())[a.data.offset:]
		for i := int64(0); i < int64(a.data.length); i++ {
			code := codes[i]
			offset := offsets[i]
			switch {
			case offset < 0:
				return fmt.Errorf("arrow/array: union value at position %d has negative offset %d", i, offset)
			case offset >= int32(childLengths[code]):
				return fmt.Errorf("arrow/array: union value at position %d has offset larger than child length (%d >= %d)",
					i, offset, childLengths[code])
			case offset < int32(lastOffsets[code]):
				return fmt.Errorf("arrow/array: union value at position %d has non-monotonic offset %d", i, offset)
			}
			lastOffsets[code] = int64(offset)
		}
	}

	return nil
}

type SparseUnion struct {
	union
}

func NewSparseUnion(dt *arrow.SparseUnionType, length int, children []arrow.Array, typeIDs *memory.Buffer, offset int) *SparseUnion {
	childData := make([]arrow.ArrayData, len(children))
	for i, c := range children {
		childData[i] = c.Data()
	}
	data := NewData(dt, length, []*memory.Buffer{nil, typeIDs}, childData, 0, offset)
	defer data.Release()
	return NewSparseUnionData(data)
}

func NewSparseUnionData(data arrow.ArrayData) *SparseUnion {
	a := &SparseUnion{}
	a.refCount = 1
	a.setData(data.(*Data))
	return a
}

func NewSparseUnionFromArrays(typeIDs arrow.Array, children []arrow.Array, codes ...arrow.UnionTypeCode) (*SparseUnion, error) {
	return NewSparseUnionFromArraysWithFieldCodes(typeIDs, children, []string{}, codes)
}

func NewSparseUnionFromArraysWithFields(typeIDs arrow.Array, children []arrow.Array, fields []string) (*SparseUnion, error) {
	return NewSparseUnionFromArraysWithFieldCodes(typeIDs, children, fields, []arrow.UnionTypeCode{})
}

func NewSparseUnionFromArraysWithFieldCodes(typeIDs arrow.Array, children []arrow.Array, fields []string, codes []arrow.UnionTypeCode) (*SparseUnion, error) {
	switch {
	case typeIDs.DataType().ID() != arrow.INT8:
		return nil, errors.New("arrow/array: union array type ids must be signed int8")
	case typeIDs.NullN() != 0:
		return nil, errors.New("arrow/array: union type ids may not have nulls")
	case len(fields) > 0 && len(fields) != len(children):
		return nil, errors.New("arrow/array: field names must have the same length as children")
	case len(codes) > 0 && len(codes) != len(children):
		return nil, errors.New("arrow/array: type codes must have same length as children")
	}

	buffers := []*memory.Buffer{nil, typeIDs.Data().Buffers()[1]}
	ty := arrow.SparseUnionFromArrays(children, fields, codes)

	childData := make([]arrow.ArrayData, len(children))
	for i, c := range children {
		childData[i] = c.Data()
		if c.Len() != typeIDs.Len() {
			return nil, errors.New("arrow/array: sparse union array must have len(child) == len(typeids) for all children")
		}
	}

	data := NewData(ty, typeIDs.Len(), buffers, childData, 0, typeIDs.Data().Offset())
	defer data.Release()
	return NewSparseUnionData(data), nil
}

func (a *SparseUnion) setData(data *Data) {
	a.union.setData(data)
	debug.Assert(a.data.dtype.ID() == arrow.SPARSE_UNION, "arrow/array: invalid data type for SparseUnion")
	debug.Assert(len(a.data.buffers) == 2, "arrow/array: sparse unions should have exactly 2 buffers")
	debug.Assert(a.data.buffers[0] == nil, "arrow/array: validity bitmap for sparse unions should be nil")
}

func (a *SparseUnion) getOneForMarshal(i int) interface{} {
	childID := a.ChildID(i)
	field := a.unionType.Fields()[childID]
	data := a.Field(childID)

	if data.IsNull(i) {
		return nil
	}

	return map[string]interface{}{field.Name: data.(arraymarshal).getOneForMarshal(i)}
}

func (a *SparseUnion) MarshalJSON() ([]byte, error) {
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

func (a *SparseUnion) String() string {
	var b strings.Builder
	b.WriteByte('[')

	fieldList := a.unionType.Fields()
	for i := 0; i < a.Len(); i++ {
		if i > 0 {
			b.WriteString(" ")
		}

		field := fieldList[a.ChildID(i)]
		f := a.Field(a.ChildID(i))
		fmt.Fprintf(&b, "{%s=%v}", field.Name, f.(arraymarshal).getOneForMarshal(i))
	}
	b.WriteByte(']')
	return b.String()
}

func (a *SparseUnion) GetFlattenedField(mem memory.Allocator, index int) (arrow.Array, error) {
	if index < 0 || index >= a.NumFields() {
		return nil, fmt.Errorf("arrow/array: index out of range: %d", index)
	}

	childData := a.data.childData[index]
	if a.data.offset != 0 || a.data.length != childData.Len() {
		childData = NewSliceData(childData, int64(a.data.offset), int64(a.data.offset+a.data.length))
		// NewSliceData doesn't break the slice reference for buffers
		// since we're going to replace the null bitmap buffer we need to break the
		// slice reference so that we don't affect a.children's references
		newBufs := make([]*memory.Buffer, len(childData.Buffers()))
		copy(newBufs, childData.(*Data).buffers)
		childData.(*Data).buffers = newBufs
	} else {
		childData = childData.(*Data).Copy()
	}
	defer childData.Release()

	// synthesize a null bitmap based on the union discriminant
	// make sure hte bitmap has extra bits corresponding to the child's offset
	flattenedNullBitmap := memory.NewResizableBuffer(mem)
	flattenedNullBitmap.Resize(childData.Len() + childData.Offset())

	var (
		childNullBitmap       = childData.Buffers()[0]
		childOffset           = childData.Offset()
		typeCode              = a.unionType.TypeCodes()[index]
		codes                 = a.RawTypeCodes()
		offset          int64 = 0
	)
	bitutils.GenerateBitsUnrolled(flattenedNullBitmap.Bytes(), int64(childOffset), int64(a.data.length),
		func() bool {
			b := codes[offset] == typeCode
			offset++
			return b
		})

	if childNullBitmap != nil {
		defer childNullBitmap.Release()
		bitutil.BitmapAnd(flattenedNullBitmap.Bytes(), childNullBitmap.Bytes(),
			int64(childOffset), int64(childOffset), flattenedNullBitmap.Bytes(),
			int64(childOffset), int64(childData.Len()))
	}
	childData.(*Data).buffers[0] = flattenedNullBitmap
	childData.(*Data).nulls = childData.Len() - bitutil.CountSetBits(flattenedNullBitmap.Bytes(), childOffset, childData.Len())
	return MakeFromData(childData), nil
}

func arraySparseUnionEqual(l, r *SparseUnion) bool {
	childIDs := l.unionType.ChildIDs()
	leftCodes, rightCodes := l.RawTypeCodes(), r.RawTypeCodes()

	for i := 0; i < l.data.length; i++ {
		typeID := leftCodes[i]
		if typeID != rightCodes[i] {
			return false
		}

		childNum := childIDs[typeID]
		eq := SliceEqual(l.children[childNum], int64(i), int64(i+1),
			r.children[childNum], int64(i), int64(i+1))
		if !eq {
			return false
		}
	}
	return true
}

func arraySparseUnionApproxEqual(l, r *SparseUnion, opt equalOption) bool {
	childIDs := l.unionType.ChildIDs()
	leftCodes, rightCodes := l.RawTypeCodes(), r.RawTypeCodes()

	for i := 0; i < l.data.length; i++ {
		typeID := leftCodes[i]
		if typeID != rightCodes[i] {
			return false
		}

		childNum := childIDs[typeID]
		eq := sliceApproxEqual(l.children[childNum], int64(i+l.data.offset), int64(i+l.data.offset+1),
			r.children[childNum], int64(i+r.data.offset), int64(i+r.data.offset+1), opt)
		if !eq {
			return false
		}
	}
	return true
}

type DenseUnion struct {
	union
	offsets []int32
}

func NewDenseUnion(dt *arrow.DenseUnionType, length int, children []arrow.Array, typeIDs, valueOffsets *memory.Buffer, offset int) *DenseUnion {
	childData := make([]arrow.ArrayData, len(children))
	for i, c := range children {
		childData[i] = c.Data()
	}

	data := NewData(dt, length, []*memory.Buffer{nil, typeIDs, valueOffsets}, childData, 0, offset)
	defer data.Release()
	return NewDenseUnionData(data)
}

func NewDenseUnionData(data arrow.ArrayData) *DenseUnion {
	a := &DenseUnion{}
	a.refCount = 1
	a.setData(data.(*Data))
	return a
}

func NewDenseUnionFromArrays(typeIDs, offsets arrow.Array, children []arrow.Array, codes ...arrow.UnionTypeCode) (*DenseUnion, error) {
	return NewDenseUnionFromArraysWithFieldCodes(typeIDs, offsets, children, []string{}, codes)
}

func NewDenseUnionFromArraysWithFields(typeIDs, offsets arrow.Array, children []arrow.Array, fields []string) (*DenseUnion, error) {
	return NewDenseUnionFromArraysWithFieldCodes(typeIDs, offsets, children, fields, []arrow.UnionTypeCode{})
}

func NewDenseUnionFromArraysWithFieldCodes(typeIDs, offsets arrow.Array, children []arrow.Array, fields []string, codes []arrow.UnionTypeCode) (*DenseUnion, error) {
	switch {
	case offsets.DataType().ID() != arrow.INT32:
		return nil, errors.New("arrow/array: union offsets must be signed int32")
	case typeIDs.DataType().ID() != arrow.INT8:
		return nil, errors.New("arrow/array: union type_ids must be signed int8")
	case typeIDs.NullN() != 0:
		return nil, errors.New("arrow/array: union typeIDs may not have nulls")
	case offsets.NullN() != 0:
		return nil, errors.New("arrow/array: nulls are not allowed in offsets for NewDenseUnionFromArrays*")
	case len(fields) > 0 && len(fields) != len(children):
		return nil, errors.New("arrow/array: fields must be the same length as children")
	case len(codes) > 0 && len(codes) != len(children):
		return nil, errors.New("arrow/array: typecodes must have the same length as children")
	}

	ty := arrow.DenseUnionFromArrays(children, fields, codes)
	buffers := []*memory.Buffer{nil, typeIDs.Data().Buffers()[1], offsets.Data().Buffers()[1]}

	childData := make([]arrow.ArrayData, len(children))
	for i, c := range children {
		childData[i] = c.Data()
	}

	data := NewData(ty, typeIDs.Len(), buffers, childData, 0, typeIDs.Data().Offset())
	defer data.Release()
	return NewDenseUnionData(data), nil
}

func (a *DenseUnion) ValueOffsets() *memory.Buffer { return a.data.buffers[2] }

func (a *DenseUnion) ValueOffset(i int) int32 { return a.offsets[i+a.data.offset] }

func (a *DenseUnion) RawValueOffsets() []int32 { return a.offsets[a.data.offset:] }

func (a *DenseUnion) setData(data *Data) {
	a.union.setData(data)
	debug.Assert(a.data.dtype.ID() == arrow.DENSE_UNION, "arrow/array: invalid data type for DenseUnion")
	debug.Assert(len(a.data.buffers) == 3, "arrow/array: sparse unions should have exactly 3 buffers")
	debug.Assert(a.data.buffers[0] == nil, "arrow/array: validity bitmap for dense unions should be nil")

	a.offsets = arrow.Int32Traits.CastFromBytes(a.data.buffers[2].Bytes())
}

func (a *DenseUnion) getOneForMarshal(i int) interface{} {
	childID := a.ChildID(i)
	field := a.unionType.Fields()[childID]
	data := a.Field(childID)

	if data.IsNull(i) {
		return nil
	}

	offsets := a.RawValueOffsets()
	return map[string]interface{}{field.Name: data.(arraymarshal).getOneForMarshal(int(offsets[i]))}
}

func (a *DenseUnion) MarshalJSON() ([]byte, error) {
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

func (a *DenseUnion) String() string {
	var b strings.Builder
	b.WriteByte('[')

	offsets := a.RawValueOffsets()

	fieldList := a.unionType.Fields()
	for i := 0; i < a.Len(); i++ {
		if i > 0 {
			b.WriteString(" ")
		}

		field := fieldList[a.ChildID(i)]
		f := a.Field(a.ChildID(i))
		fmt.Fprintf(&b, "{%s=%v}", field.Name, f.(arraymarshal).getOneForMarshal(int(offsets[i])))
	}
	b.WriteByte(']')
	return b.String()
}

func arrayDenseUnionEqual(l, r *DenseUnion) bool {
	childIDs := l.unionType.ChildIDs()
	leftCodes, rightCodes := l.RawTypeCodes(), r.RawTypeCodes()
	leftOffsets, rightOffsets := l.RawValueOffsets(), r.RawValueOffsets()

	for i := 0; i < l.data.length; i++ {
		typeID := leftCodes[i]
		if typeID != rightCodes[i] {
			return false
		}

		childNum := childIDs[typeID]
		eq := SliceEqual(l.children[childNum], int64(leftOffsets[i]), int64(leftOffsets[i]+1),
			r.children[childNum], int64(rightOffsets[i]), int64(rightOffsets[i]+1))
		if !eq {
			return false
		}
	}
	return true
}

func arrayDenseUnionApproxEqual(l, r *DenseUnion, opt equalOption) bool {
	childIDs := l.unionType.ChildIDs()
	leftCodes, rightCodes := l.RawTypeCodes(), r.RawTypeCodes()
	leftOffsets, rightOffsets := l.RawValueOffsets(), r.RawValueOffsets()

	for i := 0; i < l.data.length; i++ {
		typeID := leftCodes[i]
		if typeID != rightCodes[i] {
			return false
		}

		childNum := childIDs[typeID]
		eq := sliceApproxEqual(l.children[childNum], int64(leftOffsets[i]), int64(leftOffsets[i]+1),
			r.children[childNum], int64(rightOffsets[i]), int64(rightOffsets[i]+1), opt)
		if !eq {
			return false
		}
	}
	return true
}

var (
	_ arrow.Array = (*SparseUnion)(nil)
	_ arrow.Array = (*DenseUnion)(nil)
	_ Union       = (*SparseUnion)(nil)
	_ Union       = (*DenseUnion)(nil)
)
