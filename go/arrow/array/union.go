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
	"math"
	"reflect"
	"strings"
	"sync/atomic"

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

const kMaxElems = math.MaxInt32

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
}

func (a *union) NumFields() int { return len(a.unionType.Fields()) }

func (a *union) Mode() arrow.UnionMode { return a.unionType.Mode() }

func (a *union) UnionType() arrow.UnionType { return a.unionType }

func (a *union) TypeCodes() *memory.Buffer {
	return a.data.buffers[1]
}

func (a *union) RawTypeCodes() []arrow.UnionTypeCode {
	if a.data.length > 0 {
		return a.typecodes[a.data.offset:]
	}
	return []arrow.UnionTypeCode{}
}

func (a *union) TypeCode(i int) arrow.UnionTypeCode {
	return a.typecodes[i+a.data.offset]
}

func (a *union) ChildID(i int) int {
	return a.unionType.ChildIDs()[a.typecodes[i+a.data.offset]]
}

func (a *union) setData(data *Data) {
	a.unionType = data.dtype.(arrow.UnionType)
	debug.Assert(len(data.buffers) >= 2, "arrow/array: invalid number of union array buffers")

	if data.length > 0 {
		a.typecodes = arrow.Int8Traits.CastFromBytes(data.buffers[1].Bytes())
	} else {
		a.typecodes = []int8{}
	}
	a.children = make([]arrow.Array, len(data.childData))
	for i, child := range data.childData {
		if a.unionType.Mode() == arrow.SparseMode && (data.offset != 0 || child.Len() != data.length) {
			child = NewSliceData(child, int64(data.offset), int64(data.offset+data.length))
			defer child.Release()
		}
		a.children[i] = MakeFromData(child)
	}
	a.array.setData(data)
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

	if data.length > 0 {
		a.offsets = arrow.Int32Traits.CastFromBytes(a.data.buffers[2].Bytes())
	} else {
		a.offsets = []int32{}
	}
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

type UnionBuilder interface {
	Builder
	AppendNulls(int)
	AppendEmptyValues(int)
	AppendChild(newChild Builder, fieldName string) (newCode arrow.UnionTypeCode)
	Append(arrow.UnionTypeCode)
	Mode() arrow.UnionMode
}

type unionBuilder struct {
	builder

	childFields []arrow.Field
	codes       []arrow.UnionTypeCode
	mode        arrow.UnionMode

	children        []Builder
	typeIDtoBuilder []Builder
	typeIDtoChildID []int
	// for all typeID < denseTypeID, typeIDtoBuilder[typeID] != nil
	denseTypeID  arrow.UnionTypeCode
	typesBuilder *int8BufferBuilder
}

func newUnionBuilder(mem memory.Allocator, children []Builder, typ arrow.UnionType) unionBuilder {
	if children == nil {
		children = make([]Builder, 0)
	}
	b := unionBuilder{
		builder:         builder{refCount: 1, mem: mem},
		mode:            typ.Mode(),
		codes:           typ.TypeCodes(),
		children:        children,
		typeIDtoChildID: make([]int, typ.MaxTypeCode()+1),
		typeIDtoBuilder: make([]Builder, typ.MaxTypeCode()+1),
		childFields:     make([]arrow.Field, len(children)),
		typesBuilder:    newInt8BufferBuilder(mem),
	}

	b.typeIDtoChildID[0] = arrow.InvalidUnionChildID
	for i := 1; i < len(b.typeIDtoChildID); i *= 2 {
		copy(b.typeIDtoChildID[i:], b.typeIDtoChildID[:i])
	}

	debug.Assert(len(children) == len(typ.TypeCodes()), "mismatched typecodes and children")
	debug.Assert(len(b.typeIDtoBuilder)-1 <= int(arrow.MaxUnionTypeCode), "too many typeids")

	copy(b.childFields, typ.Fields())
	for i, c := range children {
		c.Retain()
		typeID := typ.TypeCodes()[i]
		b.typeIDtoChildID[typeID] = i
		b.typeIDtoBuilder[typeID] = c
	}

	return b
}

func (b *unionBuilder) Mode() arrow.UnionMode { return b.mode }

func (b *unionBuilder) reserve(elements int, resize func(int)) {
	// union has no null bitmap, ever so we can skip that handling
	if b.length+elements > b.capacity {
		b.capacity = bitutil.NextPowerOf2(b.length + elements)
		resize(b.capacity)
	}
}

func (b *unionBuilder) Release() {
	debug.Assert(atomic.LoadInt64(&b.refCount) > 0, "too many releases")

	if atomic.AddInt64(&b.refCount, -1) == 0 {
		for _, c := range b.children {
			c.Release()
		}
		b.typesBuilder.Release()
	}
}

func (b *unionBuilder) Type() arrow.DataType {
	fields := make([]arrow.Field, len(b.childFields))
	for i, f := range b.childFields {
		fields[i] = f
		fields[i].Type = b.children[i].Type()
	}

	switch b.mode {
	case arrow.SparseMode:
		return arrow.SparseUnionOf(fields, b.codes)
	case arrow.DenseMode:
		return arrow.DenseUnionOf(fields, b.codes)
	default:
		panic("invalid union builder mode")
	}
}

func (b *unionBuilder) AppendChild(newChild Builder, fieldName string) arrow.UnionTypeCode {
	newChild.Retain()
	b.children = append(b.children, newChild)
	newType := b.nextTypeID()

	b.typeIDtoChildID[newType] = len(b.children) - 1
	b.typeIDtoBuilder[newType] = newChild
	b.childFields = append(b.childFields, arrow.Field{Name: fieldName, Nullable: true})
	b.codes = append(b.codes, newType)

	return newType
}

func (b *unionBuilder) nextTypeID() arrow.UnionTypeCode {
	// find typeID such that typeIDtoBuilder[typeID] == nil
	// use that for the new child. Start searching at denseTypeID
	// since typeIDtoBuilder is densely packed up at least to denseTypeID
	for ; int(b.denseTypeID) < len(b.typeIDtoBuilder); b.denseTypeID++ {
		if b.typeIDtoBuilder[b.denseTypeID] == nil {
			id := b.denseTypeID
			b.denseTypeID++
			return id
		}
	}

	debug.Assert(len(b.typeIDtoBuilder) < int(arrow.MaxUnionTypeCode), "too many children typeids")
	// typeIDtoBuilder is already densely packed, so just append the new child
	b.typeIDtoBuilder = append(b.typeIDtoBuilder, nil)
	b.typeIDtoChildID = append(b.typeIDtoChildID, arrow.InvalidUnionChildID)
	id := b.denseTypeID
	b.denseTypeID++
	return id

}

func (b *unionBuilder) newData() *Data {
	length := b.typesBuilder.Len()
	typesBuffer := b.typesBuilder.Finish()
	defer typesBuffer.Release()
	childData := make([]arrow.ArrayData, len(b.children))
	for i, b := range b.children {
		childData[i] = b.newData()
		defer childData[i].Release()
	}

	return NewData(b.Type(), length, []*memory.Buffer{nil, typesBuffer}, childData, 0, 0)
}

type SparseUnionBuilder struct {
	unionBuilder
}

func NewEmptySparseUnionBuilder(mem memory.Allocator) *SparseUnionBuilder {
	return &SparseUnionBuilder{
		unionBuilder: newUnionBuilder(mem, nil, arrow.SparseUnionOf([]arrow.Field{}, []arrow.UnionTypeCode{})),
	}
}

func NewSparseUnionBuilder(mem memory.Allocator, typ *arrow.SparseUnionType) *SparseUnionBuilder {
	children := make([]Builder, len(typ.Fields()))
	for i, f := range typ.Fields() {
		children[i] = NewBuilder(mem, f.Type)
	}
	return NewSparseUnionBuilderWithBuilders(mem, typ, children)
}

func NewSparseUnionBuilderWithBuilders(mem memory.Allocator, typ *arrow.SparseUnionType, children []Builder) *SparseUnionBuilder {
	return &SparseUnionBuilder{
		unionBuilder: newUnionBuilder(mem, children, typ),
	}
}

func (b *SparseUnionBuilder) Reserve(n int) {
	b.reserve(n, b.Resize)
}

func (b *SparseUnionBuilder) Resize(n int) {
	b.typesBuilder.resize(n)
}

func (b *SparseUnionBuilder) AppendNull() {
	firstChildCode := b.codes[0]
	b.typesBuilder.AppendValue(firstChildCode)
	b.typeIDtoBuilder[firstChildCode].AppendNull()
	for _, c := range b.codes[1:] {
		b.typeIDtoBuilder[c].AppendEmptyValue()
	}
}

func (b *SparseUnionBuilder) AppendNulls(n int) {
	firstChildCode := b.codes[0]
	b.Reserve(n)
	for _, c := range b.codes {
		b.typeIDtoBuilder[c].Reserve(n)
	}
	for i := 0; i < n; i++ {
		b.typesBuilder.AppendValue(firstChildCode)
		b.typeIDtoBuilder[firstChildCode].AppendNull()
		for _, c := range b.codes[1:] {
			b.typeIDtoBuilder[c].AppendEmptyValue()
		}
	}
}

func (b *SparseUnionBuilder) AppendEmptyValue() {
	b.typesBuilder.AppendValue(b.codes[0])
	for _, c := range b.codes {
		b.typeIDtoBuilder[c].AppendEmptyValue()
	}
}

func (b *SparseUnionBuilder) AppendEmptyValues(n int) {
	b.Reserve(n)
	firstChildCode := b.codes[0]
	for _, c := range b.codes {
		b.typeIDtoBuilder[c].Reserve(n)
	}
	for i := 0; i < n; i++ {
		b.typesBuilder.AppendValue(firstChildCode)
		for _, c := range b.codes {
			b.typeIDtoBuilder[c].AppendEmptyValue()
		}
	}
}

func (b *SparseUnionBuilder) Append(nextType arrow.UnionTypeCode) {
	b.typesBuilder.AppendValue(nextType)
}

func (b *SparseUnionBuilder) NewArray() arrow.Array {
	return b.NewSparseUnionArray()
}

func (b *SparseUnionBuilder) NewSparseUnionArray() (a *SparseUnion) {
	data := b.newData()
	a = NewSparseUnionData(data)
	data.Release()
	return
}

func (b *SparseUnionBuilder) UnmarshalJSON(data []byte) (err error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	t, err := dec.Token()
	if err != nil {
		return err
	}

	if delim, ok := t.(json.Delim); !ok || delim != '[' {
		return fmt.Errorf("sparse union builder must unpack from json array, found %s", t)
	}
	return b.unmarshal(dec)
}

func (b *SparseUnionBuilder) unmarshal(dec *json.Decoder) error {
	for dec.More() {
		if err := b.unmarshalOne(dec); err != nil {
			return err
		}
	}
	return nil
}

func (b *SparseUnionBuilder) unmarshalOne(dec *json.Decoder) error {
	t, err := dec.Token()
	if err != nil {
		return err
	}

	switch t {
	case json.Delim('['):
		// should be [type_id, Value]
		typeID, err := dec.Token()
		if err != nil {
			return err
		}

		var typeCode int8

		switch tid := typeID.(type) {
		case json.Number:
			id, err := tid.Int64()
			if err != nil {
				return err
			}
			typeCode = int8(id)
		case float64:
			if tid != float64(int64(tid)) {
				return &json.UnmarshalTypeError{
					Offset: dec.InputOffset(),
					Type:   reflect.TypeOf(int8(0)),
					Struct: fmt.Sprint(b.Type()),
					Value:  "float",
				}
			}
			typeCode = int8(tid)
		}

		childNum := b.typeIDtoChildID[typeCode]
		if childNum == arrow.InvalidUnionChildID {
			return &json.UnmarshalTypeError{
				Offset: dec.InputOffset(),
				Value:  "invalid type code",
			}
		}

		for i, c := range b.children {
			if i != childNum {
				c.AppendNull()
			}
		}

		b.Append(typeCode)
		if err := b.children[childNum].unmarshalOne(dec); err != nil {
			return err
		}

		endArr, err := dec.Token()
		if err != nil {
			return err
		}

		if endArr != json.Delim(']') {
			return &json.UnmarshalTypeError{
				Offset: dec.InputOffset(),
				Value:  "union value array should have exactly 2 elements",
			}
		}
	case nil:
		b.AppendNull()
	default:
		return &json.UnmarshalTypeError{
			Offset: dec.InputOffset(),
			Value:  fmt.Sprint(t),
			Struct: fmt.Sprint(b.Type()),
		}
	}
	return nil
}

type DenseUnionBuilder struct {
	unionBuilder

	offsetsBuilder *int32BufferBuilder
}

func NewEmptyDenseUnionBuilder(mem memory.Allocator) *DenseUnionBuilder {
	return &DenseUnionBuilder{
		unionBuilder:   newUnionBuilder(mem, nil, arrow.DenseUnionOf([]arrow.Field{}, []arrow.UnionTypeCode{})),
		offsetsBuilder: newInt32BufferBuilder(mem),
	}
}

func NewDenseUnionBuilder(mem memory.Allocator, typ *arrow.DenseUnionType) *DenseUnionBuilder {
	children := make([]Builder, len(typ.Fields()))
	for i, f := range typ.Fields() {
		children[i] = NewBuilder(mem, f.Type)
	}
	return NewDenseUnionBuilderWithBuilders(mem, typ, children)
}

func NewDenseUnionBuilderWithBuilders(mem memory.Allocator, typ *arrow.DenseUnionType, children []Builder) *DenseUnionBuilder {
	return &DenseUnionBuilder{
		unionBuilder:   newUnionBuilder(mem, children, typ),
		offsetsBuilder: newInt32BufferBuilder(mem),
	}
}

func (b *DenseUnionBuilder) Reserve(n int) {
	b.reserve(n, b.Resize)
}

func (b *DenseUnionBuilder) Resize(n int) {
	b.typesBuilder.resize(n)
	b.offsetsBuilder.resize(n * arrow.Int32SizeBytes)
}

func (b *DenseUnionBuilder) AppendNull() {
	firstChildCode := b.codes[0]
	childBuilder := b.typeIDtoBuilder[firstChildCode]
	b.typesBuilder.AppendValue(firstChildCode)
	b.offsetsBuilder.AppendValue(int32(childBuilder.Len()))
	childBuilder.AppendNull()
}

func (b *DenseUnionBuilder) AppendNulls(n int) {
	// only append 1 null to the child builder, use the same offset twice
	firstChildCode := b.codes[0]
	childBuilder := b.typeIDtoBuilder[firstChildCode]
	b.Reserve(n)
	for i := 0; i < n; i++ {
		b.typesBuilder.AppendValue(firstChildCode)
		b.offsetsBuilder.AppendValue(int32(childBuilder.Len()))
	}
	// only append a single null to the child builder, the offsets all refer to the same value
	childBuilder.AppendNull()
}

func (b *DenseUnionBuilder) AppendEmptyValue() {
	firstChildCode := b.codes[0]
	childBuilder := b.typeIDtoBuilder[firstChildCode]
	b.typesBuilder.AppendValue(firstChildCode)
	b.offsetsBuilder.AppendValue(int32(childBuilder.Len()))
	childBuilder.AppendEmptyValue()
}

func (b *DenseUnionBuilder) AppendEmptyValues(n int) {
	// only append 1 null to the child builder, use the same offset twice
	firstChildCode := b.codes[0]
	childBuilder := b.typeIDtoBuilder[firstChildCode]
	b.Reserve(n)
	for i := 0; i < n; i++ {
		b.typesBuilder.AppendValue(firstChildCode)
		b.offsetsBuilder.AppendValue(int32(childBuilder.Len()))
	}
	// only append a single null to the child builder, the offsets all refer to the same value
	childBuilder.AppendEmptyValue()
}

// Append appends the necessary offset and type code to the builder
// and must be followed up with an append to the appropriate child builder
func (b *DenseUnionBuilder) Append(nextType arrow.UnionTypeCode) {
	b.typesBuilder.AppendValue(nextType)
	bldr := b.typeIDtoBuilder[nextType]
	if bldr.Len() == kMaxElems {
		panic("a dense UnionArray cannot contain more than 2^31 - 1 elements from a single child")
	}

	b.offsetsBuilder.AppendValue(int32(bldr.Len()))
}

func (b *DenseUnionBuilder) Release() {
	debug.Assert(atomic.LoadInt64(&b.refCount) > 0, "too many releases")

	if atomic.AddInt64(&b.refCount, -1) == 0 {
		for _, c := range b.children {
			c.Release()
		}
		b.typesBuilder.Release()
		b.offsetsBuilder.Release()
	}
}

func (b *DenseUnionBuilder) newData() *Data {
	data := b.unionBuilder.newData()
	data.buffers = append(data.buffers, b.offsetsBuilder.Finish())
	return data
}

func (b *DenseUnionBuilder) NewArray() arrow.Array {
	return b.NewDenseUnionArray()
}

func (b *DenseUnionBuilder) NewDenseUnionArray() (a *DenseUnion) {
	data := b.newData()
	a = NewDenseUnionData(data)
	data.Release()
	return
}

func (b *DenseUnionBuilder) UnmarshalJSON(data []byte) (err error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	t, err := dec.Token()
	if err != nil {
		return err
	}

	if delim, ok := t.(json.Delim); !ok || delim != '[' {
		return fmt.Errorf("dense union builder must unpack from json array, found %s", t)
	}
	return b.unmarshal(dec)
}

func (b *DenseUnionBuilder) unmarshal(dec *json.Decoder) error {
	for dec.More() {
		if err := b.unmarshalOne(dec); err != nil {
			return err
		}
	}
	return nil
}

func (b *DenseUnionBuilder) unmarshalOne(dec *json.Decoder) error {
	t, err := dec.Token()
	if err != nil {
		return err
	}

	switch t {
	case json.Delim('['):
		// should be [type_id, Value]
		typeID, err := dec.Token()
		if err != nil {
			return err
		}

		var typeCode int8

		switch tid := typeID.(type) {
		case json.Number:
			id, err := tid.Int64()
			if err != nil {
				return err
			}
			typeCode = int8(id)
		case float64:
			if tid != float64(int64(tid)) {
				return &json.UnmarshalTypeError{
					Offset: dec.InputOffset(),
					Type:   reflect.TypeOf(int8(0)),
					Struct: fmt.Sprint(b.Type()),
					Value:  "float",
				}
			}
			typeCode = int8(tid)
		}

		childNum := b.typeIDtoChildID[typeCode]
		if childNum == arrow.InvalidUnionChildID {
			return &json.UnmarshalTypeError{
				Offset: dec.InputOffset(),
				Value:  "invalid type code",
			}
		}

		b.Append(typeCode)
		if err := b.children[childNum].unmarshalOne(dec); err != nil {
			return err
		}

		endArr, err := dec.Token()
		if err != nil {
			return err
		}

		if endArr != json.Delim(']') {
			return &json.UnmarshalTypeError{
				Offset: dec.InputOffset(),
				Value:  "union value array should have exactly 2 elements",
			}
		}
	case nil:
		b.AppendNull()
	default:
		return &json.UnmarshalTypeError{
			Offset: dec.InputOffset(),
			Value:  fmt.Sprint(t),
			Struct: fmt.Sprint(b.Type()),
		}
	}
	return nil
}

var (
	_ arrow.Array  = (*SparseUnion)(nil)
	_ arrow.Array  = (*DenseUnion)(nil)
	_ Union        = (*SparseUnion)(nil)
	_ Union        = (*DenseUnion)(nil)
	_ Builder      = (*SparseUnionBuilder)(nil)
	_ Builder      = (*DenseUnionBuilder)(nil)
	_ UnionBuilder = (*SparseUnionBuilder)(nil)
)
