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
	"errors"
	"fmt"
	"math"
	"sync/atomic"
	"unsafe"

	"github.com/apache/arrow/go/v7/arrow"
	"github.com/apache/arrow/go/v7/arrow/bitutil"
	"github.com/apache/arrow/go/v7/arrow/decimal128"
	"github.com/apache/arrow/go/v7/arrow/float16"
	"github.com/apache/arrow/go/v7/arrow/internal/debug"
	"github.com/apache/arrow/go/v7/arrow/memory"
	"github.com/apache/arrow/go/v7/internal/hashing"
	"github.com/goccy/go-json"
)

// Dictionary represents the type for dictionary-encoded data with a data
// dependent dictionary.
//
// A dictionary array contains an array of non-negative integers (the "dictionary"
// indices") along with a data type containing a "dictionary" corresponding to
// the distinct values represented in the data.
//
// For example, the array:
//
//      ["foo", "bar", "foo", "bar", "foo", "bar"]
//
// with dictionary ["bar", "foo"], would have the representation of:
//
//      indices: [1, 0, 1, 0, 1, 0]
//      dictionary: ["bar", "foo"]
//
// The indices in principle may be any integer type.
type Dictionary struct {
	array

	indices Interface
	dict    Interface
}

func NewDictionaryArray(typ arrow.DataType, indices, dict Interface) *Dictionary {
	a := &Dictionary{}
	a.array.refCount = 1
	dictdata := NewData(typ, indices.Len(), indices.Data().buffers, indices.Data().childData, indices.NullN(), indices.Data().offset)
	dictdata.dictionary = dict.Data()
	dict.Data().Retain()

	defer dictdata.Release()
	a.setData(dictdata)
	return a
}

func checkIndexBounds(indices *Data, upperlimit uint64) error {
	if indices.length == 0 {
		return nil
	}

	var maxval uint64
	switch indices.dtype.ID() {
	case arrow.UINT8:
		maxval = math.MaxUint8
	case arrow.UINT16:
		maxval = math.MaxUint16
	case arrow.UINT32:
		maxval = math.MaxUint32
	case arrow.UINT64:
		maxval = math.MaxUint64
	}
	isSigned := maxval == 0
	if !isSigned && upperlimit > maxval {
		return nil
	}

	// TODO(mtopol): lift BitSetRunReader from parquet to utils
	// and use it here for performance improvement.
	var nullbitmap []byte
	if indices.buffers[0] != nil {
		nullbitmap = indices.buffers[0].Bytes()
	}

	var outOfBounds func(i int) error
	switch indices.dtype.ID() {
	case arrow.INT8:
		data := arrow.Int8Traits.CastFromBytes(indices.buffers[1].Bytes())
		outOfBounds = func(i int) error {
			if data[i] < 0 || data[i] >= int8(upperlimit) {
				return fmt.Errorf("index %d out of bounds", data[i])
			}
			return nil
		}
	case arrow.UINT8:
		data := arrow.Uint8Traits.CastFromBytes(indices.buffers[1].Bytes())
		outOfBounds = func(i int) error {
			if data[i] >= uint8(upperlimit) {
				return fmt.Errorf("index %d out of bounds", data[i])
			}
			return nil
		}
	case arrow.INT16:
		data := arrow.Int16Traits.CastFromBytes(indices.buffers[1].Bytes())
		outOfBounds = func(i int) error {
			if data[i] < 0 || data[i] >= int16(upperlimit) {
				return fmt.Errorf("index %d out of bounds", data[i])
			}
			return nil
		}
	case arrow.UINT16:
		data := arrow.Uint16Traits.CastFromBytes(indices.buffers[1].Bytes())
		outOfBounds = func(i int) error {
			if data[i] >= uint16(upperlimit) {
				return fmt.Errorf("index %d out of bounds", data[i])
			}
			return nil
		}
	case arrow.INT32:
		data := arrow.Int32Traits.CastFromBytes(indices.buffers[1].Bytes())
		outOfBounds = func(i int) error {
			if data[i] < 0 || data[i] >= int32(upperlimit) {
				return fmt.Errorf("index %d out of bounds", data[i])
			}
			return nil
		}
	case arrow.UINT32:
		data := arrow.Uint32Traits.CastFromBytes(indices.buffers[1].Bytes())
		outOfBounds = func(i int) error {
			if data[i] >= uint32(upperlimit) {
				return fmt.Errorf("index %d out of bounds", data[i])
			}
			return nil
		}
	case arrow.INT64:
		data := arrow.Int64Traits.CastFromBytes(indices.buffers[1].Bytes())
		outOfBounds = func(i int) error {
			if data[i] < 0 || data[i] >= int64(upperlimit) {
				return fmt.Errorf("index %d out of bounds", data[i])
			}
			return nil
		}
	case arrow.UINT64:
		data := arrow.Uint64Traits.CastFromBytes(indices.buffers[1].Bytes())
		outOfBounds = func(i int) error {
			if data[i] >= upperlimit {
				return fmt.Errorf("index %d out of bounds", data[i])
			}
			return nil
		}
	default:
		return fmt.Errorf("invalid type for bounds checking: %T", indices.dtype)
	}

	for i := 0; i < indices.length; i++ {
		if len(nullbitmap) > 0 && bitutil.BitIsNotSet(nullbitmap, i+indices.offset) {
			continue
		}

		if err := outOfBounds(i + indices.offset); err != nil {
			return err
		}
	}
	return nil
}

func NewValidatedDictionaryArray(typ *arrow.DictionaryType, indices, dict Interface) (*Dictionary, error) {
	if indices.DataType().ID() != typ.IndexType.ID() {
		return nil, fmt.Errorf("dictionary type index (%T) does not match indices array type (%T)", typ.IndexType, indices.DataType())
	}

	if !arrow.TypeEqual(typ.ValueType, dict.DataType()) {
		return nil, fmt.Errorf("dictionary value type (%T) does not match dict array type (%T)", typ.ValueType, dict.DataType())
	}

	if err := checkIndexBounds(indices.Data(), uint64(dict.Len())); err != nil {
		return nil, err
	}

	return NewDictionaryArray(typ, indices, dict), nil
}

func NewDictionaryData(data *Data) *Dictionary {
	a := &Dictionary{}
	a.refCount = 1
	a.setData(data)
	return a
}

func (d *Dictionary) Retain() {
	atomic.AddInt64(&d.refCount, 1)
}

func (d *Dictionary) Release() {
	debug.Assert(atomic.LoadInt64(&d.refCount) > 0, "too many releases")

	if atomic.AddInt64(&d.refCount, -1) == 0 {
		d.data.Release()
		d.data, d.nullBitmapBytes = nil, nil
		d.indices.Release()
		d.indices = nil
		if d.dict != nil {
			d.dict.Release()
			d.dict = nil
		}
	}
}

func (d *Dictionary) setData(data *Data) {
	d.array.setData(data)

	if data.dictionary == nil {
		panic("arrow/array: no dictionary set in Data for Dictionary array")
	}

	dictType := data.dtype.(*arrow.DictionaryType)
	debug.Assert(arrow.TypeEqual(dictType.ValueType, data.dictionary.DataType()), "mismatched dictionary value types")

	indexData := NewData(dictType.IndexType, data.length, data.buffers, data.childData, data.nulls, data.offset)
	defer indexData.Release()
	d.indices = MakeFromData(indexData)
}

// Dictionary returns the values array that makes up the dictionary for this
// array.
func (d *Dictionary) Dictionary() Interface {
	if d.dict == nil {
		d.dict = MakeFromData(d.data.dictionary)
	}
	return d.dict
}

// Indices returns the underlying array of indices as it's own array
func (d *Dictionary) Indices() Interface {
	return d.indices
}

// CanCompareIndices returns true if the dictionary arrays can be compared
// without having to unify the dictionaries themselves first.
func (d *Dictionary) CanCompareIndices(other *Dictionary) bool {
	if !arrow.TypeEqual(d.indices.DataType(), other.indices.DataType()) {
		return false
	}

	minlen := int64(min(d.data.dictionary.length, other.data.dictionary.length))
	return ArraySliceEqual(d.Dictionary(), 0, minlen, other.Dictionary(), 0, minlen)
}

func (d *Dictionary) String() string {
	return fmt.Sprintf("{ dictionary: %v\n  indices: %v }", d.Dictionary(), d.Indices())
}

func (d *Dictionary) GetValueIndex(i int) int {
	indiceData := d.data.buffers[1].Bytes()
	// we know the value is non-negative per the spec, so
	// we can use the unsigned value regardless.
	switch d.indices.DataType().ID() {
	case arrow.UINT8, arrow.INT8:
		return int(uint8(indiceData[d.data.offset+i]))
	case arrow.UINT16, arrow.INT16:
		return int(arrow.Uint16Traits.CastFromBytes(indiceData)[d.data.offset+i])
	case arrow.UINT32, arrow.INT32:
		return int(arrow.Uint32Traits.CastFromBytes(indiceData)[d.data.offset+i])
	case arrow.UINT64, arrow.INT64:
		return int(arrow.Uint64Traits.CastFromBytes(indiceData)[d.data.offset+i])
	}
	debug.Assert(false, "unreachable dictionary index")
	return -1
}

func (d *Dictionary) getOneForMarshal(i int) interface{} {
	if d.IsNull(i) {
		return nil
	}
	vidx := d.GetValueIndex(i)
	return d.Dictionary().getOneForMarshal(vidx)
}

func (d *Dictionary) MarshalJSON() ([]byte, error) {
	vals := make([]interface{}, d.Len())
	for i := 0; i < d.Len(); i++ {
		vals[i] = d.getOneForMarshal(i)
	}
	return json.Marshal(vals)
}

func arrayEqualDict(l, r *Dictionary) bool {
	return ArrayEqual(l.Dictionary(), r.Dictionary()) && ArrayEqual(l.indices, r.indices)
}

func arrayApproxEqualDict(l, r *Dictionary, opt equalOption) bool {
	return arrayApproxEqual(l.Dictionary(), r.Dictionary(), opt) && arrayApproxEqual(l.indices, r.indices, opt)
}

type indexBuilder struct {
	Builder
	Append func(int)
}

func createIndexBuilder(mem memory.Allocator, dt arrow.FixedWidthDataType) (ret indexBuilder, err error) {
	ret = indexBuilder{Builder: NewBuilder(mem, dt)}
	switch dt.ID() {
	case arrow.INT8:
		ret.Append = func(idx int) {
			ret.Builder.(*Int8Builder).Append(int8(idx))
		}
	case arrow.UINT8:
		ret.Append = func(idx int) {
			ret.Builder.(*Uint8Builder).Append(uint8(idx))
		}
	case arrow.INT16:
		ret.Append = func(idx int) {
			ret.Builder.(*Int16Builder).Append(int16(idx))
		}
	case arrow.UINT16:
		ret.Append = func(idx int) {
			ret.Builder.(*Uint16Builder).Append(uint16(idx))
		}
	case arrow.INT32:
		ret.Append = func(idx int) {
			ret.Builder.(*Int32Builder).Append(int32(idx))
		}
	case arrow.UINT32:
		ret.Append = func(idx int) {
			ret.Builder.(*Uint32Builder).Append(uint32(idx))
		}
	case arrow.INT64:
		ret.Append = func(idx int) {
			ret.Builder.(*Int64Builder).Append(int64(idx))
		}
	case arrow.UINT64:
		ret.Append = func(idx int) {
			ret.Builder.(*Uint64Builder).Append(uint64(idx))
		}
	default:
		debug.Assert(false, "dictionary index type must be integral")
		err = fmt.Errorf("dictionary index type must be integral, not %s", dt)
	}

	return
}

func createMemoTable(mem memory.Allocator, dt arrow.DataType) (ret hashing.MemoTable, err error) {
	switch dt.ID() {
	case arrow.INT8:
		ret = hashing.NewInt8MemoTable(0)
	case arrow.UINT8:
		ret = hashing.NewUint8MemoTable(0)
	case arrow.INT16:
		ret = hashing.NewInt16MemoTable(0)
	case arrow.UINT16:
		ret = hashing.NewUint16MemoTable(0)
	case arrow.INT32:
		ret = hashing.NewInt32MemoTable(0)
	case arrow.UINT32:
		ret = hashing.NewUint32MemoTable(0)
	case arrow.INT64:
		ret = hashing.NewInt64MemoTable(0)
	case arrow.UINT64:
		ret = hashing.NewUint64MemoTable(0)
	case arrow.DURATION, arrow.TIMESTAMP, arrow.DATE64, arrow.TIME64:
		ret = hashing.NewInt64MemoTable(0)
	case arrow.TIME32, arrow.DATE32, arrow.INTERVAL_MONTHS:
		ret = hashing.NewInt32MemoTable(0)
	case arrow.FLOAT16:
		ret = hashing.NewUint16MemoTable(0)
	case arrow.FLOAT32:
		ret = hashing.NewFloat32MemoTable(0)
	case arrow.FLOAT64:
		ret = hashing.NewFloat64MemoTable(0)
	case arrow.BINARY, arrow.FIXED_SIZE_BINARY, arrow.DECIMAL128, arrow.INTERVAL_DAY_TIME, arrow.INTERVAL_MONTH_DAY_NANO:
		ret = hashing.NewBinaryMemoTable(0, 0, NewBinaryBuilder(mem, arrow.BinaryTypes.Binary))
	case arrow.STRING:
		ret = hashing.NewBinaryMemoTable(0, 0, NewBinaryBuilder(mem, arrow.BinaryTypes.String))
	case arrow.NULL:
	default:
		debug.Assert(false, "unimplemented dictionary value type")
		err = fmt.Errorf("unimplemented dictionary value type, %s", dt)
	}

	return
}

type DictionaryBuilder interface {
	Builder

	NewDictionaryArray() *Dictionary
	NewDelta() (indices, delta Interface, err error)
	AppendArray(Interface) error
	ResetFull()
}

type dictionaryBuilder struct {
	builder

	dt          *arrow.DictionaryType
	deltaOffset int
	memoTable   hashing.MemoTable
	idxBuilder  indexBuilder
}

func NewDictionaryBuilderWithDict(mem memory.Allocator, dt *arrow.DictionaryType, init Interface) DictionaryBuilder {
	if init != nil && !arrow.TypeEqual(dt.ValueType, init.DataType()) {
		panic(fmt.Errorf("arrow/array: cannot initialize dictionary type %T with array of type %T", dt.ValueType, init.DataType()))
	}

	idxbldr, err := createIndexBuilder(mem, dt.IndexType.(arrow.FixedWidthDataType))
	if err != nil {
		panic(fmt.Errorf("arrow/array: unsupported builder for index type of %T", dt))
	}

	memo, err := createMemoTable(mem, dt.ValueType)
	if err != nil {
		panic(fmt.Errorf("arrow/array: unsupported builder for value type of %T", dt))
	}

	bldr := dictionaryBuilder{
		builder:    builder{refCount: 1, mem: mem},
		idxBuilder: idxbldr,
		memoTable:  memo,
		dt:         dt,
	}

	switch dt.ValueType.ID() {
	case arrow.NULL:
		ret := &NullDictionaryBuilder{bldr}
		debug.Assert(init == nil, "arrow/array: doesn't make sense to init a null dictionary")
		return ret
	case arrow.UINT8:
		ret := &Uint8DictionaryBuilder{bldr}
		if init != nil {
			if err = ret.InsertDictValues(init.(*Uint8)); err != nil {
				panic(err)
			}
		}
		return ret
	case arrow.INT8:
		ret := &Int8DictionaryBuilder{bldr}
		if init != nil {
			if err = ret.InsertDictValues(init.(*Int8)); err != nil {
				panic(err)
			}
		}
		return ret
	case arrow.UINT16:
		ret := &Uint16DictionaryBuilder{bldr}
		if init != nil {
			if err = ret.InsertDictValues(init.(*Uint16)); err != nil {
				panic(err)
			}
		}
		return ret
	case arrow.INT16:
		ret := &Int16DictionaryBuilder{bldr}
		if init != nil {
			if err = ret.InsertDictValues(init.(*Int16)); err != nil {
				panic(err)
			}
		}
		return ret
	case arrow.UINT32:
		ret := &Uint32DictionaryBuilder{bldr}
		if init != nil {
			if err = ret.InsertDictValues(init.(*Uint32)); err != nil {
				panic(err)
			}
		}
		return ret
	case arrow.INT32:
		ret := &Int32DictionaryBuilder{bldr}
		if init != nil {
			if err = ret.InsertDictValues(init.(*Int32)); err != nil {
				panic(err)
			}
		}
		return ret
	case arrow.UINT64:
		ret := &Uint64DictionaryBuilder{bldr}
		if init != nil {
			if err = ret.InsertDictValues(init.(*Uint64)); err != nil {
				panic(err)
			}
		}
		return ret
	case arrow.INT64:
		ret := &Int64DictionaryBuilder{bldr}
		if init != nil {
			if err = ret.InsertDictValues(init.(*Int64)); err != nil {
				panic(err)
			}
		}
		return ret
	case arrow.FLOAT16:
		ret := &Float16DictionaryBuilder{bldr}
		if init != nil {
			if err = ret.InsertDictValues(init.(*Float16)); err != nil {
				panic(err)
			}
		}
		return ret
	case arrow.FLOAT32:
		ret := &Float32DictionaryBuilder{bldr}
		if init != nil {
			if err = ret.InsertDictValues(init.(*Float32)); err != nil {
				panic(err)
			}
		}
		return ret
	case arrow.FLOAT64:
		ret := &Float64DictionaryBuilder{bldr}
		if init != nil {
			if err = ret.InsertDictValues(init.(*Float64)); err != nil {
				panic(err)
			}
		}
		return ret
	case arrow.STRING:
		ret := &BinaryDictionaryBuilder{bldr}
		if init != nil {
			if err = ret.InsertStringDictValues(init.(*String)); err != nil {
				panic(err)
			}
		}
		return ret
	case arrow.BINARY:
		ret := &BinaryDictionaryBuilder{bldr}
		if init != nil {
			if err = ret.InsertDictValues(init.(*Binary)); err != nil {
				panic(err)
			}
		}
		return ret
	case arrow.FIXED_SIZE_BINARY:
		ret := &FixedSizeBinaryDictionaryBuilder{
			bldr, dt.ValueType.(*arrow.FixedSizeBinaryType).ByteWidth,
		}
		if init != nil {
			if err = ret.InsertDictValues(init.(*FixedSizeBinary)); err != nil {
				panic(err)
			}
		}
		return ret
	case arrow.DATE32:
		ret := &Date32DictionaryBuilder{bldr}
		if init != nil {
			if err = ret.InsertDictValues(init.(*Date32)); err != nil {
				panic(err)
			}
		}
		return ret
	case arrow.DATE64:
		ret := &Date64DictionaryBuilder{bldr}
		if init != nil {
			if err = ret.InsertDictValues(init.(*Date64)); err != nil {
				panic(err)
			}
		}
		return ret
	case arrow.TIMESTAMP:
		ret := &TimestampDictionaryBuilder{bldr}
		if init != nil {
			if err = ret.InsertDictValues(init.(*Timestamp)); err != nil {
				panic(err)
			}
		}
		return ret
	case arrow.TIME32:
		ret := &Time32DictionaryBuilder{bldr}
		if init != nil {
			if err = ret.InsertDictValues(init.(*Time32)); err != nil {
				panic(err)
			}
		}
		return ret
	case arrow.TIME64:
		ret := &Time64DictionaryBuilder{bldr}
		if init != nil {
			if err = ret.InsertDictValues(init.(*Time64)); err != nil {
				panic(err)
			}
		}
		return ret
	case arrow.INTERVAL_MONTHS:
		ret := &MonthIntervalDictionaryBuilder{bldr}
		if init != nil {
			if err = ret.InsertDictValues(init.(*MonthInterval)); err != nil {
				panic(err)
			}
		}
		return ret
	case arrow.INTERVAL_DAY_TIME:
		ret := &DayTimeDictionaryBuilder{bldr}
		if init != nil {
			if err = ret.InsertDictValues(init.(*DayTimeInterval)); err != nil {
				panic(err)
			}
		}
		return ret
	case arrow.DECIMAL128:
		ret := &Decimal128DictionaryBuilder{bldr}
		if init != nil {
			if err = ret.InsertDictValues(init.(*Decimal128)); err != nil {
				panic(err)
			}
		}
		return ret
	case arrow.DECIMAL256:
	case arrow.LIST:
	case arrow.STRUCT:
	case arrow.SPARSE_UNION:
	case arrow.DENSE_UNION:
	case arrow.DICTIONARY:
	case arrow.MAP:
	case arrow.EXTENSION:
	case arrow.FIXED_SIZE_LIST:
	case arrow.DURATION:
		ret := &DurationDictionaryBuilder{bldr}
		if init != nil {
			if err = ret.InsertDictValues(init.(*Duration)); err != nil {
				panic(err)
			}
		}
		return ret
	case arrow.LARGE_STRING:
	case arrow.LARGE_BINARY:
	case arrow.LARGE_LIST:
	case arrow.INTERVAL_MONTH_DAY_NANO:
		ret := &MonthDayNanoDictionaryBuilder{bldr}
		if init != nil {
			if err = ret.InsertDictValues(init.(*MonthDayNanoInterval)); err != nil {
				panic(err)
			}
		}
		return ret
	}

	panic("arrow/array: unimplemented dictionary key type")
}

func NewDictionaryBuilder(mem memory.Allocator, dt *arrow.DictionaryType) DictionaryBuilder {
	return NewDictionaryBuilderWithDict(mem, dt, nil)
}

func (b *dictionaryBuilder) Release() {
	debug.Assert(atomic.LoadInt64(&b.refCount) > 0, "too many releases")

	if atomic.AddInt64(&b.refCount, -1) == 0 {
		b.idxBuilder.Release()
		b.idxBuilder.Builder = nil
		if binmemo, ok := b.memoTable.(*hashing.BinaryMemoTable); ok {
			binmemo.Release()
		}
		b.memoTable = nil
	}
}

func (b *dictionaryBuilder) AppendNull() {
	b.length += 1
	b.nulls += 1
	b.idxBuilder.AppendNull()
}

func (b *dictionaryBuilder) Reserve(n int) {
	b.idxBuilder.Reserve(n)
}

func (b *dictionaryBuilder) Resize(n int) {
	b.idxBuilder.Resize(n)
	b.length = b.idxBuilder.Len()
}

func (b *dictionaryBuilder) ResetFull() {
	b.builder.reset()
	b.idxBuilder.NewArray().Release()
	b.memoTable.Reset()
}

func (b *dictionaryBuilder) Cap() int { return b.idxBuilder.Cap() }

// UnmarshalJSON is not yet implemented for dictionary builders and will always error.
func (b *dictionaryBuilder) UnmarshalJSON([]byte) error {
	return errors.New("unmarshal json to dictionary not yet implemented")
}

func (b *dictionaryBuilder) unmarshal(dec *json.Decoder) error {
	return errors.New("unmarshal json to dictionary not yet implemented")
}

func (b *dictionaryBuilder) unmarshalOne(dec *json.Decoder) error {
	return errors.New("unmarshal json to dictionary not yet implemented")
}

func (b *dictionaryBuilder) NewArray() Interface {
	return b.NewDictionaryArray()
}

func (b *dictionaryBuilder) NewDictionaryArray() *Dictionary {
	a := &Dictionary{}
	a.refCount = 1

	indices, dict, err := b.newWithDictOffset(0)
	if err != nil {
		panic(err)
	}
	defer indices.Release()

	indices.dtype = b.dt
	indices.dictionary = dict
	a.setData(indices)
	return a
}

func (b *dictionaryBuilder) newWithDictOffset(offset int) (indices, dict *Data, err error) {
	idxarr := b.idxBuilder.NewArray()
	defer idxarr.Release()

	indices = idxarr.Data()
	indices.Retain()

	dictBuffers := make([]*memory.Buffer, 2)

	dictLength := b.memoTable.Size() - offset
	dictBuffers[1] = memory.NewResizableBuffer(b.mem)
	defer dictBuffers[1].Release()

	if bintbl, ok := b.memoTable.(*hashing.BinaryMemoTable); ok {
		switch b.dt.ValueType.ID() {
		case arrow.BINARY, arrow.STRING:
			dictBuffers = append(dictBuffers, memory.NewResizableBuffer(b.mem))
			defer dictBuffers[2].Release()

			dictBuffers[1].Resize(arrow.Int32SizeBytes * (dictLength + 1))
			offsets := arrow.Int32Traits.CastFromBytes(dictBuffers[1].Bytes())
			bintbl.CopyOffsetsSubset(offset, offsets)

			valuesz := offsets[len(offsets)-1] - offsets[0]
			dictBuffers[2].Resize(int(valuesz))
			bintbl.CopyValuesSubset(offset, dictBuffers[2].Bytes())
		default: // fixed size
			bw := int(bitutil.BytesForBits(int64(b.dt.ValueType.(arrow.FixedWidthDataType).BitWidth())))
			dictBuffers[1].Resize(dictLength * bw)
			bintbl.CopyFixedWidthValues(offset, bw, dictBuffers[1].Bytes())
		}
	} else {
		dictBuffers[1].Resize(b.memoTable.TypeTraits().BytesRequired(dictLength))
		b.memoTable.WriteOutSubset(offset, dictBuffers[1].Bytes())
	}

	var nullcount int
	if idx, ok := b.memoTable.GetNull(); ok && idx >= offset {
		dictBuffers[0] = memory.NewResizableBuffer(b.mem)
		defer dictBuffers[0].Release()

		nullcount = 1

		dictBuffers[0].Resize(int(bitutil.BytesForBits(int64(dictLength))))
		memory.Set(dictBuffers[0].Bytes(), 0xFF)
		bitutil.ClearBit(dictBuffers[0].Bytes(), idx)
	}

	b.deltaOffset = b.memoTable.Size()
	dict = NewData(b.dt.ValueType, dictLength, dictBuffers, nil, nullcount, 0)
	b.reset()
	return
}

func (b *dictionaryBuilder) NewDelta() (indices, delta Interface, err error) {
	indicesData, deltaData, err := b.newWithDictOffset(b.deltaOffset)
	if err != nil {
		return nil, nil, err
	}

	defer indicesData.Release()
	defer deltaData.Release()
	indices, delta = MakeFromData(indicesData), MakeFromData(deltaData)
	return
}

func (b *dictionaryBuilder) insertDictValue(val interface{}) error {
	_, _, err := b.memoTable.GetOrInsert(val)
	return err
}

func (b *dictionaryBuilder) appendValue(val interface{}) error {
	idx, _, err := b.memoTable.GetOrInsert(val)
	b.idxBuilder.Append(idx)
	b.length += 1
	return err
}

func getvalFn(arr Interface) func(i int) interface{} {
	switch typedarr := arr.(type) {
	case *Int8:
		return func(i int) interface{} { return typedarr.Value(i) }
	case *Uint8:
		return func(i int) interface{} { return typedarr.Value(i) }
	case *Int16:
		return func(i int) interface{} { return typedarr.Value(i) }
	case *Uint16:
		return func(i int) interface{} { return typedarr.Value(i) }
	case *Int32:
		return func(i int) interface{} { return typedarr.Value(i) }
	case *Uint32:
		return func(i int) interface{} { return typedarr.Value(i) }
	case *Int64:
		return func(i int) interface{} { return typedarr.Value(i) }
	case *Uint64:
		return func(i int) interface{} { return typedarr.Value(i) }
	case *Float16:
		return func(i int) interface{} { return typedarr.Value(i).Uint16() }
	case *Float32:
		return func(i int) interface{} { return typedarr.Value(i) }
	case *Float64:
		return func(i int) interface{} { return typedarr.Value(i) }
	case *Duration:
		return func(i int) interface{} { return int64(typedarr.Value(i)) }
	case *Timestamp:
		return func(i int) interface{} { return int64(typedarr.Value(i)) }
	case *Date64:
		return func(i int) interface{} { return int64(typedarr.Value(i)) }
	case *Time64:
		return func(i int) interface{} { return int64(typedarr.Value(i)) }
	case *Time32:
		return func(i int) interface{} { return int32(typedarr.Value(i)) }
	case *Date32:
		return func(i int) interface{} { return int32(typedarr.Value(i)) }
	case *MonthInterval:
		return func(i int) interface{} { return int32(typedarr.Value(i)) }
	case *Binary:
		return func(i int) interface{} { return typedarr.Value(i) }
	case *FixedSizeBinary:
		return func(i int) interface{} { return typedarr.Value(i) }
	case *String:
		return func(i int) interface{} { return typedarr.Value(i) }
	case *Decimal128:
		return func(i int) interface{} {
			val := typedarr.Value(i)
			return (*(*[arrow.Decimal128SizeBytes]byte)(unsafe.Pointer(&val)))[:]
		}
	case *DayTimeInterval:
		return func(i int) interface{} {
			val := typedarr.Value(i)
			return (*(*[arrow.DayTimeIntervalSizeBytes]byte)(unsafe.Pointer(&val)))[:]
		}
	case *MonthDayNanoInterval:
		return func(i int) interface{} {
			val := typedarr.Value(i)
			return (*(*[arrow.MonthDayNanoIntervalSizeBytes]byte)(unsafe.Pointer(&val)))[:]
		}
	}

	panic("arrow/array: invalid dictionary value type")
}

func (b *dictionaryBuilder) AppendArray(arr Interface) error {
	debug.Assert(arrow.TypeEqual(b.dt.ValueType, arr.DataType()), "wrong value type of array to append to dict")

	valfn := getvalFn(arr)
	for i := 0; i < arr.Len(); i++ {
		if arr.IsNull(i) {
			b.AppendNull()
		} else {
			if err := b.appendValue(valfn(i)); err != nil {
				return err
			}
		}
	}
	return nil
}

type NullDictionaryBuilder struct {
	dictionaryBuilder
}

func (b *NullDictionaryBuilder) NewArray() Interface {
	return b.NewDictionaryArray()
}

func (b *NullDictionaryBuilder) NewDictionaryArray() *Dictionary {
	idxarr := b.idxBuilder.NewArray()
	defer idxarr.Release()

	out := idxarr.Data()
	dictarr := NewNull(0)
	defer dictarr.Release()

	dictarr.data.Retain()
	out.dtype = b.dt
	out.dictionary = dictarr.data

	return NewDictionaryData(out)
}

func (b *NullDictionaryBuilder) AppendArray(arr Interface) error {
	if arr.DataType().ID() != arrow.NULL {
		return fmt.Errorf("cannot append non-null array to null dictionary")
	}

	for i := 0; i < arr.(*Null).Len(); i++ {
		b.AppendNull()
	}
	return nil
}

type Int8DictionaryBuilder struct {
	dictionaryBuilder
}

func (b *Int8DictionaryBuilder) Append(v int8) error { return b.appendValue(v) }
func (b *Int8DictionaryBuilder) InsertDictValues(arr *Int8) (err error) {
	for _, v := range arr.values {
		if err = b.insertDictValue(v); err != nil {
			break
		}
	}
	return
}

type Uint8DictionaryBuilder struct {
	dictionaryBuilder
}

func (b *Uint8DictionaryBuilder) Append(v uint8) error { return b.appendValue(v) }
func (b *Uint8DictionaryBuilder) InsertDictValues(arr *Uint8) (err error) {
	for _, v := range arr.values {
		if err = b.insertDictValue(v); err != nil {
			break
		}
	}
	return
}

type Int16DictionaryBuilder struct {
	dictionaryBuilder
}

func (b *Int16DictionaryBuilder) Append(v int16) error { return b.appendValue(v) }
func (b *Int16DictionaryBuilder) InsertDictValues(arr *Int16) (err error) {
	for _, v := range arr.values {
		if err = b.insertDictValue(v); err != nil {
			break
		}
	}
	return
}

type Uint16DictionaryBuilder struct {
	dictionaryBuilder
}

func (b *Uint16DictionaryBuilder) Append(v uint16) error { return b.appendValue(v) }
func (b *Uint16DictionaryBuilder) InsertDictValues(arr *Uint16) (err error) {
	for _, v := range arr.values {
		if err = b.insertDictValue(v); err != nil {
			break
		}
	}
	return
}

type Int32DictionaryBuilder struct {
	dictionaryBuilder
}

func (b *Int32DictionaryBuilder) Append(v int32) error { return b.appendValue(v) }
func (b *Int32DictionaryBuilder) InsertDictValues(arr *Int32) (err error) {
	for _, v := range arr.values {
		if err = b.insertDictValue(v); err != nil {
			break
		}
	}
	return
}

type Uint32DictionaryBuilder struct {
	dictionaryBuilder
}

func (b *Uint32DictionaryBuilder) Append(v uint32) error { return b.appendValue(v) }
func (b *Uint32DictionaryBuilder) InsertDictValues(arr *Uint32) (err error) {
	for _, v := range arr.values {
		if err = b.insertDictValue(v); err != nil {
			break
		}
	}
	return
}

type Int64DictionaryBuilder struct {
	dictionaryBuilder
}

func (b *Int64DictionaryBuilder) Append(v int64) error { return b.appendValue(v) }
func (b *Int64DictionaryBuilder) InsertDictValues(arr *Int64) (err error) {
	for _, v := range arr.values {
		if err = b.insertDictValue(v); err != nil {
			break
		}
	}
	return
}

type Uint64DictionaryBuilder struct {
	dictionaryBuilder
}

func (b *Uint64DictionaryBuilder) Append(v uint64) error { return b.appendValue(v) }
func (b *Uint64DictionaryBuilder) InsertDictValues(arr *Uint64) (err error) {
	for _, v := range arr.values {
		if err = b.insertDictValue(v); err != nil {
			break
		}
	}
	return
}

type DurationDictionaryBuilder struct {
	dictionaryBuilder
}

func (b *DurationDictionaryBuilder) Append(v arrow.Duration) error { return b.appendValue(int64(v)) }
func (b *DurationDictionaryBuilder) InsertDictValues(arr *Duration) (err error) {
	for _, v := range arr.values {
		if err = b.insertDictValue(int64(v)); err != nil {
			break
		}
	}
	return
}

type TimestampDictionaryBuilder struct {
	dictionaryBuilder
}

func (b *TimestampDictionaryBuilder) Append(v arrow.Timestamp) error { return b.appendValue(int64(v)) }
func (b *TimestampDictionaryBuilder) InsertDictValues(arr *Timestamp) (err error) {
	for _, v := range arr.values {
		if err = b.insertDictValue(int64(v)); err != nil {
			break
		}
	}
	return
}

type Time32DictionaryBuilder struct {
	dictionaryBuilder
}

func (b *Time32DictionaryBuilder) Append(v arrow.Time32) error { return b.appendValue(int32(v)) }
func (b *Time32DictionaryBuilder) InsertDictValues(arr *Time32) (err error) {
	for _, v := range arr.values {
		if err = b.insertDictValue(int32(v)); err != nil {
			break
		}
	}
	return
}

type Time64DictionaryBuilder struct {
	dictionaryBuilder
}

func (b *Time64DictionaryBuilder) Append(v arrow.Time64) error { return b.appendValue(int64(v)) }
func (b *Time64DictionaryBuilder) InsertDictValues(arr *Time64) (err error) {
	for _, v := range arr.values {
		if err = b.insertDictValue(int64(v)); err != nil {
			break
		}
	}
	return
}

type Date32DictionaryBuilder struct {
	dictionaryBuilder
}

func (b *Date32DictionaryBuilder) Append(v arrow.Date32) error { return b.appendValue(int32(v)) }
func (b *Date32DictionaryBuilder) InsertDictValues(arr *Date32) (err error) {
	for _, v := range arr.values {
		if err = b.insertDictValue(int32(v)); err != nil {
			break
		}
	}
	return
}

type Date64DictionaryBuilder struct {
	dictionaryBuilder
}

func (b *Date64DictionaryBuilder) Append(v arrow.Date64) error { return b.appendValue(int64(v)) }
func (b *Date64DictionaryBuilder) InsertDictValues(arr *Date64) (err error) {
	for _, v := range arr.values {
		if err = b.insertDictValue(int64(v)); err != nil {
			break
		}
	}
	return
}

type MonthIntervalDictionaryBuilder struct {
	dictionaryBuilder
}

func (b *MonthIntervalDictionaryBuilder) Append(v arrow.MonthInterval) error {
	return b.appendValue(int32(v))
}
func (b *MonthIntervalDictionaryBuilder) InsertDictValues(arr *MonthInterval) (err error) {
	for _, v := range arr.values {
		if err = b.insertDictValue(int32(v)); err != nil {
			break
		}
	}
	return
}

type Float16DictionaryBuilder struct {
	dictionaryBuilder
}

func (b *Float16DictionaryBuilder) Append(v float16.Num) error { return b.appendValue(v.Uint16()) }
func (b *Float16DictionaryBuilder) InsertDictValues(arr *Float16) (err error) {
	for _, v := range arr.values {
		if err = b.insertDictValue(v.Uint16()); err != nil {
			break
		}
	}
	return
}

type Float32DictionaryBuilder struct {
	dictionaryBuilder
}

func (b *Float32DictionaryBuilder) Append(v float32) error { return b.appendValue(v) }
func (b *Float32DictionaryBuilder) InsertDictValues(arr *Float32) (err error) {
	for _, v := range arr.values {
		if err = b.insertDictValue(v); err != nil {
			break
		}
	}
	return
}

type Float64DictionaryBuilder struct {
	dictionaryBuilder
}

func (b *Float64DictionaryBuilder) Append(v float64) error { return b.appendValue(v) }
func (b *Float64DictionaryBuilder) InsertDictValues(arr *Float64) (err error) {
	for _, v := range arr.values {
		if err = b.insertDictValue(v); err != nil {
			break
		}
	}
	return
}

type BinaryDictionaryBuilder struct {
	dictionaryBuilder
}

func (b *BinaryDictionaryBuilder) Append(v []byte) error {
	if v == nil {
		b.AppendNull()
		return nil
	}
	return b.appendValue(v)
}
func (b *BinaryDictionaryBuilder) AppendString(v string) error { return b.appendValue(v) }
func (b *BinaryDictionaryBuilder) InsertDictValues(arr *Binary) (err error) {
	if !arrow.TypeEqual(arr.DataType(), b.dt.ValueType) {
		return fmt.Errorf("dictionary insert type mismatch: cannot insert values of type %T to dictionary type %T", arr.DataType(), b.dt.ValueType)
	}

	for i := 0; i < arr.Len(); i++ {
		if err = b.insertDictValue(arr.Value(i)); err != nil {
			break
		}
	}
	return
}
func (b *BinaryDictionaryBuilder) InsertStringDictValues(arr *String) (err error) {
	if !arrow.TypeEqual(arr.DataType(), b.dt.ValueType) {
		return fmt.Errorf("dictionary insert type mismatch: cannot insert values of type %T to dictionary type %T", arr.DataType(), b.dt.ValueType)
	}

	for i := 0; i < arr.Len(); i++ {
		if err = b.insertDictValue(arr.Value(i)); err != nil {
			break
		}
	}
	return
}

type FixedSizeBinaryDictionaryBuilder struct {
	dictionaryBuilder
	byteWidth int
}

func (b *FixedSizeBinaryDictionaryBuilder) Append(v []byte) error {
	return b.appendValue(v[:b.byteWidth])
}
func (b *FixedSizeBinaryDictionaryBuilder) InsertDictValues(arr *FixedSizeBinary) (err error) {
	var (
		beg = arr.array.data.offset * b.byteWidth
		end = (arr.array.data.offset + arr.data.length) * b.byteWidth
	)
	data := arr.valueBytes[beg:end]
	for len(data) > 0 {
		if err = b.insertDictValue(data[:b.byteWidth]); err != nil {
			break
		}
		data = data[b.byteWidth:]
	}
	return
}

type Decimal128DictionaryBuilder struct {
	dictionaryBuilder
}

func (b *Decimal128DictionaryBuilder) Append(v decimal128.Num) error {
	return b.appendValue((*(*[arrow.Decimal128SizeBytes]byte)(unsafe.Pointer(&v)))[:])
}
func (b *Decimal128DictionaryBuilder) InsertDictValues(arr *Decimal128) (err error) {
	data := arrow.Decimal128Traits.CastToBytes(arr.values)
	for len(data) > 0 {
		if err = b.insertDictValue(data[:arrow.Decimal128SizeBytes]); err != nil {
			break
		}
		data = data[arrow.Decimal128SizeBytes:]
	}
	return
}

type MonthDayNanoDictionaryBuilder struct {
	dictionaryBuilder
}

func (b *MonthDayNanoDictionaryBuilder) Append(v arrow.MonthDayNanoInterval) error {
	return b.appendValue((*(*[arrow.MonthDayNanoIntervalSizeBytes]byte)(unsafe.Pointer(&v)))[:])
}
func (b *MonthDayNanoDictionaryBuilder) InsertDictValues(arr *MonthDayNanoInterval) (err error) {
	data := arrow.MonthDayNanoIntervalTraits.CastToBytes(arr.values)
	for len(data) > 0 {
		if err = b.insertDictValue(data[:arrow.MonthDayNanoIntervalSizeBytes]); err != nil {
			break
		}
		data = data[arrow.MonthDayNanoIntervalSizeBytes:]
	}
	return
}

type DayTimeDictionaryBuilder struct {
	dictionaryBuilder
}

func (b *DayTimeDictionaryBuilder) Append(v arrow.DayTimeInterval) error {
	return b.appendValue((*(*[arrow.DayTimeIntervalSizeBytes]byte)(unsafe.Pointer(&v)))[:])
}
func (b *DayTimeDictionaryBuilder) InsertDictValues(arr *DayTimeInterval) (err error) {
	data := arrow.DayTimeIntervalTraits.CastToBytes(arr.values)
	for len(data) > 0 {
		if err = b.insertDictValue(data[:arrow.DayTimeIntervalSizeBytes]); err != nil {
			break
		}
		data = data[arrow.DayTimeIntervalSizeBytes:]
	}
	return
}

var (
	_ Interface = (*Dictionary)(nil)
	_ Builder   = (*dictionaryBuilder)(nil)
)
