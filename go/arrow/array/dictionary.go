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
	"fmt"
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
	dictType *arrow.DictionaryType
	indices  Interface
	dict     Interface
}

func NewDictionaryArray(typ *arrow.DictionaryType, indices, dict Interface) *Dictionary {
	a := &Dictionary{dictType: typ}
	a.array.refCount = 1
	dictdata := NewData(indices.DataType(), indices.Len(), indices.Data().buffers, indices.Data().childData, indices.NullN(), indices.Data().offset)
	dictdata.dictionary = dict.Data()
	dict.Data().Retain()

	defer dictdata.Release()
	a.setData(dictdata)
	return a
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

	if d.dictType == nil {
		d.dictType = &arrow.DictionaryType{
			IndexType: data.dtype.(arrow.FixedWidthDataType),
			ValueType: data.dictionary.DataType(),
			Ordered:   false, // assume not ordered
		}
	}

	debug.Assert(arrow.TypeEqual(d.dictType.IndexType, data.dtype), "mismatched dictionary index types")
	debug.Assert(arrow.TypeEqual(d.dictType.ValueType, data.dictionary.DataType()), "mismatched dictionary value types")

	indexData := NewData(data.dtype, data.length, data.buffers, data.childData, data.nulls, data.offset)
	defer indexData.Release()
	d.indices = MakeFromData(indexData)

	d.data.dtype = d.dictType
}

// Dictionary returns the values array that makes up the dictionary for this
// array. The returned array needs to be explicitly released by calling Release.
func (d *Dictionary) Dictionary() Interface {
	if d.dict == nil {
		d.dict = MakeFromData(d.data.dictionary)
	}
	d.dict.Retain()
	return d.dict
}

// Indices returns a reference to the underlying Array of indices as its own array which needs to be
// manually released accordingly by calling Release on it.
func (d *Dictionary) Indices() Interface {
	d.indices.Retain()
	return d.indices
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
	ldict, rdict := l.Dictionary(), r.Dictionary()
	defer ldict.Release()
	defer rdict.Release()

	return ArrayEqual(ldict, rdict) && ArrayEqual(l.indices, r.indices)
}

func arrayApproxEqualDict(l, r *Dictionary, opt equalOption) bool {
	ldict, rdict := l.Dictionary(), r.Dictionary()
	defer ldict.Release()
	defer rdict.Release()

	return arrayApproxEqual(ldict, rdict, opt) && arrayApproxEqual(l.indices, r.indices, opt)
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
	default:
		debug.Assert(false, "unimplemented dictionary value type")
		err = fmt.Errorf("unimplemented dictionary value type, %s", dt)
	}

	return
}

type DictionaryBuilder struct {
	builder

	dt         *arrow.DictionaryType
	memoTable  hashing.MemoTable
	idxBuilder indexBuilder
}

func NewDictionaryBuilder(mem memory.Allocator, dt *arrow.DictionaryType) Builder {
	idxbldr, err := createIndexBuilder(mem, dt.IndexType)
	if err != nil {
		panic(fmt.Errorf("arrow/array: unsupported builder for index type of %T", dt))
	}

	memo, err := createMemoTable(mem, dt.ValueType)
	if err != nil {
		panic(fmt.Errorf("arrow/array: unsupported builder for value type of %T", dt))
	}

	bldr := DictionaryBuilder{
		builder:    builder{refCount: 1, mem: mem},
		idxBuilder: idxbldr,
		memoTable:  memo,
		dt:         dt,
	}

	switch dt.ValueType.ID() {
	case arrow.UINT8:
		return &Uint8DictionaryBuilder{bldr}
	case arrow.INT8:
		return &Int8DictionaryBuilder{bldr}
	case arrow.UINT16:
		return &Uint16DictionaryBuilder{bldr}
	case arrow.INT16:
		return &Int16DictionaryBuilder{bldr}
	case arrow.UINT32:
		return &Uint32DictionaryBuilder{bldr}
	case arrow.INT32:
		return &Int32DictionaryBuilder{bldr}
	case arrow.UINT64:
		return &Uint64DictionaryBuilder{bldr}
	case arrow.INT64:
		return &Int64DictionaryBuilder{bldr}
	case arrow.FLOAT16:
		return &Float16DictionaryBuilder{bldr}
	case arrow.FLOAT32:
		return &Float32DictionaryBuilder{bldr}
	case arrow.FLOAT64:
		return &Float64DictionaryBuilder{bldr}
	case arrow.STRING:
		return &BinaryDictionaryBuilder{bldr}
	case arrow.BINARY:
		return &BinaryDictionaryBuilder{bldr}
	case arrow.FIXED_SIZE_BINARY:
		return &FixedSizeBinaryDictionaryBuilder{
			bldr, dt.ValueType.(*arrow.FixedSizeBinaryType).ByteWidth,
		}
	case arrow.DATE32:
		return &Date32DictionaryBuilder{bldr}
	case arrow.DATE64:
		return &Date64DictionaryBuilder{bldr}
	case arrow.TIMESTAMP:
		return &TimestampDictionaryBuilder{bldr}
	case arrow.TIME32:
		return &Time32DictionaryBuilder{bldr}
	case arrow.TIME64:
		return &Time64DictionaryBuilder{bldr}
	case arrow.INTERVAL_MONTHS:
		return &MonthIntervalDictionaryBuilder{bldr}
	case arrow.INTERVAL_DAY_TIME:
		return &DayTimeDictionaryBuilder{bldr}
	case arrow.DECIMAL128:
		return &Decimal128DictionaryBuilder{bldr}
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
		return &DurationDictionaryBuilder{bldr}
	case arrow.LARGE_STRING:
	case arrow.LARGE_BINARY:
	case arrow.LARGE_LIST:
	case arrow.INTERVAL_MONTH_DAY_NANO:
		return &MonthDayNanoDictionaryBuilder{bldr}
	}

	panic("arrow/array: unimplemented dictionary key type")
}

func (b *DictionaryBuilder) Release() {
	debug.Assert(atomic.LoadInt64(&b.refCount) > 0, "too many releases")

	if atomic.AddInt64(&b.refCount, -1) == 0 {
		b.idxBuilder.Release()
		b.idxBuilder.Builder = nil
		b.memoTable = nil
	}
}

func (b *DictionaryBuilder) AppendNull() {
	b.length += 1
	b.nulls += 1
	b.idxBuilder.AppendNull()
}

func (b *DictionaryBuilder) Reserve(n int) {
	b.idxBuilder.Reserve(n)
}

func (b *DictionaryBuilder) Resize(n int) {
	b.idxBuilder.Resize(n)
	b.length = b.idxBuilder.Len()
}

func (b *DictionaryBuilder) Cap() int { return b.idxBuilder.Cap() }

func (b *DictionaryBuilder) UnmarshalJSON([]byte) error { return nil }

func (b *DictionaryBuilder) unmarshal(dec *json.Decoder) error { return nil }

func (b *DictionaryBuilder) unmarshalOne(dec *json.Decoder) error { return nil }

func (b *DictionaryBuilder) NewArray() Interface {
	a := &Dictionary{dictType: b.dt}
	a.refCount = 1

	indices, dict, err := b.NewWithDictOffset(0)
	if err != nil {
		panic(err)
	}
	defer indices.Release()

	indices.dictionary = dict
	a.setData(indices)
	return a
}

func (b *DictionaryBuilder) NewWithDictOffset(offset int) (indices, dict *Data, err error) {
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

	dict = NewData(b.dt.ValueType, dictLength, dictBuffers, nil, nullcount, 0)
	return
}

func (b *DictionaryBuilder) insertDictValue(val interface{}) error {
	_, _, err := b.memoTable.GetOrInsert(val)
	return err
}

func (b *DictionaryBuilder) appendValue(val interface{}) error {
	if val == nil {
		b.memoTable.GetOrInsertNull()
		b.AppendNull()
		return nil
	}
	idx, _, err := b.memoTable.GetOrInsert(val)
	b.idxBuilder.Append(idx)
	b.length += 1
	return err
}

type Int8DictionaryBuilder struct {
	DictionaryBuilder
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
	DictionaryBuilder
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
	DictionaryBuilder
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
	DictionaryBuilder
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
	DictionaryBuilder
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
	DictionaryBuilder
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
	DictionaryBuilder
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
	DictionaryBuilder
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
	DictionaryBuilder
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
	DictionaryBuilder
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
	DictionaryBuilder
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
	DictionaryBuilder
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
	DictionaryBuilder
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
	DictionaryBuilder
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
	DictionaryBuilder
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
	DictionaryBuilder
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
	DictionaryBuilder
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
	DictionaryBuilder
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
	DictionaryBuilder
}

func (b *BinaryDictionaryBuilder) Append(v []byte) error       { return b.appendValue(v) }
func (b *BinaryDictionaryBuilder) AppendString(v string) error { return b.appendValue(v) }
func (b *BinaryDictionaryBuilder) InsertDictValues(arr *Binary) (err error) {
	for i := 0; i < arr.Len(); i++ {
		if err = b.insertDictValue(arr.Value(i)); err != nil {
			break
		}
	}
	return
}
func (b *BinaryDictionaryBuilder) InsertStringDictValues(arr *String) (err error) {
	for i := 0; i < arr.Len(); i++ {
		if err = b.insertDictValue(arr.Value(i)); err != nil {
			break
		}
	}
	return
}

type FixedSizeBinaryDictionaryBuilder struct {
	DictionaryBuilder
	byteWidth int
}

func (b *FixedSizeBinaryDictionaryBuilder) Append(v []byte) error {
	return b.appendValue(v[:b.byteWidth])
}
func (b *FixedSizeBinaryDictionaryBuilder) InsertDictValues(arr *FixedSizeBinary) (err error) {
	var (
		beg = arr.array.data.offset * b.byteWidth
		end = (arr.array.data.offset + arr.data.length + 1) * b.byteWidth
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
	DictionaryBuilder
}

func (b *Decimal128DictionaryBuilder) Append(v decimal128.Num) error {
	var data [16]byte
	return b.appendValue(v.BigInt().FillBytes(data[:]))
}
func (b *Decimal128DictionaryBuilder) InsertDictValues(arr *Decimal128) (err error) {
	data := arrow.Decimal128Traits.CastToBytes(arr.values)
	for len(data) > 0 {
		if err = b.insertDictValue(data[:16]); err != nil {
			break
		}
		data = data[16:]
	}
	return
}

type MonthDayNanoDictionaryBuilder struct {
	DictionaryBuilder
}

func (b *MonthDayNanoDictionaryBuilder) Append(v arrow.MonthDayNanoInterval) error {
	return b.appendValue((*(*[16]byte)(unsafe.Pointer(&v)))[:])
}
func (b *MonthDayNanoDictionaryBuilder) InsertDictValues(arr *MonthDayNanoInterval) (err error) {
	data := arrow.MonthDayNanoIntervalTraits.CastToBytes(arr.values)
	for len(data) > 0 {
		if err = b.insertDictValue(data[:16]); err != nil {
			break
		}
		data = data[16:]
	}
	return
}

type DayTimeDictionaryBuilder struct {
	DictionaryBuilder
}

func (b *DayTimeDictionaryBuilder) Append(v arrow.DayTimeInterval) error {
	return b.appendValue((*(*[8]byte)(unsafe.Pointer(&v)))[:])
}
func (b *DayTimeDictionaryBuilder) InsertDictValues(arr *DayTimeInterval) (err error) {
	data := arrow.DayTimeIntervalTraits.CastToBytes(arr.values)
	for len(data) > 0 {
		if err = b.insertDictValue(data[:8]); err != nil {
			break
		}
		data = data[8:]
	}
	return
}

var (
	_ Interface = (*Dictionary)(nil)
	_ Builder   = (*DictionaryBuilder)(nil)
)
