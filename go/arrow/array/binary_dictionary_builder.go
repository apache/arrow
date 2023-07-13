package array

import (
	"bytes"
	"fmt"
	"sync/atomic"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/internal/debug"
	"github.com/apache/arrow/go/v13/internal/hashing"
	"github.com/apache/arrow/go/v13/internal/json"
)

type binaryDictionaryBuilder struct {
	builder

	dt          *arrow.DictionaryType
	deltaOffset int
	memoTable   *hashing.BinaryMemoTable
	idxBuilder  indexBuilder
}

func (b *binaryDictionaryBuilder) Type() arrow.DataType { return b.dt }

func (b *binaryDictionaryBuilder) Release() {
	debug.Assert(atomic.LoadInt64(&b.refCount) > 0, "too many releases")

	if atomic.AddInt64(&b.refCount, -1) == 0 {
		b.idxBuilder.Release()
		b.idxBuilder.Builder = nil
		b.memoTable.Release()
		b.memoTable = nil
	}
}

func (b *binaryDictionaryBuilder) AppendNull() {
	b.length += 1
	b.nulls += 1
	b.idxBuilder.AppendNull()
}

func (b *binaryDictionaryBuilder) AppendNulls(n int) {
	for i := 0; i < n; i++ {
		b.AppendNull()
	}
}

func (b *binaryDictionaryBuilder) AppendEmptyValue() {
	b.length += 1
	b.idxBuilder.AppendEmptyValue()
}

func (b *binaryDictionaryBuilder) AppendEmptyValues(n int) {
	for i := 0; i < n; i++ {
		b.AppendEmptyValue()
	}
}

func (b *binaryDictionaryBuilder) Reserve(n int) {
	b.idxBuilder.Reserve(n)
}

func (b *binaryDictionaryBuilder) Resize(n int) {
	b.idxBuilder.Resize(n)
	b.length = b.idxBuilder.Len()
}

func (b *binaryDictionaryBuilder) ResetFull() {
	b.builder.reset()
	b.idxBuilder.NewArray().Release()
	b.memoTable.Reset()
}

func (b *binaryDictionaryBuilder) Cap() int { return b.idxBuilder.Cap() }

func (b *binaryDictionaryBuilder) IsNull(i int) bool { return b.idxBuilder.IsNull(i) }

func (b *binaryDictionaryBuilder) UnmarshalJSON(data []byte) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	t, err := dec.Token()
	if err != nil {
		return err
	}

	if delim, ok := t.(json.Delim); !ok || delim != '[' {
		return fmt.Errorf("dictionary builder must upack from json array, found %s", delim)
	}

	return b.Unmarshal(dec)
}

func (b *binaryDictionaryBuilder) Unmarshal(dec *json.Decoder) error {
	bldr := NewBuilder(b.mem, b.dt.ValueType)
	defer bldr.Release()

	if err := bldr.Unmarshal(dec); err != nil {
		return err
	}

	arr := bldr.NewArray()
	defer arr.Release()
	return b.AppendArray(arr)
}

func (b *binaryDictionaryBuilder) AppendValueFromString(s string) error {
	bldr := NewBuilder(b.mem, b.dt.ValueType)
	defer bldr.Release()

	if err := bldr.AppendValueFromString(s); err != nil {
		return err
	}

	arr := bldr.NewArray()
	defer arr.Release()
	return b.AppendArray(arr)
}

func (b *binaryDictionaryBuilder) UnmarshalOne(dec *json.Decoder) error {
	bldr := NewBuilder(b.mem, b.dt.ValueType)
	defer bldr.Release()

	if err := bldr.UnmarshalOne(dec); err != nil {
		return err
	}

	arr := bldr.NewArray()
	defer arr.Release()
	return b.AppendArray(arr)
}

func (b *binaryDictionaryBuilder) NewArray() arrow.Array {
	return b.NewDictionaryArray()
}

func (b *binaryDictionaryBuilder) newData() *Data {
	indices, dict, err := b.newWithDictOffset(0)
	if err != nil {
		panic(err)
	}

	indices.dtype = b.dt
	indices.dictionary = dict
	return indices
}

func (b *binaryDictionaryBuilder) NewDictionaryArray() *Dictionary {
	a := &Dictionary{}
	a.refCount = 1

	indices := b.newData()
	a.setData(indices)
	indices.Release()
	return a
}

func (b *binaryDictionaryBuilder) newWithDictOffset(offset int) (indices, dict *Data, err error) {
	idxarr := b.idxBuilder.NewArray()
	defer idxarr.Release()

	indices = idxarr.Data().(*Data)
	indices.Retain()

	b.deltaOffset = b.memoTable.Size()
	dict, err = GetBinaryDictArrayData(b.mem, b.dt.ValueType, b.memoTable, offset)
	b.reset()
	return
}

// NewDelta returns the dictionary indices and a delta dictionary since the
// last time NewArray or NewDictionaryArray were called, and resets the state
// of the builder (except for the dictionary / memotable)
func (b *binaryDictionaryBuilder) NewDelta() (indices, delta arrow.Array, err error) {
	indicesData, deltaData, err := b.newWithDictOffset(b.deltaOffset)
	if err != nil {
		return nil, nil, err
	}

	defer indicesData.Release()
	defer deltaData.Release()
	indices, delta = MakeFromData(indicesData), MakeFromData(deltaData)
	return
}

func (b *binaryDictionaryBuilder) insertDictValue(val []byte) error {
	_, _, err := b.memoTable.GetOrInsert(val)
	return err
}

func (b *binaryDictionaryBuilder) appendValue(val []byte) error {
	idx, _, err := b.memoTable.GetOrInsert(val)
	b.idxBuilder.Append(idx)
	b.length += 1
	return err
}

func (b *binaryDictionaryBuilder) AppendArray(arr arrow.Array) error {
	debug.Assert(arrow.TypeEqual(b.dt.ValueType, arr.DataType()), "wrong value type of array to append to dict")

	valfn := getByteValFn(arr)
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

func (b *binaryDictionaryBuilder) AppendIndices(indices []int, valid []bool) {
	b.length += len(indices)
	switch idxbldr := b.idxBuilder.Builder.(type) {
	case *Int8Builder:
		vals := make([]int8, len(indices))
		for i, v := range indices {
			vals[i] = int8(v)
		}
		idxbldr.AppendValues(vals, valid)
	case *Int16Builder:
		vals := make([]int16, len(indices))
		for i, v := range indices {
			vals[i] = int16(v)
		}
		idxbldr.AppendValues(vals, valid)
	case *Int32Builder:
		vals := make([]int32, len(indices))
		for i, v := range indices {
			vals[i] = int32(v)
		}
		idxbldr.AppendValues(vals, valid)
	case *Int64Builder:
		vals := make([]int64, len(indices))
		for i, v := range indices {
			vals[i] = int64(v)
		}
		idxbldr.AppendValues(vals, valid)
	case *Uint8Builder:
		vals := make([]uint8, len(indices))
		for i, v := range indices {
			vals[i] = uint8(v)
		}
		idxbldr.AppendValues(vals, valid)
	case *Uint16Builder:
		vals := make([]uint16, len(indices))
		for i, v := range indices {
			vals[i] = uint16(v)
		}
		idxbldr.AppendValues(vals, valid)
	case *Uint32Builder:
		vals := make([]uint32, len(indices))
		for i, v := range indices {
			vals[i] = uint32(v)
		}
		idxbldr.AppendValues(vals, valid)
	case *Uint64Builder:
		vals := make([]uint64, len(indices))
		for i, v := range indices {
			vals[i] = uint64(v)
		}
		idxbldr.AppendValues(vals, valid)
	}
}
