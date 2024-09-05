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
	"reflect"
	"strings"
	"sync/atomic"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/bitutil"
	"github.com/apache/arrow/go/v18/arrow/decimal"
	"github.com/apache/arrow/go/v18/arrow/decimal128"
	"github.com/apache/arrow/go/v18/arrow/decimal256"
	"github.com/apache/arrow/go/v18/arrow/internal/debug"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/apache/arrow/go/v18/internal/json"
)

type decimalTypes interface {
	decimal.Decimal32 | decimal.Decimal64 | decimal.Decimal128 | decimal.Decimal256
}

type baseDecimal[T interface {
	decimalTypes
	decimal.Num[T]
}] struct {
	array

	values []T
}

func newDecimalData[T interface {
	decimalTypes
	decimal.Num[T]
}](data arrow.ArrayData) *baseDecimal[T] {
	a := &baseDecimal[T]{}
	a.refCount = 1
	a.setData(data.(*Data))
	return a
}

func (a *baseDecimal[T]) Value(i int) T { return a.values[i] }

func (a *baseDecimal[T]) ValueStr(i int) string {
	if a.IsNull(i) {
		return NullValueStr
	}
	return a.GetOneForMarshal(i).(string)
}

func (a *baseDecimal[T]) Values() []T { return a.values }

func (a *baseDecimal[T]) String() string {
	o := new(strings.Builder)
	o.WriteString("[")
	for i := 0; i < a.Len(); i++ {
		if i > 0 {
			fmt.Fprintf(o, " ")
		}
		switch {
		case a.IsNull(i):
			o.WriteString(NullValueStr)
		default:
			fmt.Fprintf(o, "%v", a.Value(i))
		}
	}
	o.WriteString("]")
	return o.String()
}

func (a *baseDecimal[T]) setData(data *Data) {
	a.array.setData(data)
	vals := data.buffers[1]
	if vals != nil {
		a.values = arrow.GetData[T](vals.Bytes())
		beg := a.array.data.offset
		end := beg + a.array.data.length
		a.values = a.values[beg:end]
	}
}

func (a *baseDecimal[T]) GetOneForMarshal(i int) any {
	if a.IsNull(i) {
		return nil
	}

	typ := a.DataType().(arrow.DecimalType)
	n, scale := a.Value(i), typ.GetScale()
	return n.ToBigFloat(scale).Text('g', int(typ.GetPrecision()))
}

func (a *baseDecimal[T]) MarshalJSON() ([]byte, error) {
	vals := make([]any, a.Len())
	for i := 0; i < a.Len(); i++ {
		vals[i] = a.GetOneForMarshal(i)
	}
	return json.Marshal(vals)
}

func arrayEqualDecimal[T interface {
	decimalTypes
	decimal.Num[T]
}](left, right *baseDecimal[T]) bool {
	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) {
			continue
		}

		if left.Value(i) != right.Value(i) {
			return false
		}
	}
	return true
}

type Decimal32 = baseDecimal[decimal.Decimal32]

func NewDecimal32Data(data arrow.ArrayData) *Decimal32 {
	return newDecimalData[decimal.Decimal32](data)
}

type Decimal64 = baseDecimal[decimal.Decimal64]

func NewDecimal64Data(data arrow.ArrayData) *Decimal64 {
	return newDecimalData[decimal.Decimal64](data)
}

type Decimal128 = baseDecimal[decimal.Decimal128]

func NewDecimal128Data(data arrow.ArrayData) *Decimal128 {
	return newDecimalData[decimal.Decimal128](data)
}

type Decimal256 = baseDecimal[decimal.Decimal256]

func NewDecimal256Data(data arrow.ArrayData) *Decimal256 {
	return newDecimalData[decimal.Decimal256](data)
}

type decimalTraits[T decimalTypes] interface {
	bytesRequired(int) int
	valFromString(string, int32, int32) (T, error)
	valFromFloat64(float64, int32, int32) (T, error)
}

type dec32Traits struct{}

func (dec32Traits) bytesRequired(n int) int { return arrow.Decimal32Traits.BytesRequired(n) }
func (dec32Traits) valFromString(v string, prec int32, scale int32) (decimal.Decimal32, error) {
	return decimal.Decimal32FromString(v, prec, scale)
}
func (dec32Traits) valFromFloat64(v float64, prec, scale int32) (decimal.Decimal32, error) {
	return decimal.Decimal32FromFloat(v, prec, scale)
}

type dec64Traits struct{}

func (dec64Traits) bytesRequired(n int) int { return arrow.Decimal64Traits.BytesRequired(n) }
func (dec64Traits) valFromString(v string, prec int32, scale int32) (decimal.Decimal64, error) {
	return decimal.Decimal64FromString(v, prec, scale)
}
func (dec64Traits) valFromFloat64(v float64, prec, scale int32) (decimal.Decimal64, error) {
	return decimal.Decimal64FromFloat(v, prec, scale)
}

type dec128Traits struct{}

func (dec128Traits) bytesRequired(n int) int { return arrow.Decimal128Traits.BytesRequired(n) }
func (dec128Traits) valFromString(v string, prec int32, scale int32) (decimal.Decimal128, error) {
	return decimal.Decimal128FromString(v, prec, scale)
}
func (dec128Traits) valFromFloat64(v float64, prec, scale int32) (decimal.Decimal128, error) {
	return decimal128.FromFloat64(v, prec, scale)
}

type dec256Traits struct{}

func (dec256Traits) bytesRequired(n int) int { return arrow.Decimal256Traits.BytesRequired(n) }
func (dec256Traits) valFromString(v string, prec int32, scale int32) (decimal.Decimal256, error) {
	return decimal.Decimal256FromString(v, prec, scale)
}
func (dec256Traits) valFromFloat64(v float64, prec, scale int32) (decimal.Decimal256, error) {
	return decimal256.FromFloat64(v, prec, scale)
}

type Decimal32Builder = baseDecimalBuilder[decimal.Decimal32]
type Decimal64Builder = baseDecimalBuilder[decimal.Decimal64]
type Decimal128Builder struct {
	*baseDecimalBuilder[decimal.Decimal128]
}

func (b *Decimal128Builder) NewDecimal128Array() *Decimal128 {
	return b.NewDecimalArray()
}

type Decimal256Builder struct {
	*baseDecimalBuilder[decimal.Decimal256]
}

func (b *Decimal256Builder) NewDecimal256Array() *Decimal256 {
	return b.NewDecimalArray()
}

type baseDecimalBuilder[T interface {
	decimalTypes
	decimal.Num[T]
}] struct {
	builder
	decimalTraits[T]

	dtype   arrow.DecimalType
	data    *memory.Buffer
	rawData []T
}

func newDecimalBuilder[T interface {
	decimalTypes
	decimal.Num[T]
}, DT arrow.DecimalType](mem memory.Allocator, dtype DT) *baseDecimalBuilder[T] {
	return &baseDecimalBuilder[T]{
		builder: builder{refCount: 1, mem: mem},
		dtype:   dtype,
	}
}

func (b *baseDecimalBuilder[T]) Type() arrow.DataType { return b.dtype }

func (b *baseDecimalBuilder[T]) Release() {
	debug.Assert(atomic.LoadInt64(&b.refCount) > 0, "too many releases")

	if atomic.AddInt64(&b.refCount, -1) == 0 {
		if b.nullBitmap != nil {
			b.nullBitmap.Release()
			b.nullBitmap = nil
		}
		if b.data != nil {
			b.data.Release()
			b.data, b.rawData = nil, nil
		}
	}
}

func (b *baseDecimalBuilder[T]) Append(v T) {
	b.Reserve(1)
	b.UnsafeAppend(v)
}

func (b *baseDecimalBuilder[T]) UnsafeAppend(v T) {
	bitutil.SetBit(b.nullBitmap.Bytes(), b.length)
	b.rawData[b.length] = v
	b.length++
}

func (b *baseDecimalBuilder[T]) AppendNull() {
	b.Reserve(1)
	b.UnsafeAppendBoolToBitmap(false)
}

func (b *baseDecimalBuilder[T]) AppendNulls(n int) {
	for i := 0; i < n; i++ {
		b.AppendNull()
	}
}

func (b *baseDecimalBuilder[T]) AppendEmptyValue() {
	var empty T
	b.Append(empty)
}

func (b *baseDecimalBuilder[T]) AppendEmptyValues(n int) {
	for i := 0; i < n; i++ {
		b.AppendEmptyValue()
	}
}

func (b *baseDecimalBuilder[T]) UnsafeAppendBoolToBitmap(isValid bool) {
	if isValid {
		bitutil.SetBit(b.nullBitmap.Bytes(), b.length)
	} else {
		b.nulls++
	}
	b.length++
}

func (b *baseDecimalBuilder[T]) AppendValues(v []T, valid []bool) {
	if len(v) != len(valid) && len(valid) != 0 {
		panic("len(v) != len(valid) && len(valid) != 0")
	}

	if len(v) == 0 {
		return
	}

	b.Reserve(len(v))
	if len(v) > 0 {
		copy(b.rawData[b.length:], v)
	}
	b.builder.unsafeAppendBoolsToBitmap(valid, len(v))
}

func (b *baseDecimalBuilder[T]) init(capacity int) {
	b.builder.init(capacity)

	b.data = memory.NewResizableBuffer(b.mem)
	bytesN := int(reflect.TypeFor[T]().Size()) * capacity
	b.data.Resize(bytesN)
	b.rawData = arrow.GetData[T](b.data.Bytes())
}

func (b *baseDecimalBuilder[T]) Reserve(n int) {
	b.builder.reserve(n, b.Resize)
}

func (b *baseDecimalBuilder[T]) Resize(n int) {
	nBuilder := n
	if n < minBuilderCapacity {
		n = minBuilderCapacity
	}

	if b.capacity == 0 {
		b.init(n)
	} else {
		b.builder.resize(nBuilder, b.init)
		b.data.Resize(b.bytesRequired(n))
		b.rawData = arrow.GetData[T](b.data.Bytes())
	}
}

func (b *baseDecimalBuilder[T]) NewDecimalArray() (a *baseDecimal[T]) {
	data := b.newData()
	a = newDecimalData[T](data)
	data.Release()
	return
}

func (b *baseDecimalBuilder[T]) NewArray() arrow.Array {
	return b.NewDecimalArray()
}

func (b *baseDecimalBuilder[T]) newData() (data *Data) {
	bytesRequired := b.bytesRequired(b.length)
	if bytesRequired > 0 && bytesRequired < b.data.Len() {
		// trim buffers
		b.data.Resize(bytesRequired)
	}
	data = NewData(b.dtype, b.length, []*memory.Buffer{b.nullBitmap, b.data}, nil, b.nulls, 0)
	b.reset()

	if b.data != nil {
		b.data.Release()
		b.data, b.rawData = nil, nil
	}

	return
}

func (b *baseDecimalBuilder[T]) AppendValueFromString(s string) error {
	if s == NullValueStr {
		b.AppendNull()
		return nil
	}

	val, err := b.valFromString(s, b.dtype.GetPrecision(), b.dtype.GetScale())
	if err != nil {
		b.AppendNull()
		return err
	}
	b.Append(val)
	return nil
}

func (b *baseDecimalBuilder[T]) UnmarshalOne(dec *json.Decoder) error {
	t, err := dec.Token()
	if err != nil {
		return err
	}

	var token T
	switch v := t.(type) {
	case float64:
		token, err = b.valFromFloat64(v, b.dtype.GetPrecision(), b.dtype.GetScale())
		if err != nil {
			return err
		}
		b.Append(token)
	case string:
		token, err = b.valFromString(v, b.dtype.GetPrecision(), b.dtype.GetScale())
		if err != nil {
			return err
		}
		b.Append(token)
	case json.Number:
		token, err = b.valFromString(v.String(), b.dtype.GetPrecision(), b.dtype.GetScale())
		if err != nil {
			return err
		}
		b.Append(token)
	case nil:
		b.AppendNull()
	default:
		return &json.UnmarshalTypeError{
			Value:  fmt.Sprint(t),
			Type:   reflect.TypeFor[T](),
			Offset: dec.InputOffset(),
		}
	}

	return nil
}

func (b *baseDecimalBuilder[T]) Unmarshal(dec *json.Decoder) error {
	for dec.More() {
		if err := b.UnmarshalOne(dec); err != nil {
			return err
		}
	}
	return nil
}

func (b *baseDecimalBuilder[T]) UnmarshalJSON(data []byte) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	t, err := dec.Token()
	if err != nil {
		return err
	}

	if delim, ok := t.(json.Delim); !ok || delim != '[' {
		return fmt.Errorf("decimal builder must unpack from json array, found %s", delim)
	}

	return b.Unmarshal(dec)
}

func NewDecimal32Builder(mem memory.Allocator, dtype *arrow.Decimal32Type) *Decimal32Builder {
	b := newDecimalBuilder[decimal.Decimal32](mem, dtype)
	b.decimalTraits = dec32Traits{}
	return b
}

func NewDecimal64Builder(mem memory.Allocator, dtype *arrow.Decimal64Type) *Decimal64Builder {
	b := newDecimalBuilder[decimal.Decimal64](mem, dtype)
	b.decimalTraits = dec64Traits{}
	return b
}

func NewDecimal128Builder(mem memory.Allocator, dtype *arrow.Decimal128Type) *Decimal128Builder {
	b := newDecimalBuilder[decimal.Decimal128](mem, dtype)
	b.decimalTraits = dec128Traits{}
	return &Decimal128Builder{b}
}

func NewDecimal256Builder(mem memory.Allocator, dtype *arrow.Decimal256Type) *Decimal256Builder {
	b := newDecimalBuilder[decimal.Decimal256](mem, dtype)
	b.decimalTraits = dec256Traits{}
	return &Decimal256Builder{b}
}

var (
	_ arrow.Array = (*Decimal32)(nil)
	_ arrow.Array = (*Decimal64)(nil)
	_ arrow.Array = (*Decimal128)(nil)
	_ arrow.Array = (*Decimal256)(nil)
	_ Builder     = (*Decimal32Builder)(nil)
	_ Builder     = (*Decimal64Builder)(nil)
	_ Builder     = (*Decimal128Builder)(nil)
	_ Builder     = (*Decimal256Builder)(nil)
)
