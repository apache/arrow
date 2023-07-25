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

package avro

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/internal/debug"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/linkedin/goavro/v2"
)

// Reader wraps goavro/OCFReader and creates array.Records from a schema.
type OCFReader struct {
	r      *goavro.OCFReader
	schema *arrow.Schema

	refs int64
	bld  *array.RecordBuilder
	cur  arrow.Record
	err  error

	chunk int
	done  bool
	next  func() bool

	mem memory.Allocator

	once sync.Once
}

// NewReader returns a reader that reads from an Avro OCF file and creates
// arrow.Records from the converted schema.
func NewOCFReader(r io.Reader, opts ...Option) *OCFReader {
	ocfr, err := goavro.NewOCFReader(r)
	if err != nil {
		panic(fmt.Errorf("%w: could not create avro ocfreader", arrow.ErrInvalid))
	}

	rr := &OCFReader{
		r:     ocfr,
		refs:  1,
		chunk: 1,
	}
	for _, opt := range opts {
		opt(rr)
	}

	codec := ocfr.Codec()
	schema := codec.Schema()
	rr.schema, err = ArrowSchemaFromAvro([]byte(schema))
	if err != nil {
		panic(fmt.Errorf("%w: could not convert avro schema", arrow.ErrInvalid))
	}
	if rr.mem == nil {
		rr.mem = memory.DefaultAllocator
	}

	rr.bld = array.NewRecordBuilder(rr.mem, rr.schema)
	rr.next = rr.next1
	switch {
	case rr.chunk < 0:
		rr.next = rr.nextall
	case rr.chunk > 1:
		rr.next = rr.nextn
	default:
		rr.next = rr.nextall
	}
	return rr
}

// Err returns the last error encountered during the iteration over the
// underlying Avro file.
func (r *OCFReader) Err() error { return r.err }

func (r *OCFReader) Schema() *arrow.Schema { return r.schema }

// Record returns the current record that has been extracted from the
// underlying Avro OCF file.
// It is valid until the next call to Next.
func (r *OCFReader) Record() arrow.Record { return r.cur }

// Next returns whether a Record could be extracted from the underlying Avro OCF.
//
// Next panics if the number of records extracted from an Avro data item does not match
// the number of fields of the associated schema. If a parse failure occurs, Next
// will return true and the Record will contain nulls where failures occurred.
// Subsequent calls to Next will return false - The user should check Err() after
// each call to Next to check if an error took place.
func (r *OCFReader) Next() bool {

	if r.cur != nil {
		r.cur.Release()
		r.cur = nil
	}

	if r.err != nil || r.done {
		return false
	}

	return r.next()
}

// next1 reads one row from the Avro file and creates a single Record
// from that row.
func (r *OCFReader) next1() bool {
	// Scan returns true when there is at least one more data item to be read from
	// the Avro OCF. Scan ought to be called prior to calling the Read method each
	// time the Read method is invoked.  See `NewOCFReader` documentation for an
	// example.
	if r.r.Scan() {
		// Read consumes one datum value from the Avro OCF stream and returns it. Read
		// is designed to be called only once after each invocation of the Scan method.
		// See `NewOCFReader` documentation for an example.
		recs, err := r.r.Read()
		if err != nil {
			r.done = true
			if errors.Is(err, io.EOF) {
				r.err = nil
			}
			r.err = err
			return false
		}

		for idx, fb := range r.bld.Fields() {
			appendData(fb, recs.(map[string]interface{})[fb.Type().(*arrow.StructType).Field(idx).Name])
		}
		r.cur = r.bld.NewRecord()
	}
	return true
}

// nextall reads the whole Avro file into memory and creates one single
// Record from all the data items.

func (r *OCFReader) nextall() bool {
	// Scan returns true when there is at least one more data item to be read from
	// the Avro OCF. Scan ought to be called prior to calling the Read method each
	// time the Read method is invoked.  See `NewOCFReader` documentation for an
	// example.
	for r.r.Scan() {
		// Read consumes one datum value from the Avro OCF stream and returns it. Read
		// is designed to be called only once after each invocation of the Scan method.
		// See `NewOCFReader` documentation for an example.
		recs, err := r.r.Read()
		if err != nil {
			r.done = true
			if errors.Is(err, io.EOF) {
				r.err = nil
			}
			r.err = err
			return false
		}

		for idx, fb := range r.bld.Fields() {
			appendData(fb, recs.(map[string]interface{})[fb.Type().(*arrow.StructType).Field(idx).Name])
		}
	}
	r.cur = r.bld.NewRecord()
	return true
}

// nextn reads n data items from the Avro file, where n is the chunk size, and
// creates a Record from these rows.
func (r *OCFReader) nextn() bool {
	var n = 0

	for i := 0; i < r.chunk && !r.done; i++ {
		if r.r.Scan() {
			// Read consumes one datum value from the Avro OCF stream and returns it. Read
			// is designed to be called only once after each invocation of the Scan method.
			// See `NewOCFReader` documentation for an example.
			recs, err := r.r.Read()
			if err != nil {
				r.done = true
				if errors.Is(err, io.EOF) {
					r.err = nil
				}
				r.err = err
				return false
			}
			for idx, fb := range r.bld.Fields() {
				appendData(fb, recs.(map[string]interface{})[fb.Type().(*arrow.StructType).Field(idx).Name])
			}
			n++
		}
	}

	if r.err != nil {
		r.done = true
	}

	r.cur = r.bld.NewRecord()
	return n > 0
}

func appendData(b array.Builder, data interface{}) {
	//		Avro					Go    			Arrow
	//		null					nil				Null
	//		boolean					bool			Boolean
	//		bytes					[]byte			Binary
	//		float					float32			Float32
	//		double					float64			Float64
	//		long					int64			Int64
	//		int						int32  			Int32
	//		string					string			String
	//		array					[]interface{}	List
	//		enum					string			Dictionary
	//		fixed					[]byte			FixedSizeBinary
	// 		map and record	map[string]interface{}	Struct
	switch bt := b.(type) {
	case *array.BinaryBuilder:
		switch data.(type) {
		case nil:
			bt.AppendNull()
		case map[string]interface{}:
			bt.Append([]byte(fmt.Sprint(data)))
		default:
			bt.Append([]byte(fmt.Sprint(data)))
		}
	case *array.BinaryDictionaryBuilder:
		bt.AppendNull()
	case *array.BooleanBuilder:
		switch dt := data.(type) {
		case nil:
			bt.AppendNull()
		case bool:
			bt.Append(dt)
		case map[string]interface{}:
			bt.Append(dt["boolean"].(bool))
		}
	case *array.DayTimeIntervalBuilder:
		bt.AppendNull()
	case *array.DayTimeDictionaryBuilder:
		bt.AppendNull()
	case *array.Date32Builder:
		switch dt := data.(type) {
		case nil:
			bt.AppendNull()
		case int32:
			bt.Append(arrow.Date32(dt))
		case map[string]interface{}:
			bt.Append(arrow.Date32(dt["int"].(int32)))
		}
	case *array.Date32DictionaryBuilder:
		bt.AppendNull()
	case *array.Date64Builder:
		bt.AppendNull()
	case *array.Date64DictionaryBuilder:
		bt.AppendNull()
	//case *array.DictionaryBuilder:
	case *array.Decimal128Builder:
		bt.AppendNull()
	case *array.Decimal128DictionaryBuilder:
		bt.AppendNull()
	case *array.Decimal256Builder:
		bt.AppendNull()
	case *array.Decimal256DictionaryBuilder:
		bt.AppendNull()
	case *array.DenseUnionBuilder:
		b.AppendNull()
	case *array.DurationBuilder:
		bt.AppendNull()
	case *array.DurationDictionaryBuilder:
		bt.AppendNull()
	case *array.FixedSizeBinaryBuilder:
		switch dt := data.(type) {
		case nil:
			bt.AppendNull()
		case []byte:
			bt.Append(dt)
		case map[string]interface{}:
			bt.Append(dt["bytes"].([]byte))
		}
	case *array.FixedSizeBinaryDictionaryBuilder:
		bt.AppendNull()
	case *array.FixedSizeListBuilder:
		bt.AppendNull()
	case *array.Float16Builder:
		bt.AppendNull()
	case *array.Float16DictionaryBuilder:
		bt.AppendNull()
	case *array.Float32Builder:
		switch dt := data.(type) {
		case nil:
			bt.AppendNull()
		case float32:
			bt.Append(dt)
		case map[string]interface{}:
			bt.Append(dt["float"].(float32))
		}
	case *array.Float32DictionaryBuilder:
		bt.AppendNull()
	case *array.Float64Builder:
		switch dt := data.(type) {
		case nil:
			bt.AppendNull()
		case float64:
			bt.Append(dt)
		case map[string]interface{}:
			bt.Append(dt["double"].(float64))
		}
	case *array.Float64DictionaryBuilder:
		bt.AppendNull()
	case *array.Int8Builder:
		bt.AppendNull()
	case *array.Int8DictionaryBuilder:
		bt.AppendNull()
	case *array.Int16Builder:
		bt.AppendNull()
	case *array.Int16DictionaryBuilder:
		bt.AppendNull()
	case *array.Int32Builder:
		switch dt := data.(type) {
		case nil:
			bt.AppendNull()
		case int32:
			bt.Append(dt)
		case map[string]interface{}:
			bt.Append(dt["int"].(int32))
		}
	case *array.Int32DictionaryBuilder:
		bt.AppendNull()
	case *array.Int64Builder:
		switch dt := data.(type) {
		case nil:
			bt.AppendNull()
		case int64:
			bt.Append(dt)
		case map[string]interface{}:
			bt.Append(data.(map[string]interface{})["long"].(int64))
		}
	case *array.Int64DictionaryBuilder:
		bt.AppendNull()
	case *array.ListBuilder:
		vb := bt.ValueBuilder()
		vb.Reserve(len(data.([]interface{})))
		for _, v := range data.([]interface{}) {
			if v == nil {
				bt.AppendNull()
				vb.AppendNull()
			} else {
				bt.Append(true)
				appendData(vb, v)
			}
		}
	case *array.MapBuilder:
		kb := bt.KeyBuilder()
		ib := bt.ItemBuilder()
		for k, v := range data.(map[string]interface{}) {
			appendData(kb, k)
			appendData(ib, v)
		}
	// Avro Duration type is not implemented in github.com/linkedin/goavro
	// Schema conversion falls back to Binary.
	// A duration logical type annotates Avro fixed type of size 12, which
	// stores three little-endian unsigned integers that represent durations
	// at different granularities of time. The first stores a number in months,
	// the second stores a number in days, and the third stores a number
	// in milliseconds.
	case *array.MonthDayNanoIntervalBuilder:
		bt.AppendNull()
	case *array.NullBuilder:
		bt.AppendNull()
	case *array.NullDictionaryBuilder:
		bt.AppendNull()
	case *array.RunEndEncodedBuilder:
		bt.AppendNull()
	case *array.SparseUnionBuilder:
		bt.AppendNull()
	case *array.StringBuilder:
		switch dt := data.(type) {
		case nil:
			bt.AppendNull()
		case string:
			bt.Append(dt)
		case map[string]interface{}:
			// avro uuid logical type
			if u, ok := data.(map[string]interface{})["uuid"]; ok {
				bt.Append(u.(string))
			} else {
				bt.Append(dt["string"].(string))
			}
		}
	//case *array.StringLikeBuilder:
	//	bt.AppendNull()
	case *array.StructBuilder:
		i := 0
		for i < bt.NumField() {
			n := data.(map[string]interface{})[bt.Type().(*arrow.StructType).Field(i).Name]
			appendData(bt.FieldBuilder(i), n)
			i++
		}
	case *array.Time32Builder:
		switch dt := data.(type) {
		case nil:
			bt.AppendNull()
		case int32:
			bt.Append(arrow.Time32(dt))
		case map[string]interface{}:
			bt.Append(arrow.Time32(dt["int"].(int32)))
		}
	case *array.Time32DictionaryBuilder:
		bt.AppendNull()
	case *array.Time64Builder:
		switch dt := data.(type) {
		case nil:
			bt.AppendNull()
		case int64:
			bt.Append(arrow.Time64(dt))
		case map[string]interface{}:
			bt.Append(arrow.Time64(dt["long"].(int64)))
		}
	case *array.Time64DictionaryBuilder:
		bt.AppendNull()
	case *array.TimestampBuilder:
		switch dt := data.(type) {
		case nil:
			bt.AppendNull()
		case int64:
			bt.Append(arrow.Timestamp(dt))
		case map[string]interface{}:
			bt.Append(arrow.Timestamp(dt["long"].(int64)))
		}
	case *array.TimestampDictionaryBuilder:
		bt.AppendNull()
	case *array.Uint8Builder:
		bt.AppendNull()
	case *array.Uint8DictionaryBuilder:
		bt.AppendNull()
	case *array.Uint16Builder:
		bt.AppendNull()
	case *array.Uint16DictionaryBuilder:
		bt.AppendNull()
	case *array.Uint32Builder:
		bt.AppendNull()
	case *array.Uint32DictionaryBuilder:
		bt.AppendNull()
	case *array.Uint64Builder:
		bt.AppendNull()
	case *array.Uint64DictionaryBuilder:
		bt.AppendNull()
	//case *array.UnionBuilder:
	//	b.AppendNull()
	default:
		bt.AppendNull()
	}
}

// Retain increases the reference count by 1.
// Retain may be called simultaneously from multiple goroutines.
func (r *OCFReader) Retain() {
	atomic.AddInt64(&r.refs, 1)
}

// Release decreases the reference count by 1.
// When the reference count goes to zero, the memory is freed.
// Release may be called simultaneously from multiple goroutines.
func (r *OCFReader) Release() {
	debug.Assert(atomic.LoadInt64(&r.refs) > 0, "too many releases")

	if atomic.AddInt64(&r.refs, -1) == 0 {
		if r.cur != nil {
			r.cur.Release()
		}
	}
}

var (
	_ array.RecordReader = (*OCFReader)(nil)
)
