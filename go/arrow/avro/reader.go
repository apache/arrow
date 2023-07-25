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

	topLevel bool
	once     sync.Once

	fieldConverter []func(val string)
	columnFilter   []string
	columnTypes    map[string]arrow.DataType

	stringsCanBeNull bool
	nulls            []string
}

// NewReader returns a reader that reads from an Avro OCF file and creates
// arrow.Records from the converted schema.
func NewOCFReader(r io.Reader, opts ...Option) *OCFReader {
	ocfr, err := goavro.NewOCFReader(r)
	if err != nil {
		panic(fmt.Errorf("%w: could not create avro ocfreader", arrow.ErrInvalid))
	}

	rr := &OCFReader{
		r: ocfr,
		//schema:           schema,
		refs:             1,
		chunk:            1,
		stringsCanBeNull: false,
	}
	//rr.r.ReuseRecord = true
	for _, opt := range opts {
		opt(rr)
	}

	codec := ocfr.Codec()
	schema := codec.Schema()
	rr.schema, err = ArrowSchemaFromAvro([]byte(schema), rr.topLevel)
	if err != nil {
		panic(fmt.Errorf("%w: could not convert avro schema", arrow.ErrInvalid))
	}
	if rr.mem == nil {
		rr.mem = memory.DefaultAllocator
	}

	rr.bld = array.NewRecordBuilder(rr.mem, rr.schema)
	rr.next = rr.next1
	/*
		switch {
		case rr.chunk < 0:
			rr.next = rr.nextall
		case rr.chunk > 1:
			rr.next = rr.nextn
		default:
			rr.next = rr.next1
		}
	*/
	return rr
}

// NewReader returns a reader that reads from the CSV file and creates
// arrow.Records from the given schema.
//
// NewReader panics if the given schema contains fields that have types that are not
// primitive types.
/*
func NewAvroReader(r io.Reader, schema *arrow.Schema, opts ...Option) *OCFReader {
	validate(schema)

	rr := &OCFReader{
		r:                csv.NewReader(r),
		schema:           schema,
		refs:             1,
		chunk:            1,
		stringsCanBeNull: false,
	}
	rr.r.ReuseRecord = true
	for _, opt := range opts {
		opt(rr)
	}

	if rr.mem == nil {
		rr.mem = memory.DefaultAllocator
	}

	rr.bld = array.NewRecordBuilder(rr.mem, rr.schema)

	switch {
	case rr.chunk < 0:
		rr.next = rr.nextall
	case rr.chunk > 1:
		rr.next = rr.nextn
	default:
		rr.next = rr.next1
	}

	return rr
}
*/

// Err returns the last error encountered during the iteration over the
// underlying Avro file.
func (r *OCFReader) Err() error { return r.err }

func (r *OCFReader) Schema() *arrow.Schema { return r.schema }

// Record returns the current record that has been extracted from the
// underlying Avro OCF file.
// It is valid until the next call to Next.
func (r *OCFReader) Record() arrow.Record { return r.cur }

// Next returns whether a Record could be extracted from the underlying Avro OCF file.
//
// Next panics if the number of records extracted from a CSV row does not match
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
	//recs := make(map[string]interface{})
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

/*
// nextall reads the whole CSV file into memory and creates one single
// Record from all the CSV rows.

	func (r *Reader) nextall() bool {
		defer func() {
			r.done = true
		}()

		var (
			recs [][]string
		)

		recs, r.err = r.r.ReadAll()
		if r.err != nil {
			return false
		}

		for _, rec := range recs {
			r.validate(rec)
			r.read(rec)
		}
		r.cur = r.bld.NewRecord()

		return true
	}

// nextn reads n rows from the CSV file, where n is the chunk size, and creates
// a Record from these rows.

	func (r *Reader) nextn() bool {
		var (
			recs []string
			n    = 0
			err  error
		)

		for i := 0; i < r.chunk && !r.done; i++ {
			recs, err = r.r.Read()
			if err != nil {
				if !errors.Is(err, io.EOF) {
					r.err = err
				}
				r.done = true
				break
			}

			r.validate(recs)
			r.read(recs)
			n++
		}

		if r.err != nil {
			r.done = true
		}

		r.cur = r.bld.NewRecord()
		return n > 0
	}
*/
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
		}
	case *array.BinaryDictionaryBuilder:
		bt.AppendNull()
	case *array.BooleanBuilder:
		switch data.(type) {
		case nil:
			bt.AppendNull()
		case bool:
			bt.Append(data.(bool))
		case map[string]interface{}:
			bt.Append(data.(map[string]interface{})["boolean"].(bool))
		}
	case *array.DayTimeIntervalBuilder:
	case *array.DayTimeDictionaryBuilder:
		bt.AppendNull()
	case *array.Date32Builder:
	case *array.Date32DictionaryBuilder:
		bt.AppendNull()
	case *array.Date64Builder:
	case *array.Date64DictionaryBuilder:
		bt.AppendNull()
	//case *array.DictionaryBuilder:
	case *array.Decimal128Builder:
	case *array.Decimal128DictionaryBuilder:
		bt.AppendNull()
	case *array.Decimal256Builder:
	case *array.Decimal256DictionaryBuilder:
		bt.AppendNull()
	case *array.DenseUnionBuilder:
		b.AppendNull()
	case *array.DurationBuilder:
	case *array.DurationDictionaryBuilder:
		b.AppendNull()
	case *array.FixedSizeBinaryBuilder:
		switch data.(type) {
		case nil:
			bt.AppendNull()
		case []byte:
			bt.Append(data.([]byte))
		case map[string]interface{}:
			bt.Append(data.(map[string]interface{})["bytes"].([]byte))
		}
	case *array.FixedSizeBinaryDictionaryBuilder:
		bt.AppendNull()
	case *array.FixedSizeListBuilder:
	case *array.Float16Builder:
		bt.AppendNull()
	case *array.Float16DictionaryBuilder:
		bt.AppendNull()
	case *array.Float32Builder:
		switch data.(type) {
		case nil:
			bt.AppendNull()
		case float32:
			bt.Append(data.(float32))
		case map[string]interface{}:
			bt.Append(data.(map[string]interface{})["float"].(float32))
		}
	case *array.Float32DictionaryBuilder:
	case *array.Float64Builder:
		switch data.(type) {
		case nil:
			bt.AppendNull()
		case float64:
			bt.Append(data.(float64))
		case map[string]interface{}:
			bt.Append(data.(map[string]interface{})["double"].(float64))
		}
	case *array.Float64DictionaryBuilder:
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
				//vb.Append(v)
			}
		}

	//case *array.ListLikeBuilder:
	//	bt.AppendNull()
	case *array.MapBuilder:

	case *array.MonthDayNanoIntervalBuilder:

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
		b.AppendNull()
	case *array.NullBuilder:
		b.AppendNull()
	case *array.NullDictionaryBuilder:
		b.AppendNull()
	case *array.RunEndEncodedBuilder:
		b.AppendNull()
	case *array.SparseUnionBuilder:
		b.AppendNull()
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
	case *array.Time32DictionaryBuilder:
		bt.AppendNull()
	case *array.Time64Builder:
	case *array.Time64DictionaryBuilder:
		bt.AppendNull()
	case *array.TimestampBuilder:
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
