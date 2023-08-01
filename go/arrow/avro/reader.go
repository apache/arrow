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
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/big"
	"sync"
	"sync/atomic"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/decimal128"
	"github.com/apache/arrow/go/v13/arrow/decimal256"

	"github.com/apache/arrow/go/v13/arrow/internal/debug"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/linkedin/goavro/v2"
)

type Reader interface {

	// Err returns the last error encountered.
	Err() error

	Schema() *arrow.Schema

	// Record returns the current record that has been extracted.
	Record() arrow.Record

	// Next returns whether a Record could be extracted.
	Next() bool
}

// Reader wraps goavro/OCFReader and creates array.Records from a schema.
type OCFReader struct {
	r          *goavro.OCFReader
	avroSchema string
	schema     *arrow.Schema

	refs        int64
	bld         *array.RecordBuilder
	cur         arrow.Record
	includeRoot bool
	tlrName     string
	err         error

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
	rr.avroSchema = codec.CanonicalSchema()
	rr.schema, rr.tlrName, err = ArrowSchemaFromAvro([]byte(rr.avroSchema), rr.includeRoot)
	if err != nil {
		panic(fmt.Errorf("%w: could not convert avro schema", arrow.ErrInvalid))
	}
	if rr.mem == nil {
		rr.mem = memory.DefaultAllocator
	}

	rr.bld = array.NewRecordBuilder(rr.mem, rr.schema)
	for idx, fb := range rr.bld.Fields() {
		addEnumSymbolsToBuilder(fb, rr.schema.Field(idx))
	}
	rr.next = rr.next1
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

func addEnumSymbolsToBuilder(b array.Builder, f arrow.Field) {
	switch bt := b.(type) {
	case *array.BinaryDictionaryBuilder:
		sb := array.NewStringBuilder(memory.DefaultAllocator)
		for _, v := range f.Metadata.Values() {
			sb.Append(v)
		}
		sa := sb.NewStringArray()
		bt.InsertStringDictValues(sa)
	case *array.StructBuilder:
		for i := 0; i < len(f.Type.(*arrow.StructType).Fields()); i++ {
			addEnumSymbolsToBuilder(bt.FieldBuilder(i), f.Type.(*arrow.StructType).Field(i))
		}
	case *array.MapBuilder:
		addEnumSymbolsToBuilder(bt.ItemBuilder(),
			arrow.Field{Name: bt.Type().(*arrow.MapType).Name(),
				Type:     bt.Type().(*arrow.MapType).Elem(),
				Metadata: f.Metadata})
	case *array.ListBuilder:
		addEnumSymbolsToBuilder(bt.ValueBuilder(),
			arrow.Field{Name: bt.Type().(*arrow.ListType).Name(),
				Type:     bt.Type().(*arrow.ListType).Elem(),
				Metadata: f.Metadata})
	}
}

// Err returns the last error encountered during the iteration over the
// underlying Avro file.
func (r *OCFReader) Err() error { return r.err }

func (r *OCFReader) AvroSchema() string { return r.avroSchema }

func (r *OCFReader) Schema() *arrow.Schema { return r.schema }

func (r *OCFReader) TLRName() string { return r.tlrName }

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
	// time the Read method is invoked.
	if r.r.Scan() {
		// Read consumes one datum value from the Avro OCF stream and returns it. Read
		// is designed to be called only once after each invocation of the Scan method.
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
			appendData(fb, recs.(map[string]interface{})[r.schema.Field(idx).Name], r.schema.Field(idx))
		}

		r.cur = r.bld.NewRecord()
	}
	return true
}

// nextall reads the whole Avro file into memory and creates one single
// Record from all the data items
func (r *OCFReader) nextall() bool {
	for r.r.Scan() {
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
			appendData(fb, recs.(map[string]interface{})[r.schema.Field(idx).Name], r.schema.Field(idx))
		}
	}
	r.cur = r.bld.NewRecord()
	return true
}

// nextn reads n data items from the Avro file, where n is the chunk size, and
// creates a Record from these rows
func (r *OCFReader) nextn() bool {
	var n = 0
	for i := 0; i < r.chunk && !r.done; i++ {
		if r.r.Scan() {
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
				appendData(fb, recs.(map[string]interface{})[r.schema.Field(idx).Name], r.schema.Field(idx))
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

// Avro data is loaded to Arrow arrays using the following type mapping:
//
//		Avro					Go    					Arrow
//	 ================================================================
//		null					nil						Null
//		boolean					bool					Boolean
//		bytes					[]byte					Binary
//		float					float32					Float32
//		double					float64					Float64
//		long					int64					Int64
//		int						int32  					Int32
//		string					string					String
//		array					[]interface{}			List
//		enum					string					Dictionary
//		fixed					[]byte					FixedSizeBinary
//		map 					map[string]interface{}	Struct
//		record					map[string]interface{}	Struct
func appendData(b array.Builder, data interface{}, field arrow.Field) {
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
		switch dt := data.(type) {
		case nil:
			bt.AppendNull()
		case string:
			bt.AppendString(dt)
		case map[string]interface{}:
			switch dt["string"].(type) {
			case nil:
				bt.AppendNull()
			case string:
				bt.AppendString(dt["string"].(string))
			}
		}
	case *array.BooleanBuilder:
		switch dt := data.(type) {
		case nil:
			bt.AppendNull()
		case bool:
			bt.Append(dt)
		case map[string]interface{}:
			switch dt["boolean"].(type) {
			case nil:
				bt.AppendNull()
			case bool:
				bt.Append(dt["boolean"].(bool))
			}
		}
	case *array.Date32Builder:
		switch dt := data.(type) {
		case nil:
			bt.AppendNull()
		case int32:
			bt.Append(arrow.Date32(dt))
		case map[string]interface{}:
			switch dt["int"].(type) {
			case nil:
				bt.AppendNull()
			case int32:
				bt.Append(arrow.Date32(dt["int"].(int32)))
			}
		}
	case *array.Decimal128Builder:
		switch dt := data.(type) {
		case nil:
			bt.AppendNull()
		case []byte:
			if len(dt) <= 38 {
				var intData int64
				buf := bytes.NewBuffer(dt)
				err := binary.Read(buf, binary.BigEndian, &intData)
				if err != nil {
					bt.AppendNull()
				}
				bt.Append(decimal128.FromI64(intData))
			} else {
				var bigIntData big.Int
				buf := bytes.NewBuffer(dt)
				err := binary.Read(buf, binary.BigEndian, &bigIntData)
				if err != nil {
					bt.AppendNull()
				}
				bt.Append(decimal128.FromBigInt(&bigIntData))
			}
		case map[string]interface{}:
			if len(dt["bytes"].([]byte)) <= 38 {
				var intData int64
				buf := bytes.NewBuffer(dt["bytes"].([]byte))
				err := binary.Read(buf, binary.BigEndian, &intData)
				if err != nil {
					bt.AppendNull()
				}
				bt.Append(decimal128.FromI64(intData))
			} else {
				var bigIntData big.Int
				buf := bytes.NewBuffer(dt["bytes"].([]byte))
				err := binary.Read(buf, binary.BigEndian, &bigIntData)
				if err != nil {
					bt.AppendNull()
				}
				bt.Append(decimal128.FromBigInt(&bigIntData))
			}
		}
	case *array.Decimal256Builder:
		switch dt := data.(type) {
		case nil:
			bt.AppendNull()
		case []byte:
			var bigIntData big.Int
			buf := bytes.NewBuffer(dt)
			err := binary.Read(buf, binary.BigEndian, &bigIntData)
			if err != nil {
				bt.AppendNull()
			}
			bt.Append(decimal256.FromBigInt(&bigIntData))
		case map[string]interface{}:
			var bigIntData big.Int
			buf := bytes.NewBuffer(dt["bytes"].([]byte))
			err := binary.Read(buf, binary.BigEndian, &bigIntData)
			if err != nil {
				bt.AppendNull()
			}
			bt.Append(decimal256.FromBigInt(&bigIntData))
		}
	case *array.FixedSizeBinaryBuilder:
		switch dt := data.(type) {
		case nil:
			bt.AppendNull()
		case []byte:
			bt.Append(dt)
		case map[string]interface{}:
			switch dt["bytes"].(type) {
			case nil:
				bt.AppendNull()
			case []byte:
				bt.Append(dt["bytes"].([]byte))
			}
		}
	case *array.Float32Builder:
		switch dt := data.(type) {
		case nil:
			bt.AppendNull()
		case float32:
			bt.Append(dt)
		case map[string]interface{}:
			switch dt["float"].(type) {
			case nil:
				bt.AppendNull()
			case float32:
				bt.Append(dt["float"].(float32))
			}
		}
	case *array.Float64Builder:
		switch dt := data.(type) {
		case nil:
			bt.AppendNull()
		case float64:
			bt.Append(dt)
		case map[string]interface{}:
			switch dt["double"].(type) {
			case nil:
				bt.AppendNull()
			case float64:
				switch dt["double"].(type) {
				case nil:
					bt.AppendNull()
				case float64:
					bt.Append(dt["double"].(float64))
				}
			}
		}
	case *array.Int32Builder:
		switch dt := data.(type) {
		case nil:
			bt.AppendNull()
		case int32:
			bt.Append(dt)
		case map[string]interface{}:
			switch dt["int"].(type) {
			case nil:
				bt.AppendNull()
			case int32:
				bt.Append(dt["int"].(int32))
			}
		}
	case *array.Int64Builder:
		switch dt := data.(type) {
		case nil:
			bt.AppendNull()
		case int64:
			bt.Append(dt)
		case map[string]interface{}:
			switch dt["long"].(type) {
			case nil:
				bt.AppendNull()
			case int64:
				bt.Append(dt["long"].(int64))
			}
		}
	case *array.ListBuilder:
		bt.Append(true)
		vb := bt.ValueBuilder()
		switch dt := data.(type) {
		case nil:
			bt.AppendNull()
		case []interface{}:
			vb.Reserve(len(dt))
			//bt.Append(true)
			for _, v := range dt {
				if v == nil {
					vb.AppendNull()
				} else {
					appendData(vb, v,
						arrow.Field{Name: bt.Type().(*arrow.ListType).Name(),
							Type:     bt.Type().(*arrow.ListType).Elem(),
							Metadata: field.Metadata})
				}
			}
		}
	case *array.MapBuilder:
		switch dt := data.(type) {
		case nil:
			bt.AppendNull()
			return
		case map[string]interface{}:
			bt.Append(true)
			kb := bt.KeyBuilder()
			ib := bt.ItemBuilder()
			for k, v := range dt {
				appendData(kb, k, arrow.Field{})
				appendData(ib, v,
					arrow.Field{Name: bt.Type().(*arrow.MapType).Name(),
						Type:     bt.Type().(*arrow.MapType).Elem(),
						Metadata: field.Metadata})
			}
		}
	case *array.StringBuilder:
		switch dt := data.(type) {
		case nil:
			bt.AppendNull()
		case string:
			bt.Append(dt)
		case map[string]interface{}:
			// avro uuid logical type
			if u, ok := dt["uuid"]; ok {
				switch dt["string"].(type) {
				case nil:
					bt.AppendNull()
				case string:
					bt.Append(u.(string))
				}
			} else {
				switch dt["string"].(type) {
				case nil:
					bt.AppendNull()
				case string:
					bt.Append(dt["string"].(string))
				}
			}
		default:
			bt.Append(fmt.Sprint(data))
		}
	case *array.StructBuilder:
		bt.Append(true)
		switch td := data.(type) {
		case nil:
			for i := 0; i < bt.NumField(); i++ {
				bt.FieldBuilder(i).AppendNull()
			}
		default:
			for i := 0; i < bt.NumField(); i++ {
				switch dt := td.(type) {
				case nil:
					bt.FieldBuilder(i).AppendNull()
				case map[string]interface{}:
					// check if type union
					if namedType, ok := field.Metadata.GetValue("typeName"); !ok {
						// non-union type
						switch nit := dt[bt.Type().(*arrow.StructType).Field(i).Name].(type) {
						case nil:
							bt.FieldBuilder(i).AppendNull()
						// array
						case []interface{}:
							if len(nit) == 0 {
								bt.FieldBuilder(i).AppendNull()
							} else {
								appendData(bt.FieldBuilder(i),
									nit,
									field.Type.(*arrow.StructType).Field(i),
								)
							}
						// primitive & complex types
						default:
							switch bt.FieldBuilder(i).(type) {
							case *array.StructBuilder:
								appendData(bt.FieldBuilder(i),
									dt[bt.Type().(*arrow.StructType).Field(i).Name],
									field.Type.(*arrow.StructType).Field(i))
							case *array.ListBuilder:
								appendData(bt.FieldBuilder(i),
									dt[bt.Type().(*arrow.StructType).Field(i).Name],
									arrow.Field{Metadata: field.Metadata})
							case *array.MapBuilder:
								appendData(bt.FieldBuilder(i),
									dt[bt.Type().(*arrow.StructType).Field(i).Name],
									arrow.Field{})
							default:
								appendData(bt.FieldBuilder(i),
									dt[bt.Type().(*arrow.StructType).Field(i).Name],
									arrow.Field{})
							}
						}
					} else {
						// Union type
						switch itt := dt[namedType].(type) {
						case nil:
							bt.FieldBuilder(i).AppendNull()
						// array
						case []interface{}:
							if len(itt) == 0 {
								bt.FieldBuilder(i).AppendNull()
							} else {
								appendData(bt.FieldBuilder(i),
									itt,
									field.Type.(*arrow.StructType).Field(i),
								)
							}
						// record
						case map[string]interface{}:
							switch t3 := itt[bt.Type().(*arrow.StructType).Field(i).Name].(type) {
							case nil:
								bt.FieldBuilder(i).AppendNull()
							case []interface{}:
								if len(t3) == 0 {
									bt.FieldBuilder(i).AppendNull()
								} else {
									appendData(bt.FieldBuilder(i),
										itt,
										field.Type.(*arrow.StructType).Field(i),
									)
								}
							case map[string]interface{}:
								appendData(bt.FieldBuilder(i),
									t3,
									field.Type.(*arrow.StructType).Field(i))
							default:
								appendData(bt.FieldBuilder(i),
									t3,
									field.Type.(*arrow.StructType).Field(i))
							}
						// primitive & complex (non-record) types
						default:
							appendData(bt.FieldBuilder(i), dt, field.Type.(*arrow.StructType).Field(i))
						}
					}
				default:
					appendData(bt.FieldBuilder(i), dt, field.Type.(*arrow.StructType).Field(i))
				}
			}
		}
	case *array.Time32Builder:
		switch dt := data.(type) {
		case nil:
			bt.AppendNull()
		case int32:
			bt.Append(arrow.Time32(dt))
		case map[string]interface{}:
			switch dt["int"].(type) {
			case nil:
				bt.AppendNull()
			case int32:
				bt.Append(arrow.Time32(dt["int"].(int32)))
			}
		}
	case *array.Time64Builder:
		switch dt := data.(type) {
		case nil:
			bt.AppendNull()
		case int64:
			bt.Append(arrow.Time64(dt))
		case map[string]interface{}:
			switch dt["long"].(type) {
			case nil:
				bt.AppendNull()
			case int64:
				bt.Append(arrow.Time64(dt["long"].(int64)))
			}
		}
	case *array.TimestampBuilder:
		switch dt := data.(type) {
		case nil:
			bt.AppendNull()
		case int64:
			bt.Append(arrow.Timestamp(dt))
		case map[string]interface{}:
			switch dt["long"].(type) {
			case nil:
				bt.AppendNull()
			case int64:
				bt.Append(arrow.Timestamp(dt["long"].(int64)))
			}
		}
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
