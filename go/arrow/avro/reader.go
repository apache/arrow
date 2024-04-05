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
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/internal/debug"
	"github.com/apache/arrow/go/v16/arrow/memory"
	"github.com/hamba/avro/v2/ocf"
	"github.com/tidwall/sjson"

	avro "github.com/hamba/avro/v2"
)

var ErrMismatchFields = errors.New("arrow/avro: number of records mismatch")

// Option configures an Avro reader/writer.
type (
	Option func(config)
	config *OCFReader
)

type schemaEdit struct {
	method string
	path   string
	value  any
}

// Reader wraps goavro/OCFReader and creates array.Records from a schema.
type OCFReader struct {
	r               *ocf.Decoder
	avroSchema      string
	avroSchemaEdits []schemaEdit
	schema          *arrow.Schema

	refs   int64
	bld    *array.RecordBuilder
	bldMap *fieldPos
	ldr    *dataLoader
	cur    arrow.Record
	err    error

	primed     bool
	readerCtx  context.Context
	readCancel func()
	maxOCF     int
	maxRec     int

	avroChan       chan any
	avroDatumCount int64
	avroChanSize   int
	recChan        chan arrow.Record

	bldDone chan struct{}

	recChanSize int
	chunk       int
	mem         memory.Allocator
}

// NewReader returns a reader that reads from an Avro OCF file and creates
// arrow.Records from the converted avro data.
func NewOCFReader(r io.Reader, opts ...Option) (*OCFReader, error) {
	ocfr, err := ocf.NewDecoder(r)
	if err != nil {
		return nil, fmt.Errorf("%w: could not create avro ocfreader", arrow.ErrInvalid)
	}

	rr := &OCFReader{
		r:            ocfr,
		refs:         1,
		chunk:        1,
		avroChanSize: 500,
		recChanSize:  10,
	}
	for _, opt := range opts {
		opt(rr)
	}

	rr.avroChan = make(chan any, rr.avroChanSize)
	rr.recChan = make(chan arrow.Record, rr.recChanSize)
	rr.bldDone = make(chan struct{})
	schema, err := avro.Parse(string(ocfr.Metadata()["avro.schema"]))
	if err != nil {
		return nil, fmt.Errorf("%w: could not parse avro header", arrow.ErrInvalid)
	}
	rr.avroSchema = schema.String()
	if len(rr.avroSchemaEdits) > 0 {
		// execute schema edits
		for _, e := range rr.avroSchemaEdits {
			err := rr.editAvroSchema(e)
			if err != nil {
				return nil, fmt.Errorf("%w: could not edit avro schema", arrow.ErrInvalid)
			}
		}
		// validate edited schema
		schema, err = avro.Parse(rr.avroSchema)
		if err != nil {
			return nil, fmt.Errorf("%w: could not parse modified avro schema", arrow.ErrInvalid)
		}
	}
	rr.schema, err = ArrowSchemaFromAvro(schema)
	if err != nil {
		return nil, fmt.Errorf("%w: could not convert avro schema", arrow.ErrInvalid)
	}
	if rr.mem == nil {
		rr.mem = memory.DefaultAllocator
	}
	rr.readerCtx, rr.readCancel = context.WithCancel(context.Background())
	go rr.decodeOCFToChan()

	rr.bld = array.NewRecordBuilder(rr.mem, rr.schema)
	rr.bldMap = newFieldPos()
	rr.ldr = newDataLoader()
	for idx, fb := range rr.bld.Fields() {
		mapFieldBuilders(fb, rr.schema.Field(idx), rr.bldMap)
	}
	rr.ldr.drawTree(rr.bldMap)
	go rr.recordFactory()
	return rr, nil
}

// Reuse allows the OCFReader to be reused to read another Avro file provided the
// new Avro file has an identical schema.
func (rr *OCFReader) Reuse(r io.Reader, opts ...Option) error {
	rr.Close()
	rr.err = nil
	ocfr, err := ocf.NewDecoder(r)
	if err != nil {
		return fmt.Errorf("%w: could not create avro ocfreader", arrow.ErrInvalid)
	}
	schema, err := avro.Parse(string(ocfr.Metadata()["avro.schema"]))
	if err != nil {
		return fmt.Errorf("%w: could not parse avro header", arrow.ErrInvalid)
	}
	if rr.avroSchema != schema.String() {
		return fmt.Errorf("%w: avro schema mismatch", arrow.ErrInvalid)
	}

	rr.r = ocfr
	for _, opt := range opts {
		opt(rr)
	}

	rr.maxOCF = 0
	rr.maxRec = 0
	rr.avroDatumCount = 0
	rr.primed = false

	rr.avroChan = make(chan any, rr.avroChanSize)
	rr.recChan = make(chan arrow.Record, rr.recChanSize)
	rr.bldDone = make(chan struct{})

	rr.readerCtx, rr.readCancel = context.WithCancel(context.Background())
	go rr.decodeOCFToChan()
	go rr.recordFactory()
	return nil
}

// Err returns the last error encountered during the iteration over the
// underlying Avro file.
func (r *OCFReader) Err() error { return r.err }

// AvroSchema returns the Avro schema of the Avro OCF
func (r *OCFReader) AvroSchema() string { return r.avroSchema }

// Schema returns the converted Arrow schema of the Avro OCF
func (r *OCFReader) Schema() *arrow.Schema { return r.schema }

// Record returns the current record that has been extracted from the
// underlying Avro OCF file.
// It is valid until the next call to Next.
func (r *OCFReader) Record() arrow.Record { return r.cur }

// Metrics returns the maximum queue depth of the Avro record read cache and of the
// converted Arrow record cache.
func (r *OCFReader) Metrics() string {
	return fmt.Sprintf("Max. OCF queue depth: %d/%d  Max. record queue depth: %d/%d", r.maxOCF, r.avroChanSize, r.maxRec, r.recChanSize)
}

// OCFRecordsReadCount returns the number of Avro datum that were read from the Avro file.
func (r *OCFReader) OCFRecordsReadCount() int64 { return r.avroDatumCount }

// Close closes the OCFReader's Avro record read cache and converted Arrow record cache. OCFReader must
// be closed if the Avro OCF's records have not been read to completion.
func (r *OCFReader) Close() {
	r.readCancel()
	r.err = r.readerCtx.Err()
}

func (r *OCFReader) editAvroSchema(e schemaEdit) error {
	var err error
	switch e.method {
	case "set":
		r.avroSchema, err = sjson.Set(r.avroSchema, e.path, e.value)
		if err != nil {
			return fmt.Errorf("%w: schema edit 'set %s = %v' failure - %v", arrow.ErrInvalid, e.path, e.value, err)
		}
	case "delete":
		r.avroSchema, err = sjson.Delete(r.avroSchema, e.path)
		if err != nil {
			return fmt.Errorf("%w: schema edit 'delete' failure - %v", arrow.ErrInvalid, err)
		}
	default:
		return fmt.Errorf("%w: schema edit method must be 'set' or 'delete'", arrow.ErrInvalid)
	}
	return nil
}

// Next returns whether a Record can be received from the converted record queue.
// The user should check Err() after call to Next that return false to check
// if an error took place.
func (r *OCFReader) Next() bool {
	if r.cur != nil {
		r.cur.Release()
		r.cur = nil
	}
	if r.maxOCF < len(r.avroChan) {
		r.maxOCF = len(r.avroChan)
	}
	if r.maxRec < len(r.recChan) {
		r.maxRec = len(r.recChan)
	}
	select {
	case r.cur = <-r.recChan:
	case <-r.bldDone:
		if len(r.recChan) > 0 {
			r.cur = <-r.recChan
		}
	}
	if r.err != nil {
		return false
	}

	return r.cur != nil
}

// WithAllocator specifies the Arrow memory allocator used while building records.
func WithAllocator(mem memory.Allocator) Option {
	return func(cfg config) {
		cfg.mem = mem
	}
}

// WithReadCacheSize specifies the size of the OCF record decode queue, default value
// is 500.
func WithReadCacheSize(n int) Option {
	return func(cfg config) {
		if n < 1 {
			cfg.avroChanSize = 500
		} else {
			cfg.avroChanSize = n
		}
	}
}

// WithRecordCacheSize specifies the size of the converted Arrow record queue, default
// value is 1.
func WithRecordCacheSize(n int) Option {
	return func(cfg config) {
		if n < 1 {
			cfg.recChanSize = 1
		} else {
			cfg.recChanSize = n
		}
	}
}

// WithSchemaEdit specifies modifications to the Avro schema. Supported methods are 'set' and
// 'delete'. Set sets the value for the specified path. Delete deletes the value for the specified path.
// A path is in dot syntax, such as "fields.1" or "fields.0.type". The modified Avro schema is
// validated before conversion to Arrow schema - NewOCFReader will return an error if the modified schema
// cannot be parsed.
func WithSchemaEdit(method, path string, value any) Option {
	return func(cfg config) {
		var e schemaEdit
		e.method = method
		e.path = path
		e.value = value
		cfg.avroSchemaEdits = append(cfg.avroSchemaEdits, e)
	}
}

// WithChunk specifies the chunk size used while reading Avro OCF files.
//
// If n is zero or 1, no chunking will take place and the reader will create
// one record per row.
// If n is greater than 1, chunks of n rows will be read.
// If n is negative, the reader will load the whole Avro OCF file into memory and
// create one big record with all the rows.
func WithChunk(n int) Option {
	return func(cfg config) {
		cfg.chunk = n
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

var _ array.RecordReader = (*OCFReader)(nil)
