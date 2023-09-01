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
	"sync/atomic"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/internal/debug"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/hamba/avro/ocf"

	avro "github.com/hamba/avro/v2"
)

var ErrMismatchFields = errors.New("arrow/avro: number of records mismatch")

// Option configures an Avro reader/writer.
type (
	Option func(config)
	config *OCFReader
)

// Reader wraps goavro/OCFReader and creates array.Records from a schema.
type OCFReader struct {
	r          *ocf.Decoder
	avroSchema string
	schema     *arrow.Schema

	refs int64
	bld  *array.RecordBuilder
	ldr  *dataLoader
	cur  arrow.Record
	err  error

	chunk int
	done  bool
	next  func() bool

	mem memory.Allocator
}

// NewReader returns a reader that reads from an Avro OCF file and creates
// arrow.Records from the converted schema.
func NewOCFReader(r io.Reader, opts ...Option) *OCFReader {
	ocfr, err := ocf.NewDecoder(r)
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

	schema, err := avro.Parse(string(ocfr.Metadata()["avro.schema"]))
	if err != nil {
		return nil
	}
	rr.avroSchema = schema.String()
	rr.schema, err = ArrowSchemaFromAvro(schema)
	if err != nil {
		panic(fmt.Errorf("%w: could not convert avro schema", arrow.ErrInvalid))
	}
	if rr.mem == nil {
		rr.mem = memory.DefaultAllocator
	}

	rr.bld = array.NewRecordBuilder(rr.mem, rr.schema)
	bldMap := new(fieldPos)
	rr.ldr = newDataLoader()
	for idx, fb := range rr.bld.Fields() {
		mapFieldBuilders(fb, rr.schema.Field(idx), bldMap)
	}
	rr.ldr.drawTree(bldMap)
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
	if r.r.HasNext() {
		// Read consumes one datum value from the Avro OCF stream and returns it. Read
		// is designed to be called only once after each invocation of the Scan method.
		// See `NewOCFReader` documentation for an example.
		var datum interface{}
		err := r.r.Decode(&datum)
		if err != nil {
			r.done = true
			if errors.Is(err, io.EOF) {
				r.err = nil
			}
			r.err = err
			return false
		}
		err = r.ldr.loadDatum(datum)
		if err != nil {
			r.err = err
			return false
		}
	}
	if r.err != nil {
		r.done = true
	}
	r.cur = r.bld.NewRecord()
	return true
}

// nextall reads the whole Avro file into memory and creates one single
// Record from all the data items.
func (r *OCFReader) nextall() bool {
	for !r.done {
		if r.r.HasNext() {
			var datum interface{}
			err := r.r.Decode(&datum)
			if err != nil {
				r.done = true
				if errors.Is(err, io.EOF) {
					r.err = nil
				}
				r.err = err
				return false
			}
			err = r.ldr.loadDatum(datum)
			if err != nil {
				r.err = err
				return false
			}
		} else {
			r.done = true
		}
	}
	if r.err != nil {
		r.done = true
	}
	r.cur = r.bld.NewRecord()
	return true
}

// nextn reads n data items from the Avro file, where n is the chunk size, and
// creates a Record from these rows.
func (r *OCFReader) nextn() bool {
	n := 0
	for i := 0; i < r.chunk && !r.done; i++ {
		if r.r.HasNext() {
			var datum interface{}
			err := r.r.Decode(&datum)
			if err != nil {
				r.done = true
				if errors.Is(err, io.EOF) {
					r.err = nil
				}
				r.err = err
				return false
			}
			err = r.ldr.loadDatum(datum)
			if err != nil {
				r.err = err
				return false
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

// WithAllocator specifies the Arrow memory allocator used while building records.
func WithAllocator(mem memory.Allocator) Option {
	return func(cfg config) {
		cfg.mem = mem
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
