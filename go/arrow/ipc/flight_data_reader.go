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

package ipc

import (
	"bytes"
	"io"
	"sync/atomic"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/arrio"
	"github.com/apache/arrow/go/arrow/flight"
	"github.com/apache/arrow/go/arrow/internal/debug"
	"github.com/apache/arrow/go/arrow/internal/flatbuf"
	"github.com/apache/arrow/go/arrow/memory"
	"golang.org/x/xerrors"
)

// FlightDataStreamReader wraps a grpc stream for receiving FlightData objects
type FlightDataStreamReader interface {
	Recv() (*flight.FlightData, error)
}

// FlightDataReader reads records from a stream of messages
type FlightDataReader struct {
	r      FlightDataStreamReader
	schema *arrow.Schema

	refCount int64
	rec      array.Record
	err      error

	types dictTypeMap
	memo  dictMemo

	mem memory.Allocator

	done bool
}

// NewFlightDataReader returns a reader that will produce records from a flight data stream
//
// implementation is generally based on the ipc.Reader, expecting the first message to be the
// schema with the subsequent messages being the record batches.
func NewFlightDataReader(r FlightDataStreamReader, opts ...Option) (*FlightDataReader, error) {
	cfg := newConfig(opts...)

	rr := &FlightDataReader{
		r:   r,
		mem: cfg.alloc,
	}

	msg, err := rr.nextMessage()
	if err != nil {
		return nil, xerrors.Errorf("arrow/ipc: could not read message schema: %w", err)
	}

	if msg.Type() != MessageSchema {
		return nil, xerrors.Errorf("arrow/ipc: invalid message type (got=%v, want=%v)", msg.Type(), MessageSchema)
	}

	// FIXME(sbinet) refactor msg-header handling (a la ipc.Reader.readSchema)
	var schemaFB flatbuf.Schema
	initFB(&schemaFB, msg.msg.Header)

	rr.types, err = dictTypesFromFB(&schemaFB)
	if err != nil {
		return nil, xerrors.Errorf("arrow/ipc: could not read dictionary types from message schema: %w", err)
	}

	// TODO(sbinet): see ipc.Reader.readSchema
	for range rr.types {
		panic("not implemented") // ReadNextDictionary
	}

	rr.schema, err = schemaFromFB(&schemaFB, &rr.memo)
	if err != nil {
		return nil, xerrors.Errorf("arrow/ipc: could not decode schema from message schema: %w", err)
	}

	if cfg.schema != nil && !cfg.schema.Equal(rr.schema) {
		return nil, errInconsistentSchema
	}

	return rr, nil
}

func (f *FlightDataReader) nextMessage() (*Message, error) {
	fd, err := f.r.Recv()
	if err != nil {
		return nil, err
	}

	return NewMessage(memory.NewBufferBytes(fd.DataHeader), memory.NewBufferBytes(fd.DataBody)), nil
}

func (f *FlightDataReader) next() bool {
	var msg *Message
	msg, f.err = f.nextMessage()
	if f.err != nil {
		f.done = true
		if f.err == io.EOF {
			f.err = nil
		}
		return false
	}

	if got, want := msg.Type(), MessageRecordBatch; got != want {
		f.err = xerrors.Errorf("arrow/ipc: invalid message type (got=%v, want=%v)", got, want)
		return false
	}

	f.rec = newRecord(f.schema, msg.meta, bytes.NewReader(msg.body.Bytes()))
	return true
}

// Record returns the current record that has been extracted from the stream.
// It is valid until the next call to Next or Read
func (f *FlightDataReader) Record() array.Record {
	return f.rec
}

// Next returns whether a record was able to be extracted from the stream or not.
func (f *FlightDataReader) Next() bool {
	if f.rec != nil {
		f.rec.Release()
		f.rec = nil
	}

	if f.err != nil || f.done {
		return false
	}

	return f.next()
}

// Read reads the current record from the flight stream and an error, if any.
// When we reach the end of the flight stream it will return (nil, io.EOF).
// Also calls release on the previous existing record if any.
func (f *FlightDataReader) Read() (array.Record, error) {
	if f.rec != nil {
		f.rec.Release()
		f.rec = nil
	}

	if !f.next() {
		if f.done {
			return nil, io.EOF
		}
		return nil, f.err
	}

	return f.rec, nil
}

// Retain increases the refcount by 1.
// Retain can be called by multiple goroutines simultaneously.
func (f *FlightDataReader) Retain() {
	atomic.AddInt64(&f.refCount, 1)
}

// Release decreases the refcount by 1.
// When the refcount is 0 the memory is freed.
// Release may be called simultaneously from multiple goroutines.
func (f *FlightDataReader) Release() {
	debug.Assert(atomic.LoadInt64(&f.refCount) > 0, "too many releases")

	if atomic.AddInt64(&f.refCount, -1) == 0 {
		if f.rec != nil {
			f.rec.Release()
			f.rec = nil
		}
		if f.r != nil {
			f.r = nil
		}
	}
}

// Err returns the last error encounted during the iteration of the stream.
func (f *FlightDataReader) Err() error { return f.err }

// Schema returns the schema of the underlying records as described by the
// first message received.
func (f *FlightDataReader) Schema() *arrow.Schema { return f.schema }

func SchemaFromFlightInfo(b []byte) (*arrow.Schema, error) {
	fb := flatbuf.GetRootAsSchema(b, 0)
	dict := newMemo()
	return schemaFromFB(fb, &dict)
}

var (
	_ array.RecordReader = (*FlightDataReader)(nil)
	_ arrio.Reader       = (*FlightDataReader)(nil)
)
