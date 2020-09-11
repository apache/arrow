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

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/flight"
	"github.com/apache/arrow/go/arrow/internal/flatbuf"
	"github.com/apache/arrow/go/arrow/memory"
	"golang.org/x/xerrors"
)

type FlightDataStreamReader interface {
	Recv() (*flight.FlightData, error)
}

type FlightDataReader struct {
	r      FlightDataStreamReader
	schema *arrow.Schema

	rec array.Record
	err error

	types dictTypeMap
	memo  dictMemo

	mem memory.Allocator

	done bool
}

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

	var schemaFB flatbuf.Schema
	initFB(&schemaFB, msg.msg.Header)

	rr.types, err = dictTypesFromFB(&schemaFB)
	if err != nil {
		return nil, xerrors.Errorf("arrow/ipc: could not read dictionary types from message schema: %w", err)
	}

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

func (f *FlightDataReader) Record() array.Record {
	return f.rec
}

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

func (f *FlightDataReader) Err() error            { return f.err }
func (f *FlightDataReader) Schema() *arrow.Schema { return f.schema }
