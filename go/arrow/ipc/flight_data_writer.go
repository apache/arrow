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

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/arrio"
	"github.com/apache/arrow/go/arrow/bitutil"
	"github.com/apache/arrow/go/arrow/flight"
	"github.com/apache/arrow/go/arrow/memory"
	flatbuffers "github.com/google/flatbuffers/go"
	"golang.org/x/xerrors"
)

// FlightDataStreamWriter wraps a grpc stream for sending FlightData
type FlightDataStreamWriter interface {
	Send(*flight.FlightData) error
}

// FlightDataWriter is a stream writer for writing with Flight RPC
type FlightDataWriter struct {
	w   FlightDataStreamWriter
	fd  flight.FlightData
	buf bytes.Buffer

	mem     memory.Allocator
	started bool
	schema  *arrow.Schema
}

// NewFlightDataWriter returns a writer for writing array Records to a flight data stream.
func NewFlightDataWriter(w FlightDataStreamWriter, opts ...Option) *FlightDataWriter {
	cfg := newConfig(opts...)
	return &FlightDataWriter{
		w:      w,
		mem:    cfg.alloc,
		schema: cfg.schema,
	}
}

func (w *FlightDataWriter) start() error {
	w.started = true

	ps := payloadsFromSchema(w.schema, w.mem, nil)
	defer ps.Release()

	for i := range ps {
		if err := w.writePayload(&ps[i]); err != nil {
			return err
		}
	}

	return nil
}

func (w *FlightDataWriter) Close() (err error) {
	if !w.started {
		err = w.start()
	}

	return err
}

// Write the provided record to the underlying stream
func (w *FlightDataWriter) Write(rec array.Record) error {
	if !w.started {
		err := w.start()
		if err != nil {
			return err
		}
	}

	schema := rec.Schema()
	if schema == nil || !schema.Equal(w.schema) {
		return errInconsistentSchema
	}

	const allow64b = true
	var (
		data = payload{}
		enc  = newRecordEncoder(w.mem, 0, kMaxNestingDepth, allow64b)
	)
	defer data.Release()

	if err := enc.Encode(&data, rec); err != nil {
		return xerrors.Errorf("arrow/ipc: could not encode record to payload: %w", err)
	}

	return w.writePayload(&data)
}

func (w *FlightDataWriter) writePayload(data *payload) (err error) {
	w.fd.DataHeader = data.meta.Bytes()
	tmp := &w.buf
	tmp.Reset()

	for _, bufs := range data.body {
		if bufs == nil {
			continue
		}

		size := int64(bufs.Len())
		padding := bitutil.CeilByte64(size) - size
		if size > 0 {
			_, err = tmp.Write(bufs.Bytes())
			if err != nil {
				return xerrors.Errorf("arrow/ipc: could not write payload message body: %w", err)
			}
		}

		if padding > 0 {
			_, err = tmp.Write(paddingBytes[:padding])
			if err != nil {
				return xerrors.Errorf("arrow/ipc: could not write payload message padding: %w", err)
			}
		}
	}

	w.fd.DataBody = tmp.Bytes()
	return w.w.Send(&w.fd)
}

func FlightInfoSchemaBytes(schema *arrow.Schema, mem memory.Allocator) []byte {
	dict := newMemo()
	b := flatbuffers.NewBuilder(1024)
	offset := schemaToFB(b, schema, &dict)
	b.Finish(offset)
	return b.FinishedBytes()
}

var (
	_ arrio.Writer = (*FlightDataWriter)(nil)
)
