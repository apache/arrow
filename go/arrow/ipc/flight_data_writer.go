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
	"fmt"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/bitutil"
	"github.com/apache/arrow/go/arrow/memory"
)

type FlightDataStream interface {
	Send(*FlightData) error
}

// FlightDataWriter is an Arrow stream writer.
type FlightDataWriter struct {
	w   FlightDataStream
	fd  FlightData
	buf bytes.Buffer

	mem     memory.Allocator
	started bool
	schema  *arrow.Schema
}

// NewFlightDataWriter returns a writer that writes records to the provided Flight stream.
func NewFlightDataWriter(w FlightDataStream, opts ...Option) *FlightDataWriter {
	cfg := newConfig(opts...)
	return &FlightDataWriter{
		w:      w,
		mem:    cfg.alloc,
		schema: cfg.schema,
	}
}

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
		return fmt.Errorf("arrow/ipc: could not encode record to payload: %w", err)
	}

	return w.writePayload(&data)
}

func (w *FlightDataWriter) Close() (err error) {
	if !w.started {
		err = w.start()
	}
	return err
}

func (w *FlightDataWriter) start() error {
	w.started = true

	// write out schema payloads
	ps := payloadsFromSchema(w.schema, w.mem, nil)
	defer ps.Release()

	for i := range ps {
		if err := w.writePayload(&ps[i]); err != nil {
			return err
		}
	}

	return nil
}

func (w *FlightDataWriter) writePayload(data *payload) (err error) {
	w.fd.DataHeader = data.meta.Bytes()
	tmp := &w.buf
	tmp.Reset()

	// now write the buffers
	for _, bufs := range data.body {
		if bufs == nil {
			continue
		}

		var (
			size    int64
			padding int64
		)

		// the buffer might be null if we are handling zero row lengths.

		size = int64(bufs.Len())
		padding = bitutil.CeilByte64(size) - size

		if size > 0 {
			_, err = tmp.Write(bufs.Bytes())
			if err != nil {
				return fmt.Errorf("arrow/ipc: could not write payload message body: %w", err)
			}
		}

		if padding > 0 {
			_, err = tmp.Write(paddingBytes[:padding])
			if err != nil {
				return fmt.Errorf("arrow/ipc: could not write payload message padding: %w", err)
			}
		}
	}

	w.fd.DataBody = tmp.Bytes()

	return w.w.Send(&w.fd)
}
