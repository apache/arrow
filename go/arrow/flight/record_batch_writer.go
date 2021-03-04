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

package flight

import (
	"bytes"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/ipc"
	"github.com/apache/arrow/go/arrow/memory"
)

// DataStreamWriter is an interface that represents an Arrow Flight stream
// writer that writes FlightData objects
type DataStreamWriter interface {
	Send(*FlightData) error
}

type flightPayloadWriter struct {
	w   DataStreamWriter
	fd  FlightData
	buf bytes.Buffer
}

func (f *flightPayloadWriter) Start() error { return nil }
func (f *flightPayloadWriter) WritePayload(payload ipc.Payload) error {
	m := payload.Meta()
	defer m.Release()

	f.fd.DataHeader = m.Bytes()
	f.buf.Reset()

	payload.SerializeBody(&f.buf)
	f.fd.DataBody = f.buf.Bytes()
	return f.w.Send(&f.fd)
}

func (f *flightPayloadWriter) Close() error { return nil }

// NewRecordWriter can be used to construct a writer for arrow flight via
// the grpc stream handler to write flight data objects and write
// record batches to the stream. Options passed here will be passed to
// ipc.NewWriter
func NewRecordWriter(w DataStreamWriter, opts ...ipc.Option) *ipc.Writer {
	return ipc.NewWriterWithPayloadWriter(&flightPayloadWriter{w: w}, opts...)
}

// SerializeSchema returns the serialized schema bytes for use in Arrow Flight
// protobuf messages.
func SerializeSchema(rec *arrow.Schema, mem memory.Allocator) []byte {
	// even though the spec says to send the message as in Schema.fbs,
	// it looks like all the implementations actually send a fully serialized
	// record batch just with no rows. So let's follow that pattern.
	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(rec), ipc.WithAllocator(mem))
	w.Close()
	return buf.Bytes()
}
