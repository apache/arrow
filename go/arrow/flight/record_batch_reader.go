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
	"sync/atomic"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/internal/debug"
	"github.com/apache/arrow/go/arrow/ipc"
	"github.com/apache/arrow/go/arrow/memory"
)

// DataStreamReader is an interface for receiving flight data messages on a stream
// such as via grpc with Arrow Flight.
type DataStreamReader interface {
	Recv() (*FlightData, error)
}

type dataMessageReader struct {
	rdr DataStreamReader

	refCount int64
	msg      *ipc.Message
	err      error
}

func (d *dataMessageReader) Message() (*ipc.Message, error) {
	fd, err := d.rdr.Recv()
	if err != nil {
		return nil, err
	}

	return ipc.NewMessage(memory.NewBufferBytes(fd.DataHeader), memory.NewBufferBytes(fd.DataBody)), nil
}

func (d *dataMessageReader) Retain() {
	atomic.AddInt64(&d.refCount, 1)
}

func (d *dataMessageReader) Release() {
	debug.Assert(atomic.LoadInt64(&d.refCount) > 0, "too many releases")

	if atomic.AddInt64(&d.refCount, -1) == 0 {
		if d.msg != nil {
			d.msg.Release()
			d.msg = nil
		}
	}
}

// NewRecordReader constructs an ipc reader using the flight data stream reader
// as the source of the ipc messages, opts passed will be passed to the underlying
// ipc.Reader such as ipc.WithSchema and ipc.WithAllocator
func NewRecordReader(r DataStreamReader, opts ...ipc.Option) (*ipc.Reader, error) {
	return ipc.NewReaderFromMessageReader(&dataMessageReader{rdr: r}, opts...)
}

// DeserializeSchema takes the schema bytes from FlightInfo or SchemaResult
// and returns the deserialized arrow schema.
func DeserializeSchema(info []byte, mem memory.Allocator) (*arrow.Schema, error) {
	// even though the Flight proto file says that the bytes should be the
	// flatbuffer message as per Schema.fbs, the current implementations send
	// a serialized recordbatch with no body rows rather than just the
	// schema message. So let's make sure to follow that.
	rdr, err := ipc.NewReader(bytes.NewReader(info), ipc.WithAllocator(mem))
	if err != nil {
		return nil, err
	}
	defer rdr.Release()
	return rdr.Schema(), nil
}
