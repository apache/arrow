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

	"github.com/apache/arrow/go/v7/arrow"
	"github.com/apache/arrow/go/v7/arrow/internal/debug"
	"github.com/apache/arrow/go/v7/arrow/ipc"
	"github.com/apache/arrow/go/v7/arrow/memory"
	"golang.org/x/xerrors"
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

	lastAppMetadata []byte
	descr           *FlightDescriptor
}

func (d *dataMessageReader) Message() (*ipc.Message, error) {
	fd, err := d.rdr.Recv()
	if err != nil {
		if d.msg != nil {
			// clear the previous message in the error case
			d.msg.Release()
			d.msg = nil
		}
		d.lastAppMetadata = nil
		d.descr = nil
		return nil, err
	}

	d.lastAppMetadata = fd.AppMetadata
	d.descr = fd.FlightDescriptor
	d.msg = ipc.NewMessage(memory.NewBufferBytes(fd.DataHeader), memory.NewBufferBytes(fd.DataBody))
	return d.msg, nil
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
		d.lastAppMetadata = nil
	}
}

// Reader is an ipc.Reader which also keeps track of the metadata from
// the FlightData messages as they come in, calling LatestAppMetadata
// will return the metadata bytes from the most recently read message.
type Reader struct {
	*ipc.Reader
	dmr *dataMessageReader
}

// Retain increases the reference count for the underlying message reader
// and ipc.Reader which are utilized by this Reader.
func (r *Reader) Retain() {
	r.Reader.Retain()
	r.dmr.Retain()
}

// Release reduces the reference count for the underlying message reader
// and ipc.Reader, when the reference counts become zero, the allocated
// memory is released for the stored record and metadata.
func (r *Reader) Release() {
	r.Reader.Release()
	r.dmr.Release()
}

// LatestAppMetadata returns the bytes from the AppMetadata field of the
// most recently read FlightData message that was processed by calling
// the Next function. The metadata returned would correspond to the record
// retrieved by calling Record().
func (r *Reader) LatestAppMetadata() []byte {
	return r.dmr.lastAppMetadata
}

// LatestFlightDescriptor returns a pointer to the last FlightDescriptor object
// that was received in the most recently read FlightData message that was
// processed by calling the Next function. The descriptor returned would correspond
// to the record retrieved by calling Record().
func (r *Reader) LatestFlightDescriptor() *FlightDescriptor {
	return r.dmr.descr
}

// NewRecordReader constructs an ipc reader using the flight data stream reader
// as the source of the ipc messages, opts passed will be passed to the underlying
// ipc.Reader such as ipc.WithSchema and ipc.WithAllocator
func NewRecordReader(r DataStreamReader, opts ...ipc.Option) (*Reader, error) {
	rdr := &Reader{dmr: &dataMessageReader{rdr: r}}
	var err error
	if rdr.Reader, err = ipc.NewReaderFromMessageReader(rdr.dmr, opts...); err != nil {
		return nil, xerrors.Errorf("arrow/flight: could not create flight reader: %w", err)
	}

	return rdr, nil
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
