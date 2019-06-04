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

package ipc // import "github.com/apache/arrow/go/arrow/ipc"

import (
	"io"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
)

const (
	errNotArrowFile             = errString("arrow/ipc: not an Arrow file")
	errInconsistentFileMetadata = errString("arrow/ipc: file is smaller than indicated metadata size")
	errInconsistentSchema       = errString("arrow/ipc: tried to write record batch with different schema")
	errMaxRecursion             = errString("arrow/ipc: max recursion depth reached")
	errBigArray                 = errString("arrow/ipc: array larger than 2^31-1 in length")

	kArrowAlignment    = 64 // buffers are padded to 64b boundaries (for SIMD)
	kTensorAlignment   = 64 // tensors are padded to 64b boundaries
	kArrowIPCAlignment = 8  // align on 8b boundaries in IPC
)

var (
	paddingBytes [kArrowAlignment]byte
	kEOS         = [4]byte{0, 0, 0, 0} // end of stream message
)

func paddedLength(nbytes int64, alignment int32) int64 {
	align := int64(alignment)
	return ((nbytes + align - 1) / align) * align
}

type errString string

func (s errString) Error() string {
	return string(s)
}

type ReadAtSeeker interface {
	io.Reader
	io.Seeker
	io.ReaderAt
}

type config struct {
	alloc  memory.Allocator
	schema *arrow.Schema
	footer struct {
		offset int64
	}
}

func newConfig(opts ...Option) *config {
	cfg := &config{
		alloc: memory.NewGoAllocator(),
	}

	for _, opt := range opts {
		opt(cfg)
	}

	return cfg
}

// Option is a functional option to configure opening or creating Arrow files
// and streams.
type Option func(*config)

// WithFooterOffset specifies the Arrow footer position in bytes.
func WithFooterOffset(offset int64) Option {
	return func(cfg *config) {
		cfg.footer.offset = offset
	}
}

// WithAllocator specifies the Arrow memory allocator used while building records.
func WithAllocator(mem memory.Allocator) Option {
	return func(cfg *config) {
		cfg.alloc = mem
	}
}

// WithSchema specifies the Arrow schema to be used for reading or writing.
func WithSchema(schema *arrow.Schema) Option {
	return func(cfg *config) {
		cfg.schema = schema
	}
}

// RecordReader is the interface that iterates over a stream of records
type RecordReader interface {
	// Read reads the current record from the underlying stream and an error, if any.
	// When the Reader reaches the end of the underlying stream, it returns (nil, io.EOF).
	Read() (array.Record, error)
}

// RecordReaderAt is the interface that wraps the ReadAt method.
type RecordReaderAt interface {
	// ReadAt reads the i-th record from the underlying stream and an error, if any.
	ReadAt(i int64) (array.Record, error)
}

// RecordWriter is the interface that wraps the Write method.
type RecordWriter interface {
	Write(rec array.Record) error
}

// Copy copies all the records available from src to dst.
// Copy returns the number of records copied and the first error
// encountered while copying, if any.
//
// A successful Copy returns err == nil, not err == EOF. Because Copy is
// defined to read from src until EOF, it does not treat an EOF from Read as an
// error to be reported.
func Copy(dst RecordWriter, src RecordReader) (n int64, err error) {
	for {
		rec, err := src.Read()
		if err != nil {
			if err == io.EOF {
				return n, nil
			}
			return n, err
		}
		err = dst.Write(rec)
		if err != nil {
			return n, err
		}
		n++
	}
}

// CopyN copies n records (or until an error) from src to dst. It returns the
// number of records copied and the earliest error encountered while copying. On
// return, written == n if and only if err == nil.
func CopyN(dst RecordWriter, src RecordReader, n int64) (written int64, err error) {
	for ; written < n; written++ {
		rec, err := src.Read()
		if err != nil {
			if err == io.EOF && written == n {
				return written, nil
			}
			return written, err
		}
		err = dst.Write(rec)
		if err != nil {
			return written, err
		}
	}

	if written != n && err == nil {
		err = io.EOF
	}
	return written, err
}

var (
	_ RecordReader = (*Reader)(nil)
	_ RecordWriter = (*Writer)(nil)
	_ RecordReader = (*FileReader)(nil)
	_ RecordWriter = (*FileWriter)(nil)

	_ RecordReaderAt = (*FileReader)(nil)
)
