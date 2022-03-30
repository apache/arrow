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

package parquet

import (
	"bytes"
	"io"

	"github.com/apache/arrow/go/v8/arrow/memory"
	"golang.org/x/xerrors"
)

// ReaderProperties are used to define how the file reader will handle buffering and allocating buffers
type ReaderProperties struct {
	alloc memory.Allocator
	// Default buffer size to utilize when reading chunks
	BufferSize int64
	// create with NewFileDecryptionProperties if dealing with an encrypted file
	FileDecryptProps *FileDecryptionProperties
	// If this is set to true, then the reader will use SectionReader to
	// just use the read stream when reading data. Otherwise we will buffer
	// the data we're going to read into memory first and then read that buffer.
	//
	// If reading from higher latency IO, like S3, it might improve performance to
	// set this to true in order to read the entire row group in at once rather than
	// make multiple smaller data requests. For low latency IO streams or if only
	// reading small portions / subsets  of the parquet file, this can be set to false
	// to reduce the amount of IO performed in order to avoid reading excess amounts of data.
	BufferedStreamEnabled bool
}

// NewReaderProperties returns the default Reader Properties using the provided allocator.
//
// If nil is passed for the allocator, then memory.DefaultAllocator will be used.
func NewReaderProperties(alloc memory.Allocator) *ReaderProperties {
	if alloc == nil {
		alloc = memory.DefaultAllocator
	}
	return &ReaderProperties{alloc, DefaultBufSize, nil, false}
}

// Allocator returns the allocator that the properties were initialized with
func (r *ReaderProperties) Allocator() memory.Allocator { return r.alloc }

// GetStream returns a section of the underlying reader based on whether or not BufferedStream is enabled.
//
// If BufferedStreamEnabled is true, it creates an io.SectionReader, otherwise it will read the entire section
// into a buffer in memory and return a bytes.NewReader for that buffer.
func (r *ReaderProperties) GetStream(source io.ReaderAt, start, nbytes int64) (io.ReadSeeker, error) {
	if r.BufferedStreamEnabled {
		return io.NewSectionReader(source, start, nbytes), nil
	}

	data := make([]byte, nbytes)
	n, err := source.ReadAt(data, start)
	if err != nil {
		return nil, xerrors.Errorf("parquet: tried reading from file, but got error: %w", err)
	}
	if n != int(nbytes) {
		return nil, xerrors.Errorf("parquet: tried reading %d bytes starting at position %d from file but only got %d", nbytes, start, n)
	}

	return bytes.NewReader(data), nil
}
