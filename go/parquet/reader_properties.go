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

	"github.com/apache/arrow/go/arrow/ipc"
	"github.com/apache/arrow/go/arrow/memory"
	"golang.org/x/xerrors"
)

// ReaderProperties are used to define how the file reader will handle buffering and allocating buffers
type ReaderProperties struct {
	alloc                 memory.Allocator
	BufferSize            int64
	FileDecryptProps      *FileDecryptionProperties
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
func (r *ReaderProperties) GetStream(source io.ReaderAt, start, nbytes int64) (ipc.ReadAtSeeker, error) {
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
