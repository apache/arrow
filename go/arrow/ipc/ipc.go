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

	"github.com/apache/arrow/go/arrow/memory"
)

const (
	errNotArrowFile             = errString("arrow/ipc: not an Arrow file")
	errInconsistentFileMetadata = errString("arrow/ipc: file is smaller than indicated metadata size")
)

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
	footer struct {
		offset int64
	}
}

func newConfig() *config {
	return &config{
		alloc: memory.NewGoAllocator(),
	}
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
