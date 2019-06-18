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

// Package csv reads CSV files and presents the extracted data as records, also
// writes data as record into CSV files
package csv

import (
	"errors"
	"fmt"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/memory"
)

var (
	ErrMismatchFields = errors.New("arrow/csv: number of records mismatch")
)

// Option configures a CSV reader/writer.
type Option func(config)
type config interface{}

// WithComma specifies the fields separation character used while parsing CSV files.
func WithComma(c rune) Option {
	return func(cfg config) {
		switch cfg := cfg.(type) {
		case *Reader:
			cfg.r.Comma = c
		case *Writer:
			cfg.w.Comma = c
		default:
			panic(fmt.Errorf("arrow/csv: unknown config type %T", cfg))
		}
	}
}

// WithComment specifies the comment character used while parsing CSV files.
func WithComment(c rune) Option {
	return func(cfg config) {
		switch cfg := cfg.(type) {
		case *Reader:
			cfg.r.Comment = c
		default:
			panic(fmt.Errorf("arrow/csv: unknown config type %T", cfg))
		}
	}
}

// WithAllocator specifies the Arrow memory allocator used while building records.
func WithAllocator(mem memory.Allocator) Option {
	return func(cfg config) {
		switch cfg := cfg.(type) {
		case *Reader:
			cfg.mem = mem
		default:
			panic(fmt.Errorf("arrow/csv: unknown config type %T", cfg))
		}
	}
}

// WithChunk specifies the chunk size used while parsing CSV files.
//
// If n is zero or 1, no chunking will take place and the reader will create
// one record per row.
// If n is greater than 1, chunks of n rows will be read.
// If n is negative, the reader will load the whole CSV file into memory and
// create one big record with all the rows.
func WithChunk(n int) Option {
	return func(cfg config) {
		switch cfg := cfg.(type) {
		case *Reader:
			cfg.chunk = n
		default:
			panic(fmt.Errorf("arrow/csv: unknown config type %T", cfg))
		}
	}
}

// WithCRLF specifies the line terminator used while writing CSV files.
// If useCRLF is true, \r\n is used as the line terminator, otherwise \n is used.
// The default value is false.
func WithCRLF(useCRLF bool) Option {
	return func(cfg config) {
		switch cfg := cfg.(type) {
		case *Writer:
			cfg.w.UseCRLF = useCRLF
		default:
			panic(fmt.Errorf("arrow/csv: unknown config type %T", cfg))
		}
	}
}

func WithHeader() Option {
	return func(cfg config) {
		switch cfg := cfg.(type) {
		case *Reader:
			cfg.header = true
		case *Writer:
			cfg.header = true
		default:
			panic(fmt.Errorf("arrow/csv: unknown config type %T", cfg))
		}
	}
}

func validate(schema *arrow.Schema) {
	for i, f := range schema.Fields() {
		switch ft := f.Type.(type) {
		case *arrow.BooleanType:
		case *arrow.Int8Type, *arrow.Int16Type, *arrow.Int32Type, *arrow.Int64Type:
		case *arrow.Uint8Type, *arrow.Uint16Type, *arrow.Uint32Type, *arrow.Uint64Type:
		case *arrow.Float32Type, *arrow.Float64Type:
		case *arrow.StringType:
		default:
			panic(fmt.Errorf("arrow/csv: field %d (%s) has invalid data type %T", i, f.Name, ft))
		}
	}
}
