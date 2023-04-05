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

package arrjson

import (
	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/memory"
)

type config struct {
	alloc  memory.Allocator
	schema *arrow.Schema
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
