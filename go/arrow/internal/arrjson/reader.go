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
	"encoding/json"
	"io"
	"sync/atomic"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/arrio"
	"github.com/apache/arrow/go/v13/arrow/internal/debug"
	"github.com/apache/arrow/go/v13/arrow/internal/dictutils"
)

type Reader struct {
	refs int64

	schema *arrow.Schema
	recs   []arrow.Record
	memo   *dictutils.Memo

	irec int // current record index. used for the arrio.Reader interface.
}

func NewReader(r io.Reader, opts ...Option) (*Reader, error) {
	dec := json.NewDecoder(r)
	dec.UseNumber()
	var raw rawJSON
	err := dec.Decode(&raw)
	if err != nil {
		return nil, err
	}

	cfg := newConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	memo := dictutils.NewMemo()
	schema := schemaFromJSON(raw.Schema, &memo)
	dictionariesFromJSON(cfg.alloc, raw.Dictionaries, &memo)
	rr := &Reader{
		refs:   1,
		schema: schema,
		recs:   recordsFromJSON(cfg.alloc, schema, raw.Records, &memo),
		memo:   &memo,
	}
	return rr, nil
}

// Retain increases the reference count by 1.
// Retain may be called simultaneously from multiple goroutines.
func (r *Reader) Retain() {
	atomic.AddInt64(&r.refs, 1)
}

// Release decreases the reference count by 1.
// When the reference count goes to zero, the memory is freed.
// Release may be called simultaneously from multiple goroutines.
func (r *Reader) Release() {
	debug.Assert(atomic.LoadInt64(&r.refs) > 0, "too many releases")

	if atomic.AddInt64(&r.refs, -1) == 0 {
		for i, rec := range r.recs {
			if r.recs[i] != nil {
				rec.Release()
				r.recs[i] = nil
			}
		}
	}
}
func (r *Reader) Schema() *arrow.Schema { return r.schema }
func (r *Reader) NumRecords() int       { return len(r.recs) }

func (r *Reader) Read() (arrow.Record, error) {
	if r.irec == r.NumRecords() {
		return nil, io.EOF
	}
	rec := r.recs[r.irec]
	r.irec++
	return rec, nil
}

var (
	_ arrio.Reader = (*Reader)(nil)
)
