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

package ipc

import (
	"io"

	"github.com/apache/arrow/go/arrow"
	"github.com/pkg/errors"
)

// Reader reads records from an io.Reader.
// Reader expects a schema (plus any dictionaries) as the first messages
// in the stream, followed by records.
type Reader struct {
	msg *MessageReader

	types  dictTypeMap
	memo   dictMemo
	schema *arrow.Schema
}

func NewReader(r io.Reader) (*Reader, error) {
	msg, err := NewMessageReader(r)
	if err != nil {
		return nil, errors.Wrap(err, "arrow/ipc: could create message reader")
	}

	rr := &Reader{
		msg:   msg,
		types: make(dictTypeMap),
		memo:  newMemo(),
	}

	panic("not implemented")
	return rr, nil
}

func (r *Reader) Schema() *arrow.Schema { return r.schema }
