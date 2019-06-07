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

package arrjson // import "github.com/apache/arrow/go/arrow/internal/arrjson"

import (
	"encoding/json"
	"io"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/ipc"
)

type Writer struct {
	enc *json.Encoder
}

func NewWriter(w io.Writer, schema *arrow.Schema) (*Writer, error) {
	ww := &Writer{enc: json.NewEncoder(w)}
	ww.enc.SetIndent("", "  ")

	err := ww.writeSchema(schema)
	if err != nil {
		return nil, err
	}
	return ww, nil
}

func (w *Writer) Write(rec array.Record) error {
	return nil
}

func (w *Writer) writeSchema(s *arrow.Schema) error {
	return nil
}

var (
	_ ipc.RecordWriter = (*Writer)(nil)
)
