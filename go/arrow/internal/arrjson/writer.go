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
	"fmt"
	"io"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/arrio"
	"github.com/apache/arrow/go/v12/arrow/internal/dictutils"
)

const (
	jsonIndent    = "  "
	jsonPrefix    = "  "
	jsonRecPrefix = "    "
)

type rawJSON struct {
	Schema       Schema       `json:"schema"`
	Records      []Record     `json:"batches"`
	Dictionaries []Dictionary `json:"dictionaries,omitempty"`
}

type Writer struct {
	w io.Writer

	nrecs  int64
	raw    rawJSON
	mapper dictutils.Mapper
}

func NewWriter(w io.Writer, schema *arrow.Schema) (*Writer, error) {
	ww := &Writer{
		w: w,
	}
	ww.mapper.ImportSchema(schema)
	ww.raw.Schema = schemaToJSON(schema, &ww.mapper)
	ww.raw.Records = make([]Record, 0)
	return ww, nil
}

func (w *Writer) Write(rec arrow.Record) error {
	if w.nrecs == 0 {
		pairs, err := dictutils.CollectDictionaries(rec, &w.mapper)
		if err != nil {
			return err
		}

		if len(pairs) > 0 {
			w.raw.Dictionaries = make([]Dictionary, 0, len(pairs))
		}

		for _, p := range pairs {
			defer p.Dict.Release()
			sc := arrow.NewSchema([]arrow.Field{{Name: fmt.Sprintf("DICT%d", p.ID), Type: p.Dict.DataType(), Nullable: true}}, nil)
			dummy := array.NewRecord(sc, []arrow.Array{p.Dict}, int64(p.Dict.Len()))
			defer dummy.Release()
			w.raw.Dictionaries = append(w.raw.Dictionaries, Dictionary{ID: p.ID, Data: recordToJSON(dummy)})
		}
	}

	w.raw.Records = append(w.raw.Records, recordToJSON(rec))
	w.nrecs++
	return nil
}

func (w *Writer) Close() error {
	if w.w == nil {
		return nil
	}

	enc := json.NewEncoder(w.w)
	enc.SetIndent("", jsonIndent)
	// ensure that we don't convert <, >, !, etc. to their unicode equivalents
	// in the output json since we're not using this in an HTML context so that
	// we can make sure that the json files match.
	enc.SetEscapeHTML(false)
	return enc.Encode(w.raw)
}

var (
	_ arrio.Writer = (*Writer)(nil)
)
