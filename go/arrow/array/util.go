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

package array

import (
	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/memory"
)

// RecordToStructArray constructs a struct array from the columns of the record batch
// by referencing them, zero-copy.
func RecordToStructArray(rec Record) *Struct {
	cols := make([]*Data, rec.NumCols())
	for i, c := range rec.Columns() {
		cols[i] = c.Data()
	}

	data := NewData(arrow.StructOf(rec.Schema().Fields()...), int(rec.NumRows()), []*memory.Buffer{nil}, cols, 0, 0)
	defer data.Release()

	return NewStructData(data)
}

// RecordFromStructArray is a convenience function for converting a struct array into
// a record batch without copying the data. If the passed in schema is nil, the fields
// of the struct will be used to define the record batch. Otherwise the passed in
// schema will be used to create the record batch. If passed in, the schema must match
// the fields of the struct column.
//
// When constructing a Record from a Struct array, the top level null bitmap is just
// dropped, so the child arrays will all use their own individual null bitmaps.
func RecordFromStructArray(in *Struct, schema *arrow.Schema) Record {
	if schema == nil {
		schema = arrow.NewSchema(in.DataType().(*arrow.StructType).Fields(), nil)
	}

	return NewRecord(schema, in.fields, int64(in.Len()))
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
