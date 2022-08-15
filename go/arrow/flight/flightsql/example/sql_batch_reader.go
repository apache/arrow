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

//go:build go1.17
// +build go1.17

package example

import (
	"database/sql"
	"reflect"
	"strings"
	"sync/atomic"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/flight/flightsql"
	"github.com/apache/arrow/go/v10/arrow/internal/debug"
	"github.com/apache/arrow/go/v10/arrow/memory"
)

func getArrowTypeFromString(dbtype string) arrow.DataType {
	dbtype = strings.ToLower(dbtype)
	if strings.HasPrefix(dbtype, "varchar") {
		return arrow.BinaryTypes.String
	}

	switch dbtype {
	case "int", "integer":
		return arrow.PrimitiveTypes.Int64
	case "real":
		return arrow.PrimitiveTypes.Float64
	case "blob":
		return arrow.BinaryTypes.Binary
	case "text", "date", "char":
		return arrow.BinaryTypes.String
	default:
		panic("invalid sqlite type: " + dbtype)
	}
}

func getArrowType(c *sql.ColumnType) arrow.DataType {
	dbtype := strings.ToLower(c.DatabaseTypeName())
	if dbtype == "" {
		switch c.ScanType().Kind() {
		case reflect.Int, reflect.Int64, reflect.Uint64:
			return arrow.PrimitiveTypes.Int64
		case reflect.Float32, reflect.Float64:
			return arrow.PrimitiveTypes.Float64
		}
	}
	return getArrowTypeFromString(dbtype)
}

const maxBatchSize = 1024

type SqlBatchReader struct {
	refCount int64

	schema *arrow.Schema
	rows   *sql.Rows
	record arrow.Record
	bldr   *array.RecordBuilder
	err    error

	rowdest []interface{}
}

func NewSqlBatchReaderWithSchema(mem memory.Allocator, schema *arrow.Schema, rows *sql.Rows) (*SqlBatchReader, error) {
	rowdest := make([]interface{}, len(schema.Fields()))
	for i, f := range schema.Fields() {
		switch f.Type.ID() {
		case arrow.UINT8:
			if f.Nullable {
				rowdest[i] = &sql.NullInt32{}
			} else {
				rowdest[i] = new(uint8)
			}
		case arrow.INT32:
			if f.Nullable {
				rowdest[i] = &sql.NullInt32{}
			} else {
				rowdest[i] = new(int32)
			}
		case arrow.INT64:
			if f.Nullable {
				rowdest[i] = &sql.NullInt64{}
			} else {
				rowdest[i] = new(int64)
			}
		case arrow.FLOAT64:
			if f.Nullable {
				rowdest[i] = &sql.NullFloat64{}
			} else {
				rowdest[i] = new(float64)
			}
		case arrow.BINARY:
			var b []byte
			rowdest[i] = &b
		case arrow.STRING:
			if f.Nullable {
				rowdest[i] = &sql.NullString{}
			} else {
				rowdest[i] = new(string)
			}
		}
	}

	return &SqlBatchReader{
		refCount: 1,
		bldr:     array.NewRecordBuilder(mem, schema),
		schema:   schema,
		rowdest:  rowdest,
		rows:     rows}, nil
}

func NewSqlBatchReader(mem memory.Allocator, rows *sql.Rows) (*SqlBatchReader, error) {
	bldr := flightsql.NewColumnMetadataBuilder()

	cols, err := rows.ColumnTypes()
	if err != nil {
		rows.Close()
		return nil, err
	}

	rowdest := make([]interface{}, len(cols))
	fields := make([]arrow.Field, len(cols))
	for i, c := range cols {
		fields[i].Name = c.Name()
		fields[i].Nullable, _ = c.Nullable()
		fields[i].Type = getArrowType(c)
		fields[i].Metadata = getColumnMetadata(bldr, getSqlTypeFromTypeName(c.DatabaseTypeName()), "")
		switch fields[i].Type.ID() {
		case arrow.UINT8:
			if fields[i].Nullable {
				rowdest[i] = &sql.NullInt32{}
			} else {
				rowdest[i] = new(uint8)
			}
		case arrow.INT32:
			if fields[i].Nullable {
				rowdest[i] = &sql.NullInt32{}
			} else {
				rowdest[i] = new(int32)
			}
		case arrow.INT64:
			if fields[i].Nullable {
				rowdest[i] = &sql.NullInt64{}
			} else {
				rowdest[i] = new(int64)
			}
		case arrow.FLOAT64:
			if fields[i].Nullable {
				rowdest[i] = &sql.NullFloat64{}
			} else {
				rowdest[i] = new(float64)
			}
		case arrow.BINARY:
			var b []byte
			rowdest[i] = &b
		case arrow.STRING:
			if fields[i].Nullable {
				rowdest[i] = &sql.NullString{}
			} else {
				rowdest[i] = new(string)
			}
		}
	}

	schema := arrow.NewSchema(fields, nil)
	return &SqlBatchReader{
		refCount: 1,
		bldr:     array.NewRecordBuilder(mem, schema),
		schema:   schema,
		rowdest:  rowdest,
		rows:     rows}, nil
}

func (r *SqlBatchReader) Retain() {
	atomic.AddInt64(&r.refCount, 1)
}

func (r *SqlBatchReader) Release() {
	debug.Assert(atomic.LoadInt64(&r.refCount) > 0, "too many releases")

	if atomic.AddInt64(&r.refCount, -1) == 0 {
		r.rows.Close()
		r.rows, r.schema, r.rowdest = nil, nil, nil
		r.bldr.Release()
		r.bldr = nil
		if r.record != nil {
			r.record.Release()
			r.record = nil
		}
	}
}
func (r *SqlBatchReader) Schema() *arrow.Schema { return r.schema }

func (r *SqlBatchReader) Record() arrow.Record { return r.record }

func (r *SqlBatchReader) Err() error { return r.err }

func (r *SqlBatchReader) Next() bool {
	if r.record != nil {
		r.record.Release()
		r.record = nil
	}

	rows := 0
	for rows < maxBatchSize && r.rows.Next() {
		if err := r.rows.Scan(r.rowdest...); err != nil {
			r.err = err
			return false
		}

		for i, v := range r.rowdest {
			fb := r.bldr.Field(i)
			switch v := v.(type) {
			case *uint8:
				fb.(*array.Uint8Builder).Append(*v)
			case *int64:
				fb.(*array.Int64Builder).Append(*v)
			case *sql.NullInt64:
				if !v.Valid {
					fb.AppendNull()
				} else {
					fb.(*array.Int64Builder).Append(v.Int64)
				}
			case *int32:
				fb.(*array.Int32Builder).Append(*v)
			case *sql.NullInt32:
				if !v.Valid {
					fb.AppendNull()
				} else {
					switch b := fb.(type) {
					case *array.Int32Builder:
						b.Append(v.Int32)
					case *array.Uint8Builder:
						b.Append(uint8(v.Int32))
					}
				}
			case *float64:
				fb.(*array.Float64Builder).Append(*v)
			case *sql.NullFloat64:
				if !v.Valid {
					fb.AppendNull()
				} else {
					fb.(*array.Float64Builder).Append(v.Float64)
				}
			case *[]byte:
				if v == nil {
					fb.AppendNull()
				} else {
					fb.(*array.BinaryBuilder).Append(*v)
				}
			case *string:
				fb.(*array.StringBuilder).Append(*v)
			case *sql.NullString:
				if !v.Valid {
					fb.AppendNull()
				} else {
					fb.(*array.StringBuilder).Append(v.String)
				}
			}
		}

		rows++
	}

	r.record = r.bldr.NewRecord()
	return rows > 0
}
