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

package metadata

import (
	"fmt"
	"reflect"

	"github.com/apache/arrow/go/v13/parquet"
	"github.com/apache/arrow/go/v13/parquet/internal/encryption"
	format "github.com/apache/arrow/go/v13/parquet/internal/gen-go/parquet"
	"github.com/apache/arrow/go/v13/parquet/schema"
)

// RowGroupMetaData is a proxy around the thrift RowGroup meta data object
type RowGroupMetaData struct {
	rowGroup      *format.RowGroup
	Schema        *schema.Schema
	version       *AppVersion
	fileDecryptor encryption.FileDecryptor
}

// NewRowGroupMetaData constructs an object from the underlying thrift objects and schema,
// decrypting if provided and necessary. This is primarily used internally and consumers
// should use the RowGroupMetaDataBuilder rather than this directly.
func NewRowGroupMetaData(rg *format.RowGroup, sc *schema.Schema, version *AppVersion, decryptor encryption.FileDecryptor) *RowGroupMetaData {
	return &RowGroupMetaData{
		rowGroup:      rg,
		Schema:        sc,
		version:       version,
		fileDecryptor: decryptor,
	}
}

// NumColumns returns the number of column metadata objects in this row group
func (r *RowGroupMetaData) NumColumns() int {
	return len(r.rowGroup.GetColumns())
}

func (r *RowGroupMetaData) Equals(other *RowGroupMetaData) bool {
	return reflect.DeepEqual(r.rowGroup, other.rowGroup)
}

// NumRows is just the number of rows in this row group. All columns have the same
// number of rows for a row group regardless of repetition and definition levels.
func (r *RowGroupMetaData) NumRows() int64 { return r.rowGroup.NumRows }

// TotalByteSize is the total size of this rowgroup on disk
func (r *RowGroupMetaData) TotalByteSize() int64 { return r.rowGroup.GetTotalByteSize() }

// FileOffset is the location in the file where the data for this rowgroup begins
func (r *RowGroupMetaData) FileOffset() int64 { return r.rowGroup.GetFileOffset() }

func (r *RowGroupMetaData) TotalCompressedSize() int64 { return r.rowGroup.GetTotalCompressedSize() }

// Ordinal is the row group number in order for the given file.
func (r *RowGroupMetaData) Ordinal() int16 { return r.rowGroup.GetOrdinal() }

// ColumnChunk returns the metadata for the requested (0-based) chunk index
func (r *RowGroupMetaData) ColumnChunk(i int) (*ColumnChunkMetaData, error) {
	if i >= r.NumColumns() {
		panic(fmt.Errorf("parquet: the file only has %d columns, requested metadata for column: %d", r.NumColumns(), i))
	}

	return NewColumnChunkMetaData(r.rowGroup.Columns[i], r.Schema.Column(i), r.version, r.rowGroup.GetOrdinal(), int16(i), r.fileDecryptor)
}

// RowGroupMetaDataBuilder is a convenience object for constructing row group
// metadata information. Primarily used in conjunction with writing new files.
type RowGroupMetaDataBuilder struct {
	rg          *format.RowGroup
	props       *parquet.WriterProperties
	schema      *schema.Schema
	colBuilders []*ColumnChunkMetaDataBuilder
	nextCol     int
}

// NewRowGroupMetaDataBuilder returns a builder using the given properties and underlying thrift object.
//
// This is primarily used internally, consumers should use the file metadatabuilder and call
// AppendRowGroup on it to get instances of RowGroupMetaDataBuilder
func NewRowGroupMetaDataBuilder(props *parquet.WriterProperties, schema *schema.Schema, rg *format.RowGroup) *RowGroupMetaDataBuilder {
	r := &RowGroupMetaDataBuilder{
		rg:          rg,
		props:       props,
		schema:      schema,
		colBuilders: make([]*ColumnChunkMetaDataBuilder, 0),
	}
	r.rg.Columns = make([]*format.ColumnChunk, schema.NumColumns())
	return r
}

// NumColumns returns the current number of columns in this metadata
func (r *RowGroupMetaDataBuilder) NumColumns() int {
	return int(len(r.rg.GetColumns()))
}

func (r *RowGroupMetaDataBuilder) NumRows() int64 {
	return r.rg.GetNumRows()
}

func (r *RowGroupMetaDataBuilder) SetNumRows(nrows int) {
	r.rg.NumRows = int64(nrows)
}

// CurrentColumn returns the current column chunk (0-based) index that is being built.
//
// Returns -1 until the first time NextColumnChunk is called.
func (r *RowGroupMetaDataBuilder) CurrentColumn() int { return r.nextCol - 1 }

// NextColumnChunk appends a new column chunk, updates the column index,
// and returns a builder for that column chunk's metadata
func (r *RowGroupMetaDataBuilder) NextColumnChunk() *ColumnChunkMetaDataBuilder {
	if r.nextCol >= r.NumColumns() {
		panic(fmt.Errorf("parquet: the schema only has %d columns, requested metadata for col: %d", r.NumColumns(), r.nextCol))
	}

	col := r.schema.Column(r.nextCol)
	if r.rg.Columns[r.nextCol] == nil {
		r.rg.Columns[r.nextCol] = &format.ColumnChunk{MetaData: format.NewColumnMetaData()}
	}
	colBldr := NewColumnChunkMetaDataBuilderWithContents(r.props, col, r.rg.Columns[r.nextCol])
	r.nextCol++
	r.colBuilders = append(r.colBuilders, colBldr)
	return colBldr
}

// Finish should be called when complete and updates the metadata with the final
// file offset, and total compressed sizes. totalBytesWritten gets written as the
// TotalByteSize for the row group and Ordinal should be the index of the row group
// being written. e.g. first row group should be 0, second is 1, and so on...
func (r *RowGroupMetaDataBuilder) Finish(totalBytesWritten int64, ordinal int16) error {
	if r.nextCol != r.NumColumns() {
		return fmt.Errorf("parquet: only %d out of %d columns are initialized", r.nextCol-1, r.schema.NumColumns())
	}

	var (
		fileOffset      int64 = 0
		totalCompressed int64 = 0
	)

	for idx, col := range r.rg.Columns {
		if col.FileOffset < 0 {
			return fmt.Errorf("parquet: Column %d is not complete", idx)
		}
		if idx == 0 {
			if col.MetaData.IsSetDictionaryPageOffset() && col.MetaData.GetDictionaryPageOffset() > 0 {
				fileOffset = col.MetaData.GetDictionaryPageOffset()
			} else {
				fileOffset = col.MetaData.DataPageOffset
			}
		}
		// sometimes column metadata is encrypted and not available to read
		// so we must get total compressed size from column builder
		totalCompressed += r.colBuilders[idx].TotalCompressedSize()
	}

	r.rg.FileOffset = &fileOffset
	r.rg.TotalCompressedSize = &totalCompressed
	r.rg.TotalByteSize = totalBytesWritten
	r.rg.Ordinal = &ordinal
	return nil
}
