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

package file

import (
	"github.com/apache/arrow/go/v13/parquet"
	"github.com/apache/arrow/go/v13/parquet/internal/encryption"
	"github.com/apache/arrow/go/v13/parquet/internal/utils"
	"github.com/apache/arrow/go/v13/parquet/metadata"
	"golang.org/x/xerrors"
)

// RowGroupWriter is the base interface for writing rowgroups, the actual writer
// will be either the SerialRowGroupWriter or the BufferedRowGroupWriter
type RowGroupWriter interface {
	// Returns the number of columns for this row group writer
	NumColumns() int
	// returns the current number of rows that have been written.
	// Returns an error if they are unequal between columns that have been written so far
	NumRows() (int, error)
	// The total compressed bytes so
	TotalCompressedBytes() int64
	// the total bytes written and flushed out
	TotalBytesWritten() int64
	// Closes any unclosed columnwriters, and closes the rowgroup, writing out
	// the metadata. subsequent calls have no effect
	// returns an error if columns contain unequal numbers of rows.
	Close() error
	// Buffered returns true if it's a BufferedRowGroupWriter and false for a
	// SerialRowGroupWriter
	Buffered() bool
}

// SerialRowGroupWriter expects each column to be written one after the other,
// data is flushed every time NextColumn is called and will panic if there is
// an unequal number of rows written per column.
type SerialRowGroupWriter interface {
	RowGroupWriter
	NextColumn() (ColumnChunkWriter, error)
	// returns the current column being built, if buffered it will equal NumColumns
	// if serialized then it will return which column is currenly being written
	CurrentColumn() int
}

// BufferedRowGroupWriter allows writing to multiple columns simultaneously, data
// will not be flushed to the underlying writer until closing the RowGroupWriter.
//
// All columns must have equal numbers of rows before closing the row group or it will panic.
type BufferedRowGroupWriter interface {
	RowGroupWriter
	Column(i int) (ColumnChunkWriter, error)
}

type rowGroupWriter struct {
	sink          utils.WriterTell
	metadata      *metadata.RowGroupMetaDataBuilder
	props         *parquet.WriterProperties
	bytesWritten  int64
	closed        bool
	ordinal       int16
	nextColumnIdx int
	nrows         int
	buffered      bool
	fileEncryptor encryption.FileEncryptor

	columnWriters []ColumnChunkWriter
	pager         PageWriter
}

func newRowGroupWriter(sink utils.WriterTell, metadata *metadata.RowGroupMetaDataBuilder, ordinal int16, props *parquet.WriterProperties, buffered bool, fileEncryptor encryption.FileEncryptor) *rowGroupWriter {
	ret := &rowGroupWriter{
		sink:          sink,
		metadata:      metadata,
		props:         props,
		ordinal:       ordinal,
		buffered:      buffered,
		fileEncryptor: fileEncryptor,
	}
	if buffered {
		ret.initColumns()
	} else {
		ret.columnWriters = []ColumnChunkWriter{nil}
	}
	return ret
}

func (rg *rowGroupWriter) Buffered() bool { return rg.buffered }

func (rg *rowGroupWriter) checkRowsWritten() error {
	if len(rg.columnWriters) == 0 {
		return nil
	}

	if !rg.buffered && rg.columnWriters[0] != nil {
		current := rg.columnWriters[0].RowsWritten()
		if rg.nrows == 0 {
			rg.nrows = current
		} else if rg.nrows != current {
			return xerrors.New("row mismatch")
		}
	} else if rg.buffered {
		current := rg.columnWriters[0].RowsWritten()
		for _, wr := range rg.columnWriters[1:] {
			if current != wr.RowsWritten() {
				return xerrors.New("row mismatch error")
			}
		}
		rg.nrows = current
	}
	return nil
}

func (rg *rowGroupWriter) NumColumns() int { return rg.metadata.NumColumns() }
func (rg *rowGroupWriter) NumRows() (int, error) {
	err := rg.checkRowsWritten()
	return rg.nrows, err
}

func (rg *rowGroupWriter) NextColumn() (ColumnChunkWriter, error) {
	if rg.buffered {
		panic("next column is not supported when a rowgroup is written by size")
	}
	if rg.columnWriters[0] != nil {
		if err := rg.checkRowsWritten(); err != nil {
			return nil, err
		}
	}

	// throw an error if more columns are being written
	colMeta := rg.metadata.NextColumnChunk()
	if rg.columnWriters[0] != nil {
		if err := rg.columnWriters[0].Close(); err != nil {
			return nil, err
		}
		rg.bytesWritten += rg.columnWriters[0].TotalBytesWritten()
	}
	rg.nextColumnIdx++

	path := colMeta.Descr().Path()
	var (
		metaEncryptor encryption.Encryptor
		dataEncryptor encryption.Encryptor
	)
	if rg.fileEncryptor != nil {
		metaEncryptor = rg.fileEncryptor.GetColumnMetaEncryptor(path)
		dataEncryptor = rg.fileEncryptor.GetColumnDataEncryptor(path)
	}

	if rg.pager == nil {
		var err error
		rg.pager, err = NewPageWriter(rg.sink, rg.props.CompressionFor(path), rg.props.CompressionLevelFor(path), colMeta, rg.ordinal, int16(rg.nextColumnIdx-1), rg.props.Allocator(), false, metaEncryptor, dataEncryptor)
		if err != nil {
			return nil, err
		}
	} else {
		rg.pager.Reset(rg.sink, rg.props.CompressionFor(path), rg.props.CompressionLevelFor(path), colMeta, rg.ordinal, int16(rg.nextColumnIdx-1), metaEncryptor, dataEncryptor)
	}

	rg.columnWriters[0] = NewColumnChunkWriter(colMeta, rg.pager, rg.props)
	return rg.columnWriters[0], nil
}

func (rg *rowGroupWriter) Column(i int) (ColumnChunkWriter, error) {
	if !rg.buffered {
		panic("column is only supported when a bufferedrowgroup is being written")
	}

	if i >= 0 && i < len(rg.columnWriters) {
		return rg.columnWriters[i], nil
	}
	return nil, xerrors.New("invalid column number requested")
}

func (rg *rowGroupWriter) CurrentColumn() int { return rg.metadata.CurrentColumn() }
func (rg *rowGroupWriter) TotalCompressedBytes() int64 {
	total := int64(0)
	for _, wr := range rg.columnWriters {
		if wr != nil {
			total += wr.TotalCompressedBytes()
		}
	}
	return total
}

func (rg *rowGroupWriter) TotalBytesWritten() int64 {
	total := int64(0)
	for _, wr := range rg.columnWriters {
		if wr != nil {
			total += wr.TotalBytesWritten()
		}
	}
	return total + rg.bytesWritten
}

func (rg *rowGroupWriter) Close() error {
	if !rg.closed {
		rg.closed = true
		if err := rg.checkRowsWritten(); err != nil {
			return err
		}

		for _, wr := range rg.columnWriters {
			if wr != nil {
				if err := wr.Close(); err != nil {
					return err
				}
				rg.bytesWritten += wr.TotalBytesWritten()
			}
		}

		rg.columnWriters = nil
		rg.metadata.SetNumRows(rg.nrows)
		rg.metadata.Finish(rg.bytesWritten, rg.ordinal)
	}
	return nil
}

func (rg *rowGroupWriter) initColumns() error {
	if rg.columnWriters == nil {
		rg.columnWriters = make([]ColumnChunkWriter, 0, rg.NumColumns())
	}
	for i := 0; i < rg.NumColumns(); i++ {
		colMeta := rg.metadata.NextColumnChunk()
		path := colMeta.Descr().Path()
		var (
			metaEncryptor encryption.Encryptor
			dataEncryptor encryption.Encryptor
		)
		if rg.fileEncryptor != nil {
			metaEncryptor = rg.fileEncryptor.GetColumnMetaEncryptor(path)
			dataEncryptor = rg.fileEncryptor.GetColumnDataEncryptor(path)
		}
		pager, err := NewPageWriter(rg.sink, rg.props.CompressionFor(path), rg.props.CompressionLevelFor(path), colMeta, rg.ordinal, int16(rg.nextColumnIdx), rg.props.Allocator(), rg.buffered, metaEncryptor, dataEncryptor)
		if err != nil {
			return err
		}
		rg.nextColumnIdx++
		rg.columnWriters = append(rg.columnWriters, NewColumnChunkWriter(colMeta, pager, rg.props))
	}
	return nil
}
