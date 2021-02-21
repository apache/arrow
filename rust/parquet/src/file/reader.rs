// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Contains file reader API and provides methods to access file metadata, row group
//! readers to read individual column chunks, or access record iterator.

use std::{boxed::Box, io::Read, sync::Arc};

use crate::column::page::PageIterator;
use crate::column::{page::PageReader, reader::ColumnReader};
use crate::errors::{ParquetError, Result};
use crate::file::metadata::*;
pub use crate::file::serialized_reader::{SerializedFileReader, SerializedPageReader};
use crate::record::reader::RowIter;
use crate::schema::types::{ColumnDescPtr, SchemaDescPtr, Type as SchemaType};

use crate::basic::Type;

use crate::column::reader::ColumnReaderImpl;

/// Length should return the total number of bytes in the input source.
/// It's mainly used to read the metadata, which is at the end of the source.
#[allow(clippy::len_without_is_empty)]
pub trait Length {
    /// Returns the amount of bytes of the inner source.
    fn len(&self) -> u64;
}

/// The ChunkReader trait generates readers of chunks of a source.
/// For a file system reader, each chunk might contain a clone of File bounded on a given range.
/// For an object store reader, each read can be mapped to a range request.
pub trait ChunkReader: Length {
    type T: Read;
    /// get a serialy readeable slice of the current reader
    /// This should fail if the slice exceeds the current bounds
    fn get_read(&self, start: u64, length: usize) -> Result<Self::T>;
}

// ----------------------------------------------------------------------
// APIs for file & row group readers

/// Parquet file reader API. With this, user can get metadata information about the
/// Parquet file, can get reader for each row group, and access record iterator.
pub trait FileReader {
    /// Get metadata information about this file.
    fn metadata(&self) -> &ParquetMetaData;

    /// Get the total number of row groups for this file.
    fn num_row_groups(&self) -> usize;

    /// Get the `i`th row group reader. Note this doesn't do bound check.
    fn get_row_group(&self, i: usize) -> Result<Box<RowGroupReader + '_>>;

    /// Get full iterator of `Row`s from a file (over all row groups).
    ///
    /// Iterator will automatically load the next row group to advance.
    ///
    /// Projected schema can be a subset of or equal to the file schema, when it is None,
    /// full file schema is assumed.
    fn get_row_iter(&self, projection: Option<SchemaType>) -> Result<RowIter>;
}

/// Parquet row group reader API. With this, user can get metadata information about the
/// row group, as well as readers for each individual column chunk.
pub trait RowGroupReader {
    /// Get metadata information about this row group.
    fn metadata(&self) -> &RowGroupMetaData;

    /// Get the total number of column chunks in this row group.
    fn num_columns(&self) -> usize;

    /// Get page reader for the `i`th column chunk.
    fn get_column_page_reader(&self, i: usize) -> Result<Box<PageReader>>;

    /// Get value reader for the `i`th column chunk.
    fn get_column_reader(&self, i: usize) -> Result<ColumnReader> {
        let schema_descr = self.metadata().schema_descr();
        let col_descr = schema_descr.column(i);
        let col_page_reader = self.get_column_page_reader(i)?;
        let col_reader = match col_descr.physical_type() {
            Type::BOOLEAN => ColumnReader::BoolColumnReader(ColumnReaderImpl::new(
                col_descr,
                col_page_reader,
            )),
            Type::INT32 => ColumnReader::Int32ColumnReader(ColumnReaderImpl::new(
                col_descr,
                col_page_reader,
            )),
            Type::INT64 => ColumnReader::Int64ColumnReader(ColumnReaderImpl::new(
                col_descr,
                col_page_reader,
            )),
            Type::INT96 => ColumnReader::Int96ColumnReader(ColumnReaderImpl::new(
                col_descr,
                col_page_reader,
            )),
            Type::FLOAT => ColumnReader::FloatColumnReader(ColumnReaderImpl::new(
                col_descr,
                col_page_reader,
            )),
            Type::DOUBLE => ColumnReader::DoubleColumnReader(ColumnReaderImpl::new(
                col_descr,
                col_page_reader,
            )),
            Type::BYTE_ARRAY => ColumnReader::ByteArrayColumnReader(
                ColumnReaderImpl::new(col_descr, col_page_reader),
            ),
            Type::FIXED_LEN_BYTE_ARRAY => ColumnReader::FixedLenByteArrayColumnReader(
                ColumnReaderImpl::new(col_descr, col_page_reader),
            ),
        };
        Ok(col_reader)
    }

    /// Get iterator of `Row`s from this row group.
    ///
    /// Projected schema can be a subset of or equal to the file schema, when it is None,
    /// full file schema is assumed.
    fn get_row_iter(&self, projection: Option<SchemaType>) -> Result<RowIter>;
}

// ----------------------------------------------------------------------
// Iterator

/// Implementation of page iterator for parquet file.
pub struct FilePageIterator {
    column_index: usize,
    row_group_indices: Box<Iterator<Item = usize>>,
    file_reader: Arc<FileReader>,
}

impl FilePageIterator {
    /// Creates a page iterator for all row groups in file.
    pub fn new(column_index: usize, file_reader: Arc<FileReader>) -> Result<Self> {
        let num_row_groups = file_reader.metadata().num_row_groups();

        let row_group_indices = Box::new(0..num_row_groups);

        Self::with_row_groups(column_index, row_group_indices, file_reader)
    }

    /// Create page iterator from parquet file reader with only some row groups.
    pub fn with_row_groups(
        column_index: usize,
        row_group_indices: Box<Iterator<Item = usize>>,
        file_reader: Arc<FileReader>,
    ) -> Result<Self> {
        // Check that column_index is valid
        let num_columns = file_reader
            .metadata()
            .file_metadata()
            .schema_descr()
            .num_columns();

        if column_index >= num_columns {
            return Err(ParquetError::IndexOutOfBound(column_index, num_columns));
        }

        // We don't check iterators here because iterator may be infinite
        Ok(Self {
            column_index,
            row_group_indices,
            file_reader,
        })
    }
}

impl Iterator for FilePageIterator {
    type Item = Result<Box<PageReader>>;

    fn next(&mut self) -> Option<Result<Box<PageReader>>> {
        self.row_group_indices.next().map(|row_group_index| {
            self.file_reader
                .get_row_group(row_group_index)
                .and_then(|r| r.get_column_page_reader(self.column_index))
        })
    }
}

impl PageIterator for FilePageIterator {
    fn schema(&mut self) -> Result<SchemaDescPtr> {
        Ok(self
            .file_reader
            .metadata()
            .file_metadata()
            .schema_descr_ptr())
    }

    fn column_schema(&mut self) -> Result<ColumnDescPtr> {
        self.schema().map(|s| s.column(self.column_index))
    }
}
