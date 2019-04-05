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

//! Provides interface for page iterator and implementation.

use std::rc::Rc;

use crate::column::page::PageReader;
use crate::file::reader::FileReader;
use crate::schema::types::ColumnDescPtr;
use crate::schema::types::SchemaDescPtr;

use crate::errors::check_in_range;
use crate::errors::Result;

/// PageIterator is used to decouple parquet row group scanning from record readers.
pub trait PageIterator: Iterator<Item = Result<Box<PageReader>>> {
    /// Get schema of parquet file.
    fn schema(&mut self) -> Result<SchemaDescPtr>;

    /// Get column schema of this page iterator.
    fn column_schema(&mut self) -> Result<ColumnDescPtr>;
}

/// Implementation for parquet file.
pub struct FilePageIterator {
    column_index: usize,
    row_group_indices: Box<Iterator<Item = usize>>,
    file_reader: Rc<FileReader>,
}

impl FilePageIterator {
    /// Create page iterator from parquet file reader.
    pub fn new(
        column_index: usize,
        row_group_indices: Box<Iterator<Item = usize>>,
        file_reader: Rc<FileReader>,
    ) -> Result<Self> {
        // Check that column_index are valid
        let num_columns = file_reader
            .metadata()
            .file_metadata()
            .schema_descr_ptr()
            .num_columns();

        check_in_range(column_index, num_columns)?;

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

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use super::FilePageIterator;

    use crate::file::reader::SerializedFileReader;
    use crate::util::test_common::get_test_file;

    #[test]
    fn test_page_iterator() {
        let file = get_test_file("alltypes_plain.parquet");
        let file_reader = SerializedFileReader::new(file).unwrap();

        let row_group_indices = Box::new(0..1);
        let mut page_iterator =
            FilePageIterator::new(0, row_group_indices, Rc::new(file_reader)).unwrap();

        // read first page
        let page = page_iterator.next();
        assert!(page.is_some());
        assert!(page.unwrap().is_ok());

        // reach end of file
        let page = page_iterator.next();
        assert!(page.is_none());
    }
}
