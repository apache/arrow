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

//! Contains implementations of the reader traits FileReader, RowGroupReader and PageReader
//! Also contains implementations of the ChunkReader for files (with buffering) and byte arrays (RAM)

use std::{convert::TryFrom, fs::File, io::Read, path::Path, sync::Arc};

use parquet_format::{PageHeader, PageType};
use thrift::protocol::TCompactInputProtocol;

use crate::basic::{Compression, Encoding, Type};
use crate::column::page::{Page, PageReader};
use crate::compression::{create_codec, Codec};
use crate::errors::{ParquetError, Result};
use crate::file::{footer, metadata::*, reader::*, statistics};
use crate::record::reader::RowIter;
use crate::record::Row;
use crate::schema::types::Type as SchemaType;
use crate::util::{io::TryClone, memory::ByteBufferPtr};

// export `SliceableCursor` and `FileSource` publically so clients can
// re-use the logic in their own ParquetFileWriter wrappers
pub use crate::util::{cursor::SliceableCursor, io::FileSource};

// ----------------------------------------------------------------------
// Implementations of traits facilitating the creation of a new reader

impl Length for File {
    fn len(&self) -> u64 {
        self.metadata().map(|m| m.len()).unwrap_or(0u64)
    }
}

impl TryClone for File {
    fn try_clone(&self) -> std::io::Result<Self> {
        self.try_clone()
    }
}

impl ChunkReader for File {
    type T = FileSource<File>;

    fn get_read(&self, start: u64, length: usize) -> Result<Self::T> {
        Ok(FileSource::new(self, start, length))
    }
}

impl Length for SliceableCursor {
    fn len(&self) -> u64 {
        SliceableCursor::len(self)
    }
}

impl ChunkReader for SliceableCursor {
    type T = SliceableCursor;

    fn get_read(&self, start: u64, length: usize) -> Result<Self::T> {
        self.slice(start, length).map_err(|e| e.into())
    }
}

impl TryFrom<File> for SerializedFileReader<File> {
    type Error = ParquetError;

    fn try_from(file: File) -> Result<Self> {
        Self::new(file)
    }
}

impl<'a> TryFrom<&'a Path> for SerializedFileReader<File> {
    type Error = ParquetError;

    fn try_from(path: &Path) -> Result<Self> {
        let file = File::open(path)?;
        Self::try_from(file)
    }
}

impl TryFrom<String> for SerializedFileReader<File> {
    type Error = ParquetError;

    fn try_from(path: String) -> Result<Self> {
        Self::try_from(Path::new(&path))
    }
}

impl<'a> TryFrom<&'a str> for SerializedFileReader<File> {
    type Error = ParquetError;

    fn try_from(path: &str) -> Result<Self> {
        Self::try_from(Path::new(&path))
    }
}

/// Conversion into a [`RowIter`](crate::record::reader::RowIter)
/// using the full file schema over all row groups.
impl IntoIterator for SerializedFileReader<File> {
    type Item = Row;
    type IntoIter = RowIter<'static>;

    fn into_iter(self) -> Self::IntoIter {
        RowIter::from_file_into(Box::new(self))
    }
}

// ----------------------------------------------------------------------
// Implementations of file & row group readers

/// A serialized implementation for Parquet [`FileReader`].
pub struct SerializedFileReader<R: ChunkReader> {
    chunk_reader: Arc<R>,
    metadata: ParquetMetaData,
}

impl<R: 'static + ChunkReader> SerializedFileReader<R> {
    /// Creates file reader from a Parquet file.
    /// Returns error if Parquet file does not exist or is corrupt.
    pub fn new(chunk_reader: R) -> Result<Self> {
        let metadata = footer::parse_metadata(&chunk_reader)?;
        Ok(Self {
            chunk_reader: Arc::new(chunk_reader),
            metadata,
        })
    }
}

impl<R: 'static + ChunkReader> FileReader for SerializedFileReader<R> {
    fn metadata(&self) -> &ParquetMetaData {
        &self.metadata
    }

    fn num_row_groups(&self) -> usize {
        self.metadata.num_row_groups()
    }

    fn get_row_group(&self, i: usize) -> Result<Box<RowGroupReader + '_>> {
        let row_group_metadata = self.metadata.row_group(i);
        // Row groups should be processed sequentially.
        let f = Arc::clone(&self.chunk_reader);
        Ok(Box::new(SerializedRowGroupReader::new(
            f,
            row_group_metadata,
        )))
    }

    fn get_row_iter(&self, projection: Option<SchemaType>) -> Result<RowIter> {
        RowIter::from_file(projection, self)
    }
}

/// A serialized implementation for Parquet [`RowGroupReader`].
pub struct SerializedRowGroupReader<'a, R: ChunkReader> {
    chunk_reader: Arc<R>,
    metadata: &'a RowGroupMetaData,
}

impl<'a, R: ChunkReader> SerializedRowGroupReader<'a, R> {
    /// Creates new row group reader from a file and row group metadata.
    fn new(chunk_reader: Arc<R>, metadata: &'a RowGroupMetaData) -> Self {
        Self {
            chunk_reader,
            metadata,
        }
    }
}

impl<'a, R: 'static + ChunkReader> RowGroupReader for SerializedRowGroupReader<'a, R> {
    fn metadata(&self) -> &RowGroupMetaData {
        &self.metadata
    }

    fn num_columns(&self) -> usize {
        self.metadata.num_columns()
    }

    // TODO: fix PARQUET-816
    fn get_column_page_reader(&self, i: usize) -> Result<Box<PageReader>> {
        let col = self.metadata.column(i);
        let (col_start, col_length) = col.byte_range();
        let file_chunk = self.chunk_reader.get_read(col_start, col_length as usize)?;
        let page_reader = SerializedPageReader::new(
            file_chunk,
            col.num_values(),
            col.compression(),
            col.column_descr().physical_type(),
        )?;
        Ok(Box::new(page_reader))
    }

    fn get_row_iter(&self, projection: Option<SchemaType>) -> Result<RowIter> {
        RowIter::from_row_group(projection, self)
    }
}

/// A serialized implementation for Parquet [`PageReader`].
pub struct SerializedPageReader<T: Read> {
    // The file source buffer which references exactly the bytes for the column trunk
    // to be read by this page reader.
    buf: T,

    // The compression codec for this column chunk. Only set for non-PLAIN codec.
    decompressor: Option<Box<Codec>>,

    // The number of values we have seen so far.
    seen_num_values: i64,

    // The number of total values in this column chunk.
    total_num_values: i64,

    // Column chunk type.
    physical_type: Type,
}

impl<T: Read> SerializedPageReader<T> {
    /// Creates a new serialized page reader from file source.
    pub fn new(
        buf: T,
        total_num_values: i64,
        compression: Compression,
        physical_type: Type,
    ) -> Result<Self> {
        let decompressor = create_codec(compression)?;
        let result = Self {
            buf,
            total_num_values,
            seen_num_values: 0,
            decompressor,
            physical_type,
        };
        Ok(result)
    }

    /// Reads Page header from Thrift.
    fn read_page_header(&mut self) -> Result<PageHeader> {
        let mut prot = TCompactInputProtocol::new(&mut self.buf);
        let page_header = PageHeader::read_from_in_protocol(&mut prot)?;
        Ok(page_header)
    }
}

impl<T: Read> PageReader for SerializedPageReader<T> {
    fn get_next_page(&mut self) -> Result<Option<Page>> {
        while self.seen_num_values < self.total_num_values {
            let page_header = self.read_page_header()?;

            // When processing data page v2, depending on enabled compression for the
            // page, we should account for uncompressed data ('offset') of
            // repetition and definition levels.
            //
            // We always use 0 offset for other pages other than v2, `true` flag means
            // that compression will be applied if decompressor is defined
            let mut offset: usize = 0;
            let mut can_decompress = true;

            if let Some(ref header_v2) = page_header.data_page_header_v2 {
                offset = (header_v2.definition_levels_byte_length
                    + header_v2.repetition_levels_byte_length)
                    as usize;
                // When is_compressed flag is missing the page is considered compressed
                can_decompress = header_v2.is_compressed.unwrap_or(true);
            }

            let compressed_len = page_header.compressed_page_size as usize - offset;
            let uncompressed_len = page_header.uncompressed_page_size as usize - offset;
            // We still need to read all bytes from buffered stream
            let mut buffer = vec![0; offset + compressed_len];
            self.buf.read_exact(&mut buffer)?;

            // TODO: page header could be huge because of statistics. We should set a
            // maximum page header size and abort if that is exceeded.
            if let Some(decompressor) = self.decompressor.as_mut() {
                if can_decompress {
                    let mut decompressed_buffer = Vec::with_capacity(uncompressed_len);
                    let decompressed_size = decompressor
                        .decompress(&buffer[offset..], &mut decompressed_buffer)?;
                    if decompressed_size != uncompressed_len {
                        return Err(general_err!(
              "Actual decompressed size doesn't match the expected one ({} vs {})",
              decompressed_size,
              uncompressed_len
            ));
                    }
                    if offset == 0 {
                        buffer = decompressed_buffer;
                    } else {
                        // Prepend saved offsets to the buffer
                        buffer.truncate(offset);
                        buffer.append(&mut decompressed_buffer);
                    }
                }
            }

            let result = match page_header.type_ {
                PageType::DictionaryPage => {
                    assert!(page_header.dictionary_page_header.is_some());
                    let dict_header =
                        page_header.dictionary_page_header.as_ref().unwrap();
                    let is_sorted = dict_header.is_sorted.unwrap_or(false);
                    Page::DictionaryPage {
                        buf: ByteBufferPtr::new(buffer),
                        num_values: dict_header.num_values as u32,
                        encoding: Encoding::from(dict_header.encoding),
                        is_sorted,
                    }
                }
                PageType::DataPage => {
                    assert!(page_header.data_page_header.is_some());
                    let header = page_header.data_page_header.unwrap();
                    self.seen_num_values += header.num_values as i64;
                    Page::DataPage {
                        buf: ByteBufferPtr::new(buffer),
                        num_values: header.num_values as u32,
                        encoding: Encoding::from(header.encoding),
                        def_level_encoding: Encoding::from(
                            header.definition_level_encoding,
                        ),
                        rep_level_encoding: Encoding::from(
                            header.repetition_level_encoding,
                        ),
                        statistics: statistics::from_thrift(
                            self.physical_type,
                            header.statistics,
                        ),
                    }
                }
                PageType::DataPageV2 => {
                    assert!(page_header.data_page_header_v2.is_some());
                    let header = page_header.data_page_header_v2.unwrap();
                    let is_compressed = header.is_compressed.unwrap_or(true);
                    self.seen_num_values += header.num_values as i64;
                    Page::DataPageV2 {
                        buf: ByteBufferPtr::new(buffer),
                        num_values: header.num_values as u32,
                        encoding: Encoding::from(header.encoding),
                        num_nulls: header.num_nulls as u32,
                        num_rows: header.num_rows as u32,
                        def_levels_byte_len: header.definition_levels_byte_length as u32,
                        rep_levels_byte_len: header.repetition_levels_byte_length as u32,
                        is_compressed,
                        statistics: statistics::from_thrift(
                            self.physical_type,
                            header.statistics,
                        ),
                    }
                }
                _ => {
                    // For unknown page type (e.g., INDEX_PAGE), skip and read next.
                    continue;
                }
            };
            return Ok(Some(result));
        }

        // We are at the end of this column chunk and no more page left. Return None.
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::basic::ColumnOrder;
    use crate::record::RowAccessor;
    use crate::schema::parser::parse_message_type;
    use crate::util::test_common::{get_test_file, get_test_path};
    use std::sync::Arc;

    #[test]
    fn test_cursor_and_file_has_the_same_behaviour() {
        let mut buf: Vec<u8> = Vec::new();
        get_test_file("alltypes_plain.parquet")
            .read_to_end(&mut buf)
            .unwrap();
        let cursor = SliceableCursor::new(buf);
        let read_from_cursor = SerializedFileReader::new(cursor).unwrap();

        let test_file = get_test_file("alltypes_plain.parquet");
        let read_from_file = SerializedFileReader::new(test_file).unwrap();

        let file_iter = read_from_file.get_row_iter(None).unwrap();
        let cursor_iter = read_from_cursor.get_row_iter(None).unwrap();

        assert!(file_iter.eq(cursor_iter));
    }

    #[test]
    fn test_file_reader_try_from() {
        // Valid file path
        let test_file = get_test_file("alltypes_plain.parquet");
        let test_path_buf = get_test_path("alltypes_plain.parquet");
        let test_path = test_path_buf.as_path();
        let test_path_str = test_path.to_str().unwrap();

        let reader = SerializedFileReader::try_from(test_file);
        assert!(reader.is_ok());

        let reader = SerializedFileReader::try_from(test_path);
        assert!(reader.is_ok());

        let reader = SerializedFileReader::try_from(test_path_str);
        assert!(reader.is_ok());

        let reader = SerializedFileReader::try_from(test_path_str.to_string());
        assert!(reader.is_ok());

        // Invalid file path
        let test_path = Path::new("invalid.parquet");
        let test_path_str = test_path.to_str().unwrap();

        let reader = SerializedFileReader::try_from(test_path);
        assert!(reader.is_err());

        let reader = SerializedFileReader::try_from(test_path_str);
        assert!(reader.is_err());

        let reader = SerializedFileReader::try_from(test_path_str.to_string());
        assert!(reader.is_err());
    }

    #[test]
    fn test_file_reader_into_iter() -> Result<()> {
        let path = get_test_path("alltypes_plain.parquet");
        let vec = vec![path.clone(), path]
            .iter()
            .map(|p| SerializedFileReader::try_from(p.as_path()).unwrap())
            .flat_map(|r| r.into_iter())
            .flat_map(|r| r.get_int(0))
            .collect::<Vec<_>>();

        // rows in the parquet file are not sorted by "id"
        // each file contains [id:4, id:5, id:6, id:7, id:2, id:3, id:0, id:1]
        assert_eq!(vec, vec![4, 5, 6, 7, 2, 3, 0, 1, 4, 5, 6, 7, 2, 3, 0, 1]);

        Ok(())
    }

    #[test]
    fn test_file_reader_into_iter_project() -> Result<()> {
        let path = get_test_path("alltypes_plain.parquet");
        let result = vec![path]
            .iter()
            .map(|p| SerializedFileReader::try_from(p.as_path()).unwrap())
            .flat_map(|r| {
                let schema = "message schema { OPTIONAL INT32 id; }";
                let proj = parse_message_type(&schema).ok();

                r.into_iter().project(proj).unwrap()
            })
            .map(|r| format!("{}", r))
            .collect::<Vec<_>>()
            .join(",");

        assert_eq!(
            result,
            "{id: 4},{id: 5},{id: 6},{id: 7},{id: 2},{id: 3},{id: 0},{id: 1}"
        );

        Ok(())
    }

    #[test]
    fn test_reuse_file_chunk() {
        // This test covers the case of maintaining the correct start position in a file
        // stream for each column reader after initializing and moving to the next one
        // (without necessarily reading the entire column).
        let test_file = get_test_file("alltypes_plain.parquet");
        let reader = SerializedFileReader::new(test_file).unwrap();
        let row_group = reader.get_row_group(0).unwrap();

        let mut page_readers = Vec::new();
        for i in 0..row_group.num_columns() {
            page_readers.push(row_group.get_column_page_reader(i).unwrap());
        }

        // Now buffer each col reader, we do not expect any failures like:
        // General("underlying Thrift error: end of file")
        for mut page_reader in page_readers {
            assert!(page_reader.get_next_page().is_ok());
        }
    }

    #[test]
    fn test_file_reader() {
        let test_file = get_test_file("alltypes_plain.parquet");
        let reader_result = SerializedFileReader::new(test_file);
        assert!(reader_result.is_ok());
        let reader = reader_result.unwrap();

        // Test contents in Parquet metadata
        let metadata = reader.metadata();
        assert_eq!(metadata.num_row_groups(), 1);

        // Test contents in file metadata
        let file_metadata = metadata.file_metadata();
        assert!(file_metadata.created_by().is_some());
        assert_eq!(
      file_metadata.created_by().as_ref().unwrap(),
      "impala version 1.3.0-INTERNAL (build 8a48ddb1eff84592b3fc06bc6f51ec120e1fffc9)"
    );
        assert!(file_metadata.key_value_metadata().is_none());
        assert_eq!(file_metadata.num_rows(), 8);
        assert_eq!(file_metadata.version(), 1);
        assert_eq!(file_metadata.column_orders(), None);

        // Test contents in row group metadata
        let row_group_metadata = metadata.row_group(0);
        assert_eq!(row_group_metadata.num_columns(), 11);
        assert_eq!(row_group_metadata.num_rows(), 8);
        assert_eq!(row_group_metadata.total_byte_size(), 671);
        // Check each column order
        for i in 0..row_group_metadata.num_columns() {
            assert_eq!(file_metadata.column_order(i), ColumnOrder::UNDEFINED);
        }

        // Test row group reader
        let row_group_reader_result = reader.get_row_group(0);
        assert!(row_group_reader_result.is_ok());
        let row_group_reader: Box<RowGroupReader> = row_group_reader_result.unwrap();
        assert_eq!(
            row_group_reader.num_columns(),
            row_group_metadata.num_columns()
        );
        assert_eq!(
            row_group_reader.metadata().total_byte_size(),
            row_group_metadata.total_byte_size()
        );

        // Test page readers
        // TODO: test for every column
        let page_reader_0_result = row_group_reader.get_column_page_reader(0);
        assert!(page_reader_0_result.is_ok());
        let mut page_reader_0: Box<PageReader> = page_reader_0_result.unwrap();
        let mut page_count = 0;
        while let Ok(Some(page)) = page_reader_0.get_next_page() {
            let is_expected_page = match page {
                Page::DictionaryPage {
                    buf,
                    num_values,
                    encoding,
                    is_sorted,
                } => {
                    assert_eq!(buf.len(), 32);
                    assert_eq!(num_values, 8);
                    assert_eq!(encoding, Encoding::PLAIN_DICTIONARY);
                    assert_eq!(is_sorted, false);
                    true
                }
                Page::DataPage {
                    buf,
                    num_values,
                    encoding,
                    def_level_encoding,
                    rep_level_encoding,
                    statistics,
                } => {
                    assert_eq!(buf.len(), 11);
                    assert_eq!(num_values, 8);
                    assert_eq!(encoding, Encoding::PLAIN_DICTIONARY);
                    assert_eq!(def_level_encoding, Encoding::RLE);
                    assert_eq!(rep_level_encoding, Encoding::BIT_PACKED);
                    assert!(statistics.is_none());
                    true
                }
                _ => false,
            };
            assert!(is_expected_page);
            page_count += 1;
        }
        assert_eq!(page_count, 2);
    }

    #[test]
    fn test_file_reader_datapage_v2() {
        let test_file = get_test_file("datapage_v2.snappy.parquet");
        let reader_result = SerializedFileReader::new(test_file);
        assert!(reader_result.is_ok());
        let reader = reader_result.unwrap();

        // Test contents in Parquet metadata
        let metadata = reader.metadata();
        assert_eq!(metadata.num_row_groups(), 1);

        // Test contents in file metadata
        let file_metadata = metadata.file_metadata();
        assert!(file_metadata.created_by().is_some());
        assert_eq!(
            file_metadata.created_by().as_ref().unwrap(),
            "parquet-mr version 1.8.1 (build 4aba4dae7bb0d4edbcf7923ae1339f28fd3f7fcf)"
        );
        assert!(file_metadata.key_value_metadata().is_some());
        assert_eq!(
            file_metadata.key_value_metadata().to_owned().unwrap().len(),
            1
        );

        assert_eq!(file_metadata.num_rows(), 5);
        assert_eq!(file_metadata.version(), 1);
        assert_eq!(file_metadata.column_orders(), None);

        let row_group_metadata = metadata.row_group(0);

        // Check each column order
        for i in 0..row_group_metadata.num_columns() {
            assert_eq!(file_metadata.column_order(i), ColumnOrder::UNDEFINED);
        }

        // Test row group reader
        let row_group_reader_result = reader.get_row_group(0);
        assert!(row_group_reader_result.is_ok());
        let row_group_reader: Box<RowGroupReader> = row_group_reader_result.unwrap();
        assert_eq!(
            row_group_reader.num_columns(),
            row_group_metadata.num_columns()
        );
        assert_eq!(
            row_group_reader.metadata().total_byte_size(),
            row_group_metadata.total_byte_size()
        );

        // Test page readers
        // TODO: test for every column
        let page_reader_0_result = row_group_reader.get_column_page_reader(0);
        assert!(page_reader_0_result.is_ok());
        let mut page_reader_0: Box<PageReader> = page_reader_0_result.unwrap();
        let mut page_count = 0;
        while let Ok(Some(page)) = page_reader_0.get_next_page() {
            let is_expected_page = match page {
                Page::DictionaryPage {
                    buf,
                    num_values,
                    encoding,
                    is_sorted,
                } => {
                    assert_eq!(buf.len(), 7);
                    assert_eq!(num_values, 1);
                    assert_eq!(encoding, Encoding::PLAIN);
                    assert_eq!(is_sorted, false);
                    true
                }
                Page::DataPageV2 {
                    buf,
                    num_values,
                    encoding,
                    num_nulls,
                    num_rows,
                    def_levels_byte_len,
                    rep_levels_byte_len,
                    is_compressed,
                    statistics,
                } => {
                    assert_eq!(buf.len(), 4);
                    assert_eq!(num_values, 5);
                    assert_eq!(encoding, Encoding::RLE_DICTIONARY);
                    assert_eq!(num_nulls, 1);
                    assert_eq!(num_rows, 5);
                    assert_eq!(def_levels_byte_len, 2);
                    assert_eq!(rep_levels_byte_len, 0);
                    assert_eq!(is_compressed, true);
                    assert!(statistics.is_some());
                    true
                }
                _ => false,
            };
            assert!(is_expected_page);
            page_count += 1;
        }
        assert_eq!(page_count, 2);
    }

    #[test]
    fn test_page_iterator() {
        let file = get_test_file("alltypes_plain.parquet");
        let file_reader = Arc::new(SerializedFileReader::new(file).unwrap());

        let mut page_iterator = FilePageIterator::new(0, file_reader.clone()).unwrap();

        // read first page
        let page = page_iterator.next();
        assert!(page.is_some());
        assert!(page.unwrap().is_ok());

        // reach end of file
        let page = page_iterator.next();
        assert!(page.is_none());

        let row_group_indices = Box::new(0..1);
        let mut page_iterator =
            FilePageIterator::with_row_groups(0, row_group_indices, file_reader).unwrap();

        // read first page
        let page = page_iterator.next();
        assert!(page.is_some());
        assert!(page.unwrap().is_ok());

        // reach end of file
        let page = page_iterator.next();
        assert!(page.is_none());
    }

    #[test]
    fn test_file_reader_key_value_metadata() {
        let file = get_test_file("binary.parquet");
        let file_reader = Arc::new(SerializedFileReader::new(file).unwrap());

        let metadata = file_reader
            .metadata
            .file_metadata()
            .key_value_metadata()
            .as_ref()
            .unwrap();

        assert_eq!(metadata.len(), 3);

        assert_eq!(metadata.get(0).unwrap().key, "parquet.proto.descriptor");

        assert_eq!(metadata.get(1).unwrap().key, "writer.model.name");
        assert_eq!(metadata.get(1).unwrap().value, Some("protobuf".to_owned()));

        assert_eq!(metadata.get(2).unwrap().key, "parquet.proto.class");
        assert_eq!(
            metadata.get(2).unwrap().value,
            Some("foo.baz.Foobaz$Event".to_owned())
        );
    }
}
