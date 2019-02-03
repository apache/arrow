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

//! Feather Reader and Writer
//!
//! The Feather reader and writer allow Feather file IO.
//! Feather support in Rust currently has the following limitations:
//!
//! * No date/time support. Date/time arrays are not yet supported in Rust, thus to
//!   prevent discarding type data, we do not read those arrays.
//! * Writing categorical data not supported. Only reading supported.
//! * A Feather file is read into a single `RecordBatch`.This is regardless of the length
//!   of arrays, as we do not yet support zero-copy of arrays.
//! * Record batches are written directly to Feather files. We do not yet have a `Table`
//!   in Rust.
//! * Null records not yet supported.
//!
//! Example:
//!
//! ```
//! use arrow::ipc::feather::*;
//! use std::fs::File;
//!
//! // read a feather file
//! let mut reader =
//!     FeatherReader::new(File::open("test/data/uk_cities.feather").unwrap());
//! let batch = reader.read().unwrap();
//!
//! // write a record batch to a feather file
//! batch
//!     .write_feather("test/data/uk_cities_out.feather")
//!     .unwrap();
//! ```

use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::sync::Arc;

use byteorder::{LittleEndian, WriteBytesExt};

use crate::array::*;
use crate::array_data::ArrayDataBuilder;
use crate::buffer::Buffer;
use crate::builder::BinaryBuilder;
use crate::datatypes::*;
use crate::error::{ArrowError, Result};
use crate::ipc::gen::feather_generated::arrow::ipc::feather::fbs;
use crate::record_batch::RecordBatch;

static FEATHER_MAGIC: [u8; 4] = [b'F', b'E', b'A', b'1'];

fn as_u32_le(array: &[u8; 4]) -> u32 {
    ((array[0] as u32) << 0)
        + ((array[1] as u32) << 8)
        + ((array[2] as u32) << 16)
        + ((array[3] as u32) << 24)
}

fn create_bytes() {}

fn get_data_type(dtype: fbs::Type) -> DataType {
    match dtype {
        fbs::Type::BOOL => DataType::Boolean,
        fbs::Type::INT8 => DataType::Int8,
        fbs::Type::INT16 => DataType::Int16,
        fbs::Type::INT32 => DataType::Int32,
        fbs::Type::INT64 => DataType::Int64,
        fbs::Type::UINT8 => DataType::UInt8,
        fbs::Type::UINT16 => DataType::UInt16,
        fbs::Type::UINT32 => DataType::UInt32,
        fbs::Type::UINT64 => DataType::UInt64,
        fbs::Type::FLOAT => DataType::Float32,
        fbs::Type::DOUBLE => DataType::Float64,
        fbs::Type::UTF8 => DataType::Utf8,
        fbs::Type::BINARY => DataType::Utf8,
        fbs::Type::CATEGORY => {
            unimplemented!("Reading CATEGORY type columns not implemented")
        }
        fbs::Type::TIMESTAMP | fbs::Type::DATE | fbs::Type::TIME => {
            unimplemented!("Reading date and time fields not implemented")
        }
    }
}

fn get_fbs_type(dtype: DataType) -> fbs::Type {
    use crate::datatypes::DataType::*;
    use fbs::Type::*;

    match dtype {
        Boolean => BOOL,
        Int8 => INT8,
        Int16 => INT16,
        Int32 => INT32,
        Int64 => INT64,
        UInt8 => UINT8,
        UInt16 => UINT16,
        UInt32 => UINT32,
        UInt64 => UINT64,
        Float16 => unimplemented!("Float16 type not supported"),
        Float32 => FLOAT,
        Float64 => DOUBLE,
        Timestamp(_) => TIMESTAMP,
        Date(_) => DATE,
        Time32(_) | Time64(_) => TIME,
        Interval(_) => unimplemented!("Interval type not supported"),
        Utf8 => UTF8,
        List(_) | Struct(_) => {
            unimplemented!("Lists and Structs types are not supported")
        }
    }
}

fn make_primitive_array(
    array: fbs::PrimitiveArray,
    len: usize,
    buffer: Buffer,
    data_type: DataType,
) -> ArrayRef {
    let array_data = ArrayDataBuilder::new(data_type.clone())
        .len(len)
        .add_buffer(buffer)
        .null_count(array.null_count() as usize)
        .build();

    match data_type {
        DataType::Int8 => Arc::new(PrimitiveArray::<Int8Type>::from(array_data)),
        DataType::Int16 => Arc::new(PrimitiveArray::<Int16Type>::from(array_data)),
        DataType::Int32 => Arc::new(PrimitiveArray::<Int32Type>::from(array_data)),
        DataType::Int64 => Arc::new(PrimitiveArray::<Int64Type>::from(array_data)),
        DataType::UInt8 => Arc::new(PrimitiveArray::<UInt8Type>::from(array_data)),
        DataType::UInt16 => Arc::new(PrimitiveArray::<UInt16Type>::from(array_data)),
        DataType::UInt32 => Arc::new(PrimitiveArray::<UInt32Type>::from(array_data)),
        DataType::UInt64 => Arc::new(PrimitiveArray::<UInt64Type>::from(array_data)),
        DataType::Float32 => Arc::new(PrimitiveArray::<Float32Type>::from(array_data)),
        DataType::Float64 => Arc::new(PrimitiveArray::<Float64Type>::from(array_data)),
        t @ _ => panic!("Building array for data type {:?} not supported", t),
    }
}

/// Feather reader
pub struct FeatherReader<R: Read + Seek> {
    reader: BufReader<R>,
}

impl<R: Read + Seek> FeatherReader<R> {
    /// Create a new Feather reader
    pub fn new(reader: R) -> Self {
        Self {
            reader: BufReader::new(reader),
        }
    }

    /// Read Feather file into a RecordBatch
    pub fn read(&mut self) -> Result<RecordBatch> {
        // check if header and footer contain correc magic bytes
        let mut magic_buffer: [u8; 4] = [0; 4];
        self.reader.read_exact(&mut magic_buffer)?;
        if magic_buffer != FEATHER_MAGIC {
            return Err(ArrowError::IoError(
                "Feather file does not contain correct header".to_string(),
            ));
        }
        self.reader.seek(SeekFrom::End(-4))?;
        self.reader.read_exact(&mut magic_buffer)?;
        if magic_buffer != FEATHER_MAGIC {
            return Err(ArrowError::IoError(
                "Feather file does not contain correct footer".to_string(),
            ));
        }
        self.reader.seek(SeekFrom::End(-8))?;
        // determine metadata length
        let mut meta_size: [u8; 4] = [0; 4];
        self.reader.read_exact(&mut meta_size).unwrap();
        let meta_len = as_u32_le(&meta_size);

        let mut meta_buffer = vec![];
        for _ in 0..meta_len {
            meta_buffer.push(0);
        }
        self.reader.seek(SeekFrom::End(-(meta_len as i64) - 8))?;
        self.reader.read_exact(&mut meta_buffer)?;

        let vecs = &meta_buffer.to_vec();

        let ctable = fbs::get_root_as_ctable(vecs);

        if ctable.version() != 2 {
            return Err(ArrowError::IpcError(
                "Only version 2 of Feather files is supported by the reader".to_string(),
            ));
        }
        let num_rows = ctable.num_rows();
        let num_columns = ctable.columns().unwrap().len() as usize;
        let mut fields = Vec::with_capacity(num_columns);
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(num_columns);
        // keep track of offsets, useful when dealing with different type metadata
        let mut last_offset = 8;

        // iterate over columns
        for i in 0..num_columns {
            let column: fbs::Column = ctable.columns().unwrap().get(i);
            let name = column.name().unwrap();
            // get type metadata, we're dealing with a 'PLAIN' array if it is null
            let type_metadata = column.metadata_type();

            match type_metadata {
                fbs::TypeMetadata::CategoryMetadata => {
                    // levels and ordered
                    let meta: fbs::CategoryMetadata =
                        column.metadata_as_category_metadata().unwrap();
                    let array: fbs::PrimitiveArray = meta.levels().unwrap();

                    // TODO make buffer reading a separate function
                    let total_bytes = array.length() * 4 + 4; // array.total_bytes();
                    let mut buffer = Vec::with_capacity(total_bytes as usize);
                    self.reader
                        .seek(SeekFrom::Start(array.offset() as u64))
                        .unwrap();
                    for _ in 0..(total_bytes) {
                        buffer.push(0);
                    }
                    self.reader.read_exact(&mut buffer).unwrap();

                    // try convert the buffer into integers
                    let mut string_pos = vec![];
                    for i in (0..(total_bytes as usize)).step_by(4) {
                        let mut arr = [0; 4];
                        arr.copy_from_slice(&buffer[i..(i + 4)]);
                        string_pos.push(as_u32_le(&arr));
                    }

                    let str_len = &string_pos[string_pos.len() - 1];

                    // get the array positions using last offset
                    let value_len = array.offset() - last_offset;
                    let value_width = value_len / num_rows;
                    let mut value_buffer = Vec::with_capacity(value_len as usize);
                    for _ in 0..value_len {
                        value_buffer.push(0);
                    }
                    self.reader
                        .seek(SeekFrom::Start(last_offset as u64))
                        .unwrap();
                    self.reader.read_exact(&mut value_buffer).unwrap();

                    // get the string categories
                    let mut buffer = Vec::with_capacity(str_len.clone() as usize);
                    for _ in 0..(*str_len as usize) {
                        buffer.push(0);
                    }
                    let mut pad = 8 - (total_bytes % 8) as u64;
                    if pad == 8 {
                        pad = 0;
                    }
                    self.reader
                        .seek(SeekFrom::Start(
                            array.offset() as u64 + total_bytes as u64 + pad,
                        ))
                        .unwrap();

                    self.reader.read_exact(&mut buffer).unwrap();

                    let strings: Vec<String> = string_pos
                        .windows(2)
                        .map(|w| {
                            String::from_utf8(
                                buffer[(w[0] as usize)..(w[1] as usize)].to_vec(),
                            )
                            .unwrap()
                        })
                        .collect();

                    // create an array of strings
                    let mut builder = BinaryBuilder::new(num_rows as usize * 100); // TODO get the exact size
                    for i in (0..value_buffer.len()).step_by(value_width as usize) {
                        let index = match value_width {
                            1 => i as usize,
                            4 => {
                                let mut arr = [0; 4];
                                arr.copy_from_slice(&value_buffer[i..(i + 4)]);
                                as_u32_le(&arr) as usize
                            }
                            _ => panic!(format!("Unsupported length {}", value_width)),
                        };
                        builder.append_string(&strings[index]).unwrap()
                    }

                    let arr = builder.finish();
                    arrays.push(Arc::new(arr));

                    let field = Field::new(
                        name,
                        get_data_type(array.type_()),
                        array.null_count() > 0,
                    );
                    fields.push(field);
                }
                fbs::TypeMetadata::TimestampMetadata
                | fbs::TypeMetadata::DateMetadata
                | fbs::TypeMetadata::TimeMetadata => {
                    return Err(ArrowError::IpcError(
                        "Date/time Feather records are currently not supported."
                            .to_string(),
                    ));
                }
                fbs::TypeMetadata::NONE => {
                    let array: fbs::PrimitiveArray = column.values().unwrap();

                    let offset = array.offset();
                    let null_count = array.null_count();
                    let total_bytes = array.total_bytes();
                    let dtype = get_data_type(array.type_());

                    // update last offset. This is useful when the next field is
                    // categorical, as categorical offsets don't include record positions,
                    // i.e. if the current offset + data length is
                    // 128, and the next column is a categorical record, its offset won't
                    // start at 128. Keeping track of the previous
                    // offset helps us determine the memory region to access for the next
                    // record's indices.
                    last_offset = offset + total_bytes;
                    assert!(last_offset > 0);

                    // create field
                    let field =
                        Field::new(name, get_data_type(array.type_()), null_count > 0);
                    fields.push(field);

                    let array_ref = if &dtype == &DataType::Utf8 {
                        // get 2 buffers, one for offsets, and another for values
                        let num_offsets = array.length() * 4 + 4;
                        let mut buffer = Vec::with_capacity(num_offsets as usize);
                        self.reader.seek(SeekFrom::Start(offset as u64)).unwrap();
                        for _ in 0..(num_offsets) {
                            buffer.push(0);
                        }
                        self.reader.read_exact(&mut buffer)?;

                        // try convert the buffer into integers
                        let mut offset_pos = vec![];
                        // LittleEndian::read_u32_into(&buffer, &mut offset_pos);
                        for i in (0..(num_offsets as usize)).step_by(4) {
                            let mut arr = [0; 4];
                            arr.copy_from_slice(&buffer[i..(i + 4)]);
                            offset_pos.push(as_u32_le(&arr));
                        }

                        let offsets_buf = Buffer::from(&buffer);

                        // get the length of the text from last offset
                        let str_len = &offset_pos[offset_pos.len() - 1];

                        let mut pad = 8 - (num_offsets % 8);
                        if pad == 8 {
                            pad = 0;
                        }

                        let str_offset = offset + num_offsets + pad;

                        let mut buffer = Vec::with_capacity(*str_len as usize);
                        self.reader
                            .seek(SeekFrom::Start(str_offset as u64))
                            .unwrap();
                        for _ in 0..(*str_len) {
                            buffer.push(0);
                        }
                        self.reader.read_exact(&mut buffer).unwrap();
                        let string_buf = Buffer::from(&buffer);

                        let array_data = ArrayDataBuilder::new(dtype)
                            .len(array.length() as usize)
                            .add_buffer(offsets_buf)
                            .add_buffer(string_buf)
                            .null_count(array.null_count() as usize)
                            .build();

                        Arc::new(BinaryArray::from(array_data))
                    } else {
                        let mut buffer = Vec::with_capacity(total_bytes as usize);
                        self.reader.seek(SeekFrom::Start(offset as u64)).unwrap();
                        for _ in 0..(total_bytes) {
                            buffer.push(0);
                        }
                        self.reader.read_exact(&mut buffer).unwrap();

                        let buf = Buffer::from(&buffer);

                        make_primitive_array(array, num_rows as usize, buf, dtype)
                    };

                    last_offset = offset + total_bytes;

                    arrays.push(array_ref);
                }
            };
        }

        Ok(RecordBatch::new(Arc::new(Schema::new(fields)), arrays))
    }
}

/// Feather writer
pub trait FeatherWriter {
    /// Write RecordBatch to Feather file
    fn write_feather(&self, path: &str) -> Result<()>;
}

impl FeatherWriter for RecordBatch {
    fn write_feather(&self, path: &str) -> Result<()> {
        let mut file: File = File::create(path)?;
        file.write(&FEATHER_MAGIC)?;
        file.write(&[0, 0, 0, 0])?;

        let num_records = self.num_rows();

        let schema = self.schema();
        let mut builder = flatbuffers::FlatBufferBuilder::new_with_capacity(256);

        // set the offset as 8, which is 4 bytes for the header, and 4 bytes for padding
        let mut current_offset = 8;
        let mut fbs_cols = vec![];

        for i in 0..self.num_columns() {
            // create metadata for each column, write column data to file
            let column = self.column(i);
            match column.data_type() {
                DataType::Boolean
                | DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                | DataType::Float32
                | DataType::Float64 => {
                    // create primitive array
                    let col_name = builder.create_string(schema.field(i).name());
                    let data = &column.data();
                    let data: &Buffer = &data.buffers()[0];
                    file.write(data.data())?;

                    // column values
                    let prim_array = fbs::PrimitiveArray::create(
                        &mut builder,
                        &fbs::PrimitiveArrayArgs {
                            type_: get_fbs_type(column.data_type().clone()),
                            encoding: fbs::Encoding::PLAIN,
                            offset: current_offset,
                            length: num_records as i64,
                            null_count: column.null_count() as i64,
                            total_bytes: data.len() as i64,
                        },
                    );

                    let fbs_column = fbs::Column::create(
                        &mut builder,
                        &fbs::ColumnArgs {
                            name: Some(col_name),
                            values: Some(prim_array),
                            metadata_type: fbs::TypeMetadata::NONE,
                            metadata: None,
                            user_metadata: None,
                        },
                    );

                    current_offset += data.len() as i64;

                    fbs_cols.push(fbs_column);
                }
                DataType::Utf8 => {
                    // assume that Utf8 data is not categorical

                    // we create 2 buffers for strings
                    let mut total_bytes = 0;
                    let col_name = builder.create_string(schema.field(i).name());
                    let data = &column.data();
                    let buf: &Buffer = &data.buffers()[0];
                    file.write(buf.data())?;

                    let mut pad = 8 - (buf.len() as i64 % 8) as i64;
                    if pad == 8 {
                        pad = 0;
                    }

                    // pad file
                    if pad > 0 {
                        let mut buffer = Vec::with_capacity(pad as usize);
                        for _ in 0..(pad as usize) {
                            buffer.push(0);
                        }

                        file.write(&buffer)?;
                    }

                    total_bytes += buf.len() as i64;

                    let buf: &Buffer = &data.buffers()[1];
                    file.write(buf.data())?;

                    let mut pad = 8 - (buf.len() as i64 % 8) as i64;
                    if pad == 8 {
                        pad = 0;
                    }

                    // pad file
                    if pad > 0 {
                        let mut buffer = Vec::with_capacity(pad as usize);
                        for _ in 0..(pad as usize) {
                            buffer.push(0);
                        }

                        file.write(&buffer)?;
                    }

                    total_bytes += buf.len() as i64 + pad as i64;

                    // column values
                    let prim_array = fbs::PrimitiveArray::create(
                        &mut builder,
                        &fbs::PrimitiveArrayArgs {
                            type_: get_fbs_type(column.data_type().clone()),
                            encoding: fbs::Encoding::PLAIN,
                            offset: current_offset,
                            length: num_records as i64,
                            null_count: column.null_count() as i64,
                            // TODO should this be a multiple of 8
                            total_bytes: total_bytes as i64,
                        },
                    );

                    current_offset += total_bytes as i64;

                    let fbs_column = fbs::Column::create(
                        &mut builder,
                        &fbs::ColumnArgs {
                            name: Some(col_name),
                            values: Some(prim_array),
                            metadata_type: fbs::TypeMetadata::NONE,
                            metadata: None,
                            user_metadata: None,
                        },
                    );

                    fbs_cols.push(fbs_column);
                }
                DataType::Float16 => {
                    return Err(ArrowError::IpcError(
                        "DataType::Float16 is currently not supported by Rust Arrow"
                            .to_string(),
                    ));
                }
                DataType::List(_) | DataType::Struct(_) => {
                    return Err(ArrowError::IpcError(
                        "Writing of lists and structs not supported in Feather"
                            .to_string(),
                    ));
                }
                DataType::Timestamp(_)
                | DataType::Date(_)
                | DataType::Time32(_)
                | DataType::Time64(_)
                | DataType::Interval(_) => {
                    return Err(ArrowError::IpcError(
                        "Date and time formats currently not supported by Rust Arrow"
                            .to_string(),
                    ));
                }
            }
        }

        let fbs_columns = builder.create_vector(&fbs_cols);

        // loop through schema and write columns
        let fbs_table = fbs::CTable::create(
            &mut builder,
            &fbs::CTableArgs {
                description: None,
                num_rows: num_records as i64,
                columns: Some(fbs_columns),
                version: 2,
                metadata: None,
            },
        );

        builder.finish(fbs_table, None);

        file.write(builder.finished_data())?;

        let meta_len = builder.finished_data().len() as u32;
        file.write_u32::<LittleEndian>(meta_len)?;

        file.write(&FEATHER_MAGIC)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::fs::File;

    use crate::csv::reader::ReaderBuilder;

    #[test]
    // TODO: write test files to tmp, add more tests
    fn read_csv_write_feather_roundtrip() {
        let file = File::open("test/data/uk_cities_with_headers.csv").unwrap();

        let builder = ReaderBuilder::new().has_headers(true).infer_schema(None);

        let mut csv = builder.build(file).unwrap();
        let batch = csv.next().unwrap().unwrap();

        // write to feather
        batch.write_feather("test/data/uk_cities.feather").unwrap();

        // read back as feather
        let mut feather_reader =
            FeatherReader::new(File::open("test/data/uk_cities.feather").unwrap());
        let batch2 = feather_reader.read().unwrap();

        assert!(batch.schema().fields() == batch2.schema().fields());

        batch2.write_feather("test/data/uk_cities.feather").unwrap();
    }
}
