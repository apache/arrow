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

//! Parquet Data source

use std::cell::RefCell;
use std::fs::File;
use std::rc::Rc;
use std::string::String;
use std::sync::Arc;

use arrow::array::Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use parquet::basic;
use parquet::column::reader::*;
use parquet::data_type::ByteArray;
use parquet::file::reader::*;
use parquet::schema::types::Type;

use crate::datasource::{RecordBatchIterator, Table};
use crate::execution::error::{ExecutionError, Result};
use arrow::builder::{BinaryBuilder, Float64Builder, Int32Builder};

pub struct ParquetTable {
    filename: String,
    schema: Arc<Schema>,
}

impl ParquetTable {
    pub fn new(filename: &str) -> Self {
        let file = File::open(filename).unwrap();
        let parquet_file = ParquetFile::open(file, None).unwrap();
        let schema = parquet_file.schema.clone();
        Self {
            filename: filename.to_string(),
            schema,
        }
    }
}

impl Table for ParquetTable {
    fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        _batch_size: usize,
    ) -> Result<Rc<RefCell<RecordBatchIterator>>> {
        let file = File::open(self.filename.clone()).unwrap();
        let parquet_file = ParquetFile::open(file, projection.clone()).unwrap();
        Ok(Rc::new(RefCell::new(parquet_file)))
    }
}

pub struct ParquetFile {
    reader: SerializedFileReader<File>,
    row_group_index: usize,
    schema: Arc<Schema>,
    projection: Option<Vec<usize>>,
    batch_size: usize,
    current_row_group: Option<Box<RowGroupReader>>,
    column_readers: Vec<ColumnReader>,
}

impl ParquetFile {
    pub fn open(file: File, projection: Option<Vec<usize>>) -> Result<Self> {
        let reader = SerializedFileReader::new(file).unwrap();

        let metadata = reader.metadata();
        let file_type = to_arrow(metadata.file_metadata().schema())?;

        match file_type.data_type() {
            DataType::Struct(fields) => {
                let schema = Schema::new(fields.clone());
                //println!("Parquet schema: {:?}", schema);
                Ok(ParquetFile {
                    reader: reader,
                    row_group_index: 0,
                    schema: Arc::new(schema),
                    projection,
                    batch_size: 64 * 1024,
                    current_row_group: None,
                    column_readers: vec![],
                })
            }
            _ => Err(ExecutionError::General(
                "Failed to read Parquet schema".to_string(),
            )),
        }
    }

    fn load_next_row_group(&mut self) {
        if self.row_group_index < self.reader.num_row_groups() {
            //println!("Loading row group {} of {}", self.row_group_index, self.reader.num_row_groups());
            let reader = self.reader.get_row_group(self.row_group_index).unwrap();

            self.column_readers = vec![];

            match &self.projection {
                None => {
                    for i in 0..reader.num_columns() {
                        self.column_readers
                            .push(reader.get_column_reader(i).unwrap());
                    }
                }
                Some(proj) => {
                    for i in proj {
                        //TODO validate index in bounds
                        self.column_readers
                            .push(reader.get_column_reader(*i).unwrap());
                    }
                }
            }

            self.current_row_group = Some(reader);
            self.row_group_index += 1;
        } else {
            panic!()
        }
    }

    fn load_batch(&mut self) -> Result<Option<RecordBatch>> {
        match &self.current_row_group {
            Some(reader) => {
                let mut batch: Vec<Arc<Array>> = Vec::with_capacity(reader.num_columns());
                let mut row_count = 0;
                for i in 0..self.column_readers.len() {
                    let array: Arc<Array> = match self.column_readers[i] {
                        ColumnReader::BoolColumnReader(ref mut _r) => {
                            return Err(ExecutionError::NotImplemented(
                                "unsupported column reader type (BOOL)".to_string(),
                            ));
                        }
                        ColumnReader::Int32ColumnReader(ref mut r) => {
                            let mut read_buffer: Vec<i32> =
                                Vec::with_capacity(self.batch_size);

                            for _ in 0..self.batch_size {
                                read_buffer.push(0);
                            }

                            match r.read_batch(
                                self.batch_size,
                                None,
                                None,
                                &mut read_buffer,
                            ) {
                                //TODO this isn't handling null values
                                Ok((count, _)) => {
                                    println!("Read {} rows", count);
                                    let mut builder = Int32Builder::new(self.batch_size);
                                    builder.append_slice(&read_buffer).unwrap();
                                    row_count = count;
                                    Arc::new(builder.finish())
                                }
                                _ => {
                                    return Err(ExecutionError::NotImplemented(format!(
                                        "Error reading parquet batch (column {})",
                                        i
                                    )));
                                }
                            }
                        }
                        ColumnReader::Int64ColumnReader(ref mut _r) => {
                            return Err(ExecutionError::NotImplemented(
                                "unsupported column reader type (INT64)".to_string(),
                            ));
                        }
                        ColumnReader::Int96ColumnReader(ref mut _r) => {
                            return Err(ExecutionError::NotImplemented(
                                "unsupported column reader type (INT96)".to_string(),
                            ));
                        }
                        ColumnReader::FloatColumnReader(ref mut _r) => {
                            return Err(ExecutionError::NotImplemented(
                                "unsupported column reader type (FLOAT)".to_string(),
                            ));
                        }
                        ColumnReader::DoubleColumnReader(ref mut r) => {
                            let mut builder = Float64Builder::new(self.batch_size);
                            let mut read_buffer: Vec<f64> =
                                Vec::with_capacity(self.batch_size);
                            match r.read_batch(
                                self.batch_size,
                                None,
                                None,
                                &mut read_buffer,
                            ) {
                                //TODO this isn't handling null values
                                Ok((count, _)) => {
                                    builder.append_slice(&read_buffer).unwrap();
                                    row_count = count;
                                    Arc::new(builder.finish())
                                }
                                _ => {
                                    return Err(ExecutionError::NotImplemented(format!(
                                        "Error reading parquet batch (column {})",
                                        i
                                    )));
                                }
                            }
                        }
                        ColumnReader::FixedLenByteArrayColumnReader(ref mut _r) => {
                            return Err(ExecutionError::NotImplemented(
                                "unsupported column reader type (FixedLenByteArray)"
                                    .to_string(),
                            ));
                        }
                        ColumnReader::ByteArrayColumnReader(ref mut r) => {
                            let mut b: Vec<ByteArray> =
                                Vec::with_capacity(self.batch_size);
                            for _ in 0..self.batch_size {
                                b.push(ByteArray::default());
                            }
                            match r.read_batch(self.batch_size, None, None, &mut b) {
                                //TODO this isn't handling null values
                                Ok((count, _)) => {
                                    row_count = count;
                                    //TODO this is horribly inefficient
                                    let mut builder = BinaryBuilder::new(row_count);
                                    for j in 0..row_count {
                                        let foo = b[j].slice(0, b[j].len());
                                        let bytes: &[u8] = foo.data();
                                        let str =
                                            String::from_utf8(bytes.to_vec()).unwrap();
                                        builder.append_string(&str).unwrap();
                                    }
                                    Arc::new(builder.finish())
                                }
                                _ => {
                                    return Err(ExecutionError::NotImplemented(format!(
                                        "Error reading parquet batch (column {})",
                                        i
                                    )));
                                }
                            }
                        }
                    };

                    println!("Adding array to batch");
                    batch.push(array);
                }

                println!("Loaded batch of {} rows", row_count);

                if row_count == 0 {
                    Ok(None)
                } else {
                    match &self.projection {
                        Some(proj) => Ok(Some(RecordBatch::try_new(
                            self.schema.projection(proj)?,
                            batch,
                        )?)),
                        None => {
                            Ok(Some(RecordBatch::try_new(self.schema.clone(), batch)?))
                        }
                    }
                }
            }
            _ => Ok(None),
        }
    }
}

fn to_arrow(t: &Type) -> Result<Field> {
    match t {
        Type::PrimitiveType {
            basic_info,
            physical_type,
            ..
        } => {
            let arrow_type = match physical_type {
                basic::Type::BOOLEAN => DataType::Boolean,
                basic::Type::INT32 => DataType::Int32,
                basic::Type::INT64 => DataType::Int64,
                basic::Type::INT96 => DataType::Int64, //TODO ???
                basic::Type::FLOAT => DataType::Float32,
                basic::Type::DOUBLE => DataType::Float64,
                basic::Type::BYTE_ARRAY => DataType::Utf8, /*match basic_info.logical_type() {
                basic::LogicalType::UTF8 => DataType::Utf8,
                _ => unimplemented!("No support for Parquet BYTE_ARRAY yet"),
                }*/
                basic::Type::FIXED_LEN_BYTE_ARRAY => {
                    unimplemented!("No support for Parquet FIXED_LEN_BYTE_ARRAY yet")
                }
            };

            Ok(Field::new(basic_info.name(), arrow_type, false))
        }
        Type::GroupType { basic_info, fields } => Ok(Field::new(
            basic_info.name(),
            DataType::Struct(
                fields
                    .iter()
                    .map(|f| to_arrow(f))
                    .collect::<Result<Vec<Field>>>()?,
            ),
            false,
        )),
    }
}

impl RecordBatchIterator for ParquetFile {
    fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    fn next(&mut self) -> Result<Option<RecordBatch>> {
        // advance the row group reader if necessary
        if self.current_row_group.is_none() {
            self.load_next_row_group();
            self.load_batch()
        } else {
            match self.load_batch() {
                Ok(Some(b)) => Ok(Some(b)),
                Ok(None) => {
                    if self.row_group_index < self.reader.num_row_groups() {
                        self.load_next_row_group();
                        self.load_batch()
                    } else {
                        Ok(None)
                    }
                }
                Err(e) => Err(e),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use std::env;

    #[test]
    fn read_read_i32_column() {
        let testdata = env::var("PARQUET_TEST_DATA").unwrap();
        let filename = format!("{}/alltypes_plain.parquet", testdata);

        let table = ParquetTable::new(&filename);

        println!("{:?}", table.schema());

        let projection = Some(vec![0]);
        let scan = table.scan(&projection, 1024).unwrap();
        let mut it = scan.borrow_mut();
        let batch = it.next().unwrap().unwrap();

        assert_eq!(1, batch.num_columns());
        assert_eq!(64 * 1024, batch.num_rows());

        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let mut values: Vec<i32> = vec![];
        for i in 0..16 {
            values.push(array.value(i));
        }

        assert_eq!(
            "[4, 5, 6, 7, 2, 3, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0]",
            format!("{:?}", values)
        );
    }
}
