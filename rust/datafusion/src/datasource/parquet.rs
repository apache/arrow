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
use parquet::record::{Row, RowAccessor};

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
    /// The schema of the underlying file
    schema: Arc<Schema>,
    projection: Vec<usize>,
    batch_size: usize,
    current_row_group: Option<Box<RowGroupReader>>,
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

                let projection = match projection {
                    Some(p) => p,
                    None => {
                        let mut p = Vec::with_capacity(schema.fields().len());
                        for i in 0..schema.fields().len() {
                            p.push(i);
                        }
                        p
                    }
                };

                Ok(ParquetFile {
                    reader: reader,
                    row_group_index: 0,
                    schema: Arc::new(schema),
                    projection,
                    batch_size: 64 * 1024,
                    current_row_group: None,
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

//            self.column_readers = vec![];
//
//            match &self.projection {
//                None => {
//                    for i in 0..reader.num_columns() {
//                        self.column_readers
//                            .push(reader.get_column_reader(i).unwrap());
//                    }
//                }
//                Some(proj) => {
//                    for i in proj {
//                        //TODO validate index in bounds
//                        self.column_readers
//                            .push(reader.get_column_reader(*i).unwrap());
//                    }
//                }
//            }

            self.current_row_group = Some(reader);
            self.row_group_index += 1;
        } else {
            panic!()
        }
    }

    fn load_batch(&mut self) -> Result<Option<RecordBatch>> {
        match &self.current_row_group {
            Some(reader) => {

                // read batch of rows into memory

//                let parquet_projection = self.projection.iter().map(|i| reader.metadata().schema_descr().column(*i)).collect();

                let mut row_iter = reader.get_row_iter(None).unwrap(); //TODO projection push down
                let mut rows: Vec<Row> = Vec::with_capacity(self.batch_size);
                while let Some(row) = row_iter.next() {
                    if rows.len() == self.batch_size {
                        break;
                    }
                    rows.push(row);
                }
                println!("Loaded {} rows into memory", rows.len());

                // convert to columnar batch
                let mut batch: Vec<Arc<Array>> = Vec::with_capacity(self.projection.len());
                for i in &self.projection {
                    let array: Arc<Array> = match self.schema.field(*i).data_type() {
                        DataType::Int32 => {
                            let mut builder = Int32Builder::new(rows.len());
                            for row in &rows {
                                //TODO null handling
                                builder.append_value(row.get_int(*i).unwrap()).unwrap();
                            }
                            Arc::new(builder.finish())
                        }
                        DataType::Utf8 => {
                            let mut builder = BinaryBuilder::new(rows.len());
                            for row in &rows {
                                //TODO null handling
                                let bytes = row.get_bytes(*i).unwrap();
                                builder.append_string(&String::from_utf8(bytes.data().to_vec()).unwrap()).unwrap();
                            }
                            Arc::new(builder.finish())
                        }
                        other => return Err(ExecutionError::NotImplemented(
                                format!("unsupported column reader type ({:?})", other)))
                    };
                    batch.push(array);
                }

                println!("Loaded batch of {} rows", rows.len());

                if rows.len() == 0 {
                    Ok(None)
                } else {
                    Ok(Some(RecordBatch::try_new(
                            self.schema.projection(&self.projection)?,
                            batch,
                        )?))
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
    use arrow::array::{BinaryArray, Int32Array};
    use std::env;

    #[test]
    fn read_read_i32_column() {
        let table = load_table("alltypes_plain.parquet");

        let projection = Some(vec![0]);
        let scan = table.scan(&projection, 1024).unwrap();
        let mut it = scan.borrow_mut();
        let batch = it.next().unwrap().unwrap();

        assert_eq!(1, batch.num_columns());
        assert_eq!(8, batch.num_rows());

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
            "[4, 5, 6, 7, 2, 3, 0, 1, 0, 0, 9, 0, 1, 0, 0, 0]",
            format!("{:?}", values)
        );
    }

    #[test]
    fn read_read_string_column() {
        let table = load_table("alltypes_plain.parquet");

        let projection = Some(vec![9]);
        let scan = table.scan(&projection, 1024).unwrap();
        let mut it = scan.borrow_mut();
        let batch = it.next().unwrap().unwrap();

        assert_eq!(1, batch.num_columns());
        assert_eq!(8, batch.num_rows());

        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let mut values: Vec<String> = vec![];
        for i in 0..8 {
            let str: String = String::from_utf8(array.value(i).to_vec()).unwrap();
            values.push(str);
        }

        assert_eq!(
            "[\"0\", \"1\", \"0\", \"1\", \"0\", \"1\", \"0\", \"1\"]",
            format!("{:?}", values)
        );
    }

    fn load_table(name: &str) -> Box<Table> {
        let testdata = env::var("PARQUET_TEST_DATA").unwrap();
        let filename = format!("{}/{}", testdata, name);
        let table = ParquetTable::new(&filename);
        println!("{:?}", table.schema());
        Box::new(table)
    }
}
