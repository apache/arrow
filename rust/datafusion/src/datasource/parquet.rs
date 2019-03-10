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

use std::fs::File;
use std::string::String;
use std::sync::{Arc, Mutex};

use arrow::array::Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use parquet::basic;
use parquet::column::reader::*;
use parquet::data_type::ByteArray;
use parquet::file::reader::*;
use parquet::schema::types::Type;

use crate::datasource::{RecordBatchIterator, ScanResult, Table};
use crate::execution::error::{ExecutionError, Result};
use arrow::builder::BooleanBuilder;
use arrow::builder::Int64Builder;
use arrow::builder::{BinaryBuilder, Float32Builder, Float64Builder, Int32Builder};
use parquet::data_type::Int96;

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
    ) -> Result<Vec<ScanResult>> {
        let file = File::open(self.filename.clone()).unwrap();
        let parquet_file = ParquetFile::open(file, projection.clone()).unwrap();
        Ok(vec![Arc::new(Mutex::new(parquet_file))])
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
    column_readers: Vec<ColumnReader>,
}

impl ParquetFile {
    pub fn open(file: File, projection: Option<Vec<usize>>) -> Result<Self> {
        println!("open()");

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

                let projected_fields: Vec<Field> = projection
                    .iter()
                    .map(|i| schema.fields()[*i].clone())
                    .collect();

                let projected_schema = Arc::new(Schema::new(projected_fields));

                Ok(ParquetFile {
                    reader: reader,
                    row_group_index: 0,
                    schema: projected_schema,
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
            let reader = self.reader.get_row_group(self.row_group_index).unwrap();

            self.column_readers = vec![];

            for i in &self.projection {
                self.column_readers
                    .push(reader.get_column_reader(*i).unwrap());
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
                        ColumnReader::BoolColumnReader(ref mut r) => {
                            let mut read_buffer: Vec<bool> =
                                Vec::with_capacity(self.batch_size);

                            for _ in 0..self.batch_size {
                                read_buffer.push(false);
                            }

                            match r.read_batch(
                                self.batch_size,
                                None,
                                None,
                                &mut read_buffer,
                            ) {
                                Ok((count, _)) => {
                                    let mut builder = BooleanBuilder::new(count);
                                    builder.append_slice(&read_buffer[0..count]).unwrap();
                                    row_count = count;
                                    Arc::new(builder.finish())
                                }
                                Err(e) => {
                                    return Err(ExecutionError::NotImplemented(format!(
                                        "Error reading parquet batch (column {}): {:?}",
                                        i, e
                                    )));
                                }
                            }
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
                                Ok((count, _)) => {
                                    let mut builder = Int32Builder::new(count);
                                    builder.append_slice(&read_buffer[0..count]).unwrap();
                                    row_count = count;
                                    Arc::new(builder.finish())
                                }
                                Err(e) => {
                                    return Err(ExecutionError::NotImplemented(format!(
                                        "Error reading parquet batch (column {}): {:?}",
                                        i, e
                                    )));
                                }
                            }
                        }
                        ColumnReader::Int64ColumnReader(ref mut r) => {
                            let mut read_buffer: Vec<i64> =
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
                                Ok((count, _)) => {
                                    let mut builder = Int64Builder::new(count);
                                    builder.append_slice(&read_buffer[0..count]).unwrap();
                                    row_count = count;
                                    Arc::new(builder.finish())
                                }
                                Err(e) => {
                                    return Err(ExecutionError::NotImplemented(format!(
                                        "Error reading parquet batch (column {}): {:?}",
                                        i, e
                                    )));
                                }
                            }
                        }
                        ColumnReader::Int96ColumnReader(ref mut r) => {
                            let mut read_buffer: Vec<Int96> =
                                Vec::with_capacity(self.batch_size);

                            for _ in 0..self.batch_size {
                                read_buffer.push(Int96::new());
                            }

                            match r.read_batch(
                                self.batch_size,
                                None,
                                None,
                                &mut read_buffer,
                            ) {
                                Ok((count, _)) => {
                                    let mut builder = Int64Builder::new(count);

                                    for i in 0..count {
                                        let v = read_buffer[i].data();
                                        let value: u128 = (v[0] as u128) << 64
                                            | (v[1] as u128) << 32
                                            | (v[2] as u128);
                                        let ms: i64 = (value / 1000000) as i64;
                                        builder.append_value(ms).unwrap();
                                    }
                                    row_count = count;
                                    Arc::new(builder.finish())
                                }
                                Err(e) => {
                                    return Err(ExecutionError::NotImplemented(format!(
                                        "Error reading parquet batch (column {}): {:?}",
                                        i, e
                                    )));
                                }
                            }
                        }
                        ColumnReader::FloatColumnReader(ref mut r) => {
                            let mut builder = Float32Builder::new(self.batch_size);
                            let mut read_buffer: Vec<f32> =
                                Vec::with_capacity(self.batch_size);
                            for _ in 0..self.batch_size {
                                read_buffer.push(0.0);
                            }
                            match r.read_batch(
                                self.batch_size,
                                None,
                                None,
                                &mut read_buffer,
                            ) {
                                Ok((count, _)) => {
                                    builder.append_slice(&read_buffer[0..count]).unwrap();
                                    row_count = count;
                                    Arc::new(builder.finish())
                                }
                                Err(e) => {
                                    return Err(ExecutionError::NotImplemented(format!(
                                        "Error reading parquet batch (column {}): {:?}",
                                        i, e
                                    )));
                                }
                            }
                        }
                        ColumnReader::DoubleColumnReader(ref mut r) => {
                            let mut builder = Float64Builder::new(self.batch_size);
                            let mut read_buffer: Vec<f64> =
                                Vec::with_capacity(self.batch_size);
                            for _ in 0..self.batch_size {
                                read_buffer.push(0.0);
                            }
                            match r.read_batch(
                                self.batch_size,
                                None,
                                None,
                                &mut read_buffer,
                            ) {
                                Ok((count, _)) => {
                                    builder.append_slice(&read_buffer[0..count]).unwrap();
                                    row_count = count;
                                    Arc::new(builder.finish())
                                }
                                Err(e) => {
                                    return Err(ExecutionError::NotImplemented(format!(
                                        "Error reading parquet batch (column {}): {:?}",
                                        i, e
                                    )));
                                }
                            }
                        }
                        ColumnReader::FixedLenByteArrayColumnReader(ref mut r) => {
                            let mut b: Vec<ByteArray> =
                                Vec::with_capacity(self.batch_size);
                            for _ in 0..self.batch_size {
                                b.push(ByteArray::default());
                            }
                            match r.read_batch(self.batch_size, None, None, &mut b) {
                                Ok((count, _)) => {
                                    row_count = count;
                                    let mut builder = BinaryBuilder::new(row_count);
                                    for j in 0..row_count {
                                        let slice = b[j].slice(0, b[j].len());
                                        builder
                                            .append_string(
                                                &String::from_utf8(slice.data().to_vec())
                                                    .unwrap(),
                                            )
                                            .unwrap();
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
                        ColumnReader::ByteArrayColumnReader(ref mut r) => {
                            let mut b: Vec<ByteArray> =
                                Vec::with_capacity(self.batch_size);
                            for _ in 0..self.batch_size {
                                b.push(ByteArray::default());
                            }
                            match r.read_batch(self.batch_size, None, None, &mut b) {
                                Ok((count, _)) => {
                                    row_count = count;
                                    let mut builder = BinaryBuilder::new(row_count);
                                    for j in 0..row_count {
                                        let slice = b[j].slice(0, b[j].len());
                                        builder
                                            .append_string(
                                                &String::from_utf8(slice.data().to_vec())
                                                    .unwrap(),
                                            )
                                            .unwrap();
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
                    Ok(Some(RecordBatch::try_new(self.schema.clone(), batch)?))
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
                basic::Type::INT96 => DataType::Int64,
                basic::Type::FLOAT => DataType::Float32,
                basic::Type::DOUBLE => DataType::Float64,
                basic::Type::BYTE_ARRAY => DataType::Utf8,
                basic::Type::FIXED_LEN_BYTE_ARRAY => DataType::Utf8,
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
    use arrow::array::BooleanArray;
    use arrow::array::Float32Array;
    use arrow::array::Float64Array;
    use arrow::array::Int64Array;
    use arrow::array::{BinaryArray, Int32Array};
    use std::env;

    #[test]
    fn read_alltypes_plain_parquet() {
        let table = load_table("alltypes_plain.parquet");

        let projection = None;
        let scan = table.scan(&projection, 1024).unwrap();
        let mut it = scan[0].lock().unwrap();
        let batch = it.next().unwrap().unwrap();

        assert_eq!(11, batch.num_columns());
        assert_eq!(8, batch.num_rows());
    }

    #[test]
    fn read_bool_alltypes_plain_parquet() {
        let table = load_table("alltypes_plain.parquet");

        let projection = Some(vec![1]);
        let scan = table.scan(&projection, 1024).unwrap();
        let mut it = scan[0].lock().unwrap();
        let batch = it.next().unwrap().unwrap();

        assert_eq!(1, batch.num_columns());
        assert_eq!(8, batch.num_rows());

        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        let mut values: Vec<bool> = vec![];
        for i in 0..batch.num_rows() {
            values.push(array.value(i));
        }

        assert_eq!(
            "[true, false, true, false, true, false, true, false]",
            format!("{:?}", values)
        );
    }

    #[test]
    fn read_i32_alltypes_plain_parquet() {
        let table = load_table("alltypes_plain.parquet");

        let projection = Some(vec![0]);
        let scan = table.scan(&projection, 1024).unwrap();
        let mut it = scan[0].lock().unwrap();
        let batch = it.next().unwrap().unwrap();

        assert_eq!(1, batch.num_columns());
        assert_eq!(8, batch.num_rows());

        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let mut values: Vec<i32> = vec![];
        for i in 0..batch.num_rows() {
            values.push(array.value(i));
        }

        assert_eq!("[4, 5, 6, 7, 2, 3, 0, 1]", format!("{:?}", values));
    }

    #[test]
    fn read_i96_alltypes_plain_parquet() {
        let table = load_table("alltypes_plain.parquet");

        let projection = Some(vec![10]);
        let scan = table.scan(&projection, 1024).unwrap();
        let mut it = scan[0].lock().unwrap();
        let batch = it.next().unwrap().unwrap();

        assert_eq!(1, batch.num_columns());
        assert_eq!(8, batch.num_rows());

        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let mut values: Vec<i64> = vec![];
        for i in 0..batch.num_rows() {
            values.push(array.value(i));
        }

        assert_eq!("[2, 7842670136425819125, 2, 7842670136425819125, 2, 7842670136425819125, 2, 7842670136425819125]", format!("{:?}", values));
    }

    #[test]
    fn read_f32_alltypes_plain_parquet() {
        let table = load_table("alltypes_plain.parquet");

        let projection = Some(vec![6]);
        let scan = table.scan(&projection, 1024).unwrap();
        let mut it = scan[0].lock().unwrap();
        let batch = it.next().unwrap().unwrap();

        assert_eq!(1, batch.num_columns());
        assert_eq!(8, batch.num_rows());

        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        let mut values: Vec<f32> = vec![];
        for i in 0..batch.num_rows() {
            values.push(array.value(i));
        }

        assert_eq!(
            "[0.0, 1.1, 0.0, 1.1, 0.0, 1.1, 0.0, 1.1]",
            format!("{:?}", values)
        );
    }

    #[test]
    fn read_f64_alltypes_plain_parquet() {
        let table = load_table("alltypes_plain.parquet");

        let projection = Some(vec![7]);
        let scan = table.scan(&projection, 1024).unwrap();
        let mut it = scan[0].lock().unwrap();
        let batch = it.next().unwrap().unwrap();

        assert_eq!(1, batch.num_columns());
        assert_eq!(8, batch.num_rows());

        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let mut values: Vec<f64> = vec![];
        for i in 0..batch.num_rows() {
            values.push(array.value(i));
        }

        assert_eq!(
            "[0.0, 10.1, 0.0, 10.1, 0.0, 10.1, 0.0, 10.1]",
            format!("{:?}", values)
        );
    }

    #[test]
    fn read_utf8_alltypes_plain_parquet() {
        let table = load_table("alltypes_plain.parquet");

        let projection = Some(vec![9]);
        let scan = table.scan(&projection, 1024).unwrap();
        let mut it = scan[0].lock().unwrap();
        let batch = it.next().unwrap().unwrap();

        assert_eq!(1, batch.num_columns());
        assert_eq!(8, batch.num_rows());

        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let mut values: Vec<String> = vec![];
        for i in 0..batch.num_rows() {
            let str: String = String::from_utf8(array.value(i).to_vec()).unwrap();
            values.push(str);
        }

        assert_eq!(
            "[\"0\", \"1\", \"0\", \"1\", \"0\", \"1\", \"0\", \"1\"]",
            format!("{:?}", values)
        );
    }

    #[test]
    fn read_int64_nullable_impala_parquet() {
        let table = load_table("nullable.impala.parquet");

        let projection = Some(vec![0]);
        let scan = table.scan(&projection, 1024).unwrap();
        let mut it = scan[0].lock().unwrap();
        let batch = it.next().unwrap().unwrap();

        assert_eq!(1, batch.num_columns());
        assert_eq!(7, batch.num_rows());

        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let mut values: Vec<i64> = vec![];
        for i in 0..batch.num_rows() {
            values.push(array.value(i));
        }

        assert_eq!("[1, 2, 3, 4, 5, 6, 7]", format!("{:?}", values));
    }

    fn load_table(name: &str) -> Box<Table> {
        let testdata = env::var("PARQUET_TEST_DATA").unwrap();
        let filename = format!("{}/{}", testdata, name);
        let table = ParquetTable::new(&filename);
        println!("Loading file {} with schema:", name);
        for field in table.schema().fields() {
            println!("\t{:?}", field);
        }
        Box::new(table)
    }
}
