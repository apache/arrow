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

//! Parquet DataSource

use std::fs::File;
use std::rc::Rc;

use crate::array::ListArray;
use crate::builder::*;
use crate::datasource::DataSource;
use crate::datatypes::*;
use crate::error::{ArrowError, Result};
use crate::record_batch::RecordBatch;

use crate::parquet::basic;
use crate::parquet::column::reader::*;
use crate::parquet::data_type::{ByteArray, Int96};
use crate::parquet::file::reader::*;
use crate::parquet::schema::types::Type;

pub struct ParquetFile {
    reader: SerializedFileReader<File>,
    row_group_index: usize,
    schema: Rc<Schema>,
    projection: Option<Vec<usize>>,
    batch_size: usize,
    current_row_group: Option<Box<RowGroupReader>>,
    column_readers: Vec<Option<ColumnReader>>,
}

impl ParquetFile {
    pub fn open(file: File, projection: Option<Vec<usize>>) -> Result<Self> {
        let reader = SerializedFileReader::new(file).unwrap();

        let metadata = reader.metadata();
        let file_type = to_arrow(metadata.file_metadata().schema());

        match file_type.data_type() {
            DataType::Struct(fields) => {
                let schema = Schema::new(fields.clone());
                //println!("Parquet schema: {:?}", schema);
                Ok(ParquetFile {
                    reader: reader,
                    row_group_index: 0,
                    schema: Rc::new(schema),
                    projection,
                    batch_size: 64 * 1024,
                    current_row_group: None,
                    column_readers: vec![],
                })
            }
            _ => Err(ArrowError::IOError(
                "Failed to read Parquet schema".to_string(),
            )),
        }
    }

    pub fn set_batch_size(&mut self, batch_size: usize) {
        self.batch_size = batch_size
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
                            .push(Some(reader.get_column_reader(i).unwrap()));
                    }
                }
                Some(proj) => {
                    for i in 0..reader.num_columns() {
                        if proj.contains(&i) {
                            self.column_readers
                                .push(Some(reader.get_column_reader(i).unwrap()));
                        } else {
                            //println!("Parquet NOT LOADING COLUMN");
                            self.column_readers.push(None);
                        }
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
//        match &self.current_row_group {
//            Some(reader) => {
//                let mut batch: Vec<Value> = Vec::with_capacity(reader.num_columns());
//                let mut row_count = 0;
//                for i in 0..self.column_readers.len() {
//                    let array = match self.column_readers[i] {
//                        Some(ColumnReader::ByteArrayColumnReader(ref mut r)) => {
//                            let mut b: Vec<ByteArray> = Vec::with_capacity(self.batch_size);
//                            for _ in 0..self.batch_size {
//                                b.push(ByteArray::default());
//                            }
//                            match r.read_batch(self.batch_size, None, None, &mut b) {
//                                Ok((count, _)) => {
//                                    row_count = count;
//
//                                    let mut builder: ListBuilder<u8> =
//                                        ListBuilder::with_capacity(count);
//                                    for j in 0..row_count {
//                                        builder.push(b[j].slice(0, b[j].len()).data());
//                                    }
//                                    Array::new(
//                                        count,
//                                        ArrayData::Utf8(ListArray::from(builder.finish())),
//                                    )
//                                }
//                                _ => panic!("Error reading parquet batch (column {})", i),
//                            }
//                        }
//                        Some(ColumnReader::BoolColumnReader(ref mut r)) => {
//                            let mut builder: BoolArrayBuilder =
//                                Builder::with_capacity(self.batch_size);
//                            match r.read_batch(
//                                self.batch_size,
//                                None,
//                                None,
//                                builder.slice_mut(0, self.batch_size),
//                            ) {
//                                Ok((count, _)) => {
//                                    row_count = count;
//                                    builder.set_len(count);
//                                    Array::from(builder.finish())
//                                }
//                                _ => panic!("Error reading parquet batch (column {})", i),
//                            }
//                        }
//                        Some(ColumnReader::Int32ColumnReader(ref mut r)) => {
//                            let mut builder: Builder<i32> = Builder::with_capacity(self.batch_size);
//                            match r.read_batch(
//                                self.batch_size,
//                                None,
//                                None,
//                                builder.slice_mut(0, self.batch_size),
//                            ) {
//                                Ok((count, _)) => {
//                                    row_count = count;
//                                    builder.set_len(count);
//                                    Array::from(builder.finish())
//                                }
//                                _ => panic!("Error reading parquet batch (column {})", i),
//                            }
//                        }
//                        Some(ColumnReader::Int64ColumnReader(ref mut r)) => {
//                            let mut builder: Builder<i64> = Builder::with_capacity(self.batch_size);
//                            match r.read_batch(
//                                self.batch_size,
//                                None,
//                                None,
//                                builder.slice_mut(0, self.batch_size),
//                            ) {
//                                Ok((count, _)) => {
//                                    row_count = count;
//                                    builder.set_len(count);
//                                    Array::from(builder.finish())
//                                }
//                                _ => panic!("Error reading parquet batch (column {})", i),
//                            }
//                        }
//                        Some(ColumnReader::Int96ColumnReader(ref mut r)) => {
//                            let mut temp: Vec<Int96> = Vec::with_capacity(self.batch_size);
//                            for _ in 0..self.batch_size {
//                                temp.push(Int96::new());
//                            }
//                            // let mut slice: &[Int96] = &temp;
//
//                            match r.read_batch(self.batch_size, None, None, &mut temp) {
//                                Ok((count, _)) => {
//                                    row_count = count;
//                                    //                                        builder.set_len(count);
//
//                                    let mut builder: Builder<
//                                        i64,
//                                    > = Builder::with_capacity(self.batch_size);
//                                    for i in 0..count {
//                                        let v = temp[i].data();
//                                        let value: u128 = (v[0] as u128) << 64
//                                            | (v[1] as u128) << 32
//                                            | (v[2] as u128);
//                                        //println!("value: {}", value);
//
//                                        let ms: i64 = (value / 1000000) as i64;
//                                        builder.push(ms);
//                                    }
//
//                                    Array::from(builder.finish())
//                                }
//                                _ => panic!("Error reading parquet batch (column {})", i),
//                            }
//                        }
//                        Some(ColumnReader::FloatColumnReader(ref mut r)) => {
//                            let mut builder: Builder<f32> = Builder::with_capacity(self.batch_size);
//                            match r.read_batch(
//                                self.batch_size,
//                                None,
//                                None,
//                                builder.slice_mut(0, self.batch_size),
//                            ) {
//                                Ok((count, _)) => {
//                                    row_count = count;
//                                    builder.set_len(count);
//                                    Array::from(builder.finish())
//                                }
//                                _ => panic!("Error reading parquet batch (column {})", i),
//                            }
//                        }
//                        Some(ColumnReader::DoubleColumnReader(ref mut r)) => {
//                            let mut builder: Builder<f64> = Builder::with_capacity(self.batch_size);
//                            match r.read_batch(
//                                self.batch_size,
//                                None,
//                                None,
//                                builder.slice_mut(0, self.batch_size),
//                            ) {
//                                Ok((count, _)) => {
//                                    row_count = count;
//                                    builder.set_len(count);
//                                    Array::from(builder.finish())
//                                }
//                                _ => panic!("Error reading parquet batch (column {})", i),
//                            }
//                        }
//                        Some(ColumnReader::FixedLenByteArrayColumnReader(_)) => unimplemented!(),
//                        None => {
//                            Array::from(vec![0_i32]) //TODO: really want to return scalar null
//                        }
//                    };
//
//                    batch.push(Value::Column(Rc::new(array)));
//                }
//
//                //                println!("Loaded batch of {} rows", row_count);
//
//                if row_count == 0 {
//                    None
//                } else {
//                    Some(Ok(Rc::new(DefaultRecordBatch {
//                        schema: self.schema.clone(),
//                        data: batch,
//                        row_count,
//                    })))
//                }
//            }
//            _ => None,
//        }
        panic!();
    }
}

impl DataSource for ParquetFile {
    fn next(&mut self) -> Result<Option<RecordBatch>> {
        // advance the row group reader if necessary
        if self.current_row_group.is_none() {
            self.load_next_row_group();
            self.load_batch()
        } else {
            match self.load_batch()? {
                Some(b) => Ok(Some(b)),
                None => if self.row_group_index < self.reader.num_row_groups() {
                    self.load_next_row_group();
                    self.load_batch()
                } else {
                    Ok(None)
                },
            }
        }
    }

//    fn schema(&self) -> &Rc<Schema> {
//        &self.schema
//    }
}

fn to_arrow(t: &Type) -> Field {
    match t {
        Type::PrimitiveType {
            basic_info,
            physical_type,
            ..
            //type_length,
            //scale,
            //precision,
        } => {
//                println!("basic_info: {:?}", basic_info);

            let arrow_type = match physical_type {
                basic::Type::BOOLEAN => DataType::Boolean,
                basic::Type::INT32 => DataType::Int32,
                basic::Type::INT64 => DataType::Int64,
                basic::Type::INT96 => DataType::Int64, //TODO ???
                basic::Type::FLOAT => DataType::Float32,
                basic::Type::DOUBLE => DataType::Float64,
                basic::Type::BYTE_ARRAY => match basic_info.logical_type() {
                    basic::LogicalType::UTF8 => DataType::Utf8,
                    _ => unimplemented!("No support for Parquet BYTE_ARRAY yet"),
                }
                basic::Type::FIXED_LEN_BYTE_ARRAY => unimplemented!("No support for Parquet FIXED_LEN_BYTE_ARRAY yet")
            };

            Field::new(basic_info.name(), arrow_type, false)
        }
        Type::GroupType { basic_info, fields } => {
            Field::new(
                basic_info.name(),
                DataType::Struct(
                    fields.iter().map(|f| to_arrow(f)).collect()
                ),
                false)
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use std::cell::RefCell;
    use std::rc::Rc;

    #[test]
    fn test_parquet() {
        let file = File::open("test/data/uk_cities.parquet").unwrap();
        let mut parquet = ParquetFile::open(file, None).unwrap();
        let batch = parquet.next().unwrap().unwrap();
        println!("Schema: {:?}", batch.schema());
        println!("rows: {}; cols: {}", batch.num_rows(), batch.num_columns());
    }

}
