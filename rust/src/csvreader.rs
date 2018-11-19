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

//! CSV Support

use std::fs::File;
use std::io::{BufReader};
use std::sync::Arc;

use array::ArrayRef;
use builder::{ArrayBuilder, PrimitiveArrayBuilder, ListArrayBuilder};
use datatypes::{Schema, DataType};
use error::ArrowError;
use record_batch::RecordBatch;

use csv;
use csv::{StringRecord, StringRecordsIntoIter};

pub struct CsvFile {
    schema: Arc<Schema>,
    projection: Option<Vec<usize>>,
    record_iter: StringRecordsIntoIter<BufReader<File>>,
    batch_size: usize,
}

impl CsvFile {

    pub fn open(
        file: File,
        schema: Arc<Schema>,
        has_headers: bool,
        batch_size: usize,
        projection: Option<Vec<usize>>,
    ) -> Self {
        let csv_reader = csv::ReaderBuilder::new()
            .has_headers(has_headers)
            .from_reader(BufReader::new(file));

        let record_iter = csv_reader.into_records();
        CsvFile {
            schema: schema.clone(),
            projection,
            record_iter,
            batch_size,
        }
    }

}

macro_rules! build_primitive_array {
    ($ROWS:expr, $COL_INDEX:expr, $TY:ty) => {{

        let mut builder = PrimitiveArrayBuilder::<$TY>::new($ROWS.len() as i64);
        for row_index in 0..$ROWS.len() {
            match $ROWS[row_index].get(*$COL_INDEX) {
                Some(s) if s.len() > 0 => builder.push(s.parse::<$TY> ().unwrap()).unwrap(),
                _ => builder.push_null().unwrap()
            }
        }
        Ok(Arc::new(builder.finish()) as ArrayRef)
    }};
}

impl CsvFile {

    /// Read the next batch of rows
    pub fn next(&mut self) -> Option<Result<Arc<RecordBatch>, ArrowError>> {
        // read a batch of rows into memory
        let mut rows: Vec<StringRecord> = Vec::with_capacity(self.batch_size);
        for _ in 0..self.batch_size {
            match self.record_iter.next() {
                Some(Ok(r)) => {
                    rows.push(r);
                }
                Some(Err(_)) => {
                    return Some(Err(ArrowError::ParseError("Error reading CSV file".to_string())));
                }
                None => break,
            }
        }

        // return early if no data was loaded
        if rows.len() == 0 {
            return None;
        }

        let projection: Vec<usize> = match self.projection {
            Some(ref v) => v.clone(),
            None => self
                .schema
                .fields()
                .iter()
                .enumerate()
                .map(|(i, _)| i)
                .collect(),
        };

        let arrays: Result<Vec<ArrayRef>, ArrowError> = projection.iter()
            .map(|i| {

                let field = self.schema.field(*i);

                match field.data_type() {
                    &DataType::Boolean => build_primitive_array!(rows, i, bool),
                    &DataType::Int8 => build_primitive_array!(rows, i, i8),
                    &DataType::Int16 => build_primitive_array!(rows, i, i16),
                    &DataType::Int32 => build_primitive_array!(rows, i, i32),
                    &DataType::Int64 => build_primitive_array!(rows, i, i64),
                    &DataType::UInt8 => build_primitive_array!(rows, i, u8),
                    &DataType::UInt16 => build_primitive_array!(rows, i, u16),
                    &DataType::UInt32 => build_primitive_array!(rows, i, u32),
                    &DataType::UInt64 => build_primitive_array!(rows, i, u64),
                    &DataType::Float16 => build_primitive_array!(rows, i, f32),
                    &DataType::Float32 => build_primitive_array!(rows, i, f32),
                    &DataType::Float64 => build_primitive_array!(rows, i, f64),
                    &DataType::Utf8 => {
                        let mut values_builder: PrimitiveArrayBuilder<u8> = PrimitiveArrayBuilder::<u8>::new(rows.len() as i64);
                        let mut list_builder = ListArrayBuilder::new(values_builder);
                        for row_index in 0..rows.len() {
                            match rows[row_index].get(*i) {
                                Some(s) => {
                                    list_builder.values().push_slice(s.as_bytes()).unwrap();
                                    list_builder.append(true).unwrap();
                                }
                                _ => {
                                    list_builder.append(false).unwrap();
                                }
                            }
                        }
                        Ok(Arc::new(list_builder.finish()) as ArrayRef)
                    },
                    _ => Err(ArrowError::ParseError("Unsupported data type".to_string()))
                }
            }).collect();

        match arrays {
            Ok(arr) => Some(Ok(Arc::new(RecordBatch::new(
                self.schema.clone(),
                arr
            )))),
            Err(e) => Some(Err(e))
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use array::{ListArray, PrimitiveArray};
    use datatypes::Field;

    #[test]
    fn test_csv() {
        let schema = Schema::new(vec![
            Field::new("city", DataType::Utf8, false),
            Field::new("lat", DataType::Float64, false),
            Field::new("lng", DataType::Float64, false),
        ]);

        let file = File::open("test/data/uk_cities.csv").unwrap();

        let mut csv = CsvFile::open(file, Arc::new(schema), false, 1024,None);
        let batch = csv.next().unwrap().unwrap();
        assert_eq!(37, batch.num_rows());
        assert_eq!(3, batch.num_columns());

        // access data from a primitive array
        let lat = batch.column(1).as_any().downcast_ref::<PrimitiveArray<f64>>().unwrap();
        assert_eq!(57.653484, lat.value(0));

        // access data from a string array (ListArray<u8>)
        let city = batch.column(0).as_any().downcast_ref::<ListArray>().unwrap();
        let city_values = city.values();
        let buffer: &PrimitiveArray<u8> = city_values.as_any().downcast_ref::<PrimitiveArray<u8>>().unwrap();

        let i = 13;
        let offset = city.value_offset(i) as i64;
        let len = city.value_length(i) as i64;
        let city_name: String = String::from_utf8(buffer.value_slice(offset, len).to_vec()).unwrap();

        assert_eq!("Aberdeen, Aberdeen City, UK", city_name);

    }

    #[test]
    fn test_csv_with_projection() {
        let schema = Schema::new(vec![
            Field::new("city", DataType::Utf8, false),
            Field::new("lat", DataType::Float64, false),
            Field::new("lng", DataType::Float64, false),
        ]);

        let file = File::open("test/data/uk_cities.csv").unwrap();

        let mut csv = CsvFile::open(file, Arc::new(schema), false, 1024,Some(vec![0,1]));
        let batch = csv.next().unwrap().unwrap();
        assert_eq!(37, batch.num_rows());
        assert_eq!(2, batch.num_columns());
    }

    #[test]
    fn test_nulls() {
        let schema = Schema::new(vec![
            Field::new("c_int", DataType::UInt64, false),
            Field::new("c_float", DataType::Float32, false),
            Field::new("c_string", DataType::Utf8, false),
        ]);

        let file = File::open("test/data/null_test.csv").unwrap();

        let mut csv = CsvFile::open(file, Arc::new(schema), true, 1024, None);
        let batch = csv.next().unwrap().unwrap();

        assert_eq!(false, batch.column(1).is_null(0));
        assert_eq!(false, batch.column(1).is_null(1));
        assert_eq!(true, batch.column(1).is_null(2));
        assert_eq!(false, batch.column(1).is_null(3));
        assert_eq!(false, batch.column(1).is_null(4));
    }

}
