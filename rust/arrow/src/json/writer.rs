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

//! JSON Writer
//!
//! This JSON writer allows converting Arrow record batches into array of JSON objects. It also
//! provides a Writer struct to help serialize record batches directly into line-delimited JSON
//! objects as bytes.
//!
//! Serialize record batches into array of JSON objects:
//!
//! ```
//! use std::sync::Arc;
//!
//! use arrow::array::Int32Array;
//! use arrow::datatypes::{DataType, Field, Schema};
//! use arrow::json;
//! use arrow::record_batch::RecordBatch;
//!
//! let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
//! let a = Int32Array::from(vec![1, 2, 3]);
//! let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)]).unwrap();
//!
//! let json_rows = json::writer::record_batches_to_json_rows(&[batch]);
//! assert_eq!(
//!     serde_json::Value::Object(json_rows[1].clone()),
//!     serde_json::json!({"a": 2}),
//! );
//! ```
//!
//! Serialize record batches into line-delimited JSON bytes:
//!
//! ```
//! use std::sync::Arc;
//!
//! use arrow::array::Int32Array;
//! use arrow::datatypes::{DataType, Field, Schema};
//! use arrow::json;
//! use arrow::record_batch::RecordBatch;
//!
//! let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
//! let a = Int32Array::from(vec![1, 2, 3]);
//! let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)]).unwrap();
//!
//! let buf = Vec::new();
//! let mut writer = json::Writer::new(buf);
//! writer.write_batches(&vec![batch]).unwrap();
//! ```

use std::io::{BufWriter, Write};
use std::iter;

use serde_json::map::Map as JsonMap;
use serde_json::Value;

use crate::array::*;
use crate::datatypes::*;
use crate::error::Result;
use crate::record_batch::RecordBatch;

fn set_column_by_primitive_type<T: ArrowPrimitiveType>(
    rows: &mut [JsonMap<String, Value>],
    row_count: usize,
    array: &ArrayRef,
    col_name: &str,
) {
    let primitive_arr = as_primitive_array::<T>(array);

    rows.iter_mut()
        .zip(primitive_arr.iter())
        .take(row_count)
        .for_each(|(row, maybe_value)| {
            row.insert(
                col_name.to_string(),
                match maybe_value {
                    Some(v) => v.into_json_value().unwrap_or(Value::Null),
                    None => Value::Null,
                },
            );
        });
}

fn set_column_for_json_rows(
    rows: &mut [JsonMap<String, Value>],
    row_count: usize,
    array: &ArrayRef,
    col_name: &str,
) {
    match array.data_type() {
        DataType::Null => {
            for row in rows.iter_mut().take(row_count) {
                row.insert(col_name.to_string(), Value::Null);
            }
        }
        DataType::Boolean => {
            let arr = as_boolean_array(array);
            rows.iter_mut().zip(arr.iter()).take(row_count).for_each(
                |(row, maybe_value)| {
                    row.insert(
                        col_name.to_string(),
                        match maybe_value {
                            Some(v) => v.into(),
                            None => Value::Null,
                        },
                    );
                },
            );
        }
        DataType::Int8 => {
            set_column_by_primitive_type::<Int8Type>(rows, row_count, array, col_name)
        }
        DataType::Int16 => {
            set_column_by_primitive_type::<Int16Type>(rows, row_count, array, col_name)
        }
        DataType::Int32 => {
            set_column_by_primitive_type::<Int32Type>(rows, row_count, array, col_name)
        }
        DataType::Int64 => {
            set_column_by_primitive_type::<Int64Type>(rows, row_count, array, col_name)
        }
        DataType::UInt8 => {
            set_column_by_primitive_type::<UInt8Type>(rows, row_count, array, col_name)
        }
        DataType::UInt16 => {
            set_column_by_primitive_type::<UInt16Type>(rows, row_count, array, col_name)
        }
        DataType::UInt32 => {
            set_column_by_primitive_type::<UInt32Type>(rows, row_count, array, col_name)
        }
        DataType::UInt64 => {
            set_column_by_primitive_type::<UInt64Type>(rows, row_count, array, col_name)
        }
        DataType::Float32 => {
            set_column_by_primitive_type::<Float32Type>(rows, row_count, array, col_name)
        }
        DataType::Float64 => {
            set_column_by_primitive_type::<Float64Type>(rows, row_count, array, col_name)
        }
        DataType::Utf8 => {
            let strarr = as_string_array(array);
            rows.iter_mut().zip(strarr.iter()).take(row_count).for_each(
                |(row, maybe_value)| {
                    row.insert(
                        col_name.to_string(),
                        match maybe_value {
                            Some(v) => v.into(),
                            None => Value::Null,
                        },
                    );
                },
            );
        }
        DataType::Struct(_) => {
            let arr = as_struct_array(array);
            let inner_col_names = arr.column_names();

            let mut inner_objs = iter::repeat(JsonMap::new())
                .take(row_count)
                .collect::<Vec<JsonMap<String, Value>>>();

            arr.columns()
                .iter()
                .enumerate()
                .for_each(|(j, struct_col)| {
                    set_column_for_json_rows(
                        &mut inner_objs,
                        row_count,
                        struct_col,
                        inner_col_names[j],
                    );
                });

            rows.iter_mut()
                .take(row_count)
                .zip(inner_objs.into_iter())
                .for_each(|(row, obj)| {
                    row.insert(col_name.to_string(), Value::Object(obj));
                });
        }
        _ => {
            panic!(format!("Unsupported datatype: {:#?}", array.data_type()));
        }
    }
}

pub fn record_batches_to_json_rows(
    batches: &[RecordBatch],
) -> Vec<JsonMap<String, Value>> {
    let mut rows: Vec<JsonMap<String, Value>> = iter::repeat(JsonMap::new())
        .take(batches.iter().map(|b| b.num_rows()).sum())
        .collect();

    if !rows.is_empty() {
        let schema = batches[0].schema();
        let mut base = 0;
        batches.iter().for_each(|batch| {
            let row_count = batch.num_rows();
            batch.columns().iter().enumerate().for_each(|(j, col)| {
                let col_name = schema.field(j).name();
                set_column_for_json_rows(&mut rows[base..], row_count, col, col_name);
            });
            base += row_count;
        });
    }

    rows
}

/// A JSON writer
#[derive(Debug)]
pub struct Writer<W: Write> {
    writer: BufWriter<W>,
}

impl<W: Write> Writer<W> {
    pub fn new(writer: W) -> Self {
        Self::from_buf_writer(BufWriter::new(writer))
    }

    pub fn from_buf_writer(writer: BufWriter<W>) -> Self {
        Self { writer }
    }

    pub fn write_row(&mut self, row: &Value) -> Result<()> {
        self.writer.write_all(&serde_json::to_vec(row)?)?;
        self.writer.write_all(b"\n")?;
        Ok(())
    }

    pub fn write_batches(&mut self, batches: &[RecordBatch]) -> Result<()> {
        for row in record_batches_to_json_rows(batches) {
            self.write_row(&Value::Object(row))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::fs::{read_to_string, File};
    use std::sync::Arc;

    use crate::json::reader::*;

    use super::*;

    #[test]
    fn write_simple_rows() {
        let schema = Schema::new(vec![
            Field::new("c1", DataType::Int32, false),
            Field::new("c2", DataType::Utf8, false),
        ]);

        let a = Int32Array::from(vec![Some(1), Some(2), Some(3), None, Some(5)]);
        let b = StringArray::from(vec![Some("a"), Some("b"), Some("c"), Some("d"), None]);

        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a), Arc::new(b)])
                .unwrap();

        let mut buf = Vec::new();
        {
            let mut writer = Writer::new(&mut buf);
            writer.write_batches(&vec![batch]).unwrap();
        }

        assert_eq!(
            String::from_utf8(buf).unwrap(),
            r#"{"c1":1,"c2":"a"}
{"c1":2,"c2":"b"}
{"c1":3,"c2":"c"}
{"c1":null,"c2":"d"}
{"c1":5,"c2":null}
"#
        );
    }

    fn test_write_for_file(test_file: &str) {
        let builder = ReaderBuilder::new()
            .infer_schema(None)
            .with_batch_size(1024);
        let mut reader: Reader<File> = builder
            .build::<File>(File::open(test_file).unwrap())
            .unwrap();
        let batch = reader.next().unwrap().unwrap();

        let mut buf = Vec::new();
        {
            let mut writer = Writer::new(&mut buf);
            writer.write_batches(&vec![batch]).unwrap();
        }

        let result = String::from_utf8(buf).unwrap();
        let expected = read_to_string(test_file).unwrap();
        for (r, e) in result.lines().zip(expected.lines()) {
            assert_eq!(
                serde_json::from_str::<Value>(r).unwrap(),
                serde_json::from_str::<Value>(e).unwrap()
            );
        }
    }

    #[test]
    fn write_basic_rows() {
        test_write_for_file("test/data/basic.json");
    }
}
