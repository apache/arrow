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

fn primitive_array_to_json<T: ArrowPrimitiveType>(array: &ArrayRef) -> Vec<Value> {
    as_primitive_array::<T>(array)
        .iter()
        .map(|maybe_value| match maybe_value {
            Some(v) => v.into_json_value().unwrap_or(Value::Null),
            None => Value::Null,
        })
        .collect()
}

pub fn array_to_json_array(array: &ArrayRef) -> Vec<Value> {
    match array.data_type() {
        DataType::Null => iter::repeat(Value::Null).take(array.len()).collect(),
        DataType::Boolean => as_boolean_array(array)
            .iter()
            .map(|maybe_value| match maybe_value {
                Some(v) => v.into(),
                None => Value::Null,
            })
            .collect(),

        DataType::Utf8 => as_string_array(array)
            .iter()
            .map(|maybe_value| match maybe_value {
                Some(v) => v.into(),
                None => Value::Null,
            })
            .collect(),
        DataType::Int8 => primitive_array_to_json::<Int8Type>(array),
        DataType::Int16 => primitive_array_to_json::<Int16Type>(array),
        DataType::Int32 => primitive_array_to_json::<Int32Type>(array),
        DataType::Int64 => primitive_array_to_json::<Int64Type>(array),
        DataType::UInt8 => primitive_array_to_json::<UInt8Type>(array),
        DataType::UInt16 => primitive_array_to_json::<UInt16Type>(array),
        DataType::UInt32 => primitive_array_to_json::<UInt32Type>(array),
        DataType::UInt64 => primitive_array_to_json::<UInt64Type>(array),
        DataType::Float32 => primitive_array_to_json::<Float32Type>(array),
        DataType::Float64 => primitive_array_to_json::<Float64Type>(array),
        _ => {
            panic!(format!(
                "Unsupported datatype for array conversion: {:#?}",
                array.data_type()
            ));
        }
    }
}

macro_rules! set_column_by_array_type {
    ($cast_fn:ident, $col_name:ident, $rows:ident, $array:ident, $row_count:ident) => {
        let arr = $cast_fn($array);
        $rows.iter_mut().zip(arr.iter()).take($row_count).for_each(
            |(row, maybe_value)| {
                if let Some(v) = maybe_value {
                    row.insert($col_name.to_string(), v.into());
                }
            },
        );
    };
}

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
            // when value is null, we simply skip setting the key
            if let Some(j) = maybe_value.and_then(|v| v.into_json_value()) {
                row.insert(col_name.to_string(), j);
            }
        });
}

fn set_column_for_json_rows(
    rows: &mut [JsonMap<String, Value>],
    row_count: usize,
    array: &ArrayRef,
    col_name: &str,
) {
    match array.data_type() {
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
        DataType::Null => {
            // when value is null, we simply skip setting the key
        }
        DataType::Boolean => {
            set_column_by_array_type!(as_boolean_array, col_name, rows, array, row_count);
        }
        DataType::Utf8 => {
            set_column_by_array_type!(as_string_array, col_name, rows, array, row_count);
        }
        DataType::Struct(_) => {
            let structarr = as_struct_array(array);
            let inner_col_names = structarr.column_names();

            let mut inner_objs = iter::repeat(JsonMap::new())
                .take(row_count)
                .collect::<Vec<JsonMap<String, Value>>>();

            structarr
                .columns()
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
        DataType::List(_) => {
            let listarr = as_list_array(array);
            rows.iter_mut()
                .zip(listarr.iter())
                .take(row_count)
                .for_each(|(row, maybe_value)| {
                    if let Some(v) = maybe_value {
                        row.insert(
                            col_name.to_string(),
                            Value::Array(array_to_json_array(&v)),
                        );
                    }
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

    use crate::buffer::*;
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
{"c2":"d"}
{"c1":5}
"#
        );
    }

    #[test]
    fn write_nested_structs() {
        let schema = Schema::new(vec![
            Field::new(
                "c1",
                DataType::Struct(vec![
                    Field::new("c11", DataType::Int32, false),
                    Field::new(
                        "c12",
                        DataType::Struct(vec![Field::new("c121", DataType::Utf8, false)]),
                        false,
                    ),
                ]),
                false,
            ),
            Field::new("c2", DataType::Utf8, false),
        ]);

        let a = StructArray::from(vec![
            (
                Field::new("c11", DataType::Int32, false),
                Arc::new(Int32Array::from(vec![Some(1), None, Some(5)])) as ArrayRef,
            ),
            (
                Field::new(
                    "c12",
                    DataType::Struct(vec![Field::new("c121", DataType::Utf8, false)]),
                    false,
                ),
                Arc::new(StructArray::from(vec![(
                    Field::new("c121", DataType::Utf8, false),
                    Arc::new(StringArray::from(vec![Some("e"), Some("f"), Some("g")]))
                        as ArrayRef,
                )])) as ArrayRef,
            ),
        ]);
        let b = StringArray::from(vec![Some("a"), Some("b"), Some("c")]);

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
            r#"{"c1":{"c11":1,"c12":{"c121":"e"}},"c2":"a"}
{"c1":{"c12":{"c121":"f"}},"c2":"b"}
{"c1":{"c11":5,"c12":{"c121":"g"}},"c2":"c"}
"#
        );
    }

    #[test]
    fn write_struct_with_list_field() {
        let schema = Schema::new(vec![
            Field::new(
                "c_struct",
                DataType::List(Box::new(Field::new("c_list", DataType::Utf8, false))),
                false,
            ),
            Field::new("c2", DataType::Int32, false),
        ]);

        let a_values = StringArray::from(vec!["a", "a1", "b", "c", "d", "e"]);
        //  [["a", "a1"], ["b"], ["c"], ["d"], ["e"]]
        let a_value_offsets = Buffer::from(&[0, 2, 3, 4, 5, 6].to_byte_slice());
        let a_list_data = ArrayData::builder(DataType::List(Box::new(Field::new(
            "c_list",
            DataType::Utf8,
            false,
        ))))
        .len(5)
        .add_buffer(a_value_offsets)
        .add_child_data(a_values.data())
        .null_bit_buffer(Buffer::from(vec![0b00011111]))
        .build();
        let a = ListArray::from(a_list_data);

        let b = Int32Array::from(vec![1, 2, 3, 4, 5]);

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
            r#"{"c_struct":["a","a1"],"c2":1}
{"c_struct":["b"],"c2":2}
{"c_struct":["c"],"c2":3}
{"c_struct":["d"],"c2":4}
{"c_struct":["e"],"c2":5}
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
            let mut expected_json = serde_json::from_str::<Value>(e).unwrap();
            // remove null value from object to make comparision consistent:
            if let Value::Object(obj) = expected_json {
                expected_json = Value::Object(
                    obj.into_iter().filter(|(_, v)| *v != Value::Null).collect(),
                );
            }
            assert_eq!(serde_json::from_str::<Value>(r).unwrap(), expected_json,);
        }
    }

    #[test]
    fn write_basic_rows() {
        test_write_for_file("test/data/basic.json");
    }

    #[test]
    fn write_arrays() {
        test_write_for_file("test/data/arrays.json");
    }

    #[test]
    fn write_basic_nulls() {
        test_write_for_file("test/data/basic_nulls.json");
    }
}
