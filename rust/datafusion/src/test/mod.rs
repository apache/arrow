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

//! Common unit test utility methods

use crate::datasource::{MemTable, TableProvider};
use crate::error::Result;
use crate::logical_plan::{LogicalPlan, LogicalPlanBuilder};
use arrow::array::{self, Int32Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use std::fs::File;
use std::io::prelude::*;
use std::io::{BufReader, BufWriter};
use std::sync::Arc;
use tempfile::TempDir;

pub fn create_table_dual() -> Box<dyn TableProvider + Send + Sync> {
    let dual_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        dual_schema.clone(),
        vec![
            Arc::new(array::Int32Array::from(vec![1])),
            Arc::new(array::StringArray::from(vec!["a"])),
        ],
    )
    .unwrap();
    let provider = MemTable::try_new(dual_schema, vec![vec![batch]]).unwrap();
    Box::new(provider)
}

/// Generated partitioned copy of a CSV file
pub fn create_partitioned_csv(filename: &str, partitions: usize) -> Result<String> {
    let testdata = arrow::util::test_util::arrow_test_data();
    let path = format!("{}/csv/{}", testdata, filename);

    let tmp_dir = TempDir::new()?;

    let mut writers = vec![];
    for i in 0..partitions {
        let filename = format!("partition-{}.csv", i);
        let filename = tmp_dir.path().join(&filename);

        let writer = BufWriter::new(File::create(&filename).unwrap());
        writers.push(writer);
    }

    let f = File::open(&path)?;
    let f = BufReader::new(f);
    for (i, line) in f.lines().enumerate() {
        let line = line.unwrap();

        if i == 0 {
            // write header to all partitions
            for w in writers.iter_mut() {
                w.write_all(line.as_bytes()).unwrap();
                w.write_all(b"\n").unwrap();
            }
        } else {
            // write data line to single partition
            let partition = i % partitions;
            writers[partition].write_all(line.as_bytes()).unwrap();
            writers[partition].write_all(b"\n").unwrap();
        }
    }
    for w in writers.iter_mut() {
        w.flush().unwrap();
    }

    Ok(tmp_dir.into_path().to_str().unwrap().to_string())
}

/// Get the schema for the aggregate_test_* csv files
pub fn aggr_test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("c1", DataType::Utf8, false),
        Field::new("c2", DataType::UInt32, false),
        Field::new("c3", DataType::Int8, false),
        Field::new("c4", DataType::Int16, false),
        Field::new("c5", DataType::Int32, false),
        Field::new("c6", DataType::Int64, false),
        Field::new("c7", DataType::UInt8, false),
        Field::new("c8", DataType::UInt16, false),
        Field::new("c9", DataType::UInt32, false),
        Field::new("c10", DataType::UInt64, false),
        Field::new("c11", DataType::Float32, false),
        Field::new("c12", DataType::Float64, false),
        Field::new("c13", DataType::Utf8, false),
    ]))
}

/// Format a batch as csv
pub fn format_batch(batch: &RecordBatch) -> Vec<String> {
    let mut rows = vec![];
    for row_index in 0..batch.num_rows() {
        let mut s = String::new();
        for column_index in 0..batch.num_columns() {
            if column_index > 0 {
                s.push(',');
            }
            let array = batch.column(column_index);

            if array.is_null(row_index) {
                s.push_str("NULL");
                continue;
            }

            match array.data_type() {
                DataType::Utf8 => s.push_str(
                    array
                        .as_any()
                        .downcast_ref::<array::StringArray>()
                        .unwrap()
                        .value(row_index),
                ),
                DataType::Int8 => s.push_str(&format!(
                    "{:?}",
                    array
                        .as_any()
                        .downcast_ref::<array::Int8Array>()
                        .unwrap()
                        .value(row_index)
                )),
                DataType::Int16 => s.push_str(&format!(
                    "{:?}",
                    array
                        .as_any()
                        .downcast_ref::<array::Int16Array>()
                        .unwrap()
                        .value(row_index)
                )),
                DataType::Int32 => s.push_str(&format!(
                    "{:?}",
                    array
                        .as_any()
                        .downcast_ref::<array::Int32Array>()
                        .unwrap()
                        .value(row_index)
                )),
                DataType::Int64 => s.push_str(&format!(
                    "{:?}",
                    array
                        .as_any()
                        .downcast_ref::<array::Int64Array>()
                        .unwrap()
                        .value(row_index)
                )),
                DataType::UInt8 => s.push_str(&format!(
                    "{:?}",
                    array
                        .as_any()
                        .downcast_ref::<array::UInt8Array>()
                        .unwrap()
                        .value(row_index)
                )),
                DataType::UInt16 => s.push_str(&format!(
                    "{:?}",
                    array
                        .as_any()
                        .downcast_ref::<array::UInt16Array>()
                        .unwrap()
                        .value(row_index)
                )),
                DataType::UInt32 => s.push_str(&format!(
                    "{:?}",
                    array
                        .as_any()
                        .downcast_ref::<array::UInt32Array>()
                        .unwrap()
                        .value(row_index)
                )),
                DataType::UInt64 => s.push_str(&format!(
                    "{:?}",
                    array
                        .as_any()
                        .downcast_ref::<array::UInt64Array>()
                        .unwrap()
                        .value(row_index)
                )),
                DataType::Float32 => s.push_str(&format!(
                    "{:?}",
                    array
                        .as_any()
                        .downcast_ref::<array::Float32Array>()
                        .unwrap()
                        .value(row_index)
                )),
                DataType::Float64 => s.push_str(&format!(
                    "{:?}",
                    array
                        .as_any()
                        .downcast_ref::<array::Float64Array>()
                        .unwrap()
                        .value(row_index)
                )),
                _ => s.push('?'),
            }
        }
        rows.push(s);
    }
    rows
}

/// all tests share a common table
pub fn test_table_scan() -> Result<LogicalPlan> {
    let schema = Schema::new(vec![
        Field::new("a", DataType::UInt32, false),
        Field::new("b", DataType::UInt32, false),
        Field::new("c", DataType::UInt32, false),
    ]);
    LogicalPlanBuilder::scan_empty("test", &schema, None)?.build()
}

pub fn assert_fields_eq(plan: &LogicalPlan, expected: Vec<&str>) {
    let actual: Vec<String> = plan
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    assert_eq!(actual, expected);
}

/// returns a table with 3 columns of i32 in memory
pub fn build_table_i32(
    a: (&str, &Vec<i32>),
    b: (&str, &Vec<i32>),
    c: (&str, &Vec<i32>),
) -> RecordBatch {
    let schema = Schema::new(vec![
        Field::new(a.0, DataType::Int32, false),
        Field::new(b.0, DataType::Int32, false),
        Field::new(c.0, DataType::Int32, false),
    ]);

    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(Int32Array::from(a.1.clone())),
            Arc::new(Int32Array::from(b.1.clone())),
            Arc::new(Int32Array::from(c.1.clone())),
        ],
    )
    .unwrap()
}

/// Returns the column names on the schema
pub fn columns(schema: &Schema) -> Vec<String> {
    schema.fields().iter().map(|f| f.name().clone()).collect()
}

pub mod user_defined;
pub mod variable;

mod tests {
    use super::*;

    use arrow::array::{BooleanArray, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    #[test]
    fn test_format_batch() -> Result<()> {
        let array_int32 = Int32Array::from(vec![1000, 2000]);
        let array_string = StringArray::from(vec!["bow \u{1F3F9}", "arrow \u{2191}"]);

        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
        ]);

        let record_batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(array_int32), Arc::new(array_string)],
        )?;

        let result = format_batch(&record_batch);

        assert_eq!(result, vec!["1000,bow \u{1F3F9}", "2000,arrow \u{2191}"]);

        Ok(())
    }

    #[test]
    fn test_format_batch_unknown() -> Result<()> {
        // Use any Array type not yet handled by format_batch().
        let array_bool = BooleanArray::from(vec![false, true]);

        let schema = Schema::new(vec![Field::new("a", DataType::Boolean, false)]);

        let record_batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array_bool)])?;

        let result = format_batch(&record_batch);

        assert_eq!(result, vec!["?", "?"]);

        Ok(())
    }
}
