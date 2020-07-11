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

//! Defines common code used in execution plans

use std::fs;
use std::fs::metadata;
use std::sync::{Arc, Mutex};

use crate::error::{ExecutionError, Result};

use crate::logicalplan::ScalarValue;
use arrow::array::{self, ArrayRef};
use arrow::datatypes::{DataType, SchemaRef};
use arrow::error::Result as ArrowResult;
use arrow::record_batch::{RecordBatch, RecordBatchReader};

/// Iterator over a vector of record batches
pub struct RecordBatchIterator {
    schema: SchemaRef,
    batches: Vec<Arc<RecordBatch>>,
    index: usize,
}

impl RecordBatchIterator {
    /// Create a new RecordBatchIterator
    pub fn new(schema: SchemaRef, batches: Vec<Arc<RecordBatch>>) -> Self {
        RecordBatchIterator {
            schema,
            index: 0,
            batches,
        }
    }
}

impl RecordBatchReader for RecordBatchIterator {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn next_batch(&mut self) -> ArrowResult<Option<RecordBatch>> {
        if self.index < self.batches.len() {
            self.index += 1;
            Ok(Some(self.batches[self.index - 1].as_ref().clone()))
        } else {
            Ok(None)
        }
    }
}

/// Create a vector of record batches from an iterator
pub fn collect(
    it: Arc<Mutex<dyn RecordBatchReader + Send + Sync>>,
) -> Result<Vec<RecordBatch>> {
    let mut reader = it.lock().unwrap();
    let mut results: Vec<RecordBatch> = vec![];
    loop {
        match reader.next_batch() {
            Ok(Some(batch)) => {
                results.push(batch);
            }
            Ok(None) => {
                // end of result set
                return Ok(results);
            }
            Err(e) => return Err(ExecutionError::from(e)),
        }
    }
}

/// Recursively build a list of files in a directory with a given extension
pub fn build_file_list(dir: &str, filenames: &mut Vec<String>, ext: &str) -> Result<()> {
    let metadata = metadata(dir)?;
    if metadata.is_file() {
        if dir.ends_with(ext) {
            filenames.push(dir.to_string());
        }
    } else {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if let Some(path_name) = path.to_str() {
                if path.is_dir() {
                    build_file_list(path_name, filenames, ext)?;
                } else {
                    if path_name.ends_with(ext) {
                        filenames.push(path_name.to_string());
                    }
                }
            } else {
                return Err(ExecutionError::General("Invalid path".to_string()));
            }
        }
    }
    Ok(())
}

/// Get a value from an array as a ScalarValue
pub fn get_scalar_value(array: &ArrayRef, row: usize) -> Result<Option<ScalarValue>> {
    if array.is_null(row) {
        return Ok(None);
    }
    let value: Option<ScalarValue> = match array.data_type() {
        DataType::UInt8 => {
            let array = array
                .as_any()
                .downcast_ref::<array::UInt8Array>()
                .expect("Failed to cast array");
            Some(ScalarValue::UInt8(array.value(row)))
        }
        DataType::UInt16 => {
            let array = array
                .as_any()
                .downcast_ref::<array::UInt16Array>()
                .expect("Failed to cast array");
            Some(ScalarValue::UInt16(array.value(row)))
        }
        DataType::UInt32 => {
            let array = array
                .as_any()
                .downcast_ref::<array::UInt32Array>()
                .expect("Failed to cast array");
            Some(ScalarValue::UInt32(array.value(row)))
        }
        DataType::UInt64 => {
            let array = array
                .as_any()
                .downcast_ref::<array::UInt64Array>()
                .expect("Failed to cast array");
            Some(ScalarValue::UInt64(array.value(row)))
        }
        DataType::Int8 => {
            let array = array
                .as_any()
                .downcast_ref::<array::Int8Array>()
                .expect("Failed to cast array");
            Some(ScalarValue::Int8(array.value(row)))
        }
        DataType::Int16 => {
            let array = array
                .as_any()
                .downcast_ref::<array::Int16Array>()
                .expect("Failed to cast array");
            Some(ScalarValue::Int16(array.value(row)))
        }
        DataType::Int32 => {
            let array = array
                .as_any()
                .downcast_ref::<array::Int32Array>()
                .expect("Failed to cast array");
            Some(ScalarValue::Int32(array.value(row)))
        }
        DataType::Int64 => {
            let array = array
                .as_any()
                .downcast_ref::<array::Int64Array>()
                .expect("Failed to cast array");
            Some(ScalarValue::Int64(array.value(row)))
        }
        DataType::Float32 => {
            let array = array
                .as_any()
                .downcast_ref::<array::Float32Array>()
                .unwrap();
            Some(ScalarValue::Float32(array.value(row)))
        }
        DataType::Float64 => {
            let array = array
                .as_any()
                .downcast_ref::<array::Float64Array>()
                .unwrap();
            Some(ScalarValue::Float64(array.value(row)))
        }
        other => {
            return Err(ExecutionError::ExecutionError(format!(
                "Unsupported data type {:?} for result of aggregate expression",
                other
            )));
        }
    };
    Ok(value)
}
