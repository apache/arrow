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
use std::sync::Arc;
use std::task::{Context, Poll};

use super::{RecordBatchStream, SendableRecordBatchStream};
use crate::error::{DataFusionError, Result};

use array::{
    BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    Int8Array, LargeStringArray, StringArray, UInt16Array, UInt32Array, UInt64Array,
    UInt8Array,
};
use arrow::datatypes::{DataType, SchemaRef};
use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;
use arrow::{
    array::{self, ArrayRef},
    datatypes::Schema,
};
use futures::{Stream, TryStreamExt};

/// Stream of record batches
pub struct SizedRecordBatchStream {
    schema: SchemaRef,
    batches: Vec<Arc<RecordBatch>>,
    index: usize,
}

impl SizedRecordBatchStream {
    /// Create a new RecordBatchIterator
    pub fn new(schema: SchemaRef, batches: Vec<Arc<RecordBatch>>) -> Self {
        SizedRecordBatchStream {
            schema,
            index: 0,
            batches,
        }
    }
}

impl Stream for SizedRecordBatchStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        Poll::Ready(if self.index < self.batches.len() {
            self.index += 1;
            Some(Ok(self.batches[self.index - 1].as_ref().clone()))
        } else {
            None
        })
    }
}

impl RecordBatchStream for SizedRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Create a vector of record batches from a stream
pub async fn collect(stream: SendableRecordBatchStream) -> Result<Vec<RecordBatch>> {
    stream
        .try_collect::<Vec<_>>()
        .await
        .map_err(DataFusionError::from)
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
                } else if path_name.ends_with(ext) {
                    filenames.push(path_name.to_string());
                }
            } else {
                return Err(DataFusionError::Plan("Invalid path".to_string()));
            }
        }
    }
    Ok(())
}

/// creates an empty record batch.
pub fn create_batch_empty(schema: &Schema) -> ArrowResult<RecordBatch> {
    let columns = schema
        .fields()
        .iter()
        .map(|f| match f.data_type() {
            DataType::Float32 => {
                Ok(Arc::new(Float32Array::from(vec![] as Vec<f32>)) as ArrayRef)
            }
            DataType::Float64 => {
                Ok(Arc::new(Float64Array::from(vec![] as Vec<f64>)) as ArrayRef)
            }
            DataType::Int64 => {
                Ok(Arc::new(Int64Array::from(vec![] as Vec<i64>)) as ArrayRef)
            }
            DataType::Int32 => {
                Ok(Arc::new(Int32Array::from(vec![] as Vec<i32>)) as ArrayRef)
            }
            DataType::Int16 => {
                Ok(Arc::new(Int16Array::from(vec![] as Vec<i16>)) as ArrayRef)
            }
            DataType::Int8 => {
                Ok(Arc::new(Int8Array::from(vec![] as Vec<i8>)) as ArrayRef)
            }
            DataType::UInt64 => {
                Ok(Arc::new(UInt64Array::from(vec![] as Vec<u64>)) as ArrayRef)
            }
            DataType::UInt32 => {
                Ok(Arc::new(UInt32Array::from(vec![] as Vec<u32>)) as ArrayRef)
            }
            DataType::UInt16 => {
                Ok(Arc::new(UInt16Array::from(vec![] as Vec<u16>)) as ArrayRef)
            }
            DataType::UInt8 => {
                Ok(Arc::new(UInt8Array::from(vec![] as Vec<u8>)) as ArrayRef)
            }
            DataType::Utf8 => {
                Ok(Arc::new(StringArray::from(vec![] as Vec<&str>)) as ArrayRef)
            }
            DataType::LargeUtf8 => {
                Ok(Arc::new(LargeStringArray::from(vec![] as Vec<&str>)) as ArrayRef)
            }
            DataType::Boolean => {
                Ok(Arc::new(BooleanArray::from(vec![] as Vec<bool>)) as ArrayRef)
            }
            _ => Err(DataFusionError::NotImplemented(format!(
                "Cannot convert datatype {:?} to array",
                f.data_type()
            ))),
        })
        .collect::<Result<_>>()
        .map_err(DataFusionError::into_arrow_external_error)?;
    RecordBatch::try_new(Arc::new(schema.to_owned()), columns)
}
