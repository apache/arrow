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

//! According to the [Arrow Metadata Specification](https://arrow.apache.org/docs/metadata.html):
//!
//! > A record batch is a collection of top-level named, equal length Arrow arrays
//! > (or vectors). If one of the arrays contains nested data, its child arrays are not
//! > required to be the same length as the top-level arrays.

use std::sync::Arc;

use crate::array::*;
use crate::datatypes::*;
use crate::error::{ArrowError, Result};

/// A batch of column-oriented data
#[derive(Clone)]
pub struct RecordBatch {
    schema: Arc<Schema>,
    columns: Vec<Arc<Array>>,
}

impl RecordBatch {
    /// Creates a `RecordBatch` from a schema and columns
    ///
    /// Expects the following:
    ///  * the vec of columns to not be empty
    ///  * the schema and column data types to have equal lengths and match
    ///  * each array in columns to have the same length
    pub fn try_new(schema: Arc<Schema>, columns: Vec<ArrayRef>) -> Result<Self> {
        // check that there are some columns
        if columns.is_empty() {
            return Err(ArrowError::InvalidArgumentError(
                "at least one column must be defined to create a record batch"
                    .to_string(),
            ));
        }
        // check that number of fields in schema match column length
        if schema.fields().len() != columns.len() {
            return Err(ArrowError::InvalidArgumentError(
                "number of columns must match number of fields in schema".to_string(),
            ));
        }
        // check that all columns have the same row count, and match the schema
        let len = columns[0].data().len();
        for i in 0..columns.len() {
            if columns[i].len() != len {
                return Err(ArrowError::InvalidArgumentError(
                    "all columns in a record batch must have the same length".to_string(),
                ));
            }
            if columns[i].data_type() != schema.field(i).data_type() {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "column types must match schema types, expected {:?} but found {:?} at column index {}", 
                    schema.field(i).data_type(),
                    columns[i].data_type(),
                    i)));
            }
        }
        Ok(RecordBatch { schema, columns })
    }

    /// Returns the schema of the record batch
    pub fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    /// Number of columns in the record batch
    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    /// Number of rows in each column
    pub fn num_rows(&self) -> usize {
        self.columns[0].data().len()
    }

    /// Get a reference to a column's array by index
    pub fn column(&self, i: usize) -> &ArrayRef {
        &self.columns[i]
    }
}

unsafe impl Send for RecordBatch {}
unsafe impl Sync for RecordBatch {}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::array_data::*;
    use crate::buffer::*;

    #[test]
    fn create_record_batch() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
        ]);

        let v = vec![1, 2, 3, 4, 5];
        let array_data = ArrayData::builder(DataType::Int32)
            .len(5)
            .add_buffer(Buffer::from(v.to_byte_slice()))
            .build();
        let a = Int32Array::from(array_data);

        let v = vec![b'a', b'b', b'c', b'd', b'e'];
        let offset_data = vec![0, 1, 2, 3, 4, 5, 6];
        let array_data = ArrayData::builder(DataType::Utf8)
            .len(5)
            .add_buffer(Buffer::from(offset_data.to_byte_slice()))
            .add_buffer(Buffer::from(v.to_byte_slice()))
            .build();
        let b = BinaryArray::from(array_data);

        let record_batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a), Arc::new(b)])
                .unwrap();

        assert_eq!(5, record_batch.num_rows());
        assert_eq!(2, record_batch.num_columns());
        assert_eq!(&DataType::Int32, record_batch.schema().field(0).data_type());
        assert_eq!(&DataType::Utf8, record_batch.schema().field(1).data_type());
        assert_eq!(5, record_batch.column(0).data().len());
        assert_eq!(5, record_batch.column(1).data().len());
    }

    #[test]
    fn create_record_batch_schema_mismatch() {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

        let a = Int64Array::from(vec![1, 2, 3, 4, 5]);

        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)]);
        assert!(!batch.is_ok());
    }

    #[test]
    fn create_record_batch_record_mismatch() {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

        let a = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let b = Int32Array::from(vec![1, 2, 3, 4, 5]);

        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a), Arc::new(b)]);
        assert!(!batch.is_ok());
    }
}
