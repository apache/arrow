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

use crate::array::*;
use crate::datatypes::*;
use std::sync::Arc;

/// A batch of column-oriented data
pub struct RecordBatch {
    schema: Arc<Schema>,
    columns: Vec<Arc<Array>>,
}

impl RecordBatch {
    pub fn new(schema: Arc<Schema>, columns: Vec<ArrayRef>) -> Self {
        // assert that there are some columns
        assert!(
            columns.len() > 0,
            "at least one column must be defined to create a record batch"
        );
        // assert that all columns have the same row count
        let len = columns[0].data().len();
        for i in 1..columns.len() {
            assert_eq!(
                len,
                columns[i].len(),
                "all columns in a record batch must have the same length"
            );
        }
        RecordBatch { schema, columns }
    }

    pub fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    pub fn num_rows(&self) -> usize {
        self.columns[0].data().len()
    }

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

        let record_batch = RecordBatch::new(Arc::new(schema), vec![Arc::new(a), Arc::new(b)]);

        assert_eq!(5, record_batch.num_rows());
        assert_eq!(2, record_batch.num_columns());
        assert_eq!(&DataType::Int32, record_batch.schema().field(0).data_type());
        assert_eq!(&DataType::Utf8, record_batch.schema().field(1).data_type());
        assert_eq!(5, record_batch.column(0).data().len());
        assert_eq!(5, record_batch.column(1).data().len());
    }
}
