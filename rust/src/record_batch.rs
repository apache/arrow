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

use super::array::*;
use super::datatypes::*;
use std::rc::Rc;

/// A batch of column-oriented data
pub struct RecordBatch {
    schema: Rc<Schema>,
    columns: Vec<Rc<Array>>
}

impl RecordBatch {
    pub fn new(schema: Rc<Schema>, columns: Vec<Rc<Array>>) -> Self {
        assert!(columns.len() > 0);
        RecordBatch { schema, columns }
    }

    pub fn schema(&self) -> &Rc<Schema> {
        &self.schema
    }

    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    pub fn num_rows(&self) -> usize {
        self.columns[0].len()
    }

    pub fn column(&self, i: usize) -> &Rc<Array> {
        &self.columns[i]
    }

    pub fn column_data(&self, i: usize) -> &Rc<ArrayData> {
        &self.columns[i].data()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_record_batch() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
        ]);

        let a = Array::from(vec![1, 2, 3, 4, 5]);
        let b = Array::from(vec!["a", "b", "c", "d", "e"]);

        let record_batch = RecordBatch::new(
            Rc::new(schema),
            vec![Rc::new(a), Rc::new(b)]
        );

        assert_eq!(5, record_batch.num_rows());
        assert_eq!(2, record_batch.num_columns());
        assert_eq!(
            &DataType::Int32,
            record_batch.schema().column(0).data_type()
        );
        assert_eq!(&DataType::Utf8, record_batch.schema().column(1).data_type());
        assert_eq!(5, record_batch.column(0).len());
        assert_eq!(5, record_batch.column(1).len());
        assert_eq!(5, record_batch.column_data(0).len());
        assert_eq!(5, record_batch.column_data(1).len());
    }
}
