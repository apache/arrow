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

//! In-memory data source for presenting a Vec<RecordBatch> as a data source that can be
//! queried by DataFusion. This allows data to be pre-loaded into memory and then repeatedly
//! queried without incurring additional file I/O overhead.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

use crate::datasource::{DataSource, DataSourceProvider};
use crate::execution::error::Result;

pub struct InMemoryDataSource {
    schema: Arc<Schema>,
    index: usize,
    batches: Vec<RecordBatch>,
}

impl DataSource for InMemoryDataSource {
    fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    fn next(&mut self) -> Result<Option<RecordBatch>> {
        if self.index < self.batches.len() {
            self.index += 1;
            Ok(Some(self.batches[self.index - 1].clone()))
        } else {
            Ok(None)
        }
    }
}

pub struct InMemoryDataSourceProvider {
    schema: Arc<Schema>,
    batches: Vec<RecordBatch>,
}

impl InMemoryDataSourceProvider {
    pub fn new(schema: Arc<Schema>, batches: Vec<RecordBatch>) -> Self {
        Self { schema, batches }
    }
}

impl DataSourceProvider for InMemoryDataSourceProvider {
    fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        _batch_size: usize,
    ) -> Rc<RefCell<DataSource>> {
        let columns: Vec<usize> = match projection {
            Some(p) => p.clone(),
            None => {
                let l = self.schema.fields().len();
                let mut v = Vec::with_capacity(l);
                for i in 0..l {
                    v.push(i);
                }
                v
            }
        };

        let projected_schema = Arc::new(Schema::new(
            columns
                .iter()
                .map(|i| self.schema.field(*i).clone())
                .collect(),
        ));

        Rc::new(RefCell::new(InMemoryDataSource {
            schema: self.schema.clone(),
            index: 0,
            batches: self
                .batches
                .iter()
                .map(|batch| {
                    RecordBatch::new(
                        projected_schema.clone(),
                        columns.iter().map(|i| batch.column(*i).clone()).collect(),
                    )
                })
                .collect(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn test1() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]));

        let batch = RecordBatch::new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![4, 5, 6])),
                Arc::new(Int32Array::from(vec![7, 8, 9])),
            ],
        );

        let provider = InMemoryDataSourceProvider::new(schema, vec![batch]);

        // scan with no projection
        let scan1 = provider.scan(&None, 1024);
        let batch1 = scan1.borrow_mut().next().unwrap().unwrap();
        assert_eq!(3, batch1.schema().fields().len());
        assert_eq!(3, batch1.num_columns());

        // scan with projection
        let scan2 = provider.scan(&Some(vec![2, 1]), 1024);
        let batch2 = scan2.borrow_mut().next().unwrap().unwrap();
        assert_eq!(2, batch2.schema().fields().len());
        assert_eq!("c", batch2.schema().field(0).name());
        assert_eq!("b", batch2.schema().field(1).name());
        assert_eq!(2, batch2.num_columns());
    }
}
