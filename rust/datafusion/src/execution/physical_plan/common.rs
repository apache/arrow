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

use std::sync::{Arc, Mutex};

use crate::error::Result;
use crate::execution::physical_plan::BatchIterator;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

/// Iterator over a vector of record batches
pub struct RecordBatchIterator {
    schema: Arc<Schema>,
    batches: Vec<Arc<RecordBatch>>,
    index: usize,
}

impl RecordBatchIterator {
    /// Create a new RecordBatchIterator
    pub fn new(schema: Arc<Schema>, batches: Vec<Arc<RecordBatch>>) -> Self {
        RecordBatchIterator {
            schema,
            index: 0,
            batches,
        }
    }
}

impl BatchIterator for RecordBatchIterator {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn next(&mut self) -> Result<Option<RecordBatch>> {
        if self.index < self.batches.len() {
            self.index += 1;
            Ok(Some(self.batches[self.index - 1].as_ref().clone()))
        } else {
            Ok(None)
        }
    }
}

/// Create a vector of record batches from an iterator
pub fn collect(it: Arc<Mutex<dyn BatchIterator>>) -> Result<Vec<RecordBatch>> {
    let mut it = it.lock().unwrap();
    let mut results: Vec<RecordBatch> = vec![];
    loop {
        match it.next() {
            Ok(Some(batch)) => {
                results.push(batch);
            }
            Ok(None) => {
                // end of result set
                return Ok(results);
            }
            Err(e) => return Err(e),
        }
    }
}
