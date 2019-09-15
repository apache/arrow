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

//! Defines the merge plan for executing partitions in parallel and then merging the results
//! into a single partition

use crate::error::Result;
use crate::execution::physical_plan::common;
use crate::execution::physical_plan::common::RecordBatchIterator;
use crate::execution::physical_plan::{BatchIterator, Partition};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::JoinHandle;

/// Merge execution plan executes partitions in parallel and combines them into a single
/// partition. No guarantees are made about the order of the resulting partition.
pub struct MergeExec {
    /// Input schema
    schema: Arc<Schema>,
    /// Input partitions
    partitions: Vec<Arc<dyn Partition>>,
}

impl MergeExec {
    /// Create a new MergeExec
    pub fn new(schema: Arc<Schema>, partitions: Vec<Arc<dyn Partition>>) -> Self {
        MergeExec { schema, partitions }
    }
}

impl Partition for MergeExec {
    fn execute(&self) -> Result<Arc<Mutex<dyn BatchIterator>>> {
        let threads: Vec<JoinHandle<Result<Vec<RecordBatch>>>> = self
            .partitions
            .iter()
            .map(|p| {
                let p = p.clone();
                thread::spawn(move || {
                    let it = p.execute()?;
                    common::collect(it)
                })
            })
            .collect();

        // combine the results from each thread
        let mut combined_results: Vec<Arc<RecordBatch>> = vec![];
        for thread in threads {
            let result = thread.join().unwrap().unwrap();
            result
                .iter()
                .for_each(|batch| combined_results.push(Arc::new(batch.clone())));
        }

        Ok(Arc::new(Mutex::new(RecordBatchIterator::new(
            self.schema.clone(),
            combined_results,
        ))))
    }
}
