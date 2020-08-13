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

use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

use crate::error::{ExecutionError, Result};
use crate::execution::physical_plan::common::RecordBatchIterator;
use crate::execution::physical_plan::Partition;
use crate::execution::physical_plan::{common, ExecutionPlan};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::{RecordBatch, RecordBatchReader};

/// Merge execution plan executes partitions in parallel and combines them into a single
/// partition. No guarantees are made about the order of the resulting partition.
#[derive(Debug)]
pub struct MergeExec {
    /// Input schema
    schema: SchemaRef,
    /// Input partitions
    partitions: Vec<Arc<dyn Partition>>,
    /// Maximum number of concurrent threads
    concurrency: usize,
}

impl MergeExec {
    /// Create a new MergeExec
    pub fn new(
        schema: SchemaRef,
        partitions: Vec<Arc<dyn Partition>>,
        max_concurrency: usize,
    ) -> Self {
        MergeExec {
            schema,
            partitions,
            concurrency: max_concurrency,
        }
    }
}

impl ExecutionPlan for MergeExec {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn partitions(&self) -> Result<Vec<Arc<dyn Partition>>> {
        Ok(vec![Arc::new(MergePartition {
            schema: self.schema.clone(),
            partitions: self.partitions.clone(),
            concurrency: self.concurrency,
        })])
    }
}

#[derive(Debug)]
struct MergePartition {
    /// Input schema
    schema: SchemaRef,
    /// Input partitions
    partitions: Vec<Arc<dyn Partition>>,
    /// Maximum number of concurrent threads
    concurrency: usize,
}

fn collect_from_thread(
    thread: JoinHandle<Result<Vec<RecordBatch>>>,
    combined_results: &mut Vec<Arc<RecordBatch>>,
) -> Result<()> {
    match thread.join() {
        Ok(join) => {
            join?
                .iter()
                .for_each(|batch| combined_results.push(Arc::new(batch.clone())));
            Ok(())
        }
        Err(e) => Err(ExecutionError::General(format!(
            "Error collecting batches from thread: {:?}",
            e
        ))),
    }
}

impl Partition for MergePartition {
    fn execute(&self) -> Result<Arc<Mutex<dyn RecordBatchReader + Send + Sync>>> {
        match self.partitions.len() {
            0 => Err(ExecutionError::General(
                "MergeExec requires at least one input partition".to_owned(),
            )),
            1 => {
                // bypass any threading if there is a single partition
                self.partitions[0].execute()
            }
            _ => {
                let partitions_per_thread =
                    (self.partitions.len() / self.concurrency).max(1);
                let chunks = self.partitions.chunks(partitions_per_thread);
                let threads: Vec<JoinHandle<Result<Vec<RecordBatch>>>> = chunks
                    .map(|chunk| {
                        let chunk = chunk.to_vec();
                        thread::spawn(move || {
                            let mut batches = vec![];
                            for partition in chunk {
                                let it = partition.execute()?;
                                common::collect(it).iter().for_each(|b| {
                                    b.iter().for_each(|b| batches.push(b.clone()))
                                });
                            }
                            Ok(batches)
                        })
                    })
                    .collect();

                // combine the results from each thread
                let mut combined_results: Vec<Arc<RecordBatch>> = vec![];
                for thread in threads {
                    collect_from_thread(thread, &mut combined_results)?;
                }

                Ok(Arc::new(Mutex::new(RecordBatchIterator::new(
                    self.schema.clone(),
                    combined_results,
                ))))
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::execution::physical_plan::common;
    use crate::execution::physical_plan::csv::{CsvExec, CsvReadOptions};
    use crate::test;

    #[test]
    fn merge() -> Result<()> {
        let schema = test::aggr_test_schema();

        let num_partitions = 4;
        let path =
            test::create_partitioned_csv("aggregate_test_100.csv", num_partitions)?;

        let csv =
            CsvExec::try_new(&path, CsvReadOptions::new().schema(&schema), None, 1024)?;

        // input should have 4 partitions
        let input = csv.partitions()?;
        assert_eq!(input.len(), num_partitions);

        let merge = MergeExec::new(schema.clone(), input, 2);

        // output of MergeExec should have a single partition
        let merged = merge.partitions()?;
        assert_eq!(merged.len(), 1);

        // the result should contain 4 batches (one per input partition)
        let iter = merged[0].execute()?;
        let batches = common::collect(iter)?;
        assert_eq!(batches.len(), num_partitions);

        // there should be a total of 100 rows
        let row_count: usize = batches.iter().map(|batch| batch.num_rows()).sum();
        assert_eq!(row_count, 100);

        Ok(())
    }
}
