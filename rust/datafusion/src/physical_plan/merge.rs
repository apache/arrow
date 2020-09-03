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
use crate::physical_plan::common::RecordBatchIterator;
use crate::physical_plan::Partitioning;
use crate::physical_plan::{common, ExecutionPlan};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::{RecordBatch, RecordBatchReader};

/// Merge execution plan executes partitions in parallel and combines them into a single
/// partition. No guarantees are made about the order of the resulting partition.
#[derive(Debug)]
pub struct MergeExec {
    /// Input execution plan
    input: Arc<dyn ExecutionPlan>,
    /// Maximum number of concurrent threads
    concurrency: usize,
}

impl MergeExec {
    /// Create a new MergeExec
    pub fn new(input: Arc<dyn ExecutionPlan>, max_concurrency: usize) -> Self {
        MergeExec {
            input,
            concurrency: max_concurrency,
        }
    }
}

impl ExecutionPlan for MergeExec {
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match children.len() {
            1 => Ok(Arc::new(MergeExec::new(
                children[0].clone(),
                self.concurrency,
            ))),
            _ => Err(ExecutionError::General(
                "MergeExec wrong number of children".to_string(),
            )),
        }
    }

    fn execute(
        &self,
        partition: usize,
    ) -> Result<Arc<Mutex<dyn RecordBatchReader + Send + Sync>>> {
        // MergeExec produces a single partition
        if 0 != partition {
            return Err(ExecutionError::General(format!(
                "MergeExec invalid partition {}",
                partition
            )));
        }

        let input_partitions = self.input.output_partitioning().partition_count();
        match input_partitions {
            0 => Err(ExecutionError::General(
                "MergeExec requires at least one input partition".to_owned(),
            )),
            1 => {
                // bypass any threading if there is a single partition
                self.input.execute(0)
            }
            _ => {
                let partitions_per_thread = (input_partitions / self.concurrency).max(1);
                let range: Vec<usize> = (0..input_partitions).collect();
                let chunks = range.chunks(partitions_per_thread);
                let threads: Vec<JoinHandle<Result<Vec<RecordBatch>>>> = chunks
                    .map(|chunk| {
                        let chunk = chunk.to_vec();
                        let input = self.input.clone();
                        thread::spawn(move || {
                            let mut batches = vec![];
                            for partition in chunk {
                                let it = input.execute(partition)?;
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
                    self.input.schema(),
                    combined_results,
                ))))
            }
        }
    }
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

#[cfg(test)]
mod tests {

    use super::*;
    use crate::physical_plan::common;
    use crate::physical_plan::csv::{CsvExec, CsvReadOptions};
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
        assert_eq!(csv.output_partitioning().partition_count(), num_partitions);

        let merge = MergeExec::new(Arc::new(csv), 2);

        // output of MergeExec should have a single partition
        assert_eq!(merge.output_partitioning().partition_count(), 1);

        // the result should contain 4 batches (one per input partition)
        let iter = merge.execute(0)?;
        let batches = common::collect(iter)?;
        assert_eq!(batches.len(), num_partitions);

        // there should be a total of 100 rows
        let row_count: usize = batches.iter().map(|batch| batch.num_rows()).sum();
        assert_eq!(row_count, 100);

        Ok(())
    }
}
