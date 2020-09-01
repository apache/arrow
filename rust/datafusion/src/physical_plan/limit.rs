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

//! Defines the LIMIT plan

use std::sync::{Arc, Mutex};

use crate::error::{ExecutionError, Result};
use crate::physical_plan::memory::MemoryIterator;
use crate::physical_plan::{Distribution, ExecutionPlan, Partitioning};
use arrow::array::ArrayRef;
use arrow::compute::limit;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::{RecordBatch, RecordBatchReader};

/// Limit execution plan
#[derive(Debug)]
pub struct GlobalLimitExec {
    /// Input execution plan
    input: Arc<dyn ExecutionPlan>,
    /// Maximum number of rows to return
    limit: usize,
    /// Number of threads to run parallel LocalLimitExec on
    concurrency: usize,
}

impl GlobalLimitExec {
    /// Create a new MergeExec
    pub fn new(input: Arc<dyn ExecutionPlan>, limit: usize, concurrency: usize) -> Self {
        GlobalLimitExec {
            input,
            limit,
            concurrency,
        }
    }
}

impl ExecutionPlan for GlobalLimitExec {
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn required_child_distribution(&self) -> Distribution {
        Distribution::SinglePartition
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
            1 => Ok(Arc::new(GlobalLimitExec::new(
                children[0].clone(),
                self.limit,
                self.concurrency,
            ))),
            _ => Err(ExecutionError::General(
                "GlobalLimitExec wrong number of children".to_string(),
            )),
        }
    }

    fn execute(
        &self,
        partition: usize,
    ) -> Result<Arc<Mutex<dyn RecordBatchReader + Send + Sync>>> {
        // GlobalLimitExec has a single output partition
        if 0 != partition {
            return Err(ExecutionError::General(format!(
                "GlobalLimitExec invalid partition {}",
                partition
            )));
        }

        // GlobalLimitExec requires a single input partition
        if 1 != self.input.output_partitioning().partition_count() {
            return Err(ExecutionError::General(
                "GlobalLimitExec requires a single input partition".to_owned(),
            ));
        }

        let it = self.input.execute(0)?;
        Ok(Arc::new(Mutex::new(MemoryIterator::try_new(
            collect_with_limit(it, self.limit)?,
            self.input.schema(),
            None,
        )?)))
    }
}

/// LocalLimitExec applies a limit to a single partition
#[derive(Debug)]
pub struct LocalLimitExec {
    input: Arc<dyn ExecutionPlan>,
    limit: usize,
}

impl LocalLimitExec {
    /// Create a new LocalLimitExec partition
    pub fn new(input: Arc<dyn ExecutionPlan>, limit: usize) -> Self {
        Self { input, limit }
    }
}

impl ExecutionPlan for LocalLimitExec {
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn output_partitioning(&self) -> Partitioning {
        self.input.output_partitioning()
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match children.len() {
            1 => Ok(Arc::new(LocalLimitExec::new(
                children[0].clone(),
                self.limit,
            ))),
            _ => Err(ExecutionError::General(
                "LocalLimitExec wrong number of children".to_string(),
            )),
        }
    }

    fn execute(
        &self,
        partition: usize,
    ) -> Result<Arc<Mutex<dyn RecordBatchReader + Send + Sync>>> {
        let it = self.input.execute(partition)?;
        Ok(Arc::new(Mutex::new(MemoryIterator::try_new(
            collect_with_limit(it, self.limit)?,
            self.input.schema(),
            None,
        )?)))
    }
}

/// Truncate a RecordBatch to maximum of n rows
pub fn truncate_batch(batch: &RecordBatch, n: usize) -> Result<RecordBatch> {
    let limited_columns: Result<Vec<ArrayRef>> = (0..batch.num_columns())
        .map(|i| limit(batch.column(i), n).map_err(|error| ExecutionError::from(error)))
        .collect();

    Ok(RecordBatch::try_new(
        batch.schema().clone(),
        limited_columns?,
    )?)
}

/// Create a vector of record batches from an iterator
fn collect_with_limit(
    reader: Arc<Mutex<dyn RecordBatchReader + Send + Sync>>,
    limit: usize,
) -> Result<Vec<RecordBatch>> {
    let mut count = 0;
    let mut reader = reader.lock().unwrap();
    let mut results: Vec<RecordBatch> = vec![];
    loop {
        match reader.next_batch() {
            Ok(Some(batch)) => {
                let capacity = limit - count;
                if batch.num_rows() <= capacity {
                    count += batch.num_rows();
                    results.push(batch);
                } else {
                    let batch = truncate_batch(&batch, capacity)?;
                    count += batch.num_rows();
                    results.push(batch);
                }
                if count == limit {
                    return Ok(results);
                }
            }
            Ok(None) => {
                // end of result set
                return Ok(results);
            }
            Err(e) => return Err(ExecutionError::from(e)),
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::physical_plan::common;
    use crate::physical_plan::csv::{CsvExec, CsvReadOptions};
    use crate::physical_plan::merge::MergeExec;
    use crate::test;

    #[test]
    fn limit() -> Result<()> {
        let schema = test::aggr_test_schema();

        let num_partitions = 4;
        let path =
            test::create_partitioned_csv("aggregate_test_100.csv", num_partitions)?;

        let csv =
            CsvExec::try_new(&path, CsvReadOptions::new().schema(&schema), None, 1024)?;

        // input should have 4 partitions
        assert_eq!(csv.output_partitioning().partition_count(), num_partitions);

        let limit =
            GlobalLimitExec::new(Arc::new(MergeExec::new(Arc::new(csv), 2)), 7, 2);

        // the result should contain 4 batches (one per input partition)
        let iter = limit.execute(0)?;
        let batches = common::collect(iter)?;

        // there should be a total of 100 rows
        let row_count: usize = batches.iter().map(|batch| batch.num_rows()).sum();
        assert_eq!(row_count, 7);

        Ok(())
    }
}
