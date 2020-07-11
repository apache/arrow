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

use crate::error::{ExecutionError, Result};
use crate::execution::physical_plan::common::RecordBatchIterator;
use crate::execution::physical_plan::ExecutionPlan;
use crate::execution::physical_plan::Partition;
use arrow::array::ArrayRef;
use arrow::compute::limit;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::{RecordBatch, RecordBatchReader};
use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::JoinHandle;

/// Limit execution plan
pub struct LimitExec {
    /// Input schema
    schema: SchemaRef,
    /// Input partitions
    partitions: Vec<Arc<dyn Partition>>,
    /// Maximum number of rows to return
    limit: usize,
}

impl LimitExec {
    /// Create a new MergeExec
    pub fn new(
        schema: SchemaRef,
        partitions: Vec<Arc<dyn Partition>>,
        limit: usize,
    ) -> Self {
        LimitExec {
            schema,
            partitions,
            limit,
        }
    }
}

impl ExecutionPlan for LimitExec {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn partitions(&self) -> Result<Vec<Arc<dyn Partition>>> {
        Ok(vec![Arc::new(LimitPartition {
            schema: self.schema.clone(),
            partitions: self.partitions.clone(),
            limit: self.limit,
        })])
    }
}

struct LimitPartition {
    /// Input schema
    schema: SchemaRef,
    /// Input partitions
    partitions: Vec<Arc<dyn Partition>>,
    /// Maximum number of rows to return
    limit: usize,
}

impl Partition for LimitPartition {
    fn execute(&self) -> Result<Arc<Mutex<dyn RecordBatchReader + Send + Sync>>> {
        // collect up to "limit" rows on each partition
        let threads: Vec<JoinHandle<Result<Vec<RecordBatch>>>> = self
            .partitions
            .iter()
            .map(|p| {
                let p = p.clone();
                let limit = self.limit;
                thread::spawn(move || {
                    let it = p.execute()?;
                    collect_with_limit(it, limit)
                })
            })
            .collect();

        // combine the results from each thread, up to the limit
        let mut combined_results: Vec<Arc<RecordBatch>> = vec![];
        let mut count = 0;
        for thread in threads {
            let join = thread.join().expect("Failed to join thread");
            let result = join?;
            for batch in result {
                let capacity = self.limit - count;
                if batch.num_rows() <= capacity {
                    count += batch.num_rows();
                    combined_results.push(Arc::new(batch.clone()))
                } else {
                    let batch = truncate_batch(&batch, capacity)?;
                    count += batch.num_rows();
                    combined_results.push(Arc::new(batch.clone()))
                }
                if count == self.limit {
                    break;
                }
            }
        }

        Ok(Arc::new(Mutex::new(RecordBatchIterator::new(
            self.schema.clone(),
            combined_results,
        ))))
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
    use crate::execution::physical_plan::common;
    use crate::execution::physical_plan::csv::{CsvExec, CsvReadOptions};
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
        let input = csv.partitions()?;
        assert_eq!(input.len(), num_partitions);

        let limit = LimitExec::new(schema.clone(), input, 7);
        let partitions = limit.partitions()?;

        // the result should contain 4 batches (one per input partition)
        let iter = partitions[0].execute()?;
        let batches = common::collect(iter)?;

        // there should be a total of 100 rows
        let row_count: usize = batches.iter().map(|batch| batch.num_rows()).sum();
        assert_eq!(row_count, 7);

        Ok(())
    }
}
