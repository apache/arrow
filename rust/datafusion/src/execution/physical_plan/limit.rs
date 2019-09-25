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

use crate::error::Result;
use crate::execution::physical_plan::common::RecordBatchIterator;
use crate::execution::physical_plan::{common, ExecutionPlan};
use crate::execution::physical_plan::{BatchIterator, Partition};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::JoinHandle;

//// Limit execution plan
pub struct LimitExec {
    /// Input schema
    schema: Arc<Schema>,
    /// Input partitions
    partitions: Vec<Arc<dyn Partition>>,
}

impl LimitExec {
    /// Create a new MergeExec
    pub fn new(schema: Arc<Schema>, partitions: Vec<Arc<dyn Partition>>) -> Self {
        LimitExec { schema, partitions }
    }
}

impl ExecutionPlan for LimitExec {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn partitions(&self) -> Result<Vec<Arc<dyn Partition>>> {
        Ok(vec![Arc::new(LimitPartition {
            schema: self.schema.clone(),
            partitions: self.partitions.clone(),
        })])
    }
}

struct LimitPartition {
    /// Input schema
    schema: Arc<Schema>,
    /// Input partitions
    partitions: Vec<Arc<dyn Partition>>,
}

impl Partition for LimitPartition {
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
            let join = thread.join().expect("Failed to join thread");
            let result = join?;
            result
                .iter()
                .for_each(|batch| combined_results.push(Arc::new(batch.clone())));
        }

        combined_results.

        Ok(Arc::new(Mutex::new(RecordBatchIterator::new(
            self.schema.clone(),
            combined_results,
        ))))
    }
}

fn limit(it: Arc<Mutex<dyn BatchIterator>>, limit: usize) -> Result<Vec<Arc<RecordBatch>>> {

    let mut num_consumed_rows = 0;
    let mut batches: Vec<Arc<RecordBatch>> = vec![];
    let capacity = limit - num_consumed_rows;

    if capacity <= 0 {
        return Ok(None);
    }

    if batch.num_rows() >= capacity {
        let limited_columns: Result<Vec<ArrayRef>> = (0..batch.num_columns())
            .map(|i| match limit(batch.column(i), capacity) {
                Ok(result) => Ok(result),
                Err(error) => Err(ExecutionError::from(error)),
            })
            .collect();

        let limited_batch: RecordBatch =
            RecordBatch::try_new(self.schema.clone(), limited_columns?)?;
        self.num_consumed_rows += capacity;

        Ok(Some(limited_batch))
    } else {
        self.num_consumed_rows += batch.num_rows();
        Ok(Some(batch))
    }
}

}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::execution::physical_plan::common;
    use crate::execution::physical_plan::csv::CsvExec;
    use crate::test;

    #[test]
    fn merge() -> Result<()> {
        let schema = test::aggr_test_schema();

        let num_partitions = 4;
        let path =
            test::create_partitioned_csv("aggregate_test_100.csv", num_partitions)?;

        let csv = CsvExec::try_new(&path, schema.clone(), true, None, 1024)?;

        // input should have 4 partitions
        let input = csv.partitions()?;
        assert_eq!(input.len(), num_partitions);

        let merge = MergeExec::new(schema.clone(), input);

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
