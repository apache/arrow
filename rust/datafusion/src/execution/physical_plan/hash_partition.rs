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

//! Defines the execution plan for a repartition operation

use std::hash::Hash;
use std::hash::Hasher;
use std::sync::{Arc, Mutex};

use arrow::array::BooleanBuilder;
use arrow::compute::filter;
use arrow::datatypes::Schema;
use arrow::record_batch::{RecordBatch, RecordBatchReader};

use crate::error::Result;
use crate::execution::physical_plan::common::RecordBatchIterator;
use crate::execution::physical_plan::expressions::col;
use crate::execution::physical_plan::hash::{create_key, KeyScalar};
use crate::execution::physical_plan::ExecutionPlan;
use crate::execution::physical_plan::Partition;

/// Executes partitions and combines them into a new set of partitions partitioned by a partitioning key.
/// This operation is also known as a Shuffle or Exchange.
/// The resulting partitions are guaranteed to have rows whose `partitioning_key` do not intersect.
/// Specifically, each row is mapped to the partition index `hash(partitioning_key) % new_partitions`.
pub struct RepartitionExec {
    /// Input schema
    schema: Arc<Schema>,
    /// Input partitions
    partitions: Vec<Arc<dyn Partition>>,
    /// The new number of partitions
    new_partitions: usize,
    /// The partitioning key used to map rows to each of the partitions.
    partitioning_key: Vec<String>,
}

impl RepartitionExec {
    /// Create a new RepartitionExec
    pub fn new(
        schema: Arc<Schema>,
        partitions: Vec<Arc<dyn Partition>>,
        new_partitions: usize,
        partitioning_key: Vec<String>,
    ) -> Self {
        RepartitionExec {
            schema,
            partitions,
            new_partitions,
            partitioning_key: partitioning_key.clone(),
        }
    }
}

impl ExecutionPlan for RepartitionExec {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn partitions(&self) -> Result<Vec<Arc<dyn Partition>>> {
        // create `new_partitions` new partitions, on which each has a different index `i`.
        let mut partitions: Vec<Arc<dyn Partition>> =
            Vec::with_capacity(self.new_partitions);
        for i in 0..self.new_partitions {
            partitions.push(Arc::new(HashPartition {
                schema: self.schema.clone(),
                partitions: self.partitions.clone(),
                index: i,
                new_partitions: self.new_partitions,
                partitioning_key: self.partitioning_key.clone(),
            }))
        }
        Ok(partitions)
    }
}

struct HashPartition {
    /// Input schema
    schema: Arc<Schema>,
    /// Input partitions
    partitions: Vec<Arc<dyn Partition>>,
    /// index of the partition, whose hash of the row is mapped to
    index: usize,
    /// number of new partitions
    new_partitions: usize,
    /// the columns used to compute the hash
    partitioning_key: Vec<String>,
}

fn selection_for_partition(
    partitioning_key: &[String],
    predicate: &dyn Fn(&Vec<KeyScalar>) -> bool,
    partition: &Arc<dyn Partition>,
) -> Result<Option<RecordBatch>> {
    let iterator = partition.execute()?;
    let mut input = iterator.lock().unwrap();

    match input.next_batch()? {
        None => Ok(None),
        Some(batch) => {
            // evaluate the keys
            let keys_values = partitioning_key
                .iter()
                .map(|name| col(name).evaluate(&batch))
                .collect::<Result<Vec<_>>>()?;

            // evaluate the predicate for this partition from the keys_values.

            // Arrow does not provide a method
            // for this hashing out-of-the-box and thus we need to do it row by row.
            let mut builder = BooleanBuilder::new(batch.num_rows());
            for row in 0..batch.num_rows() {
                let mut key = Vec::with_capacity(keys_values.len());
                for i in 0..keys_values.len() {
                    key.push(create_key(&keys_values[i], row)?);
                }
                builder.append_value(predicate(&key))?;
            }

            let predicate_array = builder.finish();

            // filter each array based on `predicate`
            let mut filtered_arrays = vec![];
            for i in 0..batch.num_columns() {
                let array = batch.column(i);
                let filtered_array = filter(array.as_ref(), &predicate_array)?;
                filtered_arrays.push(filtered_array);
            }
            Ok(Some(RecordBatch::try_new(
                batch.schema().clone(),
                filtered_arrays,
            )?))
        }
    }
}

impl Partition for HashPartition {
    /// Execute the partitioning
    fn execute(&self) -> Result<Arc<Mutex<dyn RecordBatchReader + Send + Sync>>> {
        let r = self.partitions.iter().filter_map(|partition| {
            selection_for_partition(
                &self.partitioning_key,
                &|key: &Vec<KeyScalar>| {
                    // construct the hash
                    let mut hasher = fnv::FnvHasher::default();
                    key.hash(&mut hasher);
                    let key_hash = hasher.finish() as usize;

                    // map the hash to the partition's index
                    (key_hash % self.new_partitions) == self.index
                },
                partition,
                // This is poor implementation. How to fix this?
            )
            .expect("Valid partition")
        });

        let mut batches = vec![];
        r.for_each(|batch| batches.push(Arc::new(batch)));

        Ok(Arc::new(Mutex::new(RecordBatchIterator::new(
            self.schema.clone(),
            batches,
        ))))
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::execution::physical_plan::csv::{CsvExec, CsvReadOptions};
    use crate::test;
    use arrow::array::{Array, StringArray};
    use std::collections::{HashMap, HashSet};

    #[test]
    fn repartition() -> Result<()> {
        let schema = test::aggr_test_schema();

        let num_partitions = 4;
        let path =
            test::create_partitioned_csv("aggregate_test_100.csv", num_partitions)?;

        let csv =
            CsvExec::try_new(&path, CsvReadOptions::new().schema(&schema), None, 1024)?;

        // input should have 4 partitions
        let input = csv.partitions()?;
        assert_eq!(input.len(), num_partitions);

        let new_num_partitions = 2;
        let repartition = RepartitionExec::new(
            schema.clone(),
            input,
            new_num_partitions,
            vec!["c1".to_string()],
        );

        // compute some statistics over the partitions
        let mut partition_count = 0;
        let mut row_count = 0;
        let mut batch_count = 0;
        let mut hash_all = HashMap::new();
        for partition in repartition.partitions()? {
            partition_count += 1;
            let mut hash = HashSet::new();
            let iterator = partition.execute()?;
            let mut iterator = iterator.lock().unwrap();
            while let Some(batch) = iterator.next_batch()? {
                row_count += batch.num_rows();
                batch_count += 1;
                let array = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                for i in 0..array.data().len() {
                    hash.insert(array.value(i).to_string());
                }
            }
            hash_all.insert(partition_count, hash);
        }
        // correct number of rows and partitions
        assert_eq!(new_num_partitions, partition_count);
        assert_eq!(100, row_count);
        // old partitions * new partitions
        assert_eq!(new_num_partitions * num_partitions, batch_count);

        // there is no intersection of the items across partitions
        for i in hash_all.keys() {
            for j in hash_all.keys() {
                if j >= i {
                    continue;
                }
                let lhs = hash_all.get(i).unwrap();
                let rhs = hash_all.get(j).unwrap();
                assert_eq!(lhs.intersection(rhs).next(), None)
            }
        }

        Ok(())
    }
}
