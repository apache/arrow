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

//! Defines the join plan for executing partitions in parallel and then joining the results
//! into a set of partitions.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use arrow::array::Array;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::{RecordBatch, RecordBatchReader};

use super::{utils::build_array, ExecutionPlan};
use crate::common::{build_join_schema, check_join_is_valid, JoinHow};
use crate::error::{ExecutionError, Result};
use crate::execution::physical_plan::common::RecordBatchIterator;
use crate::execution::physical_plan::expressions::col;
use crate::execution::physical_plan::hash::{create_key, KeyScalar};
use crate::execution::physical_plan::Partition;

// A mapping "on" value -> list of row indexes with this key's value
// E.g. [1, 2] -> [3, 6, 8] indicates that rows 3, 6 and 8 have (column1, column2) = [1, 2]
type JoinHashMap = HashMap<Vec<KeyScalar>, Vec<usize>>;

/// join execution plan executes partitions in parallel and combines them into a set of
/// partitions.
pub struct HashJoinExec {
    /// left side
    left: Arc<dyn ExecutionPlan>,
    /// right side
    right: Arc<dyn ExecutionPlan>,
    /// Set of common columns used to join on
    on: HashSet<String>,
    /// How the join is performed
    how: JoinHow,
    /// The schema once the join is applied
    schema: SchemaRef,
}

impl HashJoinExec {
    /// Create a new HashJoinExec
    pub fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: &HashSet<String>,
        how: &JoinHow,
    ) -> Result<Self> {
        let left_schema = left.schema();
        let right_schema = right.schema();
        check_join_is_valid(&left_schema, &right_schema, &on)?;

        let on = on.iter().map(|s| s.clone()).collect::<HashSet<_>>();

        let schema = Arc::new(build_join_schema(&left_schema, &right_schema, &on, &how)?);

        Ok(HashJoinExec {
            left,
            right,
            on: on.clone(),
            how: how.clone(),
            schema,
        })
    }
}

impl ExecutionPlan for HashJoinExec {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn partitions(&self) -> Result<Vec<Arc<dyn Partition>>> {
        self.left
            .partitions()?
            .iter()
            .map(move |p| {
                let projection: Arc<dyn Partition> = Arc::new(HashJoinPartition {
                    schema: self.schema.clone(),
                    on: self.on.clone(),
                    how: self.how.clone(),
                    left: p.clone(),
                    rights: self.right.partitions()?.clone(),
                });

                Ok(projection)
            })
            .collect::<Result<Vec<_>>>()
    }
}

/// Partition with a computed hash table
struct HashJoinPartition {
    /// Input schema
    schema: Arc<Schema>,
    /// columns used to compute the hash
    on: HashSet<String>,
    /// how to join
    how: JoinHow,
    /// left partition
    left: Arc<dyn Partition>,
    /// partitions on the right
    rights: Vec<Arc<dyn Partition>>,
}

/// returns a HashMap
/// The size of this vector corresponds to the total size of a joined batch
fn build_hash_batch(on: &HashSet<String>, batch: &RecordBatch) -> Result<JoinHashMap> {
    let mut hash: JoinHashMap = HashMap::new();

    // evaluate the keys
    let keys_values = on
        .iter()
        .map(|name| col(name).evaluate(batch))
        .collect::<Result<Vec<_>>>()?;

    // build the hash map
    for row in 0..batch.num_rows() {
        let mut key = Vec::with_capacity(keys_values.len());
        for i in 0..keys_values.len() {
            key.push(create_key(&keys_values[i], row)?);
        }
        match hash.get_mut(&key) {
            Some(v) => v.push(row),
            None => {
                hash.insert(key, vec![row]);
            }
        };
    }
    Ok(hash)
}

fn build_join_batch(
    on: &HashSet<String>,
    schema: &Schema,
    how: &JoinHow,
    left: &RecordBatch,
    right: &RecordBatch,
    left_hash: &HashMap<Vec<KeyScalar>, Vec<usize>>,
) -> Result<RecordBatch> {
    let right_hash = build_hash_batch(on, right)?;

    let join_indexes: Vec<(usize, usize)> =
        build_join_indexes(&left_hash, &right_hash, how)?;

    // build the columns for the RecordBatch
    let mut columns: Vec<Arc<dyn Array>> = vec![];
    for field in schema.fields() {
        // pick the column (left or right) based on the field name
        // if two fields have the same name on left and right, the left is given preference
        let (is_left, array) = match left.schema().index_of(field.name()) {
            Ok(i) => Ok((true, left.column(i))),
            Err(_) => {
                match right.schema().index_of(field.name()) {
                    Ok(i) => Ok((false, right.column(i))),
                    _ => Err(ExecutionError::InternalError(
                        format!("During execution, the column {} was not found in neither the left or right side of the join", field.name()).to_string()
                    ))
                }
            }
        }?;

        // pick the (left or right) indexes of the array
        let indexes = join_indexes
            .iter()
            .map(|(left, right)| if is_left { *left } else { *right })
            .collect();

        // build of the array out of the indexes. On a join, we expect more entries (due to duplicates)
        let array = build_array(&array, &indexes, field.data_type())?;
        columns.push(array);
    }
    Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
}

/// returns a vector with (index from left, index from right).
/// The size of this vector corresponds to the total size of a joined batch
fn build_join_indexes(
    left: &JoinHashMap,
    right: &JoinHashMap,
    how: &JoinHow,
) -> Result<Vec<(usize, usize)>> {
    // unfortunately rust does not support intersection of map keys :(
    let left_set: HashSet<Vec<KeyScalar>> = left.keys().cloned().collect();
    let left_right: HashSet<Vec<KeyScalar>> = right.keys().cloned().collect();

    match how {
        JoinHow::Inner => {
            let inner = left_set.intersection(&left_right);

            let mut indexes = Vec::new(); // unknown a prior size
            for key in inner {
                // the unwrap never happens by construction of the key
                let left_indexes = left.get(key).unwrap();
                let right_indexes = right.get(key).unwrap();

                // for every item on the left and right with this key, add the respective pair
                left_indexes.iter().for_each(|x| {
                    right_indexes.iter().for_each(|y| {
                        indexes.push((*x, *y));
                    })
                })
            }
            Ok(indexes)
        }
    }
}

/// filter values base on predicate
pub fn build_joined_partition(
    schema: &Schema,
    on: &HashSet<String>,
    how: &JoinHow,
    left: &Arc<dyn Partition>,
    right: &Arc<dyn Partition>,
) -> Result<Option<RecordBatch>> {
    let iterator = left.execute()?;
    let mut input = iterator.lock().unwrap();

    match input.next_batch()? {
        None => Ok(None),
        Some(left) => {
            let left_hash = build_hash_batch(on, &left)?;

            let iterator_other = right.execute()?;
            let mut input_other = iterator_other.lock().unwrap();
            match input_other.next_batch()? {
                None => Ok(None),
                Some(right) => Ok(Some(build_join_batch(
                    on, schema, how, &left, &right, &left_hash,
                )?)),
            }
        }
    }
}

impl Partition for HashJoinPartition {
    /// Execute the join
    fn execute(&self) -> Result<Arc<Mutex<dyn RecordBatchReader + Send + Sync>>> {
        let batches = self
            .rights
            .iter()
            .map(|right| {
                build_joined_partition(
                    &self.schema,
                    &self.on,
                    &self.how,
                    &self.left,
                    &right,
                )
            })
            .collect::<Result<Vec<Option<_>>>>()?;
        let batches = batches
            .iter()
            .filter_map(|x| match x {
                Some(x) => Some(Arc::new(x.clone())),
                None => None,
            })
            .collect::<Vec<_>>();

        Ok(Arc::new(Mutex::new(RecordBatchIterator::new(
            self.schema.clone(),
            batches,
        ))))
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::{
        execution::physical_plan::{common, memory::MemoryExec, ExecutionPlan},
        test::{build_table_i32, columns, format_batch},
    };
    use std::collections::HashSet;
    use std::sync::Arc;

    fn build_table(
        a: (&str, &Vec<i32>),
        b: (&str, &Vec<i32>),
        c: (&str, &Vec<i32>),
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let (batch, schema) = build_table_i32(a, b, c)?;
        Ok(Arc::new(MemoryExec::try_new(
            &vec![vec![batch]],
            Arc::new(schema),
            None,
        )?))
    }

    fn join(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: &[&str],
    ) -> Result<HashJoinExec> {
        let on = on.iter().map(|s| s.to_string()).collect::<HashSet<_>>();
        HashJoinExec::try_new(left, right, &on, &JoinHow::Inner)
    }

    /// Asserts that the rows are the same, taking into account that their order
    /// is irrelevant
    fn assert_same_rows(result: &[String], expected: &[&str]) {
        assert_eq!(result.len(), expected.len());

        // convert to set since row order is irrelevant
        let result = result
            .iter()
            .map(|s| s.clone())
            .collect::<HashSet<_>>();

        let expected = expected
            .iter()
            .map(|s| s.to_string())
            .collect::<HashSet<_>>();
        assert_eq!(result, expected);
    }

    #[test]
    fn join_one() -> Result<()> {
        let t1 = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![4, 5, 5]), // this has a repetition
            ("c1", &vec![7, 8, 9]),
        )?;
        let t2 = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![4, 5, 6]),
            ("c2", &vec![70, 80, 90]),
        )?;
        let on = vec!["b1"];

        let join = join(t1, t2, &on)?;

        let columns = columns(&join.schema());
        assert_eq!(columns, vec!["b1", "a1", "c1", "a2", "c2"]);

        let batches = common::collect(join.partitions()?[0].execute()?)?;
        assert_eq!(batches.len(), 1);

        let result = format_batch(&batches[0]);
        let expected = vec!["5,2,8,20,80", "5,3,9,20,80", "4,1,7,10,70"];

        assert_same_rows(&result, &expected);

        Ok(())
    }

    #[test]
    fn join_two() -> Result<()> {
        let t1 = build_table(
            ("a1", &vec![1, 2, 2]),
            ("b2", &vec![1, 2, 2]),
            ("c1", &vec![7, 8, 9]),
        )?;
        let t2 = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b2", &vec![1, 2, 2]),
            ("c2", &vec![70, 80, 90]),
        )?;
        let on = vec!["a1", "b2"];

        let join = join(t1, t2, &on)?;

        let columns = columns(&join.schema());
        assert_eq!(columns, vec!["a1", "b2", "c1", "c2"]);

        let batches = common::collect(join.partitions()?[0].execute()?)?;
        assert_eq!(batches.len(), 1);

        let result = format_batch(&batches[0]);
        let expected = vec!["1,1,7,70", "2,2,8,80", "2,2,9,80"];

        assert_same_rows(&result, &expected);

        Ok(())
    }
}
