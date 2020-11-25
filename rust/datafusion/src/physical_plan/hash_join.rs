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

use std::sync::Arc;
use std::{
    any::Any,
    collections::{HashMap, HashSet},
};

use async_trait::async_trait;
use futures::{Stream, StreamExt, TryStreamExt};

use arrow::array::{make_array, Array, MutableArrayData};
use arrow::datatypes::{Schema, SchemaRef};
use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;

use super::{expressions::col, hash_aggregate::create_key};
use super::{
    hash_utils::{build_join_schema, check_join_is_valid, JoinOn, JoinType},
    merge::MergeExec,
};
use crate::error::{DataFusionError, Result};

use super::{
    group_scalar::GroupByScalar, ExecutionPlan, Partitioning, RecordBatchStream,
    SendableRecordBatchStream,
};

// An index of (batch, row) uniquely identifying a row in a part.
type Index = (usize, usize);
// A pair (left index, right index)
// Note that while this is currently equal to `Index`, the `JoinIndex` is semantically different
// as a left join may issue None indices, in which case
type JoinIndex = Option<(usize, usize)>;
// Maps ["on" value] -> [list of indices with this key's value]
// E.g. [1, 2] -> [(0, 3), (1, 6), (0, 8)] indicates that (column1, column2) = [1, 2] is true
// for rows 3 and 8 from batch 0 and row 6 from batch 1.
type JoinHashMap = HashMap<Vec<GroupByScalar>, Vec<Index>>;
type JoinLeftData = (JoinHashMap, Vec<RecordBatch>);

/// join execution plan executes partitions in parallel and combines them into a set of
/// partitions.
#[derive(Debug)]
pub struct HashJoinExec {
    /// left (build) side which gets hashed
    left: Arc<dyn ExecutionPlan>,
    /// right (probe) side which are filtered by the hash table
    right: Arc<dyn ExecutionPlan>,
    /// Set of common columns used to join on
    on: Vec<(String, String)>,
    /// How the join is performed
    join_type: JoinType,
    /// The schema once the join is applied
    schema: SchemaRef,
}

impl HashJoinExec {
    /// Tries to create a new [HashJoinExec].
    /// # Error
    /// This function errors when it is not possible to join the left and right sides on keys `on`.
    pub fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: &JoinOn,
        join_type: &JoinType,
    ) -> Result<Self> {
        let left_schema = left.schema();
        let right_schema = right.schema();
        check_join_is_valid(&left_schema, &right_schema, &on)?;

        let schema = Arc::new(build_join_schema(
            &left_schema,
            &right_schema,
            on,
            &join_type,
        ));

        let on = on
            .iter()
            .map(|(l, r)| (l.to_string(), r.to_string()))
            .collect();

        Ok(HashJoinExec {
            left,
            right,
            on,
            join_type: join_type.clone(),
            schema,
        })
    }
}

#[async_trait]
impl ExecutionPlan for HashJoinExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.left.clone(), self.right.clone()]
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match children.len() {
            2 => Ok(Arc::new(HashJoinExec::try_new(
                children[0].clone(),
                children[1].clone(),
                &self.on,
                &self.join_type,
            )?)),
            _ => Err(DataFusionError::Internal(
                "HashJoinExec wrong number of children".to_string(),
            )),
        }
    }

    fn output_partitioning(&self) -> Partitioning {
        self.right.output_partitioning()
    }

    async fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream> {
        // merge all parts into a single stream
        // this is currently expensive as we re-compute this for every part from the right
        // TODO: Fix this issue: we can't share this state across parts on the right.
        // We need to change this `execute` to allow sharing state across parts...
        let merge = MergeExec::new(self.left.clone());
        let stream = merge.execute(0).await?;

        let on_left = self
            .on
            .iter()
            .map(|on| on.0.clone())
            .collect::<HashSet<_>>();
        let on_right = self
            .on
            .iter()
            .map(|on| on.1.clone())
            .collect::<HashSet<_>>();

        // This operation performs 2 steps at once:
        // 1. creates a [JoinHashMap] of all batches from the stream
        // 2. stores the batches in a vector.
        let initial = (JoinHashMap::new(), Vec::new(), 0);
        let left_data = stream
            .try_fold(initial, |mut acc, batch| async {
                let hash = &mut acc.0;
                let values = &mut acc.1;
                let index = acc.2;
                update_hash(&on_left, &batch, hash, index).unwrap();
                values.push(batch);
                acc.2 += 1;
                Ok(acc)
            })
            .await?;
        // we have the batches and the hash map with their keys. We can how create a stream
        // over the right that uses this information to issue new batches.

        let stream = self.right.execute(partition).await?;
        Ok(Box::pin(HashJoinStream {
            schema: self.schema.clone(),
            on_right,
            join_type: self.join_type.clone(),
            left_data: (left_data.0, left_data.1),
            right: stream,
        }))
    }
}

/// Updates `hash` with new entries from [RecordBatch] evaluated against the expressions `on`,
/// assuming that the [RecordBatch] corresponds to the `index`th
fn update_hash(
    on: &HashSet<String>,
    batch: &RecordBatch,
    hash: &mut JoinHashMap,
    index: usize,
) -> Result<()> {
    // evaluate the keys
    let keys_values = on
        .iter()
        .map(|name| Ok(col(name).evaluate(batch)?.into_array(batch.num_rows())))
        .collect::<Result<Vec<_>>>()?;

    let mut key = Vec::with_capacity(keys_values.len());
    for _ in 0..keys_values.len() {
        key.push(GroupByScalar::UInt32(0));
    }

    // update the hash map
    for row in 0..batch.num_rows() {
        create_key(&keys_values, row, &mut key)?;
        match hash.get_mut(&key) {
            Some(v) => v.push((index, row)),
            None => {
                hash.insert(key.clone(), vec![(index, row)]);
            }
        };
    }
    Ok(())
}

/// A stream that issues [RecordBatch]es as they arrive from the right  of the join.
struct HashJoinStream {
    /// Input schema
    schema: Arc<Schema>,
    /// columns from the right used to compute the hash
    on_right: HashSet<String>,
    /// type of the join
    join_type: JoinType,
    /// information from the left
    left_data: JoinLeftData,
    /// right
    right: SendableRecordBatchStream,
}

impl RecordBatchStream for HashJoinStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Returns a new [RecordBatch] by combining the `left` and `right` according to `indices`.
/// The resulting batch has [Schema] `schema`.
/// # Error
/// This function errors when:
/// *
fn build_batch_from_indices(
    schema: &Schema,
    left: &Vec<RecordBatch>,
    right: &RecordBatch,
    indices: &[(JoinIndex, JoinIndex)],
) -> ArrowResult<RecordBatch> {
    if left.is_empty() {
        todo!("Create empty record batch");
    }
    // this is just for symmetry of the code below.
    let right = vec![right.clone()];

    // build the columns of the new [RecordBatch]:
    // 1. pick whether the column is from the left or right
    // 2. based on the pick, `take` items from the different recordBatches
    let mut columns: Vec<Arc<dyn Array>> = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        // pick the column (left or right) based on the field name.
        // Note that we take `.data()` to gather the [ArrayData] of each array.
        let (is_left, arrays) = match left[0].schema().index_of(field.name()) {
            Ok(i) => Ok((true, left.iter().map(|batch| batch.column(i).data()).collect::<Vec<_>>())),
            Err(_) => {
                match right[0].schema().index_of(field.name()) {
                    Ok(i) => Ok((false, right.iter().map(|batch| batch.column(i).data()).collect::<Vec<_>>())),
                    _ => Err(DataFusionError::Internal(
                        format!("During execution, the column {} was not found in neither the left or right side of the join", field.name()).to_string()
                    ))
                }
            }
        }.map_err(DataFusionError::into_arrow_external_error)?;

        // create a vector of references to be passed to [MutableArrayData]
        let arrays = arrays
            .iter()
            .map(|array| array.as_ref())
            .collect::<Vec<_>>();
        let capacity = arrays.iter().map(|array| array.len()).sum();
        let mut mutable = MutableArrayData::new(arrays, capacity);

        let array = if is_left {
            // build the array using the left
            for (join_index, _) in indices {
                match join_index {
                    Some((batch, row)) => mutable.extend(*batch, *row, *row + 1),
                    // something like `mutable.extend_nulls(*row, *row + 1)`
                    None => unimplemented!(),
                }
            }
            make_array(Arc::new(mutable.freeze()))
        } else {
            // build the array using the right
            for (_, join_index) in indices {
                match join_index {
                    Some((batch, row)) => mutable.extend(*batch, *row, *row + 1),
                    // something like `mutable.extend_nulls(*row, *row + 1)`
                    None => unimplemented!(),
                }
            }
            make_array(Arc::new(mutable.freeze()))
        };
        columns.push(array);
    }
    Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
}

fn build_batch(
    batch: &RecordBatch,
    left_data: &JoinLeftData,
    on_right: &HashSet<String>,
    join_type: &JoinType,
    schema: &Schema,
) -> ArrowResult<RecordBatch> {
    let mut right_hash = JoinHashMap::with_capacity(batch.num_rows());
    update_hash(on_right, batch, &mut right_hash, 0).unwrap();

    let indices = build_join_indexes(&left_data.0, &right_hash, join_type).unwrap();

    build_batch_from_indices(schema, &left_data.1, &batch, &indices)
}

/// returns a vector with (index from left, index from right).
/// The size of this vector corresponds to the total size of a joined batch
// For a join on column A:
// left       right
//     batch 1
// A B         A D
// ---------------
// 1 a         3 6
// 2 b         1 2
// 3 c         2 4
//     batch 2
// A B         A D
// ---------------
// 1 a         5 10
// 2 b         2 2
// 4 d         1 1
// indices (batch, batch_row)
// left       right
// (0, 2)     (0, 0)
// (0, 0)     (0, 1)
// (0, 1)     (0, 2)
// (1, 0)     (0, 1)
// (1, 1)     (0, 2)
// (0, 1)     (1, 1)
// (0, 0)     (1, 2)
// (1, 1)     (1, 1)
// (1, 0)     (1, 2)
fn build_join_indexes(
    left: &JoinHashMap,
    right: &JoinHashMap,
    join_type: &JoinType,
) -> Result<Vec<(JoinIndex, JoinIndex)>> {
    match join_type {
        JoinType::Inner => {
            // inner => key intersection
            // unfortunately rust does not support intersection of map keys :(
            let left_set: HashSet<Vec<GroupByScalar>> = left.keys().cloned().collect();
            let left_right: HashSet<Vec<GroupByScalar>> = right.keys().cloned().collect();
            let inner = left_set.intersection(&left_right);

            let mut indexes = Vec::new(); // unknown a prior size
            for key in inner {
                // the unwrap never happens by construction of the key
                let left_indexes = left.get(key).unwrap();
                let right_indexes = right.get(key).unwrap();

                // for every item on the left and right with this key, add the respective pair
                left_indexes.iter().for_each(|x| {
                    right_indexes.iter().for_each(|y| {
                        // on an inner join, left and right indices are present
                        indexes.push((Some(*x), Some(*y)));
                    })
                })
            }
            Ok(indexes)
        }
    }
}

impl Stream for HashJoinStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.right
            .poll_next_unpin(cx)
            .map(|maybe_batch| match maybe_batch {
                Some(Ok(batch)) => Some(build_batch(
                    &batch,
                    &self.left_data,
                    &self.on_right,
                    &self.join_type,
                    &self.schema,
                )),
                other => other,
            })
    }
}

#[cfg(test)]
mod tests {

    use crate::{
        physical_plan::{common, memory::MemoryExec},
        test::{build_table_i32, columns, format_batch},
    };

    use super::*;
    use std::collections::HashSet;
    use std::sync::Arc;

    fn build_table(
        a: (&str, &Vec<i32>),
        b: (&str, &Vec<i32>),
        c: (&str, &Vec<i32>),
    ) -> Arc<dyn ExecutionPlan> {
        let batch = build_table_i32(a, b, c);
        let schema = batch.schema();
        Arc::new(MemoryExec::try_new(&vec![vec![batch]], schema, None).unwrap())
    }

    fn join(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: &[(&str, &str)],
    ) -> Result<HashJoinExec> {
        let on: Vec<_> = on
            .iter()
            .map(|(l, r)| (l.to_string(), r.to_string()))
            .collect();
        HashJoinExec::try_new(left, right, &on, &JoinType::Inner)
    }

    /// Asserts that the rows are the same, taking into account that their order
    /// is irrelevant
    fn assert_same_rows(result: &[String], expected: &[&str]) {
        // convert to set since row order is irrelevant
        let result = result.iter().map(|s| s.clone()).collect::<HashSet<_>>();

        let expected = expected
            .iter()
            .map(|s| s.to_string())
            .collect::<HashSet<_>>();
        assert_eq!(result, expected);
    }

    #[tokio::test]
    async fn join_one() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![4, 5, 5]), // this has a repetition
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![4, 5, 6]),
            ("c2", &vec![70, 80, 90]),
        );
        let on = &[("b1", "b1")];

        let join = join(left, right, on)?;

        let columns = columns(&join.schema());
        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "c2"]);

        let stream = join.execute(0).await?;
        let batches = common::collect(stream).await?;

        let result = format_batch(&batches[0]);
        let expected = vec!["2,5,8,20,80", "3,5,9,20,80", "1,4,7,10,70"];

        assert_same_rows(&result, &expected);

        Ok(())
    }

    #[tokio::test]
    async fn join_one_no_shared_column_names() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![4, 5, 5]), // this has a repetition
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b2", &vec![4, 5, 6]),
            ("c2", &vec![70, 80, 90]),
        );
        let on = &[("b1", "b2")];

        let join = join(left, right, on)?;

        let columns = columns(&join.schema());
        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b2", "c2"]);

        let stream = join.execute(0).await?;
        let batches = common::collect(stream).await?;

        let result = format_batch(&batches[0]);
        let expected = vec!["2,5,8,20,5,80", "3,5,9,20,5,80", "1,4,7,10,4,70"];

        assert_same_rows(&result, &expected);

        Ok(())
    }

    #[tokio::test]
    async fn join_two() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 2]),
            ("b2", &vec![1, 2, 2]),
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b2", &vec![1, 2, 2]),
            ("c2", &vec![70, 80, 90]),
        );
        let on = &[("a1", "a1"), ("b2", "b2")];

        let join = join(left, right, on)?;

        let columns = columns(&join.schema());
        assert_eq!(columns, vec!["a1", "b2", "c1", "c2"]);

        let stream = join.execute(0).await?;
        let batches = common::collect(stream).await?;
        assert_eq!(batches.len(), 1);

        let result = format_batch(&batches[0]);
        let expected = vec!["1,1,7,70", "2,2,8,80", "2,2,9,80"];

        assert_same_rows(&result, &expected);

        Ok(())
    }

    /// Test where the left has 2 parts, the right with 1 part => 1 part
    #[tokio::test]
    async fn join_one_two_parts_left() -> Result<()> {
        let batch1 = build_table_i32(
            ("a1", &vec![1, 2]),
            ("b2", &vec![1, 2]),
            ("c1", &vec![7, 8]),
        );
        let batch2 =
            build_table_i32(("a1", &vec![2]), ("b2", &vec![2]), ("c1", &vec![9]));
        let schema = batch1.schema();
        let left = Arc::new(
            MemoryExec::try_new(&vec![vec![batch1], vec![batch2]], schema, None).unwrap(),
        );

        let right = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b2", &vec![1, 2, 2]),
            ("c2", &vec![70, 80, 90]),
        );
        let on = &[("a1", "a1"), ("b2", "b2")];

        let join = join(left, right, on)?;

        let columns = columns(&join.schema());
        assert_eq!(columns, vec!["a1", "b2", "c1", "c2"]);

        let stream = join.execute(0).await?;
        let batches = common::collect(stream).await?;
        assert_eq!(batches.len(), 1);

        let result = format_batch(&batches[0]);
        let expected = vec!["1,1,7,70", "2,2,8,80", "2,2,9,80"];

        assert_same_rows(&result, &expected);

        Ok(())
    }

    /// Test where the left has 1 part, the right has 2 parts => 2 parts
    #[tokio::test]
    async fn join_one_two_parts_right() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![4, 5, 5]), // this has a repetition
            ("c1", &vec![7, 8, 9]),
        );

        let batch1 = build_table_i32(
            ("a2", &vec![10, 20]),
            ("b1", &vec![4, 6]),
            ("c2", &vec![70, 80]),
        );
        let batch2 =
            build_table_i32(("a2", &vec![30]), ("b1", &vec![5]), ("c2", &vec![90]));
        let schema = batch1.schema();
        let right = Arc::new(
            MemoryExec::try_new(&vec![vec![batch1], vec![batch2]], schema, None).unwrap(),
        );

        let on = &[("b1", "b1")];

        let join = join(left, right, on)?;

        let columns = columns(&join.schema());
        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "c2"]);

        // first part
        let stream = join.execute(0).await?;
        let batches = common::collect(stream).await?;
        assert_eq!(batches.len(), 1);

        let result = format_batch(&batches[0]);
        let expected = vec!["1,4,7,10,70"];
        assert_same_rows(&result, &expected);

        // second part
        let stream = join.execute(1).await?;
        let batches = common::collect(stream).await?;
        assert_eq!(batches.len(), 1);
        let result = format_batch(&batches[0]);
        let expected = vec!["2,5,8,30,90", "3,5,9,30,90"];

        assert_same_rows(&result, &expected);

        Ok(())
    }
}
