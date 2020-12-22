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

use arrow::array::ArrayRef;
use std::sync::Arc;
use std::{any::Any, collections::HashSet};

use async_trait::async_trait;
use futures::{Stream, StreamExt, TryStreamExt};
use hashbrown::HashMap;
use tokio::sync::Mutex;

use arrow::array::{make_array, Array, MutableArrayData};
use arrow::datatypes::DataType;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;

use arrow::array::{
    Int16Array, Int32Array, Int64Array, Int8Array, StringArray, UInt16Array, UInt32Array,
    UInt64Array, UInt8Array,
};

use super::expressions::col;
use super::{
    hash_utils::{build_join_schema, check_join_is_valid, JoinOn, JoinType},
    merge::MergeExec,
};
use crate::error::{DataFusionError, Result};

use super::{ExecutionPlan, Partitioning, RecordBatchStream, SendableRecordBatchStream};
use ahash::RandomState;

// An index of (batch, row) uniquely identifying a row in a part.
type Index = (usize, usize);
// A pair (left index, right index)
// Note that while this is currently equal to `Index`, the `JoinIndex` is semantically different
// as a left join may issue None indices, in which case
type JoinIndex = Option<(usize, usize)>;
// An index of row uniquely identifying a row in a batch
type RightIndex = Option<usize>;

// Maps ["on" value] -> [list of indices with this key's value]
// E.g. [1, 2] -> [(0, 3), (1, 6), (0, 8)] indicates that (column1, column2) = [1, 2] is true
// for rows 3 and 8 from batch 0 and row 6 from batch 1.
type JoinHashMap = HashMap<Vec<u8>, Vec<Index>, RandomState>;
type JoinLeftData = Arc<(JoinHashMap, Vec<RecordBatch>)>;

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
    /// Build-side
    build_side: Arc<Mutex<Option<JoinLeftData>>>,
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
            join_type: *join_type,
            schema,
            build_side: Arc::new(Mutex::new(None)),
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
        // we only want to compute the build side once
        let left_data = {
            let mut build_side = self.build_side.lock().await;
            match build_side.as_ref() {
                Some(stream) => stream.clone(),
                None => {
                    // merge all left parts into a single stream
                    let merge = MergeExec::new(self.left.clone());
                    let stream = merge.execute(0).await?;

                    let on_left = self
                        .on
                        .iter()
                        .map(|on| on.0.clone())
                        .collect::<HashSet<_>>();

                    // This operation performs 2 steps at once:
                    // 1. creates a [JoinHashMap] of all batches from the stream
                    // 2. stores the batches in a vector.
                    let initial = (JoinHashMap::default(), Vec::new(), 0);
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

                    let left_side = Arc::new((left_data.0, left_data.1));
                    *build_side = Some(left_side.clone());
                    left_side
                }
            }
        };

        // we have the batches and the hash map with their keys. We can how create a stream
        // over the right that uses this information to issue new batches.

        let stream = self.right.execute(partition).await?;
        let on_right = self
            .on
            .iter()
            .map(|on| on.1.clone())
            .collect::<HashSet<_>>();
        Ok(Box::pin(HashJoinStream {
            schema: self.schema.clone(),
            on_right,
            join_type: self.join_type,
            left_data,
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

    // update the hash map
    for row in 0..batch.num_rows() {
        create_key(&keys_values, row, &mut key)?;

        hash.raw_entry_mut()
            .from_key(&key)
            .and_modify(|_, v| v.push((index, row)))
            .or_insert_with(|| (key.clone(), vec![(index, row)]));
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
    join_type: &JoinType,
    indices: &[(JoinIndex, RightIndex)],
) -> ArrowResult<RecordBatch> {
    if left.is_empty() {
        todo!("Create empty record batch");
    }
    // this is just for symmetry of the code below.
    let right = vec![right.clone()];

    let (primary_is_left, primary, secondary) = match join_type {
        JoinType::Inner | JoinType::Left => (true, left, &right),
        JoinType::Right => (false, &right, left),
    };

    // build the columns of the new [RecordBatch]:
    // 1. pick whether the column is from the left or right
    // 2. based on the pick, `take` items from the different recordBatches
    let mut columns: Vec<Arc<dyn Array>> = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        // pick the column (left or right) based on the field name.
        // Note that we take `.data()` to gather the [ArrayData] of each array.
        let (is_primary, arrays) = match primary[0].schema().index_of(field.name()) {
            Ok(i) => Ok((true, primary.iter().map(|batch| batch.column(i).data()).collect::<Vec<_>>())),
            Err(_) => {
                match secondary[0].schema().index_of(field.name()) {
                    Ok(i) => Ok((false, secondary.iter().map(|batch| batch.column(i).data()).collect::<Vec<_>>())),
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
        let mut mutable = MutableArrayData::new(arrays, true, capacity);

        let is_left =
            (is_primary && primary_is_left) || (!is_primary && !primary_is_left);
        if is_left {
            // use the left indices
            for (join_index, _) in indices {
                match join_index {
                    Some((batch, row)) => mutable.extend(*batch, *row, *row + 1),
                    None => mutable.extend_nulls(1),
                }
            }
        } else {
            // use the right indices
            for (_, join_index) in indices {
                match join_index {
                    Some(row) => mutable.extend(0, *row, *row + 1),
                    None => mutable.extend_nulls(1),
                }
            }
        };
        let array = make_array(Arc::new(mutable.freeze()));
        columns.push(array);
    }
    Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
}

/// Create a key `Vec<u8>` that is used as key for the hashmap
pub(crate) fn create_key(
    group_by_keys: &[ArrayRef],
    row: usize,
    vec: &mut Vec<u8>,
) -> Result<()> {
    vec.clear();
    for i in 0..group_by_keys.len() {
        let col = &group_by_keys[i];
        match col.data_type() {
            DataType::UInt8 => {
                let array = col.as_any().downcast_ref::<UInt8Array>().unwrap();
                vec.extend(array.value(row).to_le_bytes().iter());
            }
            DataType::UInt16 => {
                let array = col.as_any().downcast_ref::<UInt16Array>().unwrap();
                vec.extend(array.value(row).to_le_bytes().iter());
            }
            DataType::UInt32 => {
                let array = col.as_any().downcast_ref::<UInt32Array>().unwrap();
                vec.extend(array.value(row).to_le_bytes().iter());
            }
            DataType::UInt64 => {
                let array = col.as_any().downcast_ref::<UInt64Array>().unwrap();
                vec.extend(array.value(row).to_le_bytes().iter());
            }
            DataType::Int8 => {
                let array = col.as_any().downcast_ref::<Int8Array>().unwrap();
                vec.extend(array.value(row).to_le_bytes().iter());
            }
            DataType::Int16 => {
                let array = col.as_any().downcast_ref::<Int16Array>().unwrap();
                vec.extend(array.value(row).to_le_bytes().iter());
            }
            DataType::Int32 => {
                let array = col.as_any().downcast_ref::<Int32Array>().unwrap();
                vec.extend(array.value(row).to_le_bytes().iter());
            }
            DataType::Int64 => {
                let array = col.as_any().downcast_ref::<Int64Array>().unwrap();
                vec.extend(array.value(row).to_le_bytes().iter());
            }
            DataType::Utf8 => {
                let array = col.as_any().downcast_ref::<StringArray>().unwrap();
                let value = array.value(row);
                // store the size
                vec.extend(value.len().to_le_bytes().iter());
                // store the string value
                vec.extend(array.value(row).as_bytes().iter());
            }
            _ => {
                // This is internal because we should have caught this before.
                return Err(DataFusionError::Internal(
                    "Unsupported GROUP BY data type".to_string(),
                ));
            }
        }
    }
    Ok(())
}

fn build_batch(
    batch: &RecordBatch,
    left_data: &JoinLeftData,
    on_right: &HashSet<String>,
    join_type: &JoinType,
    schema: &Schema,
) -> ArrowResult<RecordBatch> {
    let indices = build_join_indexes(&left_data.0, &batch, join_type, on_right).unwrap();

    build_batch_from_indices(schema, &left_data.1, &batch, join_type, &indices)
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
    right: &RecordBatch,
    join_type: &JoinType,
    right_on: &HashSet<String>,
) -> Result<Vec<(JoinIndex, RightIndex)>> {
    let keys_values = right_on
        .iter()
        .map(|name| Ok(col(name).evaluate(right)?.into_array(right.num_rows())))
        .collect::<Result<Vec<_>>>()?;

    let mut key = Vec::with_capacity(keys_values.len());

    match join_type {
        JoinType::Inner => {
            let mut indexes = Vec::new(); // unknown a prior size

            // Visit all of the right rows
            for row in 0..right.num_rows() {
                // Get the key and find it in the build index
                create_key(&keys_values, row, &mut key)?;
                let left_indexes = left.get(&key);

                // for every item on the left and right with this key, add the respective pair
                left_indexes.unwrap_or(&vec![]).iter().for_each(|x| {
                    // on an inner join, left and right indices are present
                    indexes.push((Some(*x), Some(row)));
                })
            }
            Ok(indexes)
        }
        JoinType::Left => {
            let mut indexes = Vec::new(); // unknown a prior size

            // Keep track of which item is visited in the build input
            // TODO: this can be stored more efficiently with a marker
            let mut is_visited = HashSet::new();

            // First visit all of the rows
            for row in 0..right.num_rows() {
                create_key(&keys_values, row, &mut key)?;
                let left_indexes = left.get(&key);

                if let Some(indices) = left_indexes {
                    is_visited.insert(key.clone());

                    indices.iter().for_each(|x| {
                        indexes.push((Some(*x), Some(row)));
                    })
                };
            }
            // Add the remaining left rows to the result set with None on the right side
            for (key, indices) in left {
                if !is_visited.contains(key) {
                    indices.iter().for_each(|x| {
                        indexes.push((Some(*x), None));
                    });
                }
            }

            Ok(indexes)
        }
        JoinType::Right => {
            let mut indexes = Vec::new(); // unknown a prior size
            for row in 0..right.num_rows() {
                create_key(&keys_values, row, &mut key)?;

                let left_indices = left.get(&key);

                match left_indices {
                    Some(indices) => {
                        indices.iter().for_each(|x| {
                            indexes.push((Some(*x), Some(row)));
                        });
                    }
                    None => {
                        // when no match, add the row with None for the left side
                        indexes.push((None, Some(row)));
                    }
                }
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
        join_type: &JoinType,
    ) -> Result<HashJoinExec> {
        let on: Vec<_> = on
            .iter()
            .map(|(l, r)| (l.to_string(), r.to_string()))
            .collect();
        HashJoinExec::try_new(left, right, &on, join_type)
    }

    /// Asserts that the rows are the same, taking into account that their order
    /// is irrelevant
    fn assert_same_rows(result: &[String], expected: &[&str]) {
        // convert to set since row order is irrelevant
        let result = result.iter().cloned().collect::<HashSet<_>>();

        let expected = expected
            .iter()
            .map(|s| s.to_string())
            .collect::<HashSet<_>>();
        assert_eq!(result, expected);
    }

    #[tokio::test]
    async fn join_inner_one() -> Result<()> {
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

        let join = join(left, right, on, &JoinType::Inner)?;

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
    async fn join_inner_one_no_shared_column_names() -> Result<()> {
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

        let join = join(left, right, on, &JoinType::Inner)?;

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
    async fn join_inner_two() -> Result<()> {
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

        let join = join(left, right, on, &JoinType::Inner)?;

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
    async fn join_inner_one_two_parts_left() -> Result<()> {
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

        let join = join(left, right, on, &JoinType::Inner)?;

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
    async fn join_inner_one_two_parts_right() -> Result<()> {
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

        let join = join(left, right, on, &JoinType::Inner)?;

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

    #[tokio::test]
    async fn join_left_one() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![4, 5, 7]), // 7 does not exist on the right
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![4, 5, 6]),
            ("c2", &vec![70, 80, 90]),
        );
        let on = &[("b1", "b1")];

        let join = join(left, right, on, &JoinType::Left)?;

        let columns = columns(&join.schema());
        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "c2"]);

        let stream = join.execute(0).await?;
        let batches = common::collect(stream).await?;

        let result = format_batch(&batches[0]);
        let expected = vec!["1,4,7,10,70", "2,5,8,20,80", "3,7,9,NULL,NULL"];

        assert_same_rows(&result, &expected);

        Ok(())
    }

    #[tokio::test]
    async fn join_right_one() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![4, 5, 7]),
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![4, 5, 6]), // 6 does not exist on the left
            ("c2", &vec![70, 80, 90]),
        );
        let on = &[("b1", "b1")];

        let join = join(left, right, on, &JoinType::Right)?;

        let columns = columns(&join.schema());
        assert_eq!(columns, vec!["a1", "c1", "a2", "b1", "c2"]);

        let stream = join.execute(0).await?;
        let batches = common::collect(stream).await?;

        let result = format_batch(&batches[0]);
        let expected = vec!["1,7,10,4,70", "2,8,20,5,80", "NULL,NULL,30,6,90"];

        assert_same_rows(&result, &expected);

        Ok(())
    }
}
