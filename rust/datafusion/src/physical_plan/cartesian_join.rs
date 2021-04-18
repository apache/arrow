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

use futures::StreamExt;
use std::{any::Any, sync::Arc};

use arrow::datatypes::{Schema, SchemaRef};
use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;
use futures::{Stream, TryStreamExt};

use futures::lock::Mutex;

use super::{hash_utils::check_join_is_valid, merge::MergeExec};
use crate::{
    error::{DataFusionError, Result},
    scalar::ScalarValue,
};
use async_trait::async_trait;
use std::time::Instant;

use super::{ExecutionPlan, Partitioning, RecordBatchStream, SendableRecordBatchStream};
use crate::physical_plan::coalesce_batches::concat_batches;
use log::debug;

/// Data of the left side
type JoinLeftData = Vec<RecordBatch>;

/// executes partitions in parallel and combines them into a set of
/// partitions by combining all values from the left with all values on the right
#[derive(Debug)]
pub struct CartesianJoinExec {
    /// left (build) side which gets loaded in memory
    left: Arc<dyn ExecutionPlan>,
    /// right (probe) side which are combined with left side
    right: Arc<dyn ExecutionPlan>,
    /// The schema once the join is applied
    schema: SchemaRef,
    /// Build-side data
    build_side: Arc<Mutex<Option<JoinLeftData>>>,
}

impl CartesianJoinExec {
    /// Tries to create a new [CartesianJoinExec].
    /// # Error
    /// This function errors when it is not possible to join the left and right sides on keys `on`.
    pub fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        let left_schema = left.schema();
        let right_schema = right.schema();
        check_join_is_valid(&left_schema, &right_schema, &[])?;

        let left_schema = left.schema();
        let left_fields = left_schema.fields().iter();
        let right_schema = left.schema();

        let right_fields = right_schema.fields().iter();

        // left then right
        let all_columns = left_fields.chain(right_fields).cloned().collect();

        let schema = Arc::new(Schema::new(all_columns));

        Ok(CartesianJoinExec {
            left,
            right,
            schema,
            build_side: Arc::new(Mutex::new(None)),
        })
    }

    /// left (build) side which gets hashed
    pub fn left(&self) -> &Arc<dyn ExecutionPlan> {
        &self.left
    }

    /// right (probe) side which are filtered by the hash table
    pub fn right(&self) -> &Arc<dyn ExecutionPlan> {
        &self.right
    }
}

#[async_trait]
impl ExecutionPlan for CartesianJoinExec {
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
            2 => Ok(Arc::new(CartesianJoinExec::try_new(
                children[0].clone(),
                children[1].clone(),
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
        // we only want to compute the build side once for PartitionMode::CollectLeft
        let left_data = {
            let mut build_side = self.build_side.lock().await;

            match build_side.as_ref() {
                Some(stream) => stream.clone(),
                None => {
                    let start = Instant::now();

                    // merge all left parts into a single stream
                    let merge = MergeExec::new(self.left.clone());
                    let stream = merge.execute(0).await?;

                    // This operation performs 2 steps at once:
                    // 1. creates a [JoinHashMap] of all batches from the stream
                    // 2. stores the batches in a vector.
                    let (batches, num_rows) = stream
                        .try_fold((Vec::new(), 0 as usize), |mut acc, batch| async {
                            acc.1 += batch.num_rows();
                            acc.0.push(batch);
                            Ok(acc)
                        })
                        .await?;

                    *build_side = Some(batches.clone());

                    debug!(
                        "Built build-side of cartesian join containing {} rows in {} ms",
                        num_rows,
                        start.elapsed().as_millis()
                    );

                    batches
                }
            }
        };

        // we have the batches and the hash map with their keys. We can how create a stream
        // over the right that uses this information to issue new batches.

        let stream = self.right.execute(partition).await?;

        Ok(Box::pin(CartesianJoinStream {
            schema: self.schema.clone(),
            left_data,
            right: stream,
            num_input_batches: 0,
            num_input_rows: 0,
            num_output_batches: 0,
            num_output_rows: 0,
            join_time: 0,
        }))
    }
}

/// A stream that issues [RecordBatch]es as they arrive from the right  of the join.
struct CartesianJoinStream {
    /// Input schema
    schema: Arc<Schema>,
    /// data from the left side
    left_data: JoinLeftData,
    /// right
    right: SendableRecordBatchStream,
    /// number of input batches
    num_input_batches: usize,
    /// number of input rows
    num_input_rows: usize,
    /// number of batches produced
    num_output_batches: usize,
    /// number of rows produced
    num_output_rows: usize,
    /// total time for joining probe-side batches to the build-side batches
    join_time: usize,
}

impl RecordBatchStream for CartesianJoinStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
fn build_batch(
    batch: &RecordBatch,
    left_data: &JoinLeftData,
    schema: &Schema,
) -> Result<RecordBatch> {
    let mut batches = Vec::new();
    let mut num_rows = 0;
    for left in left_data.iter() {
        for x in 0..batch.num_rows() {
            // for each value on the left, repeat the value of the right
            let arrays = batch
                .columns()
                .iter()
                .map(|arr| {
                    let scalar = ScalarValue::try_from_array(arr, x)?;
                    Ok(scalar.to_array_of_size(left.num_rows()))
                })
                .collect::<Result<Vec<_>>>()?;

            let batch = RecordBatch::try_new(
                Arc::new(schema.clone()),
                arrays
                    .iter()
                    .chain(left.columns().iter())
                    .cloned()
                    .collect(),
            )?;
            batches.push(batch);
            num_rows += left.num_rows();
        }
    }
    Ok(concat_batches(&Arc::new(schema.clone()), &batches, num_rows).unwrap())
}

impl Stream for CartesianJoinStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.right
            .poll_next_unpin(cx)
            .map(|maybe_batch| match maybe_batch {
                Some(Ok(batch)) => {
                    let start = Instant::now();
                    let result = build_batch(&batch, &self.left_data, &self.schema);
                    self.num_input_batches += 1;
                    self.num_input_rows += batch.num_rows();
                    if let Ok(ref batch) = result {
                        self.join_time += start.elapsed().as_millis() as usize;
                        self.num_output_batches += 1;
                        self.num_output_rows += batch.num_rows();
                    }
                    Some(result.map_err(|x| x.into_arrow_external_error()))
                }
                other => {
                    debug!(
                        "Processed {} probe-side input batches containing {} rows and \
                        produced {} output batches containing {} rows in {} ms",
                        self.num_input_batches,
                        self.num_input_rows,
                        self.num_output_batches,
                        self.num_output_rows,
                        self.join_time
                    );
                    other
                }
            })
    }
}
