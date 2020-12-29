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

//! CoalesceBatchesExec combines small batches into larger batches for more efficient use of
//! vectorized processing by upstream operators.

use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::error::{DataFusionError, Result};
use crate::physical_plan::{
    ExecutionPlan, Partitioning, RecordBatchStream, SendableRecordBatchStream,
};

use arrow::array::{make_array, MutableArrayData};
use arrow::datatypes::SchemaRef;
use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::stream::{Stream, StreamExt};
use log::debug;

/// CoalesceBatchesExec combines small batches into larger batches for more efficient use of
/// vectorized processing by upstream operators.
#[derive(Debug)]
pub struct CoalesceBatchesExec {
    /// The input plan
    input: Arc<dyn ExecutionPlan>,
    /// Minimum number of rows for coalesces batches
    target_batch_size: usize,
}

impl CoalesceBatchesExec {
    /// Create a new CoalesceBatchesExec
    pub fn new(input: Arc<dyn ExecutionPlan>, target_batch_size: usize) -> Self {
        Self {
            input,
            target_batch_size,
        }
    }
}

#[async_trait]
impl ExecutionPlan for CoalesceBatchesExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Get the schema for this execution plan
    fn schema(&self) -> SchemaRef {
        // The coalesce batches operator does not make any changes to the schema of its input
        self.input.schema()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        // The coalesce batches operator does not make any changes to the partitioning of its input
        self.input.output_partitioning()
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match children.len() {
            1 => Ok(Arc::new(CoalesceBatchesExec::new(
                children[0].clone(),
                self.target_batch_size,
            ))),
            _ => Err(DataFusionError::Internal(
                "CoalesceBatchesExec wrong number of children".to_string(),
            )),
        }
    }

    async fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(CoalesceBatchesStream {
            input: self.input.execute(partition).await?,
            schema: self.input.schema().clone(),
            target_batch_size: self.target_batch_size.clone(),
            buffer: Vec::new(),
            buffered_rows: 0,
        }))
    }
}

struct CoalesceBatchesStream {
    /// The input plan
    input: SendableRecordBatchStream,
    /// The input schema
    schema: SchemaRef,
    /// Minimum number of rows for coalesces batches
    target_batch_size: usize,
    /// Buffered batches
    buffer: Vec<RecordBatch>,
    /// Buffered row count
    buffered_rows: usize,
}

impl Stream for CoalesceBatchesStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            let input_batch = self.input.poll_next_unpin(cx);
            match input_batch {
                Poll::Ready(x) => match x {
                    Some(Ok(ref batch)) => {
                        if batch.num_rows() >= self.target_batch_size
                            && self.buffer.is_empty()
                        {
                            return Poll::Ready(Some(Ok(batch.clone())));
                        } else if batch.num_rows() == 0 {
                            // discard empty batches
                        } else {
                            // add to the buffered batches
                            self.buffer.push(batch.clone());
                            self.buffered_rows += batch.num_rows();
                            // check to see if we have enough batches yet
                            if self.buffered_rows >= self.target_batch_size {
                                // combine the batches and return
                                let batch = concat_batches(
                                    &self.schema,
                                    &self.buffer,
                                    self.buffered_rows,
                                )?;
                                // reset buffer state
                                self.buffer.clear();
                                self.buffered_rows = 0;
                                // return batch
                                return Poll::Ready(Some(Ok(batch)));
                            }
                        }
                    }
                    None => {
                        // we have reached the end of the input stream but there could still
                        // be buffered batches
                        if self.buffer.is_empty() {
                            return Poll::Ready(None);
                        } else {
                            // combine the batches and return
                            let batch = concat_batches(
                                &self.schema,
                                &self.buffer,
                                self.buffered_rows,
                            )?;
                            // reset buffer state
                            self.buffer.clear();
                            self.buffered_rows = 0;
                            // return batch
                            return Poll::Ready(Some(Ok(batch)));
                        }
                    }
                    other => return Poll::Ready(other),
                },
                Poll::Pending => return Poll::Pending,
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // we can't predict the size of incoming batches so re-use the size hint from the input
        self.input.size_hint()
    }
}

impl RecordBatchStream for CoalesceBatchesStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

fn concat_batches(
    schema: &SchemaRef,
    batches: &[RecordBatch],
    row_count: usize,
) -> ArrowResult<RecordBatch> {
    let mut arrays = Vec::with_capacity(schema.fields().len());
    for i in 0..schema.fields().len() {
        let source_arrays = batches
            .iter()
            .map(|batch| batch.column(i).data_ref().as_ref())
            .collect();
        let mut array_data = MutableArrayData::new(
            source_arrays,
            true, //TODO
            row_count,
        );
        for j in 0..batches.len() {
            array_data.extend(j, 0, batches[j].num_rows());
        }
        let data = array_data.freeze();
        arrays.push(make_array(Arc::new(data)));
    }
    debug!(
        "Combined {} batches containing {} rows",
        batches.len(),
        row_count
    );
    RecordBatch::try_new(schema.clone(), arrays)
}
