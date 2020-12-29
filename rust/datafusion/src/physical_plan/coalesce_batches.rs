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

use arrow::datatypes::SchemaRef;
use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::stream::{Stream, StreamExt};

#[derive(Debug)]
struct CoalesceBatchesExec {
    /// The input plan
    input: Arc<dyn ExecutionPlan>,
    /// Minimum number of rows for coalesces batches
    target_batch_size: usize,
}

impl CoalesceBatchesExec {
    fn new(input: Arc<dyn ExecutionPlan>, target_batch_size: usize) -> Self {
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
}

impl Stream for CoalesceBatchesStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.input.poll_next_unpin(cx).map(|x| match x {
            Some(Ok(batch)) => {
                unimplemented!()
            }
            other => other,
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // same number of record batches
        self.input.size_hint()
    }
}

impl RecordBatchStream for CoalesceBatchesStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
