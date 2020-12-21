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

///! The repartition operator maps N input partitions to M output partitions based on a
///! partitioning scheme.
use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::error::{DataFusionError, Result};
use crate::physical_plan::{ExecutionPlan, Partitioning, PhysicalExpr};
use arrow::datatypes::SchemaRef;
use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;

use super::{RecordBatchStream, SendableRecordBatchStream};
use async_trait::async_trait;

use futures::stream::Stream;

/// Partitioning schemes
#[derive(Debug, Clone)]
pub enum PartitioningScheme {
    /// Allocate batches using a round-robin algorithm
    RoundRobinBatch,
    /// Allocate rows using a round-robin algorithm. This provides finer-grained partitioning
    /// than `RoundRobinBatch` but also has much more overhead.
    RoundRobinRow,
    /// Allocate rows based on a hash of one of more expressions
    Hash(Vec<Arc<dyn PhysicalExpr>>),
}

/// partition. No guarantees are made about the order of the resulting partition.
#[derive(Debug)]
pub struct RepartitionExec {
    /// Input execution plan
    input: Arc<dyn ExecutionPlan>,
    /// Partitioning scheme to use
    partitioning_scheme: PartitioningScheme,
    /// Number of output partitions
    num_partitions: usize,
}

#[async_trait]
impl ExecutionPlan for RepartitionExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Get the schema for this execution plan
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match children.len() {
            1 => Ok(Arc::new(RepartitionExec::try_new(
                children[0].clone(),
                self.partitioning_scheme.clone(),
                self.num_partitions,
            )?)),
            _ => Err(DataFusionError::Internal(
                "RepartitionExec wrong number of children".to_string(),
            )),
        }
    }

    fn output_partitioning(&self) -> Partitioning {
        //TODO needs more work
        Partitioning::UnknownPartitioning(self.num_partitions)
    }

    async fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream> {
        // lock mutex
        // if first call to this method {
        //   create one channel per *output* partition
        //   launch one async task per *input* partition
        // }

        // now return stream for the specified *output* partition which will
        // read from the channel

        Ok(Box::pin(RepartitionStream {
            schema: self.input.schema(),
            //input: the *output* channel to read from
        }))
    }
}

impl RepartitionExec {
    /// Create a new MergeExec
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        partioning_scheme: PartitioningScheme,
        num_partitions: usize,
    ) -> Result<Self> {
        match &partioning_scheme {
            PartitioningScheme::RoundRobinBatch => Ok(RepartitionExec {
                input,
                partitioning_scheme: partioning_scheme,
                num_partitions,
            }),
            other => Err(DataFusionError::NotImplemented(format!(
                "Partitioning scheme not supported yet: {:?}",
                other
            ))),
        }
    }

    async fn process_input_partition(&self, partition: usize) -> Result<()> {
        let input = self.input.execute(partition).await?;
        // for each input batch {
        //   compute output partition based on partitioning schema
        //     send batch to the appropriate output channel, or split batch into
        //     multiple batches if using row-based partitioning
        //   }
        // }
        Ok(())
    }
}

struct RepartitionStream {
    schema: SchemaRef,
    //input: Channel to read
}

impl Stream for RepartitionStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        unimplemented!()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        unimplemented!()
    }
}

impl RecordBatchStream for RepartitionStream {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
