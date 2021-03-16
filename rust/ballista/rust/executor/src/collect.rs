// Copyright 2020 Andy Grove
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The CollectExec operator retrieves results from the cluster and returns them as a single
//! vector of [RecordBatch].

use std::sync::Arc;
use std::task::{Context, Poll};
use std::{any::Any, pin::Pin};

use arrow::datatypes::SchemaRef;
use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::{ExecutionPlan, Partitioning, SendableRecordBatchStream};
use datafusion::{error::Result, physical_plan::RecordBatchStream};
use futures::stream::SelectAll;
use futures::Stream;

/// The CollectExec operator retrieves results from the cluster and returns them as a single
/// vector of [RecordBatch].
#[derive(Debug, Clone)]
pub struct CollectExec {
    plan: Arc<dyn ExecutionPlan>,
}

impl CollectExec {
    pub fn new(plan: Arc<dyn ExecutionPlan>) -> Self {
        Self { plan }
    }
}

#[async_trait]
impl ExecutionPlan for CollectExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.plan.schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.plan.clone()]
    }

    fn with_new_children(
        &self,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        unimplemented!()
    }

    async fn execute(
        &self,
        partition: usize,
    ) -> Result<Pin<Box<dyn RecordBatchStream + Send + Sync>>> {
        assert_eq!(0, partition);
        let num_partitions = self.plan.output_partitioning().partition_count();

        let mut futures = Vec::with_capacity(num_partitions);
        for i in 0..num_partitions {
            futures.push(self.plan.execute(i));
        }

        let mut streams = Vec::with_capacity(num_partitions);
        for result in futures::future::join_all(futures).await {
            match result {
                Ok(stream) => {
                    streams.push(stream);
                }
                Err(e) => {
                    return Err(DataFusionError::Execution(format!(
                        "BallistaError: {:?}",
                        e
                    )));
                }
            }
        }

        Ok(Box::pin(MergedRecordBatchStream {
            schema: self.schema(),
            select_all: Box::pin(futures::stream::select_all(streams)),
        }))
    }
}

struct MergedRecordBatchStream {
    schema: SchemaRef,
    select_all: Pin<Box<SelectAll<SendableRecordBatchStream>>>,
}

impl Stream for MergedRecordBatchStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.select_all.as_mut().poll_next(cx)
    }
}

impl RecordBatchStream for MergedRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
