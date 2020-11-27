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

//! Defines the merge plan for executing partitions in parallel and then merging the results
//! into a single partition

use std::any::Any;
use std::sync::Arc;

use futures::channel::mpsc;
use futures::sink::SinkExt;
use futures::stream::StreamExt;
use futures::Stream;

use async_trait::async_trait;

use arrow::record_batch::RecordBatch;
use arrow::{
    datatypes::SchemaRef,
    error::{ArrowError, Result as ArrowResult},
};

use super::RecordBatchStream;
use crate::error::{DataFusionError, Result};
use crate::physical_plan::ExecutionPlan;
use crate::physical_plan::Partitioning;

use super::SendableRecordBatchStream;
use pin_project_lite::pin_project;

/// Merge execution plan executes partitions in parallel and combines them into a single
/// partition. No guarantees are made about the order of the resulting partition.
#[derive(Debug)]
pub struct MergeExec {
    /// Input execution plan
    input: Arc<dyn ExecutionPlan>,
}

impl MergeExec {
    /// Create a new MergeExec
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        MergeExec { input }
    }
}

#[async_trait]
impl ExecutionPlan for MergeExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match children.len() {
            1 => Ok(Arc::new(MergeExec::new(children[0].clone()))),
            _ => Err(DataFusionError::Internal(
                "MergeExec wrong number of children".to_string(),
            )),
        }
    }

    async fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream> {
        // MergeExec produces a single partition
        if 0 != partition {
            return Err(DataFusionError::Internal(format!(
                "MergeExec invalid partition {}",
                partition
            )));
        }

        let input_partitions = self.input.output_partitioning().partition_count();
        match input_partitions {
            0 => Err(DataFusionError::Internal(
                "MergeExec requires at least one input partition".to_owned(),
            )),
            1 => {
                // bypass any threading if there is a single partition
                self.input.execute(0).await
            }
            _ => {
                // use a stream that allows each sender to put in at
                // least one result in an attempt to maximize
                // parallelism.
                let (sender, receiver) =
                    mpsc::channel::<ArrowResult<RecordBatch>>(input_partitions);

                // spawn independent tasks whose resulting streams (of batches)
                // are sent to the channel for consumption.
                for part_i in (0..input_partitions) {
                    let input = self.input.clone();
                    let mut sender = sender.clone();
                    tokio::spawn(async move {
                        let mut stream = match input.execute(part_i).await {
                            Err(e) => {
                                // If send fails, plan being torn
                                // down, no place to send the error
                                let arrow_error = ArrowError::ExternalError(Box::new(e));
                                sender.send(Err(arrow_error)).await.ok();
                                return;
                            }
                            Ok(stream) => stream,
                        };

                        while let Some(item) = stream.next().await {
                            // If send fails, plan being torn down,
                            // there is no place to send the error
                            sender.send(item).await.ok();
                        }
                    });
                }

                Ok(Box::pin(MergeStream {
                    input: receiver,
                    schema: self.schema(),
                }))
            }
        }
    }
}

pin_project! {
    struct MergeStream {
        schema: SchemaRef,
        #[pin]
        input: mpsc::Receiver<ArrowResult<RecordBatch>>,
    }
}

impl Stream for MergeStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.project();
        this.input.poll_next(cx)
    }
}

impl RecordBatchStream for MergeStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::physical_plan::common;
    use crate::physical_plan::csv::{CsvExec, CsvReadOptions};
    use crate::test;

    #[tokio::test]
    async fn merge() -> Result<()> {
        let schema = test::aggr_test_schema();

        let num_partitions = 4;
        let path =
            test::create_partitioned_csv("aggregate_test_100.csv", num_partitions)?;

        let csv =
            CsvExec::try_new(&path, CsvReadOptions::new().schema(&schema), None, 1024)?;

        // input should have 4 partitions
        assert_eq!(csv.output_partitioning().partition_count(), num_partitions);

        let merge = MergeExec::new(Arc::new(csv));

        // output of MergeExec should have a single partition
        assert_eq!(merge.output_partitioning().partition_count(), 1);

        // the result should contain 4 batches (one per input partition)
        let iter = merge.execute(0).await?;
        let batches = common::collect(iter).await?;
        assert_eq!(batches.len(), num_partitions);

        // there should be a total of 100 rows
        let row_count: usize = batches.iter().map(|batch| batch.num_rows()).sum();
        assert_eq!(row_count, 100);

        Ok(())
    }
}
