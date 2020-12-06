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

//! Execution plan for reading Parquet files

use std::any::Any;
use std::fs::File;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::{fmt, thread};

use super::{RecordBatchStream, SendableRecordBatchStream};
use crate::error::{DataFusionError, Result};
use crate::physical_plan::ExecutionPlan;
use crate::physical_plan::{common, Partitioning};
use arrow::datatypes::{Schema, SchemaRef};
use arrow::error::{ArrowError, Result as ArrowResult};
use arrow::record_batch::RecordBatch;
use parquet::file::reader::SerializedFileReader;

use crossbeam::channel::{bounded, Receiver, RecvError, Sender};
use fmt::Debug;
use parquet::arrow::{ArrowReader, ParquetFileArrowReader};

use async_trait::async_trait;
use futures::stream::Stream;

/// Execution plan for scanning a Parquet file
#[derive(Debug, Clone)]
pub struct ParquetExec {
    /// Path to directory containing partitioned Parquet files with the same schema
    filenames: Vec<String>,
    /// Schema after projection is applied
    schema: SchemaRef,
    /// Projection for which columns to load
    projection: Vec<usize>,
    /// Batch size
    batch_size: usize,
}

impl ParquetExec {
    /// Create a new Parquet reader execution plan
    pub fn try_new(
        path: &str,
        projection: Option<Vec<usize>>,
        batch_size: usize,
    ) -> Result<Self> {
        let mut filenames: Vec<String> = vec![];
        common::build_file_list(path, &mut filenames, ".parquet")?;
        if filenames.is_empty() {
            Err(DataFusionError::Plan("No files found".to_string()))
        } else {
            let file = File::open(&filenames[0])?;
            let file_reader = Arc::new(SerializedFileReader::new(file)?);
            let mut arrow_reader = ParquetFileArrowReader::new(file_reader);
            let schema = arrow_reader.get_schema()?;

            Ok(Self::new(filenames, schema, projection, batch_size))
        }
    }

    /// Create a new Parquet reader execution plan with provided files and schema
    pub fn new(
        filenames: Vec<String>,
        schema: Schema,
        projection: Option<Vec<usize>>,
        batch_size: usize,
    ) -> Self {
        let projection = match projection {
            Some(p) => p,
            None => (0..schema.fields().len()).collect(),
        };

        let projected_schema = Schema::new(
            projection
                .iter()
                .map(|i| schema.field(*i).clone())
                .collect(),
        );

        Self {
            filenames,
            schema: Arc::new(projected_schema),
            projection,
            batch_size,
        }
    }
}

#[async_trait]
impl ExecutionPlan for ParquetExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        // this is a leaf node and has no children
        vec![]
    }

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.filenames.len())
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(Arc::new(self.clone()))
        } else {
            Err(DataFusionError::Internal(format!(
                "Children cannot be replaced in {:?}",
                self
            )))
        }
    }

    async fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream> {
        // because the parquet implementation is not thread-safe, it is necessary to execute
        // on a thread and communicate with channels
        let (response_tx, response_rx): (
            Sender<Option<ArrowResult<RecordBatch>>>,
            Receiver<Option<ArrowResult<RecordBatch>>>,
        ) = bounded(2);

        let filename = self.filenames[partition].clone();
        let projection = self.projection.clone();
        let batch_size = self.batch_size;

        thread::spawn(move || {
            if let Err(e) = read_file(&filename, projection, batch_size, response_tx) {
                println!("Parquet reader thread terminated due to error: {:?}", e);
            }
        });

        Ok(Box::pin(ParquetStream {
            schema: self.schema.clone(),
            response_rx,
        }))
    }
}

fn send_result(
    response_tx: &Sender<Option<ArrowResult<RecordBatch>>>,
    result: Option<ArrowResult<RecordBatch>>,
) -> Result<()> {
    response_tx
        .send(result)
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;
    Ok(())
}

fn read_file(
    filename: &str,
    projection: Vec<usize>,
    batch_size: usize,
    response_tx: Sender<Option<ArrowResult<RecordBatch>>>,
) -> Result<()> {
    let file = File::open(&filename)?;
    let file_reader = Arc::new(SerializedFileReader::new(file)?);
    let mut arrow_reader = ParquetFileArrowReader::new(file_reader);
    let mut batch_reader =
        arrow_reader.get_record_reader_by_columns(projection, batch_size)?;
    loop {
        match batch_reader.next() {
            Some(Ok(batch)) => send_result(&response_tx, Some(Ok(batch)))?,
            None => {
                // finished reading file
                send_result(&response_tx, None)?;
                break;
            }
            Some(Err(e)) => {
                let err_msg =
                    format!("Error reading batch from {}: {}", filename, e.to_string());
                // send error to operator
                send_result(
                    &response_tx,
                    Some(Err(ArrowError::ParquetError(err_msg.clone()))),
                )?;
                // terminate thread with error
                return Err(DataFusionError::Execution(err_msg));
            }
        }
    }
    Ok(())
}

struct ParquetStream {
    schema: SchemaRef,
    response_rx: Receiver<Option<ArrowResult<RecordBatch>>>,
}

impl Stream for ParquetStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match self.response_rx.recv() {
            Ok(batch) => Poll::Ready(batch),
            // RecvError means receiver has exited and closed the channel
            Err(RecvError) => Poll::Ready(None),
        }
    }
}

impl RecordBatchStream for ParquetStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use std::env;

    #[tokio::test]
    async fn test() -> Result<()> {
        let testdata =
            env::var("PARQUET_TEST_DATA").expect("PARQUET_TEST_DATA not defined");
        let filename = format!("{}/alltypes_plain.parquet", testdata);
        let parquet_exec = ParquetExec::try_new(&filename, Some(vec![0, 1, 2]), 1024)?;
        assert_eq!(parquet_exec.output_partitioning().partition_count(), 1);

        let mut results = parquet_exec.execute(0).await?;
        let batch = results.next().await.unwrap()?;

        assert_eq!(8, batch.num_rows());
        assert_eq!(3, batch.num_columns());

        let schema = batch.schema();
        let field_names: Vec<&str> =
            schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(vec!["id", "bool_col", "tinyint_col"], field_names);

        let batch = results.next().await;
        assert!(batch.is_none());

        let batch = results.next().await;
        assert!(batch.is_none());

        let batch = results.next().await;
        assert!(batch.is_none());

        Ok(())
    }
}
