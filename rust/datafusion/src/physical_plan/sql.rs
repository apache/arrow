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
use std::sync::Arc;
use std::task::{Context, Poll};
use std::{fmt, thread};

use super::{RecordBatchStream, SendableRecordBatchStream};
use crate::error::{DataFusionError, Result};
use crate::physical_plan::ExecutionPlan;
use crate::physical_plan::Partitioning;
use arrow::datatypes::SchemaRef;
use arrow::error::{ArrowError, Result as ArrowResult};
use arrow::record_batch::RecordBatch;
use arrow_sql::postgres::PostgresReadIterator;
use crossbeam::channel::{bounded, Receiver, RecvError, Sender};
use fmt::Debug;

use async_trait::async_trait;
use futures::stream::Stream;

/// Execution plan for scanning a SQL data source
#[derive(Debug, Clone)]
pub struct SqlExec {
    /// SQL connection
    connection: String,
    /// SQL query
    query: String,
    /// Schema after projection is applied
    schema: SchemaRef,
    /// Projection for which columns to load
    projection: Vec<usize>,
    /// Batch size
    batch_size: usize,
}

impl SqlExec {
    /// Create a new SQL reader execution plan
    pub fn try_new(
        connection: &str,
        query: &str,
        projection: Option<Vec<usize>>,
        batch_size: usize,
    ) -> Result<Self> {
        // TODO: we could/should determine the type of database at this point
        let reader =
            PostgresReadIterator::try_new(connection, query, Some(1), batch_size)?;
        let schema = Arc::new(reader.schema().clone());
        let projection =
            projection.unwrap_or_else(|| (0..schema.fields().len()).collect());

        Ok(Self {
            connection: connection.to_string(),
            query: query.to_string(),
            schema,
            projection,
            batch_size,
        })
    }
}

#[async_trait]
impl ExecutionPlan for SqlExec {
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
        // TODO: we could determine the record length (via a cancellable query?)
        // then use this to determine partitioning
        Partitioning::UnknownPartitioning(1)
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

    async fn execute(&self, _partition: usize) -> Result<SendableRecordBatchStream> {
        // each connection should be created on its own thread
        let (response_tx, response_rx): (
            Sender<Option<ArrowResult<RecordBatch>>>,
            Receiver<Option<ArrowResult<RecordBatch>>>,
        ) = bounded(2);

        let connection = self.connection.clone();
        let query = self.query.clone();
        let projection = self.projection.clone();
        let batch_size = self.batch_size;

        thread::spawn(move || {
            if let Err(e) =
                run_query(&connection, &query, projection, batch_size, response_tx)
            {
                println!("Parquet reader thread terminated due to error: {:?}", e);
            }
        });

        Ok(Box::pin(SqlStream {
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

fn run_query(
    connection: &str,
    query: &str,
    _projection: Vec<usize>,
    batch_size: usize,
    response_tx: Sender<Option<ArrowResult<RecordBatch>>>,
) -> Result<()> {
    let mut batch_reader =
        PostgresReadIterator::try_new(connection, query, None, batch_size)?;
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
                    format!("Error reading batch from SQL query: {}", e.to_string());
                // send error to operator
                send_result(
                    &response_tx,
                    Some(Err(ArrowError::SqlError(err_msg.clone()))),
                )?;
                // terminate thread with error
                return Err(DataFusionError::Execution(err_msg));
            }
        }
    }
    Ok(())
}

struct SqlStream {
    schema: SchemaRef,
    response_rx: Receiver<Option<ArrowResult<RecordBatch>>>,
}

impl Stream for SqlStream {
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

impl RecordBatchStream for SqlStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    #[tokio::test]
    async fn test() -> Result<()> {
        let connection = "";
        let query = "";
        let sql_exec = SqlExec::try_new(connection, query, None, 1024)?;
        assert_eq!(sql_exec.output_partitioning().partition_count(), 1);

        let mut results = sql_exec.execute(0).await?;
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
