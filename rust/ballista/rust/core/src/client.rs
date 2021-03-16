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

//! Client API for sending requests to executors.

use std::sync::Arc;
use std::{collections::HashMap, pin::Pin};
use std::{
    convert::{TryFrom, TryInto},
    task::{Context, Poll},
};

use crate::error::{ballista_error, BallistaError, Result};
use crate::memory_stream::MemoryStream;
use crate::serde::protobuf::{self};
use crate::serde::scheduler::{
    Action, ExecutePartition, ExecutePartitionResult, PartitionId, PartitionStats,
};

use arrow::record_batch::RecordBatch;
use arrow::{
    array::{StringArray, StructArray},
    error::{ArrowError, Result as ArrowResult},
};
use arrow::{datatypes::Schema, datatypes::SchemaRef};
use arrow_flight::utils::flight_data_to_arrow_batch;
use arrow_flight::Ticket;
use arrow_flight::{flight_service_client::FlightServiceClient, FlightData};
use datafusion::physical_plan::common::collect;
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use datafusion::{logical_plan::LogicalPlan, physical_plan::RecordBatchStream};
use futures::{Stream, StreamExt};
use log::debug;
use prost::Message;
use tonic::Streaming;
use uuid::Uuid;

/// Client for interacting with Ballista executors.
#[derive(Clone)]
pub struct BallistaClient {
    flight_client: FlightServiceClient<tonic::transport::channel::Channel>,
}

impl BallistaClient {
    /// Create a new BallistaClient to connect to the executor listening on the specified
    /// host and port

    pub async fn try_new(host: &str, port: u16) -> Result<Self> {
        let addr = format!("http://{}:{}", host, port);
        debug!("BallistaClient connecting to {}", addr);
        let flight_client = FlightServiceClient::connect(addr.clone())
            .await
            .map_err(|e| {
                BallistaError::General(format!(
                    "Error connecting to Ballista scheduler or executor at {}: {:?}",
                    addr, e
                ))
            })?;
        debug!("BallistaClient connected OK");

        Ok(Self { flight_client })
    }

    /// Execute one partition of a physical query plan against the executor
    pub async fn execute_partition(
        &mut self,
        job_id: String,
        stage_id: usize,
        partition_id: Vec<usize>,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Vec<ExecutePartitionResult>> {
        let action = Action::ExecutePartition(ExecutePartition {
            job_id,
            stage_id,
            partition_id,
            plan,
            shuffle_locations: Default::default(),
        });
        let stream = self.execute_action(&action).await?;
        let batches = collect(stream).await?;

        batches
            .iter()
            .map(|batch| {
                if batch.num_rows() != 1 {
                    Err(BallistaError::General(
                        "execute_partition received wrong number of rows".to_owned(),
                    ))
                } else {
                    let path = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .expect("execute_partition expected column 0 to be a StringArray");

                    let stats = batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<StructArray>()
                        .expect("execute_partition expected column 1 to be a StructArray");

                    Ok(ExecutePartitionResult::new(
                        path.value(0),
                        PartitionStats::from_arrow_struct_array(stats),
                    ))
                }
            })
            .collect::<Result<Vec<_>>>()
    }

    /// Fetch a partition from an executor
    pub async fn fetch_partition(
        &mut self,
        job_id: &str,
        stage_id: usize,
        partition_id: usize,
    ) -> Result<SendableRecordBatchStream> {
        let action = Action::FetchPartition(PartitionId::new(job_id, stage_id, partition_id));
        self.execute_action(&action).await
    }

    /// Execute an action and retrieve the results
    pub async fn execute_action(&mut self, action: &Action) -> Result<SendableRecordBatchStream> {
        let serialized_action: protobuf::Action = action.to_owned().try_into()?;

        let mut buf: Vec<u8> = Vec::with_capacity(serialized_action.encoded_len());

        serialized_action
            .encode(&mut buf)
            .map_err(|e| BallistaError::General(format!("{:?}", e)))?;

        let request = tonic::Request::new(Ticket { ticket: buf });

        let mut stream = self
            .flight_client
            .do_get(request)
            .await
            .map_err(|e| BallistaError::General(format!("{:?}", e)))?
            .into_inner();

        // the schema should be the first message returned, else client should error
        match stream
            .message()
            .await
            .map_err(|e| BallistaError::General(format!("{:?}", e)))?
        {
            Some(flight_data) => {
                // convert FlightData to a stream
                let schema = Arc::new(Schema::try_from(&flight_data)?);

                // all the remaining stream messages should be dictionary and record batches
                Ok(Box::pin(FlightDataStream::new(stream, schema)))
            }
            None => Err(ballista_error(
                "Did not receive schema batch from flight server",
            )),
        }
    }
}

struct FlightDataStream {
    stream: Streaming<FlightData>,
    schema: SchemaRef,
}

impl FlightDataStream {
    pub fn new(stream: Streaming<FlightData>, schema: SchemaRef) -> Self {
        Self { stream, schema }
    }
}

impl Stream for FlightDataStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.stream.poll_next_unpin(cx).map(|x| match x {
            Some(flight_data_chunk_result) => {
                let converted_chunk = flight_data_chunk_result
                    .map_err(|e| ArrowError::from_external_error(Box::new(e)))
                    .and_then(|flight_data_chunk| {
                        flight_data_to_arrow_batch(&flight_data_chunk, self.schema.clone(), &[])
                    });
                Some(converted_chunk)
            }
            None => None,
        })
    }
}

impl RecordBatchStream for FlightDataStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
