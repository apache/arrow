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

use std::collections::HashMap;
use std::convert::TryFrom;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use arrow::{
    array::ArrayRef,
    datatypes::Schema,
    datatypes::SchemaRef,
    ipc::{self, reader},
    record_batch::RecordBatch,
};
use arrow_flight::{
    flight_descriptor::DescriptorType, flight_service_server::FlightService,
    flight_service_server::FlightServiceServer, Action, ActionType, Criteria, Empty,
    FlightData, FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest,
    HandshakeResponse, PutResult, SchemaResult, Ticket,
};
use futures::{channel::mpsc, sink::SinkExt, Stream, StreamExt};
use tokio::sync::Mutex;
use tonic::{transport::Server, Request, Response, Status, Streaming};

type TonicStream<T> = Pin<Box<dyn Stream<Item = T> + Send + Sync + 'static>>;

type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
type Result<T = (), E = Error> = std::result::Result<T, E>;

pub async fn scenario_setup(port: &str) -> Result {
    let addr: SocketAddr = format!("0.0.0.0:{}", port).parse()?;

    let service = FlightServiceImpl {
        server_location: format!("grpc+tcp://{}", addr),
        ..Default::default()
    };
    let svc = FlightServiceServer::new(service);

    Server::builder().add_service(svc).serve(addr).await?;

    Ok(())
}

#[derive(Debug, Clone)]
struct IntegrationDataset {
    schema: Schema,
    chunks: Vec<RecordBatch>,
}

#[derive(Clone, Default)]
pub struct FlightServiceImpl {
    server_location: String,
    uploaded_chunks: Arc<Mutex<HashMap<String, IntegrationDataset>>>,
}

impl FlightServiceImpl {
    fn endpoint_from_path(&self, path: &str) -> FlightEndpoint {
        super::endpoint(path, &self.server_location)
    }
}

#[tonic::async_trait]
impl FlightService for FlightServiceImpl {
    type HandshakeStream = TonicStream<Result<HandshakeResponse, Status>>;
    type ListFlightsStream = TonicStream<Result<FlightInfo, Status>>;
    type DoGetStream = TonicStream<Result<FlightData, Status>>;
    type DoPutStream = TonicStream<Result<PutResult, Status>>;
    type DoActionStream = TonicStream<Result<arrow_flight::Result, Status>>;
    type ListActionsStream = TonicStream<Result<ActionType, Status>>;
    type DoExchangeStream = TonicStream<Result<FlightData, Status>>;

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();

        let key = String::from_utf8(ticket.ticket.to_vec())
            .map_err(|e| Status::invalid_argument(format!("Invalid ticket: {:?}", e)))?;

        let uploaded_chunks = self.uploaded_chunks.lock().await;

        let flight = uploaded_chunks.get(&key).ok_or_else(|| {
            Status::not_found(format!("Could not find flight. {}", key))
        })?;

        let options = arrow::ipc::writer::IpcWriteOptions::default();

        let schema = std::iter::once({
            Ok(arrow_flight::utils::flight_data_from_arrow_schema(
                &flight.schema,
                &options,
            ))
        });

        let batches = flight
            .chunks
            .iter()
            .enumerate()
            .flat_map(|(counter, batch)| {
                let (dictionary_flight_data, mut batch_flight_data) =
                    arrow_flight::utils::flight_data_from_arrow_batch(batch, &options);

                // Only the record batch's FlightData gets app_metadata
                let metadata = counter.to_string().into_bytes();
                batch_flight_data.app_metadata = metadata;

                dictionary_flight_data
                    .into_iter()
                    .chain(std::iter::once(batch_flight_data))
                    .map(Ok)
            });

        let output = futures::stream::iter(schema.chain(batches).collect::<Vec<_>>());

        Ok(Response::new(Box::pin(output) as Self::DoGetStream))
    }

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let descriptor = request.into_inner();

        match descriptor.r#type {
            t if t == DescriptorType::Path as i32 => {
                let path = &descriptor.path;
                if path.is_empty() {
                    return Err(Status::invalid_argument("Invalid path"));
                }

                let uploaded_chunks = self.uploaded_chunks.lock().await;
                let flight = uploaded_chunks.get(&path[0]).ok_or_else(|| {
                    Status::not_found(format!("Could not find flight. {}", path[0]))
                })?;

                let endpoint = self.endpoint_from_path(&path[0]);

                let total_records: usize =
                    flight.chunks.iter().map(|chunk| chunk.num_rows()).sum();

                let options = arrow::ipc::writer::IpcWriteOptions::default();
                let schema = arrow_flight::utils::ipc_message_from_arrow_schema(
                    &flight.schema,
                    &options,
                )
                .expect(
                    "Could not generate schema bytes from schema stored by a DoPut; \
                         this should be impossible",
                );

                let info = FlightInfo {
                    schema,
                    flight_descriptor: Some(descriptor.clone()),
                    endpoint: vec![endpoint],
                    total_records: total_records as i64,
                    total_bytes: -1,
                };

                Ok(Response::new(info))
            }
            other => Err(Status::unimplemented(format!("Request type: {}", other))),
        }
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let mut input_stream = request.into_inner();
        let flight_data = input_stream
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("Must send some FlightData"))?;

        let descriptor = flight_data
            .flight_descriptor
            .clone()
            .ok_or_else(|| Status::invalid_argument("Must have a descriptor"))?;

        if descriptor.r#type != DescriptorType::Path as i32 || descriptor.path.is_empty()
        {
            return Err(Status::invalid_argument("Must specify a path"));
        }

        let key = descriptor.path[0].clone();

        let schema = Schema::try_from(&flight_data)
            .map_err(|e| Status::invalid_argument(format!("Invalid schema: {:?}", e)))?;
        let schema_ref = Arc::new(schema.clone());

        let (response_tx, response_rx) = mpsc::channel(10);

        let uploaded_chunks = self.uploaded_chunks.clone();

        tokio::spawn(async {
            let mut error_tx = response_tx.clone();
            if let Err(e) = save_uploaded_chunks(
                uploaded_chunks,
                schema_ref,
                input_stream,
                response_tx,
                schema,
                key,
            )
            .await
            {
                error_tx.send(Err(e)).await.expect("Error sending error")
            }
        });

        Ok(Response::new(Box::pin(response_rx) as Self::DoPutStream))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
}

async fn send_app_metadata(
    tx: &mut mpsc::Sender<Result<PutResult, Status>>,
    app_metadata: &[u8],
) -> Result<(), Status> {
    tx.send(Ok(PutResult {
        app_metadata: app_metadata.to_vec(),
    }))
    .await
    .map_err(|e| Status::internal(format!("Could not send PutResult: {:?}", e)))
}

async fn record_batch_from_message(
    message: ipc::Message<'_>,
    data_body: &[u8],
    schema_ref: SchemaRef,
    dictionaries_by_field: &[Option<ArrayRef>],
) -> Result<RecordBatch, Status> {
    let ipc_batch = message.header_as_record_batch().ok_or_else(|| {
        Status::internal("Could not parse message header as record batch")
    })?;

    let arrow_batch_result = reader::read_record_batch(
        data_body,
        ipc_batch,
        schema_ref,
        &dictionaries_by_field,
    );

    arrow_batch_result.map_err(|e| {
        Status::internal(format!("Could not convert to RecordBatch: {:?}", e))
    })
}

async fn dictionary_from_message(
    message: ipc::Message<'_>,
    data_body: &[u8],
    schema_ref: SchemaRef,
    dictionaries_by_field: &mut [Option<ArrayRef>],
) -> Result<(), Status> {
    let ipc_batch = message.header_as_dictionary_batch().ok_or_else(|| {
        Status::internal("Could not parse message header as dictionary batch")
    })?;

    let dictionary_batch_result =
        reader::read_dictionary(data_body, ipc_batch, &schema_ref, dictionaries_by_field);
    dictionary_batch_result.map_err(|e| {
        Status::internal(format!("Could not convert to Dictionary: {:?}", e))
    })
}

async fn save_uploaded_chunks(
    uploaded_chunks: Arc<Mutex<HashMap<String, IntegrationDataset>>>,
    schema_ref: Arc<Schema>,
    mut input_stream: Streaming<FlightData>,
    mut response_tx: mpsc::Sender<Result<PutResult, Status>>,
    schema: Schema,
    key: String,
) -> Result<(), Status> {
    let mut chunks = vec![];
    let mut uploaded_chunks = uploaded_chunks.lock().await;

    let mut dictionaries_by_field = vec![None; schema_ref.fields().len()];

    while let Some(Ok(data)) = input_stream.next().await {
        let message = arrow::ipc::root_as_message(&data.data_header[..])
            .map_err(|e| Status::internal(format!("Could not parse message: {:?}", e)))?;

        match message.header_type() {
            ipc::MessageHeader::Schema => {
                return Err(Status::internal(
                    "Not expecting a schema when messages are read",
                ))
            }
            ipc::MessageHeader::RecordBatch => {
                send_app_metadata(&mut response_tx, &data.app_metadata).await?;

                let batch = record_batch_from_message(
                    message,
                    &data.data_body,
                    schema_ref.clone(),
                    &dictionaries_by_field,
                )
                .await?;

                chunks.push(batch);
            }
            ipc::MessageHeader::DictionaryBatch => {
                dictionary_from_message(
                    message,
                    &data.data_body,
                    schema_ref.clone(),
                    &mut dictionaries_by_field,
                )
                .await?;
            }
            t => {
                return Err(Status::internal(format!(
                    "Reading types other than record batches not yet supported, \
                                              unable to read {:?}",
                    t
                )));
            }
        }
    }

    let dataset = IntegrationDataset { schema, chunks };
    uploaded_chunks.insert(key, dataset);

    Ok(())
}
