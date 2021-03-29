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

use std::pin::Pin;
use std::sync::Arc;

use arrow_flight::{
    flight_service_server::FlightService, flight_service_server::FlightServiceServer,
    Action, ActionType, BasicAuth, Criteria, Empty, FlightData, FlightDescriptor,
    FlightInfo, HandshakeRequest, HandshakeResponse, PutResult, SchemaResult, Ticket,
};
use futures::{channel::mpsc, sink::SinkExt, Stream, StreamExt};
use tokio::sync::Mutex;
use tonic::{
    metadata::MetadataMap, transport::Server, Request, Response, Status, Streaming,
};
type TonicStream<T> = Pin<Box<dyn Stream<Item = T> + Send + Sync + 'static>>;

type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
type Result<T = (), E = Error> = std::result::Result<T, E>;

use prost::Message;

use crate::{AUTH_PASSWORD, AUTH_USERNAME};

pub async fn scenario_setup(port: &str) -> Result {
    let service = AuthBasicProtoScenarioImpl {
        username: AUTH_USERNAME.into(),
        password: AUTH_PASSWORD.into(),
        peer_identity: Arc::new(Mutex::new(None)),
    };
    let addr = super::listen_on(port).await?;
    let svc = FlightServiceServer::new(service);

    let server = Server::builder().add_service(svc).serve(addr);

    // NOTE: Log output used in tests to signal server is ready
    println!("Server listening on localhost:{}", addr.port());
    server.await?;
    Ok(())
}

#[derive(Clone)]
pub struct AuthBasicProtoScenarioImpl {
    username: Arc<str>,
    password: Arc<str>,
    peer_identity: Arc<Mutex<Option<String>>>,
}

impl AuthBasicProtoScenarioImpl {
    async fn check_auth(
        &self,
        metadata: &MetadataMap,
    ) -> Result<GrpcServerCallContext, Status> {
        let token = metadata
            .get_bin("auth-token-bin")
            .and_then(|v| v.to_bytes().ok())
            .and_then(|b| String::from_utf8(b.to_vec()).ok());
        self.is_valid(token).await
    }

    async fn is_valid(
        &self,
        token: Option<String>,
    ) -> Result<GrpcServerCallContext, Status> {
        match token {
            Some(t) if t == *self.username => Ok(GrpcServerCallContext {
                peer_identity: self.username.to_string(),
            }),
            _ => Err(Status::unauthenticated("Invalid token")),
        }
    }
}

struct GrpcServerCallContext {
    peer_identity: String,
}

impl GrpcServerCallContext {
    pub fn peer_identity(&self) -> &str {
        &self.peer_identity
    }
}

#[tonic::async_trait]
impl FlightService for AuthBasicProtoScenarioImpl {
    type HandshakeStream = TonicStream<Result<HandshakeResponse, Status>>;
    type ListFlightsStream = TonicStream<Result<FlightInfo, Status>>;
    type DoGetStream = TonicStream<Result<FlightData, Status>>;
    type DoPutStream = TonicStream<Result<PutResult, Status>>;
    type DoActionStream = TonicStream<Result<arrow_flight::Result, Status>>;
    type ListActionsStream = TonicStream<Result<ActionType, Status>>;
    type DoExchangeStream = TonicStream<Result<FlightData, Status>>;

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        self.check_auth(request.metadata()).await?;
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        self.check_auth(request.metadata()).await?;
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        let (tx, rx) = mpsc::channel(10);

        tokio::spawn({
            let username = self.username.clone();
            let password = self.password.clone();

            async move {
                let requests = request.into_inner();

                requests
                    .for_each(move |req| {
                        let mut tx = tx.clone();
                        let req = req.expect("Error reading handshake request");
                        let HandshakeRequest { payload, .. } = req;

                        let auth = BasicAuth::decode(&*payload)
                            .expect("Error parsing handshake request");

                        let resp = if *auth.username == *username
                            && *auth.password == *password
                        {
                            Ok(HandshakeResponse {
                                payload: username.as_bytes().to_vec(),
                                ..HandshakeResponse::default()
                            })
                        } else {
                            Err(Status::unauthenticated(format!(
                                "Don't know user {}",
                                auth.username
                            )))
                        };

                        async move {
                            tx.send(resp)
                                .await
                                .expect("Error sending handshake response");
                        }
                    })
                    .await;
            }
        });

        Ok(Response::new(Box::pin(rx)))
    }

    async fn list_flights(
        &self,
        request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        self.check_auth(request.metadata()).await?;
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.check_auth(request.metadata()).await?;
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        self.check_auth(request.metadata()).await?;
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let flight_context = self.check_auth(request.metadata()).await?;
        // Respond with the authenticated username.
        let buf = flight_context.peer_identity().as_bytes().to_vec();
        let result = arrow_flight::Result { body: buf };
        let output = futures::stream::once(async { Ok(result) });
        Ok(Response::new(Box::pin(output) as Self::DoActionStream))
    }

    async fn list_actions(
        &self,
        request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        self.check_auth(request.metadata()).await?;
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn do_exchange(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        self.check_auth(request.metadata()).await?;
        Err(Status::unimplemented("Not yet implemented"))
    }
}
