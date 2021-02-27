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

use arrow_flight::{
    flight_descriptor::DescriptorType, flight_service_server::FlightService,
    flight_service_server::FlightServiceServer, Action, ActionType, Criteria, Empty,
    FlightData, FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse,
    PutResult, SchemaResult, Ticket,
};
use futures::Stream;
use tonic::{transport::Server, Request, Response, Status, Streaming};

type TonicStream<T> = Pin<Box<dyn Stream<Item = T> + Send + Sync + 'static>>;

type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
type Result<T = (), E = Error> = std::result::Result<T, E>;

pub async fn scenario_setup(port: &str) -> Result {
    let service = MiddlewareScenarioImpl {};
    let svc = FlightServiceServer::new(service);
    let addr = super::listen_on(port).await?;

    let server = Server::builder().add_service(svc).serve(addr);

    // NOTE: Log output used in tests to signal server is ready
    println!("Server listening on localhost:{}", addr.port());
    server.await?;
    Ok(())
}

#[derive(Clone, Default)]
pub struct MiddlewareScenarioImpl {}

#[tonic::async_trait]
impl FlightService for MiddlewareScenarioImpl {
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
        _request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
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
        let middleware_header = request.metadata().get("x-middleware").cloned();

        let descriptor = request.into_inner();

        if descriptor.r#type == DescriptorType::Cmd as i32 && descriptor.cmd == b"success"
        {
            // Return a fake location - the test doesn't read it
            let endpoint = super::endpoint("foo", "grpc+tcp://localhost:10010");

            let info = FlightInfo {
                flight_descriptor: Some(descriptor),
                endpoint: vec![endpoint],
                ..Default::default()
            };

            let mut response = Response::new(info);
            if let Some(value) = middleware_header {
                response.metadata_mut().insert("x-middleware", value);
            }

            return Ok(response);
        }

        let mut status = Status::unknown("Unknown");
        if let Some(value) = middleware_header {
            status.metadata_mut().insert("x-middleware", value);
        }

        Err(status)
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
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
