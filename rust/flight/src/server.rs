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

use tonic::{transport::Server, Request, Response, Status};
use tonic::codegen::*;
use tokio::sync::mpsc;

pub mod arrow_flight {
    tonic::include_proto!("arrow.flight.protocol"); // The string specified here must match the proto package name
}
use arrow_flight::{
    server,
    server::FlightService,
    Action,
    ActionType,
    Criteria,
    Empty,
    FlightInfo,
    FlightData,
    FlightDescriptor,
    PutResult,
    SchemaResult,
    Ticket,
    HandshakeRequest, HandshakeResponse
};

pub struct FlightServiceImpl {}

#[tonic::async_trait]
impl FlightService for FlightServiceImpl {

    type HandshakeStream = mpsc::Receiver<Result<HandshakeResponse, Status>>;

    async fn handshake(
        &self, 
        request: tonic::Request<tonic::Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, tonic::Status> {
        unimplemented!()
    }

    type ListFlightsStream = mpsc::Receiver<Result<FlightInfo, Status>>;

    async fn list_flights(
        &self,
        request: tonic::Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, tonic::Status> {
        unimplemented!()
    }

//    async fn get_flight_info(
//        &self,
//        request: tonic::Request<FlightDescriptor>,
//    ) -> Result<tonic::Response<FlightInfo>, tonic::Status> {
//        unimplemented!()
//    }
//
//    async fn get_schema(
//        &self,
//        request: tonic::Request<FlightDescriptor>,
//    ) -> Result<tonic::Response<SchemaResult>, tonic::Status> {
//        unimplemented!()
//    }
//
//    async fn do_get(
//        &self,
//        request: tonic::Request<Ticket>,
//    ) -> Result<
//        tonic::Response<tonic::codec::Streaming<FlightData>>,
//        tonic::Status,
//    > {
//        unimplemented!()
//    }
//
//    async fn do_put(
//        &self,
//        request: impl tonic::IntoStreamingRequest<Message = FlightData>,
//    ) -> Result<
//        tonic::Response<tonic::codec::Streaming<PutResult>>,
//        tonic::Status,
//    > {
//        unimplemented!()
//    }
//
//    type DoActionStream = mpsc::Receiver<Result<arrow_flight::Result, Status>>;
//
//    async fn do_action(
//        &self,
//        request: tonic::Request<Action>,
//    ) -> Result<tonic::Response<tonic::codec::Streaming<arrow_flight::Result>>, tonic::Status>
//    {
//        unimplemented!()
//    }
//
//    async fn list_actions(
//        &self,
//        request: impl tonic::IntoRequest<Empty>,
//    ) -> Result<
//        tonic::Response<tonic::codec::Streaming<ActionType>>,
//        tonic::Status,
//    > {
//        unimplemented!()
//    }

}
