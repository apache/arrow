use tonic::{transport::Server, Request, Response, Status};
use tonic::codegen::*;

pub mod arrow_flight {
    tonic::include_proto!("arrow.flight.protocol"); // The string specified here must match the proto package name
}

use arrow_flight::{
    server,
    server::FlightService,
    FlightData,
    PutResult,
    HandshakeRequest
};

pub struct FlightServiceImpl {}

#[tonic::async_trait]
impl FlightService for FlightServiceImpl {

    //TODO the generated code declares a HandshakeStream type within the trait and I don't
    // know how to import it here

    async fn handshake(
        &self,
        request: tonic::Request<tonic::Streaming<HandshakeRequest>>,
    ) -> Result<tonic::Response<arrow_flight::server::FlightService::HandshakeStream>, tonic::Status> {
        unimplemented!()
    }

    //TODO implement the other methods

}
