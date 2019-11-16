use tonic::{transport::Server, Request, Response, Status};

pub mod arrow_flight {
    tonic::include_proto!("arrow.flight.protocol"); // The string specified here must match the proto package name
}

use arrow_flight::{
//    server::{Greeter, GreeterServer},
//    HelloReply, HelloRequest,
};