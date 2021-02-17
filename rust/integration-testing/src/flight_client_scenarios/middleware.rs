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

use arrow_flight::{
    flight_descriptor::DescriptorType, flight_service_client::FlightServiceClient,
    FlightDescriptor,
};
use tonic::{Request, Status};

type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
type Result<T = (), E = Error> = std::result::Result<T, E>;

pub async fn run_scenario(host: &str, port: &str) -> Result {
    let url = format!("http://{}:{}", host, port);
    let conn = tonic::transport::Endpoint::new(url)?.connect().await?;
    let mut client = FlightServiceClient::with_interceptor(conn, middleware_interceptor);

    let mut descriptor = FlightDescriptor::default();
    descriptor.set_type(DescriptorType::Cmd);
    descriptor.cmd = b"".to_vec();

    // This call is expected to fail.
    match client
        .get_flight_info(Request::new(descriptor.clone()))
        .await
    {
        Ok(_) => return Err(Box::new(Status::internal("Expected call to fail"))),
        Err(e) => {
            let headers = e.metadata();
            let middleware_header = headers.get("x-middleware");
            let value = middleware_header.map(|v| v.to_str().unwrap()).unwrap_or("");

            if value != "expected value" {
                let msg = format!(
                    "On failing call: Expected to receive header 'x-middleware: expected value', \
                     but instead got: '{}'",
                    value
                );
                return Err(Box::new(Status::internal(msg)));
            }
        }
    }

    // This call should succeed
    descriptor.cmd = b"success".to_vec();
    let resp = client.get_flight_info(Request::new(descriptor)).await?;

    let headers = resp.metadata();
    let middleware_header = headers.get("x-middleware");
    let value = middleware_header.map(|v| v.to_str().unwrap()).unwrap_or("");

    if value != "expected value" {
        let msg = format!(
            "On success call: Expected to receive header 'x-middleware: expected value', \
            but instead got: '{}'",
            value
        );
        return Err(Box::new(Status::internal(msg)));
    }

    Ok(())
}

#[allow(clippy::unnecessary_wraps)]
fn middleware_interceptor(mut req: Request<()>) -> Result<Request<()>, Status> {
    let metadata = req.metadata_mut();
    metadata.insert("x-middleware", "expected value".parse().unwrap());
    Ok(req)
}
