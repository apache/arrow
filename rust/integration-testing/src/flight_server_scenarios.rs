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

use std::net::SocketAddr;

use arrow_flight::{FlightEndpoint, Location, Ticket};
use tokio::net::TcpListener;

pub mod auth_basic_proto;
pub mod integration_test;
pub mod middleware;

type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
type Result<T = (), E = Error> = std::result::Result<T, E>;

pub async fn listen_on(port: &str) -> Result<SocketAddr> {
    let addr: SocketAddr = format!("0.0.0.0:{}", port).parse()?;

    let listener = TcpListener::bind(addr).await?;
    let addr = listener.local_addr()?;

    Ok(addr)
}

pub fn endpoint(ticket: &str, location_uri: impl Into<String>) -> FlightEndpoint {
    FlightEndpoint {
        ticket: Some(Ticket {
            ticket: ticket.as_bytes().to_vec(),
        }),
        location: vec![Location {
            uri: location_uri.into(),
        }],
    }
}
