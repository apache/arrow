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

use crate::{AUTH_PASSWORD, AUTH_USERNAME};

use arrow_flight::{
    flight_service_client::FlightServiceClient, BasicAuth, HandshakeRequest,
};
use futures::{stream, StreamExt};
use prost::Message;
use tonic::{metadata::MetadataValue, Request, Status};

type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
type Result<T = (), E = Error> = std::result::Result<T, E>;

type Client = FlightServiceClient<tonic::transport::Channel>;

pub async fn run_scenario(host: &str, port: &str) -> Result {
    let url = format!("http://{}:{}", host, port);
    let mut client = FlightServiceClient::connect(url).await?;

    let action = arrow_flight::Action::default();

    let resp = client.do_action(Request::new(action.clone())).await;
    // This client is unauthenticated and should fail.
    match resp {
        Err(e) => {
            if e.code() != tonic::Code::Unauthenticated {
                return Err(Box::new(Status::internal(format!(
                    "Expected UNAUTHENTICATED but got {:?}",
                    e
                ))));
            }
        }
        Ok(other) => {
            return Err(Box::new(Status::internal(format!(
                "Expected UNAUTHENTICATED but got {:?}",
                other
            ))));
        }
    }

    let token = authenticate(&mut client, AUTH_USERNAME, AUTH_PASSWORD)
        .await
        .expect("must respond successfully from handshake");

    let mut request = Request::new(action);
    let metadata = request.metadata_mut();
    metadata.insert_bin(
        "auth-token-bin",
        MetadataValue::from_bytes(token.as_bytes()),
    );

    let resp = client.do_action(request).await?;
    let mut resp = resp.into_inner();

    let r = resp
        .next()
        .await
        .expect("No response received")
        .expect("Invalid response received");

    let body = String::from_utf8(r.body).unwrap();
    assert_eq!(body, AUTH_USERNAME);

    Ok(())
}

async fn authenticate(
    client: &mut Client,
    username: &str,
    password: &str,
) -> Result<String> {
    let auth = BasicAuth {
        username: username.into(),
        password: password.into(),
    };
    let mut payload = vec![];
    auth.encode(&mut payload)?;

    let req = stream::once(async {
        HandshakeRequest {
            payload,
            ..HandshakeRequest::default()
        }
    });

    let rx = client.handshake(Request::new(req)).await?;
    let mut rx = rx.into_inner();

    let r = rx.next().await.expect("must respond from handshake")?;
    assert!(rx.next().await.is_none(), "must not respond a second time");

    Ok(String::from_utf8(r.payload).unwrap())
}
