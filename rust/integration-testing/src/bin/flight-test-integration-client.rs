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

use arrow_integration_testing::{
    read_json_file, ArrowFile, AUTH_PASSWORD, AUTH_USERNAME,
};

use arrow::datatypes::SchemaRef;
use arrow::ipc::{self, reader};
use arrow::record_batch::RecordBatch;

use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{
    flight_descriptor::DescriptorType, BasicAuth, FlightData, HandshakeRequest, Location,
    Ticket,
};
use arrow_flight::{utils::flight_data_to_arrow_batch, FlightDescriptor};

use clap::{App, Arg};
use futures::{channel::mpsc, sink::SinkExt, stream, StreamExt};
use prost::Message;
use tonic::{metadata::MetadataValue, Request, Status};

use std::sync::Arc;

type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
type Result<T = (), E = Error> = std::result::Result<T, E>;

type Client = FlightServiceClient<tonic::transport::Channel>;

#[tokio::main]
async fn main() -> Result {
    tracing_subscriber::fmt::init();
    let matches = App::new("rust flight-test-integration-client")
        .arg(Arg::with_name("host").long("host").takes_value(true))
        .arg(Arg::with_name("port").long("port").takes_value(true))
        .arg(Arg::with_name("path").long("path").takes_value(true))
        .arg(
            Arg::with_name("scenario")
                .long("scenario")
                .takes_value(true),
        )
        .get_matches();

    let host = matches.value_of("host").expect("Host is required");
    let port = matches.value_of("port").expect("Port is required");

    match matches.value_of("scenario") {
        Some("middleware") => middleware_scenario(host, port).await?,
        Some("auth:basic_proto") => auth_basic_proto_scenario(host, port).await?,
        Some(scenario_name) => unimplemented!("Scenario not found: {}", scenario_name),
        None => {
            let path = matches
                .value_of("path")
                .expect("Path is required if scenario is not specified");
            integration_test_scenario(host, port, path).await?;
        }
    }

    Ok(())
}

async fn middleware_scenario(host: &str, port: &str) -> Result {
    let url = format!("http://{}:{}", host, port);
    let conn = tonic::transport::Endpoint::new(url)?.connect().await?;
    let mut client = FlightServiceClient::with_interceptor(conn, middleware_interceptor);

    let mut descriptor = FlightDescriptor::default();
    descriptor.set_type(DescriptorType::Cmd);
    descriptor.cmd = b"".to_vec();

    // This call is expected to fail.
    let resp = client
        .get_flight_info(Request::new(descriptor.clone()))
        .await;
    match resp {
        Ok(_) => return Err(Box::new(Status::internal("Expected call to fail"))),
        Err(e) => {
            let headers = e.metadata();
            let middleware_header = headers.get("x-middleware");
            let value = middleware_header.map(|v| v.to_str().unwrap()).unwrap_or("");

            if value != "expected value" {
                let msg = format!(
                    "Expected to receive header 'x-middleware: expected value', \
                    but instead got: '{}'",
                    value
                );
                return Err(Box::new(Status::internal(msg)));
            }

            eprintln!("Headers received successfully on failing call.");
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
            "Expected to receive header 'x-middleware: expected value', \
            but instead got: '{}'",
            value
        );
        return Err(Box::new(Status::internal(msg)));
    }

    eprintln!("Headers received successfully on passing call.");

    Ok(())
}

fn middleware_interceptor(mut req: Request<()>) -> Result<Request<()>, Status> {
    let metadata = req.metadata_mut();
    metadata.insert("x-middleware", "expected value".parse().unwrap());
    Ok(req)
}

async fn auth_basic_proto_scenario(host: &str, port: &str) -> Result {
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

// TODO: should this be extended, abstracted, and moved out of test code and into production code?
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

async fn integration_test_scenario(host: &str, port: &str, path: &str) -> Result {
    let url = format!("http://{}:{}", host, port);

    let client = FlightServiceClient::connect(url).await?;

    let ArrowFile {
        schema, batches, ..
    } = read_json_file(path)?;

    let schema = Arc::new(schema);

    let mut descriptor = FlightDescriptor::default();
    descriptor.set_type(DescriptorType::Path);
    descriptor.path = vec![path.to_string()];

    upload_data(
        client.clone(),
        schema.clone(),
        descriptor.clone(),
        batches.clone(),
    )
    .await?;
    verify_data(client, descriptor, schema, &batches).await?;

    Ok(())
}

async fn upload_data(
    mut client: Client,
    schema: SchemaRef,
    descriptor: FlightDescriptor,
    original_data: Vec<RecordBatch>,
) -> Result {
    eprintln!("In upload_data");
    let (mut upload_tx, upload_rx) = mpsc::channel(10);

    let mut schema_flight_data = FlightData::from(&*schema);
    schema_flight_data.flight_descriptor = Some(descriptor.clone());
    upload_tx.send(schema_flight_data).await?;

    let mut original_data_iter = original_data.iter().enumerate();

    if let Some((counter, first_batch)) = original_data_iter.next() {
        eprintln!("Some batches");

        let metadata = counter.to_string().into_bytes();
        eprintln!("sending batch {:?}", metadata);

        let (dictionary_flight_data, mut batch_flight_data) =
            arrow_flight::utils::convert_to_flight_data(first_batch);

        upload_tx.send_all(&mut stream::iter(dictionary_flight_data).map(Ok)).await?;

        // Only the record batch's FlightData gets app_metadata
        batch_flight_data.app_metadata = metadata.clone();
        upload_tx.send(batch_flight_data).await?;

        let outer = client.do_put(Request::new(upload_rx)).await?;
        let mut inner = outer.into_inner();

        let r = inner
            .next()
            .await
            .expect("No response received")
            .expect("Invalid response received");
        assert_eq!(metadata, r.app_metadata);
        eprintln!("received ack for batch {:?}", metadata);

        for (counter, batch) in original_data_iter {
            let metadata = counter.to_string().into_bytes();
            eprintln!("sending batch {:?}", metadata);

            let (dictionary_flight_data, mut batch_flight_data) =
                arrow_flight::utils::convert_to_flight_data(batch);

            upload_tx.send_all(&mut stream::iter(dictionary_flight_data).map(Ok)).await?;

            // Only the record batch's FlightData gets app_metadata
            batch_flight_data.app_metadata = metadata.clone();
            upload_tx.send(batch_flight_data).await?;

            let r = inner
                .next()
                .await
                .expect("No response received")
                .expect("Invalid response received");
            assert_eq!(metadata, r.app_metadata);
            eprintln!("received ack for batch {:?}", metadata);
        }
    } else {
        eprintln!("No batches");
        drop(upload_tx);
        let outer = client.do_put(Request::new(upload_rx)).await?;
        let inner = outer.into_inner();

        dbg!(&inner);
    }

    Ok(())
}

async fn verify_data(
    mut client: Client,
    descriptor: FlightDescriptor,
    expected_schema: SchemaRef,
    expected_data: &[RecordBatch],
) -> Result {
    let resp = client.get_flight_info(Request::new(descriptor)).await?;
    let info = resp.into_inner();

    assert!(
        !info.endpoint.is_empty(),
        "No endpoints returned from Flight server",
    );
    for endpoint in info.endpoint {
        let ticket = endpoint
            .ticket
            .expect("No ticket returned from Flight server");

        assert!(
            !endpoint.location.is_empty(),
            "No locations returned from Flight server",
        );
        for location in endpoint.location {
            println!("Verifying location {:?}", location);
            consume_flight_location(
                location,
                ticket.clone(),
                &expected_data,
                expected_schema.clone(),
            )
            .await?;
        }
    }

    Ok(())
}

async fn consume_flight_location(
    location: Location,
    ticket: Ticket,
    expected_data: &[RecordBatch],
    schema: SchemaRef,
) -> Result {
    let mut location = location;
    location.uri = location.uri.replace("grpc+tcp://", "grpc://");

    dbg!(&location.uri);
    let mut client = FlightServiceClient::connect(location.uri).await?;

    dbg!(&client);

    let resp = client.do_get(ticket).await;
    dbg!(&resp);

    // If i turn on RUST_LOG=h2=debug and run this client against the C++ server, I see this:
    // Dec 02 16:46:50.047 DEBUG h2::codec::framed_read: received; frame=Reset { stream_id: StreamId(1), error_code: INTERNAL_ERROR }
    // which i think is coming straight from the server, but I don't know why :(

    let mut resp = resp?.into_inner();
    dbg!(&resp);

    let _schema_again = resp.next().await.unwrap();
    let mut dictionaries_by_field = vec![None; schema.fields().len()];

    for (counter, expected_batch) in expected_data.iter().enumerate() {
        let mut actual_batch = resp.next().await.unwrap_or_else(|| {
            panic!(
                "Got fewer batches than expected, received so far: {} expected: {}",
                counter,
                expected_data.len(),
            )
        })?;
        let mut message = arrow::ipc::get_root_as_message(&actual_batch.data_header[..]);
        dbg!(message.header_type());
        while message.header_type() == ipc::MessageHeader::DictionaryBatch {
            // TODO: handle None which means parse failure
            if let Some(ipc_batch) = message.header_as_dictionary_batch() {
                let dictionary_batch_result = reader::read_dictionary(
                    &actual_batch.data_body,
                    ipc_batch,
                    &schema,
                    &mut dictionaries_by_field,
                );
                if let Err(e) = dictionary_batch_result {
                    panic!("Error reading dictionary: {:?}", e);
                } else {
                    dbg!(&dictionaries_by_field);
                }
            }

            actual_batch = resp.next().await.unwrap_or_else(|| {
                panic!(
                    "Got fewer batches than expected, received so far: {} expected: {}",
                    counter,
                    expected_data.len(),
                )
            })?;
            message = arrow::ipc::get_root_as_message(&actual_batch.data_header[..]);
        }

        let metadata = counter.to_string().into_bytes();
        assert_eq!(metadata, actual_batch.app_metadata);

        let actual_batch = flight_data_to_arrow_batch(
            &actual_batch,
            schema.clone(),
            &dictionaries_by_field,
        )
        .expect("Unable to convert flight data to Arrow batch")
        .expect("Unable to convert flight data to Arrow batch");

        assert_eq!(expected_batch.schema(), actual_batch.schema());
        assert_eq!(expected_batch.num_columns(), actual_batch.num_columns());
        assert_eq!(expected_batch.num_rows(), actual_batch.num_rows());
        let schema = expected_batch.schema();
        for i in 0..expected_batch.num_columns() {
            let field = schema.field(i);
            let field_name = field.name();

            let expected_data = expected_batch.column(i).data();
            let actual_data = actual_batch.column(i).data();

            assert_eq!(expected_data, actual_data, "Data for field {}", field_name);
        }
    }

    assert!(
        resp.next().await.is_none(),
        "Got more batches than the expected: {}",
        expected_data.len(),
    );

    Ok(())
}
