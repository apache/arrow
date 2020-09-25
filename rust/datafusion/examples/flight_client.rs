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

use std::convert::TryFrom;
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::util::pretty;

use arrow_flight::flight_descriptor;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::utils::flight_data_to_arrow_batch;
use arrow_flight::{FlightDescriptor, Ticket};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let testdata =
        std::env::var("PARQUET_TEST_DATA").expect("PARQUET_TEST_DATA not defined");

    // Create Flight client
    let mut client = FlightServiceClient::connect("http://localhost:50051").await?;

    // Call get_schema to get the schema of a Parquet file
    let request = tonic::Request::new(FlightDescriptor {
        r#type: flight_descriptor::DescriptorType::Path as i32,
        cmd: vec![],
        path: vec![format!("{}/alltypes_plain.parquet", testdata)],
    });

    let schema_result = client.get_schema(request).await?.into_inner();
    let schema = Schema::try_from(&schema_result)?;
    println!("Schema: {:?}", schema);

    // Call do_get to execute a SQL query and receive results
    let request = tonic::Request::new(Ticket {
        ticket: "SELECT id FROM alltypes_plain".into(),
    });

    let mut stream = client.do_get(request).await?.into_inner();

    // the schema should be the first message returned, else client should error
    let flight_data = stream.message().await?.unwrap();
    // convert FlightData to a stream
    let schema = Arc::new(Schema::try_from(&flight_data)?);
    println!("Schema: {:?}", schema);

    // all the remaining stream messages should be dictionary and record batches
    let mut results = vec![];
    while let Some(flight_data) = stream.message().await? {
        // the unwrap is infallible and thus safe
        let record_batch =
            flight_data_to_arrow_batch(&flight_data, schema.clone())?.unwrap();
        results.push(record_batch);
    }

    // print the results
    pretty::print_batches(&results)?;

    Ok(())
}
