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

use arrow::datatypes::Schema;
use arrow::ipc::reader;
use flight::flight_service_client::FlightServiceClient;
use flight::Ticket;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = FlightServiceClient::connect("http://localhost:50051").await?;

    let request = tonic::Request::new(Ticket {
        ticket: "SELECT id FROM alltypes_plain".into(),
    });

    let mut stream = client.do_get(request).await?.into_inner();
    let mut batch_schema: Option<Arc<Schema>> = None;

    while let Some(flight_data) = stream.message().await? {
        println!("FlightData = {:?}", flight_data);

        if let Some(schema) = reader::schema_from_bytes(&flight_data.data_header) {
            println!("Schema = {:?}", schema);
            batch_schema = Some(Arc::new(schema.clone()));
        }

        match batch_schema {
            Some(ref schema) => {
                if let Some(record_batch) = reader::recordbatch_from_bytes(
                    &flight_data.data_header,
                    schema.clone(),
                )? {
                    println!(
                        "record_batch has {} columns and {} rows",
                        record_batch.num_columns(),
                        record_batch.num_rows()
                    );
                }
            }
            None => {}
        }
    }

    Ok(())
}
