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

use arrow_integration_testing::flight_client_scenarios;

use clap::{App, Arg};

type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
type Result<T = (), E = Error> = std::result::Result<T, E>;

#[tokio::main]
async fn main() -> Result {
    #[cfg(feature = "logging")]
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
        Some("middleware") => {
            flight_client_scenarios::middleware::run_scenario(host, port).await?
        }
        Some("auth:basic_proto") => {
            flight_client_scenarios::auth_basic_proto::run_scenario(host, port).await?
        }
        Some(scenario_name) => unimplemented!("Scenario not found: {}", scenario_name),
        None => {
            let path = matches
                .value_of("path")
                .expect("Path is required if scenario is not specified");
            flight_client_scenarios::integration_test::run_scenario(host, port, path)
                .await?;
        }
    }

    Ok(())
}
