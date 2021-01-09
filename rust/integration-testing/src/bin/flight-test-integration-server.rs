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

use clap::{App, Arg};

use arrow_integration_testing::flight_server_scenarios;

type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
type Result<T = (), E = Error> = std::result::Result<T, E>;

#[tokio::main]
async fn main() -> Result {
    #[cfg(feature = "logging")]
    tracing_subscriber::fmt::init();

    let matches = App::new("rust flight-test-integration-server")
        .about("Integration testing server for Flight.")
        .arg(Arg::with_name("port").long("port").takes_value(true))
        .arg(
            Arg::with_name("scenario")
                .long("scenario")
                .takes_value(true),
        )
        .get_matches();

    let port = matches.value_of("port").unwrap_or("0");

    match matches.value_of("scenario") {
        Some("middleware") => {
            flight_server_scenarios::middleware::scenario_setup(port).await?
        }
        Some("auth:basic_proto") => {
            flight_server_scenarios::auth_basic_proto::scenario_setup(port).await?
        }
        Some(scenario_name) => unimplemented!("Scenario not found: {}", scenario_name),
        None => {
            flight_server_scenarios::integration_test::scenario_setup(port).await?;
        }
    }
    Ok(())
}
