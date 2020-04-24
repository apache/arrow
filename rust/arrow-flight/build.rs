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

use std::env;

fn main() -> Result<(), String> {
    // The current working directory can vary depending on how the project is being
    // built or released so we build an absolute path to the proto file
    let mut path = env::current_dir().map_err(|e| format!("{:?}", e))?;
    loop {
        // build path to format/Flight.proto
        let mut flight_proto = path.clone();
        flight_proto.push("format");
        flight_proto.push("Flight.proto");

        if flight_proto.exists() {
            tonic_build::compile_protos(flight_proto).map_err(|e| format!("{:?}", e))?;
            return Ok(());
        }

        if !path.pop() {
            // reached root of file system
            break;
        }
    }

    Err("Failed to locate format/Flight.proto in any parent directory".to_string())
}
