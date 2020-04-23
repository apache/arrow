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

use std::{env, path::Path};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // avoid rerunning build if the file has not changed
    println!("cargo:rerun-if-changed=../../format/Flight.proto");

    // override the build location, in order to check in the changes to proto files
    env::set_var("OUT_DIR", "src");

    // The current working directory can vary depending on how the project is being
    // built or released so we build an absolute path to the proto file
    let path = Path::new("../../format/Flight.proto");
    if path.exists() {
        tonic_build::compile_protos("../../format/Flight.proto")?;
    }

    // As the proto file is checked in, the build should not fail if the file is not found
    Ok(())
}
