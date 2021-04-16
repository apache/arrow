<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Ballista Executor - Rust
This crate contains the Ballista Executor. It can be used both as a library or as a binary.

## Run

```bash
RUST_LOG=info cargo run --release
...
[2021-02-11T05:30:13Z INFO  executor] Running with config: ExecutorConfig { host: "localhost", port: 50051, work_dir: "/var/folders/y8/fc61kyjd4n53tn444n72rjrm0000gn/T/.tmpv1LjN0", concurrent_tasks: 4 }
```

By default, the executor will bind to `localhost` and listen on port `50051`.