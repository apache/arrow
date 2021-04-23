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

# Ballista Scheduler
This crate contains the Ballista Scheduler. It can be used both as a library or as a binary.

## Run

```bash
$ RUST_LOG=info cargo run --release
...
[2021-02-11T05:29:30Z INFO  scheduler] Ballista v0.4.2-SNAPSHOT Scheduler listening on 0.0.0.0:50050
[2021-02-11T05:30:13Z INFO  ballista::scheduler] Received register_executor request for ExecutorMetadata { id: "6d10f5d2-c8c3-4e0f-afdb-1f6ec9171321", host: "localhost", port: 50051 }
```

By default, the scheduler will bind to `localhost` and listen on port `50051`.

## Connecting to Scheduler
Scheduler supports REST model also using content negotiation. 
For e.x if you want to get list of executors connected to the scheduler, 
you can do (assuming you use default config)

```bash
curl --request GET \
  --url http://localhost:50050/executors \
  --header 'Accept: application/json'
```

## Scheduler UI
A basic ui for the scheduler is in `ui/scheduler` of the ballista repo. 
It can be started using the following [yarn](https://yarnpkg.com/) command

```bash
yarn && yarn start
```
