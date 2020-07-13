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

# Apache Arrow Rust Integration Testing

See [Integration.rst](../../docs/source/format/Integration.rst) for an overview of integration testing.

This crate contains the following binaries, which are invoked by Archery during integration testing with other Arrow implementations.

| Binary | Purpose |
|--------|---------|
| arrow-file-to-stream | Converts an Arrow file to an Arrow stream |
| arrow-stream-to-file | Converts an Arrow stream to an Arrow file |
| arrow-json-integration-test | Converts between Arrow and JSON formats |
