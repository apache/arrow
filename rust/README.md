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

# Native Rust implementation of Apache Arrow

## The Rust implementation of Arrow consists of the following crates

- Arrow [(README)](arrow/README.md)
- Parquet [(README)](parquet/README.md)

## Run Tests

Parquet support in Arrow requires data to test against, this data is in a
git submodule.  To pull down this data run the following:

```bash
git submodule update --init
```

The data can then be found in `cpp/submodules/parquet_testing/data`.
Create a new environment variable called `PARQUET_TEST_DATA` to point
to this location and then `cargo test` as usual.

## Code Formatting

Our CI uses `rustfmt` to check code formatting.  Although the project is
built and tested against nightly rust we use the stable version of
`rustfmt`.  So before submitting a PR be sure to run the following
and check for lint issues:

```bash
cargo +stable fmt --all -- --check
```

