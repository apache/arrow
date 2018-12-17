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

[![Build Status](https://travis-ci.org/apache/arrow.svg?branch=master)](https://travis-ci.org/apache/arrow)
[![Coverage Status](https://coveralls.io/repos/github/apache/arrow/badge.svg)](https://coveralls.io/github/apache/arrow)

## Status

This is a native Rust implementation of Apache Arrow. The current status is:

- [x] Primitive Arrays
- [x] List Arrays
- [x] Struct Arrays
- [x] CSV Reader
- [ ] CSV Writer
- [ ] Parquet Reader
- [ ] Parquet Writer
- [ ] Arrow IPC
- [ ] Interop tests with other implementations

## Examples

The examples folder shows how to construct some different types of Arrow
arrays, including dynamic arrays created at runtime.

Examples can be run using the `cargo run --example` command. For example:

```bash
cargo run --example builders
cargo run --example dynamic_types
cargo run --example read_csv
```

## Run Tests

```bash
cargo test
```

# Publishing to crates.io

An Arrow committer can publish this crate after an official project release has
been made to crates.io using the following instructions.

Follow [these
instructions](https://doc.rust-lang.org/cargo/reference/publishing.html) to
create an account and login to crates.io before asking to be added as an owner
of the [arrow crate](https://crates.io/crates/arrow).

Checkout the tag for the version to be released. For example:

```bash
git checkout apache-arrow-0.11.0
```

If the Cargo.toml in this tag already contains `version = "0.11.0"` (as it
should) then the crate can be published with the following command:

```bash
cargo publish
```

If the Cargo.toml does not have the correct version then it will be necessary
to modify it manually. Since there is now a modified file locally that is not
committed to github it will be necessary to use the following command.

```bash
cargo publish --allow-dirty
```
