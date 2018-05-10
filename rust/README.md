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

This is a starting point for a native Rust implementation of Arrow.

The current code demonstrates arrays of primitive types and structs.

## Creating an Array from a Vec

```rust
// create a memory-aligned Arrow array from an existing Vec
let array = Array::from(vec![1,2,3,4,5]);

match array.data() {
    &ArrayData::Int32(ref buffer) => {
        println!("array contents: {:?}", buffer.iter().collect::<Vec<i32>>());
    }
    _ => {}
}
```

## Creating an Array from a Builder

```rust
let mut builder: Builder<i32> = Builder::new();
for i in 0..10 {
    builder.push(i);
}
let buffer = builder.finish();
let array = Array::from(buffer);
```

## Run Examples

Examples can be run using the `cargo run --example` command. For example:

```bash
cargo run --example array_from_builder
```

## Run Tests

```bash
cargo test
```
