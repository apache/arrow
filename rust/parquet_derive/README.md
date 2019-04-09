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

# parquet_derive-rs

A crate for automatically deriving `RecordWriter` for arbitrary, _simple_ structs. This does not generate writers for nested structures but it will work great for shallow structures.

## Usage
Add this to your Cargo.toml:
```toml
[dependencies]
parquet = "0.4"
parquet_derive = "0.4"
```

and this to your crate root:
```rust
extern crate parquet;
#[macro_use] extern crate parquet_derive;
```

Example usage of deriving a `RecordWriter` for your struct:

```rust
use parquet;
use parquet::record::RecordWriter;

#[derive(ParquetRecordWriter)]
struct ACompleteRecord<'a> {
    pub a_bool: bool,
    pub a_str: &'a str,
    pub a_string: String,
    pub a_borrowed_string: &'a String,
    pub maybe_a_str: Option<&'a str>,
    pub magic_number: i32,
    pub low_quality_pi: f32,
    pub high_quality_pi: f64,
    pub maybe_pi: Option<f32>,
    pub maybe_best_pi: Option<f64>,
}

// Initialize your parquet file
let mut writer = SerializedFileWriter::new(file, schema, props).unwrap();
let mut row_group = writer.next_row_group().unwrap();

// Build up your records
let chunks = ...

// The derived `RecordWriter` takes over here
chunks.write_to_row_group(&mut row_group);

writer.close_row_group(row_group).unwrap();
writer.close().unwrap();
```

## Features
- [X] Support writing `String`, `&str`, `bool`, `i32`, `f32`, `f64`, `Vec<u8>`
- [ ] Support writing dictionaries
- [ ] Support writing logical types like timestamp
- [X] Derive definition_levels for `Option`
- [ ] Derive definition levels for nested structures

## Requirements
- Same as `parquet-rs`

## Test
Testing a `*_derive` crate requires an intermediate crate. Go to `parquet_derive_test` and run `cargo test` for
unit tests.

## Docs
To build documentation, run `cargo doc --no-deps`.
To compile and view in the browser, run `cargo doc --no-deps --open`.

## License
Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0.
