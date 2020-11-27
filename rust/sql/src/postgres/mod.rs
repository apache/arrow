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

//! An interface for reading and writing record batches to and from PostgreSQL

pub mod reader;
pub mod writer;

/// PGCOPY header
pub const MAGIC: &[u8] = b"PGCOPY\n\xff\r\n\0";
pub const EPOCH_DAYS: i32 = 10957;
pub const EPOCH_MICROS: i64 = 946684800000000;

pub const UNSUPPORTED_TYPE_ERR: arrow::error::ArrowError =
    arrow::error::ArrowError::SqlError(String::new());

pub struct Postgres;

/// A Postgres reader that returns an iterator of record batches
pub struct PostgresReadIterator {
    client: postgres::Client,
    query: String,
    limit: usize,
    batch_size: usize,
    schema: arrow::datatypes::Schema,
    read_records: usize,
    is_complete: bool,
    buffers: Buffers,
}

/// Buffers that can be preallocated, to reduce overall allocations
pub(super) struct Buffers {
    pub(super) data_buffers: Vec<Vec<u8>>,
    pub(super) null_buffers: Vec<Vec<bool>>,
    pub(super) offset_buffers: Vec<Vec<i32>>,
}
