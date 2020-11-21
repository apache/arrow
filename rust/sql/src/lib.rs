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

#![allow(dead_code)]

use arrow::datatypes::{Schema, SchemaRef};
use arrow::error::Result;
use arrow::record_batch::RecordBatch;

// supported database modules
pub mod postgres;

///
/// a SQL data source, used to read data from a SQL database into Arrow batches
pub trait SqlDataSource {
    /// Get the schema of a table
    /// 
    /// A reader (e.g. DataFusion) is expected to use this even for queries, 
    /// as it could be useful to know what a table looks like, before parsing 
    /// its schema by projecting only the necessary columns
    fn get_table_schema(connection: &str, table_name: &str) -> Result<Schema>;

    /// Read a table into record batches, applying any necessary limit.
    ///
    /// TODO: this returns a vector of batches, but we should change it to return
    /// an iterator or a stream for its async counterpart.
    /// The idea is that we can use the batch_size and limit to alter SQL queries
    /// to skip n records
    fn read_table(
        connection: &str,
        table_name: &str,
        limit: Option<usize>,
        batch_size: usize,
    ) -> Result<Vec<RecordBatch>>;

    /// Run a SQL query, and return its results as record batches, applying any
    /// necessary limit.
    fn read_query(
        connection: &str,
        query: &str,
        limit: Option<usize>,
        batch_size: usize,
    ) -> Result<Vec<RecordBatch>>;
}

pub trait SqlDataSink {
    /// Create a new table from an Arrow schema reference
    ///
    /// TODO: provide options such as whether to overwrite existing table
    fn create_table(
        connection: &str,
        table_name: &str,
        schema: &SchemaRef,
    ) -> Result<()>;

    /// Write record batches to a SQL table
    ///
    /// TODO: provide options (append, overwrite, fail)
    /// TODO: how should we validate schemas or map against database schemas (in append)
    fn write_to_table(
        connection: &str,
        table_name: &str,
        batches: &[RecordBatch],
    ) -> Result<()>;
}
