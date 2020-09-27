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

use arrow::util::pretty;

use datafusion::error::Result;
use datafusion::prelude::*;

/// This example demonstrates executing a simple query against an Arrow data source (Parquet) and
/// fetching results, using the DataFrame trait
#[tokio::main]
async fn main() -> Result<()> {
    // create local execution context
    let mut ctx = ExecutionContext::new();

    let testdata =
        std::env::var("PARQUET_TEST_DATA").expect("PARQUET_TEST_DATA not defined");

    let filename = &format!("{}/alltypes_plain.parquet", testdata);

    // define the query using the DataFrame trait
    let df = ctx
        .read_parquet(filename)?
        .filter(col("id").gt(lit(1)))?
        .filter(col("tinyint_col").lt(col("tinyint_col")))?;

    // execute the query
    let results = df.collect().await?;

    // print the results
    pretty::print_batches(&results)?;

    Ok(())
}
