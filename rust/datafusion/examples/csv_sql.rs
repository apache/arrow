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

/// This example demonstrates executing a simple query against an Arrow data source (CSV) and
/// fetching results
fn main() -> Result<()> {
    // create local execution context
    let mut ctx = ExecutionContext::new();

    let testdata = std::env::var("ARROW_TEST_DATA").expect("ARROW_TEST_DATA not defined");

    // register csv file with the execution context
    ctx.register_csv(
        "aggregate_test_100",
        &format!("{}/csv/aggregate_test_100.csv", testdata),
        CsvReadOptions::new(),
    )?;

    // execute the query
    let df = ctx.sql(
        "SELECT c1, MIN(c12), MAX(c12) \
        FROM aggregate_test_100 \
        WHERE c11 > 0.1 AND c11 < 0.9 \
        GROUP BY c1",
    )?;
    let results = df.collect()?;

    // print the results
    pretty::print_batches(&results)?;

    Ok(())
}
