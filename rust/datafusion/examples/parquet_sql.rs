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
use datafusion::execution::context::ExecutionContext;

/// This example demonstrates executing a simple query against an Arrow data source (Parquet) and
/// fetching results
fn main() -> Result<()> {
    // create local execution context
    let mut ctx = ExecutionContext::new();

    let testdata =
        std::env::var("PARQUET_TEST_DATA").expect("PARQUET_TEST_DATA not defined");

    // register parquet file with the execution context
    ctx.register_parquet(
        "alltypes_plain",
        &format!("{}/alltypes_plain.parquet", testdata),
    )?;

    // simple selection
    let sql = "SELECT int_col, double_col, CAST(date_string_col as VARCHAR) FROM alltypes_plain WHERE id > 1 AND tinyint_col < double_col";

    // create the query plan
    let plan = ctx.create_logical_plan(&sql)?;
    let plan = ctx.optimize(&plan)?;
    let plan = ctx.create_physical_plan(&plan, 1024 * 1024)?;

    // execute the query
    let results = ctx.collect(plan.as_ref())?;

    // print the results
    pretty::print_batches(&results)?;

    Ok(())
}
