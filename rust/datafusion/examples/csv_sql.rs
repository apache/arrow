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

use arrow::datatypes::{DataType, Field, Schema};
use arrow::util::pretty;

use datafusion::error::Result;
use datafusion::execution::context::ExecutionContext;

/// This example demonstrates executing a simple query against an Arrow data source (CSV) and
/// fetching results
fn main() -> Result<()> {
    // create local execution context
    let mut ctx = ExecutionContext::new();

    // define schema for data source (csv file)
    let schema = Schema::new(vec![
        Field::new("c1", DataType::Utf8, false),
        Field::new("c2", DataType::UInt32, false),
        Field::new("c3", DataType::Int8, false),
        Field::new("c4", DataType::Int16, false),
        Field::new("c5", DataType::Int32, false),
        Field::new("c6", DataType::Int64, false),
        Field::new("c7", DataType::UInt8, false),
        Field::new("c8", DataType::UInt16, false),
        Field::new("c9", DataType::UInt32, false),
        Field::new("c10", DataType::UInt64, false),
        Field::new("c11", DataType::Float32, false),
        Field::new("c12", DataType::Float64, false),
        Field::new("c13", DataType::Utf8, false),
    ]);

    let testdata = std::env::var("ARROW_TEST_DATA").expect("ARROW_TEST_DATA not defined");

    // register csv file with the execution context
    ctx.register_csv(
        "aggregate_test_100",
        &format!("{}/csv/aggregate_test_100.csv", testdata),
        &schema,
        true,
    );

    let sql = "SELECT c1, MIN(c12), MAX(c12) FROM aggregate_test_100 WHERE c11 > 0.1 AND c11 < 0.9 GROUP BY c1";

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
