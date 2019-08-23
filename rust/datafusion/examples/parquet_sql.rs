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

extern crate arrow;
extern crate datafusion;

use arrow::array::{BinaryArray, Float64Array, Int32Array};

use datafusion::execution::context::ExecutionContext;

/// This example demonstrates executing a simple query against an Arrow data source (Parquet) and
/// fetching results
fn main() {
    // create local execution context
    let mut ctx = ExecutionContext::new();

    let testdata =
        ::std::env::var("PARQUET_TEST_DATA").expect("PARQUET_TEST_DATA not defined");

    // register parquet file with the execution context
    ctx.register_parquet(
        "alltypes_plain",
        &format!("{}/alltypes_plain.parquet", testdata),
    )
    .unwrap();

    // simple selection
    let sql = "SELECT int_col, double_col, date_string_col FROM alltypes_plain WHERE id > 1 AND tinyint_col < double_col";

    // execute the query
    let relation = ctx.sql(&sql, 1024 * 1024).unwrap();

    // display the relation
    let mut results = relation.borrow_mut();

    while let Some(batch) = results.next().unwrap() {
        println!(
            "RecordBatch has {} rows and {} columns",
            batch.num_rows(),
            batch.num_columns()
        );

        let int = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        let double = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        let date = batch
            .column(2)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();

        for i in 0..batch.num_rows() {
            let date_value: String = String::from_utf8(date.value(i).to_vec()).unwrap();

            println!(
                "Date: {}, Int: {}, Double: {}",
                date_value,
                int.value(i),
                double.value(i)
            );
        }
    }
}
