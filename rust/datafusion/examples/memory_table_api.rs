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

use std::boxed::Box;
use std::sync::Arc;

use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow::util::pretty;

use datafusion::datasource::MemTable;
use datafusion::error::Result;
use datafusion::execution::context::ExecutionContext;
use datafusion::logicalplan::lit;

/// This example demonstrates basic uses of the Table API on an in-memory table
fn main() -> Result<()> {
    // define a schema.
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, false),
        Field::new("b", DataType::Int32, false),
    ]));

    // define data.
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
            Arc::new(Int32Array::from(vec![1, 10, 10, 100])),
        ],
    )?;

    // declare a new context. In spark API, this corresponds to a new spark SQLsession
    let mut ctx = ExecutionContext::new();

    // declare a table in memory. In spark API, this corresponds to createDataFrame(...).
    let provider = MemTable::new(schema, vec![vec![batch]])?;
    ctx.register_table("t", Box::new(provider));
    let t = ctx.table("t")?;

    // construct an expression corresponding to "SELECT a, b FROM t WHERE b = 10" in SQL
    let filter = t.col("b")?.eq(&lit(10));

    let t = t.select_columns(vec!["a", "b"])?.filter(filter)?;

    // execute
    let results = t.collect(&mut ctx, 10)?;

    // print the results
    pretty::print_batches(&results)?;

    Ok(())
}
