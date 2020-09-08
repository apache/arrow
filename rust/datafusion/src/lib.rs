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

#![warn(missing_docs)]

//! DataFusion is an extensible query execution framework that uses
//! Apache Arrow as the memory model.
//!
//! DataFusion supports both SQL and a DataFrame API for building logical query plans
//! and also provides a query optimizer and execution engine capable of parallel execution
//! against partitioned data sources (CSV and Parquet) using threads.
//!
//! DataFusion currently supports simple projection, filter, and aggregate queries.
//!
/// [ExecutionContext](../execution/context/struct.ExecutionContext.html) is the main interface
/// for executing queries with DataFusion.
///
/// The following example demonstrates how to use the context to execute a query against a CSV
/// data source using the DataFrame API:
///
/// ```
/// # use datafusion::prelude::*;
/// # use datafusion::error::Result;
/// # fn main() -> Result<()> {
/// let mut ctx = ExecutionContext::new();
/// let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new())?;
/// let df = df.filter(col("a").lt_eq(col("b")))?
///            .aggregate(vec![col("a")], vec![min(col("b"))])?
///            .limit(100)?;
/// let results = df.collect();
/// # Ok(())
/// # }
/// ```
///
/// The following example demonstrates how to execute the same query using SQL:
///
/// ```
/// use datafusion::prelude::*;
///
/// let mut ctx = ExecutionContext::new();
/// ctx.register_csv("example", "tests/example.csv", CsvReadOptions::new()).unwrap();
/// let results = ctx.sql("SELECT a, MIN(b) FROM example GROUP BY a LIMIT 100").unwrap();
/// ```
extern crate arrow;
extern crate sqlparser;

pub mod dataframe;
pub mod datasource;
pub mod error;
pub mod execution;
pub mod logical_plan;
pub mod optimizer;
pub mod physical_plan;
pub mod prelude;
pub mod sql;
pub mod variable;

#[cfg(test)]
pub mod test;
