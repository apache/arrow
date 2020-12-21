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
// Clippy lints, some should be disabled incrementally
#![allow(
    clippy::float_cmp,
    clippy::module_inception,
    clippy::needless_lifetimes,
    clippy::needless_range_loop,
    clippy::new_without_default,
    clippy::ptr_arg,
    clippy::type_complexity
)]

//! DataFusion is an extensible query execution framework that uses
//! [Apache Arrow](https://arrow.apache.org) as its in-memory format.
//!
//! DataFusion supports both an SQL and a DataFrame API for building logical query plans
//! as well as a query optimizer and execution engine capable of parallel execution
//! against partitioned data sources (CSV and Parquet) using threads.
//!
//! Below is an example of how to execute a query against a CSV using [`DataFrames`](dataframe::DataFrame):
//!
//! ```rust
//! # use datafusion::prelude::*;
//! # use datafusion::error::Result;
//! # use arrow::record_batch::RecordBatch;
//!
//! # #[tokio::main]
//! # async fn main() -> Result<()> {
//! let mut ctx = ExecutionContext::new();
//!
//! // create the dataframe
//! let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new())?;
//!
//! // create a plan
//! let df = df.filter(col("a").lt_eq(col("b")))?
//!            .aggregate(vec![col("a")], vec![min(col("b"))])?
//!            .limit(100)?;
//!
//! // execute the plan
//! let results: Vec<RecordBatch> = df.collect().await?;
//! # Ok(())
//! # }
//! ```
//!
//! and how to execute a query against a CSV using SQL:
//!
//! ```
//! # use datafusion::prelude::*;
//! # use datafusion::error::Result;
//! # use arrow::record_batch::RecordBatch;
//!
//! # #[tokio::main]
//! # async fn main() -> Result<()> {
//! let mut ctx = ExecutionContext::new();
//!
//! ctx.register_csv("example", "tests/example.csv", CsvReadOptions::new())?;
//!
//! // create a plan
//! let df = ctx.sql("SELECT a, MIN(b) FROM example GROUP BY a LIMIT 100")?;
//!
//! // execute the plan
//! let results: Vec<RecordBatch> = df.collect().await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Parse, Plan, Optimize, Execute
//!
//! DataFusion is a fully fledged query engine capable of performing complex operations.
//! Specifically, when DataFusion receives an SQL query, there are different steps
//! that it passes through until a result is obtained. Broadly, they are:
//!
//! 1. The string is parsed to an Abstract syntax tree (AST) using [sqlparser](https://docs.rs/sqlparser/0.6.1/sqlparser/).
//! 2. The planner [`SqlToRel`](sql::planner::SqlToRel) converts logical expressions on the AST to logical expressions [`Expr`s](logical_plan::Expr).
//! 3. The planner [`SqlToRel`](sql::planner::SqlToRel) converts logical nodes on the AST to a [`LogicalPlan`](logical_plan::LogicalPlan).
//! 4. [`OptimizerRules`](optimizer::optimizer::OptimizerRule) are applied to the [`LogicalPlan`](logical_plan::LogicalPlan) to optimize it.
//! 5. The [`LogicalPlan`](logical_plan::LogicalPlan) is converted to an [`ExecutionPlan`](physical_plan::ExecutionPlan) by a [`PhysicalPlanner`](physical_plan::PhysicalPlanner)
//! 6. The [`ExecutionPlan`](physical_plan::ExecutionPlan) is executed against data through the [`ExecutionContext`](execution::context::ExecutionContext)
//!
//! With a [`DataFrame`](dataframe::DataFrame) API, steps 1-3 are not used as the DataFrame builds the [`LogicalPlan`](logical_plan::LogicalPlan) directly.
//!
//! Phases 1-5 are typically cheap when compared to phase 6, and thus DataFusion puts a
//! lot of effort to ensure that phase 6 runs efficiently and without errors.
//!
//! DataFusion's planning is divided in two main parts: logical planning and physical planning.
//!
//! ### Logical plan
//!
//! Logical planning yields [`logical plans`](logical_plan::LogicalPlan) and [`logical expressions`](logical_plan::Expr).
//! These are [`Schema`](arrow::datatypes::Schema)-aware traits that represent statements whose result is independent of how it should physically be executed.
//!
//! A [`LogicalPlan`](logical_plan::LogicalPlan) is a Direct Asyclic graph of other [`LogicalPlan`s](logical_plan::LogicalPlan) and each node contains logical expressions ([`Expr`s](logical_plan::Expr)).
//! All of these are located in [`logical_plan`](logical_plan).
//!
//! ### Physical plan
//!
//! A Physical plan ([`ExecutionPlan`](physical_plan::ExecutionPlan)) is a plan that can be executed against data.
//! Contrarily to a logical plan, the physical plan has concrete information about how the calculation
//! should be performed (e.g. what Rust functions are used) and how data should be loaded into memory.
//!
//! [`ExecutionPlan`](physical_plan::ExecutionPlan) uses the Arrow format as its in-memory representation of data, through the [arrow] crate.
//! We recommend going through [its documentation](arrow) for details on how the data is physically represented.
//!
//! A [`ExecutionPlan`](physical_plan::ExecutionPlan) is composed by nodes (implement the trait [`ExecutionPlan`](physical_plan::ExecutionPlan)),
//! and each node is composed by physical expressions ([`PhysicalExpr`](physical_plan::PhysicalExpr))
//! or aggreagate expressions ([`AggregateExpr`](physical_plan::AggregateExpr)).
//! All of these are located in the module [`physical_plan`](physical_plan).
//!
//! Broadly speaking,
//!
//! * an [`ExecutionPlan`](physical_plan::ExecutionPlan) receives a partition number and asyncronosly returns
//!   an iterator over [`RecordBatch`](arrow::record_batch::RecordBatch)
//!   (a node-specific struct that implements [`RecordBatchReader`](arrow::record_batch::RecordBatchReader))
//! * a [`PhysicalExpr`](physical_plan::PhysicalExpr) receives a [`RecordBatch`](arrow::record_batch::RecordBatch)
//!   and returns an [`Array`](arrow::array::Array)
//! * an [`AggregateExpr`](physical_plan::AggregateExpr) receives [`RecordBatch`es](arrow::record_batch::RecordBatch)
//!   and returns a [`RecordBatch`](arrow::record_batch::RecordBatch) of a single row(*)
//!
//! (*) Technically, it aggregates the results on each partition and then merges the results into a single partition.
//!
//! The following physical nodes are currently implemented:
//!
//! * Projection: [`ProjectionExec`](physical_plan::projection::ProjectionExec)
//! * Filter: [`FilterExec`](physical_plan::filter::FilterExec)
//! * Hash and Grouped aggregations: [`HashAggregateExec`](physical_plan::hash_aggregate::HashAggregateExec)
//! * Sort: [`SortExec`](physical_plan::sort::SortExec)
//! * Merge (partitions): [`MergeExec`](physical_plan::merge::MergeExec)
//! * Limit: [`LocalLimitExec`](physical_plan::limit::LocalLimitExec) and [`GlobalLimitExec`](physical_plan::limit::GlobalLimitExec)
//! * Scan a CSV: [`CsvExec`](physical_plan::csv::CsvExec)
//! * Scan a Parquet: [`ParquetExec`](physical_plan::parquet::ParquetExec)
//! * Scan from memory: [`MemoryExec`](physical_plan::memory::MemoryExec)
//! * Explain the plan: [`ExplainExec`](physical_plan::explain::ExplainExec)
//!
//! ## Customize
//!
//! DataFusion allows users to
//! * extend the planner to use user-defined logical and physical nodes ([`QueryPlanner`](execution::context::QueryPlanner))
//! * declare and use user-defined scalar functions ([`ScalarUDF`](physical_plan::udf::ScalarUDF))
//! * declare and use user-defined aggregate functions ([`AggregateUDF`](physical_plan::udaf::AggregateUDF))
//!
//! you can find examples of each of them in examples section.

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
pub mod scalar;
pub mod sql;
pub mod variable;

#[cfg(test)]
pub mod test;
