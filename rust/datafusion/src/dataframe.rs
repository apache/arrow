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

//! DataFrame API for building and executing query plans.

use crate::arrow::record_batch::RecordBatch;
use crate::error::Result;
use crate::logical_plan::{DFSchema, Expr, FunctionRegistry, JoinType, LogicalPlan};
use std::sync::Arc;

use async_trait::async_trait;

/// DataFrame represents a logical set of rows with the same named columns.
/// Similar to a [Pandas DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html) or
/// [Spark DataFrame](https://spark.apache.org/docs/latest/sql-programming-guide.html)
///
/// DataFrames are typically created by the `read_csv` and `read_parquet` methods on the
/// [ExecutionContext](../execution/context/struct.ExecutionContext.html) and can then be modified
/// by calling the transformation methods, such as `filter`, `select`, `aggregate`, and `limit`
/// to build up a query definition.
///
/// The query can be executed by calling the `collect` method.
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
#[async_trait]
pub trait DataFrame {
    /// Filter the DataFrame by column. Returns a new DataFrame only containing the
    /// specified columns.
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # fn main() -> Result<()> {
    /// let mut ctx = ExecutionContext::new();
    /// let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new())?;
    /// let df = df.select_columns(vec!["a", "b"])?;
    /// # Ok(())
    /// # }
    /// ```
    fn select_columns(&self, columns: Vec<&str>) -> Result<Arc<dyn DataFrame>>;

    /// Create a projection based on arbitrary expressions.
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # fn main() -> Result<()> {
    /// let mut ctx = ExecutionContext::new();
    /// let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new())?;
    /// let df = df.select(vec![col("a") * col("b"), col("c")])?;
    /// # Ok(())
    /// # }
    /// ```
    fn select(&self, expr: Vec<Expr>) -> Result<Arc<dyn DataFrame>>;

    /// Filter a DataFrame to only include rows that match the specified filter expression.
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # fn main() -> Result<()> {
    /// let mut ctx = ExecutionContext::new();
    /// let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new())?;
    /// let df = df.filter(col("a").lt_eq(col("b")))?;
    /// # Ok(())
    /// # }
    /// ```
    fn filter(&self, expr: Expr) -> Result<Arc<dyn DataFrame>>;

    /// Perform an aggregate query with optional grouping expressions.
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # fn main() -> Result<()> {
    /// let mut ctx = ExecutionContext::new();
    /// let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new())?;
    ///
    /// // The following use is the equivalent of "SELECT MIN(b) GROUP BY a"
    /// let _ = df.aggregate(vec![col("a")], vec![min(col("b"))])?;
    ///
    /// // The following use is the equivalent of "SELECT MIN(b)"
    /// let _ = df.aggregate(vec![], vec![min(col("b"))])?;
    /// # Ok(())
    /// # }
    /// ```
    fn aggregate(
        &self,
        group_expr: Vec<Expr>,
        aggr_expr: Vec<Expr>,
    ) -> Result<Arc<dyn DataFrame>>;

    /// Limit the number of rows returned from this DataFrame.
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # fn main() -> Result<()> {
    /// let mut ctx = ExecutionContext::new();
    /// let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new())?;
    /// let df = df.limit(100)?;
    /// # Ok(())
    /// # }
    /// ```
    fn limit(&self, n: usize) -> Result<Arc<dyn DataFrame>>;

    /// Sort the DataFrame by the specified sorting expressions. Any expression can be turned into
    /// a sort expression by calling its [sort](../logical_plan/enum.Expr.html#method.sort) method.
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # fn main() -> Result<()> {
    /// let mut ctx = ExecutionContext::new();
    /// let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new())?;
    /// let df = df.sort(vec![col("a").sort(true, true), col("b").sort(false, false)])?;
    /// # Ok(())
    /// # }
    /// ```
    fn sort(&self, expr: Vec<Expr>) -> Result<Arc<dyn DataFrame>>;

    /// Join this DataFrame with another DataFrame using the specified columns as join keys
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let mut ctx = ExecutionContext::new();
    /// let left = ctx.read_csv("tests/example.csv", CsvReadOptions::new())?;
    /// let right = ctx.read_csv("tests/example.csv", CsvReadOptions::new())?
    ///   .select(vec![
    ///     col("a").alias("a2"),
    ///     col("b").alias("b2"),
    ///     col("c").alias("c2")])?;
    /// let join = left.join(right, JoinType::Inner, &["a", "b"], &["a2", "b2"])?;
    /// let batches = join.collect().await?;
    /// # Ok(())
    /// # }
    /// ```
    fn join(
        &self,
        right: Arc<dyn DataFrame>,
        join_type: JoinType,
        left_cols: &[&str],
        right_cols: &[&str],
    ) -> Result<Arc<dyn DataFrame>>;

    /// Executes this DataFrame and collects all results into a vector of RecordBatch.
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let mut ctx = ExecutionContext::new();
    /// let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new())?;
    /// let batches = df.collect().await?;
    /// # Ok(())
    /// # }
    /// ```
    async fn collect(&self) -> Result<Vec<RecordBatch>>;

    /// Returns the schema describing the output of this DataFrame in terms of columns returned,
    /// where each column has a name, data type, and nullability attribute.

    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # fn main() -> Result<()> {
    /// let mut ctx = ExecutionContext::new();
    /// let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new())?;
    /// let schema = df.schema();
    /// # Ok(())
    /// # }
    /// ```
    fn schema(&self) -> &DFSchema;

    /// Return the logical plan represented by this DataFrame.
    fn to_logical_plan(&self) -> LogicalPlan;

    /// Return a DataFrame with the explanation of its plan so far.
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let mut ctx = ExecutionContext::new();
    /// let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new())?;
    /// let batches = df.limit(100)?.explain(false)?.collect().await?;
    /// # Ok(())
    /// # }
    /// ```
    fn explain(&self, verbose: bool) -> Result<Arc<dyn DataFrame>>;

    /// Return a `FunctionRegistry` used to plan udf's calls
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # fn main() -> Result<()> {
    /// let mut ctx = ExecutionContext::new();
    /// let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new())?;
    /// let f = df.registry();
    /// // use f.udf("name", vec![...]) to use the udf
    /// # Ok(())
    /// # }
    /// ```
    fn registry(&self) -> &dyn FunctionRegistry;
}
