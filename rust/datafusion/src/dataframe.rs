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
use crate::logicalplan::{Expr, LogicalPlan};
use arrow::datatypes::Schema;
use std::sync::Arc;

/// DataFrame represents a logical set of rows with the same named columns.
/// Similar to a [Pandas DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html) or [Spark DataFrame](https://spark.apache.org/docs/latest/sql-programming-guide.html)
pub trait DataFrame {
    /// Select columns by name
    fn select_columns(&self, columns: Vec<&str>) -> Result<Arc<dyn DataFrame>>;

    /// Create a projection based on arbitrary expressions
    fn select(&self, expr: Vec<Expr>) -> Result<Arc<dyn DataFrame>>;

    /// Create a selection based on a filter expression
    fn filter(&self, expr: Expr) -> Result<Arc<dyn DataFrame>>;

    /// Perform an aggregate query
    fn aggregate(
        &self,
        group_expr: Vec<Expr>,
        aggr_expr: Vec<Expr>,
    ) -> Result<Arc<dyn DataFrame>>;

    /// limit the number of rows
    fn limit(&self, n: usize) -> Result<Arc<dyn DataFrame>>;

    /// Return the logical plan
    fn to_logical_plan(&self) -> LogicalPlan;

    /// Collects the result as a vector of RecordBatch.
    fn collect(&self, batch_size: usize) -> Result<Vec<RecordBatch>>;

    /// Returns the schema
    fn schema(&self) -> &Schema;

    //TODO these methods should be removed out of this trait soon and be standalone functions
    // instead but this depends on some refactoring of how aggregate functions are registered

    /// Create an expression to represent the min() aggregate function
    fn min(&self, expr: &Expr) -> Result<Expr>;

    /// Create an expression to represent the max() aggregate function
    fn max(&self, expr: &Expr) -> Result<Expr>;

    /// Create an expression to represent the sum() aggregate function
    fn sum(&self, expr: &Expr) -> Result<Expr>;

    /// Create an expression to represent the avg() aggregate function
    fn avg(&self, expr: &Expr) -> Result<Expr>;

    /// Create an expression to represent the count() aggregate function
    fn count(&self, expr: &Expr) -> Result<Expr>;
}
