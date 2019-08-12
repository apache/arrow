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

//! Table API for building a logical query plan. This is similar to the Table API in Ibis
//! and the DataFrame API in Apache Spark

use crate::error::Result;
use crate::logicalplan::{Expr, LogicalPlan};
use std::sync::Arc;

/// Table is an abstraction of a logical query plan
pub trait Table {
    /// Select columns by name
    fn select_columns(&self, columns: Vec<&str>) -> Result<Arc<dyn Table>>;

    /// Create a projection based on arbitrary expressions
    fn select(&self, expr: Vec<Expr>) -> Result<Arc<dyn Table>>;

    /// Create a selection based on a filter expression
    fn filter(&self, expr: Expr) -> Result<Arc<dyn Table>>;

    /// Perform an aggregate query
    fn aggregate(
        &self,
        group_expr: Vec<Expr>,
        aggr_expr: Vec<Expr>,
    ) -> Result<Arc<dyn Table>>;

    /// limit the number of rows
    fn limit(&self, n: usize) -> Result<Arc<dyn Table>>;

    /// Return the logical plan
    fn to_logical_plan(&self) -> Arc<LogicalPlan>;

    /// Return an expression representing a column within this table
    fn col(&self, name: &str) -> Result<Expr>;

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

    /// Return the index of a column within this table's schema
    fn column_index(&self, name: &str) -> Result<usize>;
}
