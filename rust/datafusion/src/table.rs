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

use std::sync::Arc;
use crate::logicalplan::{Expr, LogicalPlan};

/// Table is an abstraction of a logical query plan
trait Table {

    /// Select columns by name
    fn select_columns(&self, columns: Vec<&str>) -> Arc<Table>;

    /// Select using expressions
    fn projection(&self, expr: Vec<Expr>) -> Arc<Table>;

    /// filter
    fn filter(&self, expr: Expr) -> Arc<Table>;

    /// limit the number of rows
    fn limit(&self, n: usize) -> Arc<Table>;

    fn join(&self, other: &Table, join_condition: Expr) -> Arc<Table>;

    fn aggregate(&self, expr: Vec<Expr>) -> Arc<Table>;
    
    fn group_by(&self, expr: Vec<Expr>) -> Arc<Table>;

    /// convert to a logical plan
    fn to_logical_plan(&self) -> LogicalPlan;

}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::context::ExecutionContext;

    #[test]
    fn demonstrate_api_usage() {

        let t = test_table();

        let example = t.select_columns(vec!["a", "b", "c"])
            .limit(10);

        let plan = example.to_logical_plan();

        let mut ctx = ExecutionContext::new();
        let _result = ctx.execute(&plan, 1024*1024).unwrap();

    }

    fn test_table() -> Arc<Table> {
        unimplemented!()
    }
}