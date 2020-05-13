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

//! Implementation of Table API

use std::sync::Arc;

use crate::arrow::datatypes::DataType;
use crate::arrow::record_batch::RecordBatch;
use crate::error::{ExecutionError, Result};
use crate::execution::context::ExecutionContext;
use crate::logicalplan::{Expr, LogicalPlan};
use crate::logicalplan::{LogicalPlanBuilder, ScalarValue};
use crate::table::*;

/// Implementation of Table API
pub struct TableImpl {
    plan: LogicalPlan,
}

impl TableImpl {
    /// Create a new Table based on an existing logical plan
    pub fn new(plan: &LogicalPlan) -> Self {
        Self { plan: plan.clone() }
    }
}

impl Table for TableImpl {
    /// Apply a projection based on a list of column names
    fn select_columns(&self, columns: Vec<&str>) -> Result<Arc<dyn Table>> {
        let exprs = columns
            .iter()
            .map(|name| {
                self.plan
                    .schema()
                    .index_of(name.to_owned())
                    .and_then(|i| Ok(Expr::Column(i)))
                    .map_err(|e| e.into())
            })
            .collect::<Result<Vec<_>>>()?;
        self.select(exprs)
    }

    /// Create a projection based on arbitrary expressions
    fn select(&self, expr_list: Vec<Expr>) -> Result<Arc<dyn Table>> {
        let plan = LogicalPlanBuilder::from(&self.plan)
            .project(expr_list)?
            .build()?;
        Ok(Arc::new(TableImpl::new(&plan)))
    }

    /// Create a selection based on a filter expression
    fn filter(&self, expr: Expr) -> Result<Arc<dyn Table>> {
        let plan = LogicalPlanBuilder::from(&self.plan).filter(expr)?.build()?;
        Ok(Arc::new(TableImpl::new(&plan)))
    }

    /// Perform an aggregate query
    fn aggregate(
        &self,
        group_expr: Vec<Expr>,
        aggr_expr: Vec<Expr>,
    ) -> Result<Arc<dyn Table>> {
        let plan = LogicalPlanBuilder::from(&self.plan)
            .aggregate(group_expr, aggr_expr)?
            .build()?;
        Ok(Arc::new(TableImpl::new(&plan)))
    }

    /// Limit the number of rows
    fn limit(&self, n: u32) -> Result<Arc<dyn Table>> {
        let plan = LogicalPlanBuilder::from(&self.plan)
            .limit(Expr::Literal(ScalarValue::UInt32(n)))?
            .build()?;
        Ok(Arc::new(TableImpl::new(&plan)))
    }

    /// Return an expression representing a column within this table
    fn col(&self, name: &str) -> Result<Expr> {
        Ok(Expr::Column(self.plan.schema().index_of(name)?))
    }

    /// Create an expression to represent the min() aggregate function
    fn min(&self, expr: &Expr) -> Result<Expr> {
        self.aggregate_expr("MIN", expr)
    }

    /// Create an expression to represent the max() aggregate function
    fn max(&self, expr: &Expr) -> Result<Expr> {
        self.aggregate_expr("MAX", expr)
    }

    /// Create an expression to represent the sum() aggregate function
    fn sum(&self, expr: &Expr) -> Result<Expr> {
        self.aggregate_expr("SUM", expr)
    }

    /// Create an expression to represent the avg() aggregate function
    fn avg(&self, expr: &Expr) -> Result<Expr> {
        self.aggregate_expr("AVG", expr)
    }

    /// Create an expression to represent the count() aggregate function
    fn count(&self, expr: &Expr) -> Result<Expr> {
        self.aggregate_expr("COUNT", expr)
    }

    /// Convert to logical plan
    fn to_logical_plan(&self) -> LogicalPlan {
        self.plan.clone()
    }

    fn collect(
        &self,
        ctx: &mut ExecutionContext,
        batch_size: usize,
    ) -> Result<Vec<RecordBatch>> {
        ctx.collect_plan(&self.plan.clone(), batch_size)
    }
}

impl TableImpl {
    /// Determine the data type for a given expression
    fn get_data_type(&self, expr: &Expr) -> Result<DataType> {
        match expr {
            Expr::Column(i) => Ok(self.plan.schema().field(*i).data_type().clone()),
            _ => Err(ExecutionError::General(format!(
                "Could not determine data type for expr {:?}",
                expr
            ))),
        }
    }

    /// Create an expression to represent a named aggregate function
    fn aggregate_expr(&self, name: &str, expr: &Expr) -> Result<Expr> {
        let return_type = self.get_data_type(expr)?;
        Ok(Expr::AggregateFunction {
            name: name.to_string(),
            args: vec![expr.clone()],
            return_type,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::context::ExecutionContext;
    use crate::test;

    #[test]
    fn select_columns() -> Result<()> {
        // build plan using Table API
        let t = test_table();
        let t2 = t.select_columns(vec!["c1", "c2", "c11"])?;
        let plan = t2.to_logical_plan();

        // build query using SQL
        let sql_plan = create_plan("SELECT c1, c2, c11 FROM aggregate_test_100")?;

        // the two plans should be identical
        assert_same_plan(&plan, &sql_plan);

        Ok(())
    }

    #[test]
    fn select_expr() -> Result<()> {
        // build plan using Table API
        let t = test_table();
        let t2 = t.select(vec![t.col("c1")?, t.col("c2")?, t.col("c11")?])?;
        let plan = t2.to_logical_plan();

        // build query using SQL
        let sql_plan = create_plan("SELECT c1, c2, c11 FROM aggregate_test_100")?;

        // the two plans should be identical
        assert_same_plan(&plan, &sql_plan);

        Ok(())
    }

    #[test]
    fn aggregate() -> Result<()> {
        // build plan using Table API
        let t = test_table();
        let group_expr = vec![t.col("c1")?];
        let c12 = t.col("c12")?;
        let aggr_expr = vec![
            t.min(&c12)?,
            t.max(&c12)?,
            t.avg(&c12)?,
            t.sum(&c12)?,
            t.count(&c12)?,
        ];

        let t2 = t.aggregate(group_expr.clone(), aggr_expr.clone())?;

        let plan = t2.to_logical_plan();

        // build same plan using SQL API
        let sql = "SELECT c1, MIN(c12), MAX(c12), AVG(c12), SUM(c12), COUNT(c12) \
                   FROM aggregate_test_100 \
                   GROUP BY c1";
        let sql_plan = create_plan(sql)?;

        // the two plans should be identical
        assert_same_plan(&plan, &sql_plan);

        Ok(())
    }

    #[test]
    fn limit() -> Result<()> {
        // build query using Table API
        let t = test_table();
        let t2 = t.select_columns(vec!["c1", "c2", "c11"])?.limit(10)?;
        let plan = t2.to_logical_plan();

        // build query using SQL
        let sql_plan =
            create_plan("SELECT c1, c2, c11 FROM aggregate_test_100 LIMIT 10")?;

        // the two plans should be identical
        assert_same_plan(&plan, &sql_plan);

        Ok(())
    }

    /// Compare the formatted string representation of two plans for equality
    fn assert_same_plan(plan1: &LogicalPlan, plan2: &LogicalPlan) {
        assert_eq!(format!("{:?}", plan1), format!("{:?}", plan2));
    }

    /// Create a logical plan from a SQL query
    fn create_plan(sql: &str) -> Result<LogicalPlan> {
        let mut ctx = ExecutionContext::new();
        register_aggregate_csv(&mut ctx);
        ctx.create_logical_plan(sql)
    }

    fn test_table() -> Arc<dyn Table + 'static> {
        let mut ctx = ExecutionContext::new();
        register_aggregate_csv(&mut ctx);
        ctx.table("aggregate_test_100").unwrap()
    }

    fn register_aggregate_csv(ctx: &mut ExecutionContext) {
        let schema = test::aggr_test_schema();
        let testdata = test::arrow_testdata_path();
        ctx.register_csv(
            "aggregate_test_100",
            &format!("{}/csv/aggregate_test_100.csv", testdata),
            &schema,
            true,
        );
    }
}
