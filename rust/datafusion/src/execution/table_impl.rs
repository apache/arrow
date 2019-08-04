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

use crate::arrow::datatypes::{DataType, Field, Schema};
use crate::error::{ExecutionError, Result};
use crate::logicalplan::Expr::Literal;
use crate::logicalplan::ScalarValue;
use crate::logicalplan::{Expr, LogicalPlan};
use crate::table::*;

/// Implementation of Table API
pub struct TableImpl {
    plan: Arc<LogicalPlan>,
}

impl TableImpl {
    /// Create a new Table based on an existing logical plan
    pub fn new(plan: Arc<LogicalPlan>) -> Self {
        Self { plan }
    }
}

impl Table for TableImpl {
    /// Apply a projection based on a list of column names
    fn select_columns(&self, columns: Vec<&str>) -> Result<Arc<dyn Table>> {
        let mut expr: Vec<Expr> = Vec::with_capacity(columns.len());
        for column_name in columns {
            let i = self.column_index(column_name)?;
            expr.push(Expr::Column(i));
        }
        self.select(expr)
    }

    /// Create a projection based on arbitrary expressions
    fn select(&self, expr_list: Vec<Expr>) -> Result<Arc<dyn Table>> {
        let schema = self.plan.schema();
        let mut field: Vec<Field> = Vec::with_capacity(expr_list.len());

        for expr in &expr_list {
            match expr {
                Expr::Column(i) => {
                    field.push(schema.field(*i).clone());
                }
                other => {
                    return Err(ExecutionError::NotImplemented(format!(
                        "Expr {:?} is not currently supported in this context",
                        other
                    )))
                }
            }
        }

        Ok(Arc::new(TableImpl::new(Arc::new(
            LogicalPlan::Projection {
                expr: expr_list.clone(),
                input: self.plan.clone(),
                schema: Arc::new(Schema::new(field)),
            },
        ))))
    }

    /// Create a selection based on a filter expression
    fn filter(&self, expr: Expr) -> Result<Arc<dyn Table>> {
        Ok(Arc::new(TableImpl::new(Arc::new(LogicalPlan::Selection {
            expr,
            input: self.plan.clone(),
        }))))
    }

    /// Perform an aggregate query
    fn aggregate(
        &self,
        group_expr: Vec<Expr>,
        aggr_expr: Vec<Expr>,
    ) -> Result<Arc<dyn Table>> {
        Ok(Arc::new(TableImpl::new(Arc::new(LogicalPlan::Aggregate {
            input: self.plan.clone(),
            group_expr,
            aggr_expr,
            schema: Arc::new(Schema::new(vec![])),
        }))))
    }

    /// Limit the number of rows
    fn limit(&self, n: usize) -> Result<Arc<dyn Table>> {
        Ok(Arc::new(TableImpl::new(Arc::new(LogicalPlan::Limit {
            expr: Literal(ScalarValue::UInt32(n as u32)),
            input: self.plan.clone(),
            schema: self.plan.schema().clone(),
        }))))
    }

    /// Return an expression representing a column within this table
    fn col(&self, name: &str) -> Result<Expr> {
        Ok(Expr::Column(self.column_index(name)?))
    }

    /// Return the index of a column within this table's schema
    fn column_index(&self, name: &str) -> Result<usize> {
        let schema = self.plan.schema();
        match schema.column_with_name(name) {
            Some((i, _)) => Ok(i),
            _ => Err(ExecutionError::InvalidColumn(format!(
                "No column named '{}'",
                name
            ))),
        }
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
    fn to_logical_plan(&self) -> Arc<LogicalPlan> {
        self.plan.clone()
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
    use std::env;

    #[test]
    fn column_index() {
        let t = test_table();
        assert_eq!(0, t.column_index("c1").unwrap());
        assert_eq!(1, t.column_index("c2").unwrap());
        assert_eq!(12, t.column_index("c13").unwrap());
    }

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
    fn select_invalid_column() -> Result<()> {
        let t = test_table();

        match t.col("invalid_column_name") {
            Ok(_) => panic!(),
            Err(e) => assert_eq!(
                "InvalidColumn(\"No column named \\\'invalid_column_name\\\'\")",
                format!("{:?}", e)
            ),
        }

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
    fn create_plan(sql: &str) -> Result<Arc<LogicalPlan>> {
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
        let schema = aggr_test_schema();
        let testdata = env::var("ARROW_TEST_DATA").expect("ARROW_TEST_DATA not defined");
        ctx.register_csv(
            "aggregate_test_100",
            &format!("{}/csv/aggregate_test_100.csv", testdata),
            &schema,
            true,
        );
    }

    fn aggr_test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
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
        ]))
    }

}
