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

//! Optimizer rule to replace UnresolvedColumns with Columns

use crate::error::Result;
use crate::logicalplan::LogicalPlan;
use crate::logicalplan::{Expr, LogicalPlanBuilder};
use crate::optimizer::optimizer::OptimizerRule;
use arrow::datatypes::Schema;

/// Replace UnresolvedColumns with Columns
pub struct ResolveColumnsRule {}

impl ResolveColumnsRule {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for ResolveColumnsRule {
    fn optimize(&mut self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::Projection { input, expr, .. } => {
                Ok(LogicalPlanBuilder::from(&self.optimize(input.as_ref())?)
                    .project(rewrite_expr_list(expr, &input.schema())?)?
                    .build()?)
            }
            LogicalPlan::Selection { expr, input } => Ok(LogicalPlanBuilder::from(input)
                .filter(rewrite_expr(expr, &input.schema())?)?
                .build()?),
            LogicalPlan::Aggregate {
                input,
                group_expr,
                aggr_expr,
                ..
            } => Ok(LogicalPlanBuilder::from(input)
                .aggregate(
                    rewrite_expr_list(group_expr, &input.schema())?,
                    rewrite_expr_list(aggr_expr, &input.schema())?,
                )?
                .build()?),
            LogicalPlan::Sort { input, expr, .. } => {
                Ok(LogicalPlanBuilder::from(&self.optimize(input)?)
                    .sort(rewrite_expr_list(expr, &input.schema())?)?
                    .build()?)
            }
            _ => Ok(plan.clone()),
        }
    }
}

fn rewrite_expr_list(expr: &[Expr], schema: &Schema) -> Result<Vec<Expr>> {
    Ok(expr
        .iter()
        .map(|e| rewrite_expr(e, schema))
        .collect::<Result<Vec<_>>>()?)
}

fn rewrite_expr(expr: &Expr, schema: &Schema) -> Result<Expr> {
    match expr {
        Expr::Alias(expr, alias) => Ok(rewrite_expr(&expr, schema)?.alias(&alias)),
        Expr::BinaryExpr { left, op, right } => Ok(Expr::BinaryExpr {
            left: Box::new(rewrite_expr(&left, schema)?),
            op: op.clone(),
            right: Box::new(rewrite_expr(&right, schema)?),
        }),
        Expr::Not(expr) => Ok(Expr::Not(Box::new(rewrite_expr(&expr, schema)?))),
        Expr::IsNotNull(expr) => {
            Ok(Expr::IsNotNull(Box::new(rewrite_expr(&expr, schema)?)))
        }
        Expr::IsNull(expr) => Ok(Expr::IsNull(Box::new(rewrite_expr(&expr, schema)?))),
        Expr::Cast { expr, data_type } => Ok(Expr::Cast {
            expr: Box::new(rewrite_expr(&expr, schema)?),
            data_type: data_type.clone(),
        }),
        Expr::Sort {
            expr,
            asc,
            nulls_first,
        } => Ok(Expr::Sort {
            expr: Box::new(rewrite_expr(&expr, schema)?),
            asc: asc.clone(),
            nulls_first: nulls_first.clone(),
        }),
        Expr::ScalarFunction {
            name,
            args,
            return_type,
        } => Ok(Expr::ScalarFunction {
            name: name.clone(),
            args: rewrite_expr_list(args, schema)?,
            return_type: return_type.clone(),
        }),
        Expr::AggregateFunction {
            name,
            args,
            return_type,
        } => Ok(Expr::AggregateFunction {
            name: name.clone(),
            args: rewrite_expr_list(args, schema)?,
            return_type: return_type.clone(),
        }),
        _ => Ok(expr.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logicalplan::col;
    use crate::test::*;

    #[test]
    fn aggregate_no_group_by() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(&table_scan)
            .aggregate(vec![col("a")], vec![max(col("b"))])?
            .build()?;

        // plan has unresolve columns
        let expected = "Aggregate: groupBy=[[#a]], aggr=[[MAX(#b)]]\n  TableScan: test projection=None";
        assert_eq!(format!("{:?}", plan), expected);

        // optimized plan has resolved columns
        let expected = "Aggregate: groupBy=[[#a]], aggr=[[MAX(#b)]]\n  TableScan: test projection=None";
        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    fn assert_optimized_plan_eq(plan: &LogicalPlan, expected: &str) {
        let optimized_plan = optimize(plan).expect("failed to optimize plan");
        let formatted_plan = format!("{:?}", optimized_plan);
        assert_eq!(formatted_plan, expected);
    }

    fn optimize(plan: &LogicalPlan) -> Result<LogicalPlan> {
        let mut rule = ResolveColumnsRule::new();
        rule.optimize(plan)
    }
}
