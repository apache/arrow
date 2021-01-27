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

//! Boolean comparision rule rewrites redudant comparison expression involing boolean literal into
//! unary expression.

use std::sync::Arc;

use crate::error::Result;
use crate::logical_plan::{Expr, LogicalPlan, Operator};
use crate::optimizer::optimizer::OptimizerRule;
use crate::optimizer::utils;
use crate::scalar::ScalarValue;

/// Optimizer that simplifies comparison expressions involving boolean literals.
///
/// Recursively go through all expressionss and simplify the following cases:
/// * `expr = ture` to `expr`
/// * `expr = false` to `!expr`
pub struct BooleanComparison {}

impl BooleanComparison {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for BooleanComparison {
    fn optimize(&mut self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::Filter { predicate, input } => Ok(LogicalPlan::Filter {
                predicate: optimize_expr(predicate),
                input: Arc::new(self.optimize(input)?),
            }),
            // Rest: recurse into plan, apply optimization where possible
            LogicalPlan::Projection { .. }
            | LogicalPlan::Aggregate { .. }
            | LogicalPlan::Limit { .. }
            | LogicalPlan::Repartition { .. }
            | LogicalPlan::CreateExternalTable { .. }
            | LogicalPlan::Extension { .. }
            | LogicalPlan::Sort { .. }
            | LogicalPlan::Explain { .. }
            | LogicalPlan::Join { .. } => {
                let expr = utils::expressions(plan);

                // apply the optimization to all inputs of the plan
                let inputs = utils::inputs(plan);
                let new_inputs = inputs
                    .iter()
                    .map(|plan| self.optimize(plan))
                    .collect::<Result<Vec<_>>>()?;

                utils::from_plan(plan, &expr, &new_inputs)
            }
            LogicalPlan::TableScan { .. } | LogicalPlan::EmptyRelation { .. } => {
                Ok(plan.clone())
            }
        }
    }

    fn name(&self) -> &str {
        "boolean_comparison"
    }
}

/// Recursively transverses the logical plan.
fn optimize_expr(e: &Expr) -> Expr {
    match e {
        Expr::BinaryExpr { left, op, right } => {
            let left = optimize_expr(left);
            let right = optimize_expr(right);
            match op {
                Operator::Eq => match (&left, &right) {
                    (Expr::Literal(ScalarValue::Boolean(b)), _) => match b {
                        Some(true) => right,
                        Some(false) | None => Expr::Not(Box::new(right)),
                    },
                    (_, Expr::Literal(ScalarValue::Boolean(b))) => match b {
                        Some(true) => left,
                        Some(false) | None => Expr::Not(Box::new(left)),
                    },
                    _ => Expr::BinaryExpr {
                        left: Box::new(left),
                        op: Operator::Eq,
                        right: Box::new(right),
                    },
                },
                Operator::NotEq => match (&left, &right) {
                    (Expr::Literal(ScalarValue::Boolean(b)), _) => match b {
                        Some(false) | None => right,
                        Some(true) => Expr::Not(Box::new(right)),
                    },
                    (_, Expr::Literal(ScalarValue::Boolean(b))) => match b {
                        Some(false) | None => left,
                        Some(true) => Expr::Not(Box::new(left)),
                    },
                    _ => Expr::BinaryExpr {
                        left: Box::new(left),
                        op: Operator::NotEq,
                        right: Box::new(right),
                    },
                },
                _ => Expr::BinaryExpr {
                    left: Box::new(left),
                    op: op.clone(),
                    right: Box::new(right),
                },
            }
        }
        Expr::Not(expr) => Expr::Not(Box::new(optimize_expr(&expr))),
        Expr::Case {
            expr,
            when_then_expr,
            else_expr,
        } => {
            if expr.is_none() {
                // recurse into CASE WHEN condition expressions
                Expr::Case {
                    expr: None,
                    when_then_expr: when_then_expr
                        .iter()
                        .map(|(when, then)| (Box::new(optimize_expr(when)), then.clone()))
                        .collect(),
                    else_expr: else_expr.clone(),
                }
            } else {
                // when base expression is specified, when_then_expr conditions are literal values
                // so we can just skip this case
                e.clone()
            }
        }
        Expr::Alias { .. }
        | Expr::Negative { .. }
        | Expr::Column { .. }
        | Expr::InList { .. }
        | Expr::IsNotNull { .. }
        | Expr::IsNull { .. }
        | Expr::Cast { .. }
        | Expr::ScalarVariable { .. }
        | Expr::Between { .. }
        | Expr::Literal { .. }
        | Expr::ScalarFunction { .. }
        | Expr::ScalarUDF { .. }
        | Expr::AggregateFunction { .. }
        | Expr::AggregateUDF { .. }
        | Expr::Sort { .. }
        | Expr::Wildcard => e.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical_plan::{col, lit, LogicalPlanBuilder};
    use crate::test::*;

    #[test]
    fn optimize_expr_eq() -> Result<()> {
        assert_eq!(
            optimize_expr(&Expr::BinaryExpr {
                left: Box::new(lit(1)),
                op: Operator::Eq,
                right: Box::new(lit(true)),
            }),
            lit(1),
        );

        assert_eq!(
            optimize_expr(&Expr::BinaryExpr {
                left: Box::new(lit("a")),
                op: Operator::Eq,
                right: Box::new(lit(false)),
            }),
            lit("a").not(),
        );

        Ok(())
    }

    #[test]
    fn optimize_expr_not_eq() -> Result<()> {
        assert_eq!(
            optimize_expr(&Expr::BinaryExpr {
                left: Box::new(lit(1)),
                op: Operator::NotEq,
                right: Box::new(lit(true)),
            }),
            lit(1).not(),
        );

        assert_eq!(
            optimize_expr(&Expr::BinaryExpr {
                left: Box::new(lit("a")),
                op: Operator::NotEq,
                right: Box::new(lit(false)),
            }),
            lit("a"),
        );

        Ok(())
    }

    #[test]
    fn optimize_expr_not_not_eq() -> Result<()> {
        assert_eq!(
            optimize_expr(&Expr::Not(Box::new(Expr::BinaryExpr {
                left: Box::new(lit(1)),
                op: Operator::NotEq,
                right: Box::new(lit(true)),
            }))),
            lit(1).not().not(),
        );

        assert_eq!(
            optimize_expr(&Expr::Not(Box::new(Expr::BinaryExpr {
                left: Box::new(lit("a")),
                op: Operator::NotEq,
                right: Box::new(lit(false)),
            }))),
            lit("a").not(),
        );

        Ok(())
    }

    #[test]
    fn optimize_expr_case_when_then_else() -> Result<()> {
        assert_eq!(
            optimize_expr(&Box::new(Expr::Case {
                expr: None,
                when_then_expr: vec![(
                    Box::new(Expr::BinaryExpr {
                        left: Box::new(lit("a")),
                        op: Operator::NotEq,
                        right: Box::new(lit(false)),
                    }),
                    Box::new(lit("ok")),
                )],
                else_expr: Some(Box::new(lit("not ok"))),
            })),
            Expr::Case {
                expr: None,
                when_then_expr: vec![(Box::new(lit("a")), Box::new(lit("ok")))],
                else_expr: Some(Box::new(lit("not ok"))),
            }
        );

        Ok(())
    }

    fn assert_optimized_plan_eq(plan: &LogicalPlan, expected: &str) {
        let mut rule = BooleanComparison::new();
        let optimized_plan = rule.optimize(plan).expect("failed to optimize plan");
        let formatted_plan = format!("{:?}", optimized_plan);
        assert_eq!(formatted_plan, expected);
    }

    #[test]
    fn optimize_plan_eq_expr() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(&table_scan)
            .filter(col("a").eq(lit(true)))?
            .filter(col("b").eq(lit(false)))?
            .project(vec![col("a")])?
            .build()?;

        let expected = "\
        Projection: #a\
        \n  Filter: NOT #b\
        \n    Filter: #a\
        \n      TableScan: test projection=None";

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    #[test]
    fn optimize_plan_not_eq_expr() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(&table_scan)
            .filter(col("a").not_eq(lit(true)))?
            .filter(col("b").not_eq(lit(false)))?
            .limit(1)?
            .project(vec![col("a")])?
            .build()?;

        let expected = "\
        Projection: #a\
        \n  Limit: 1\
        \n    Filter: #b\
        \n      Filter: NOT #a\
        \n        TableScan: test projection=None";

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    #[test]
    fn optimize_plan_and_expr() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(&table_scan)
            .filter(col("a").not_eq(lit(true)).and(col("b").eq(lit(true))))?
            .project(vec![col("a")])?
            .build()?;

        let expected = "\
        Projection: #a\
        \n  Filter: NOT #a And #b\
        \n    TableScan: test projection=None";

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    #[test]
    fn optimize_plan_or_expr() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(&table_scan)
            .filter(col("a").not_eq(lit(true)).or(col("b").eq(lit(false))))?
            .project(vec![col("a")])?
            .build()?;

        let expected = "\
        Projection: #a\
        \n  Filter: NOT #a Or NOT #b\
        \n    TableScan: test projection=None";

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    #[test]
    fn optimize_plan_not_expr() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(&table_scan)
            .filter(col("a").eq(lit(false)).not())?
            .project(vec![col("a")])?
            .build()?;

        let expected = "\
        Projection: #a\
        \n  Filter: NOT NOT #a\
        \n    TableScan: test projection=None";

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }
}
