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

use arrow::datatypes::DataType;

use crate::error::Result;
use crate::logical_plan::{DFSchemaRef, Expr, LogicalPlan, Operator};
use crate::optimizer::optimizer::OptimizerRule;
use crate::optimizer::utils;
use crate::scalar::ScalarValue;

/// Optimizer that simplifies comparison expressions involving boolean literals.
///
/// Recursively go through all expressionss and simplify the following cases:
/// * `expr = true` and `expr != false` to `expr` when `expr` is of boolean type
/// * `expr = false` and `expr != true` to `!expr` when `expr` is of boolean type
/// * `true = true` and `false = false` to `true`
/// * `false = true` and `true = false` to `false`
/// * `!!expr` to `expr`
pub struct ConstantFolding {}

impl ConstantFolding {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for ConstantFolding {
    fn optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::Filter { predicate, input } => Ok(LogicalPlan::Filter {
                predicate: optimize_expr(predicate, plan.schema())?,
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
        "constant_folding"
    }
}

/// Recursively transverses the logical plan.
fn optimize_expr(e: &Expr, schema: &DFSchemaRef) -> Result<Expr> {
    Ok(match e {
        Expr::BinaryExpr { left, op, right } => {
            let left = optimize_expr(left, schema)?;
            let right = optimize_expr(right, schema)?;
            match op {
                Operator::Eq => match (&left, &right) {
                    (
                        Expr::Literal(ScalarValue::Boolean(l)),
                        Expr::Literal(ScalarValue::Boolean(r)),
                    ) => Expr::Literal(ScalarValue::Boolean(Some(
                        l.unwrap_or(false) == r.unwrap_or(false),
                    ))),
                    (Expr::Literal(ScalarValue::Boolean(b)), _)
                        if right.get_type(schema)? == DataType::Boolean =>
                    {
                        match b {
                            Some(true) => right,
                            Some(false) | None => Expr::Not(Box::new(right)),
                        }
                    }
                    (_, Expr::Literal(ScalarValue::Boolean(b)))
                        if left.get_type(schema)? == DataType::Boolean =>
                    {
                        match b {
                            Some(true) => left,
                            Some(false) | None => Expr::Not(Box::new(left)),
                        }
                    }
                    _ => Expr::BinaryExpr {
                        left: Box::new(left),
                        op: Operator::Eq,
                        right: Box::new(right),
                    },
                },
                Operator::NotEq => match (&left, &right) {
                    (
                        Expr::Literal(ScalarValue::Boolean(l)),
                        Expr::Literal(ScalarValue::Boolean(r)),
                    ) => Expr::Literal(ScalarValue::Boolean(Some(
                        l.unwrap_or(false) != r.unwrap_or(false),
                    ))),
                    (Expr::Literal(ScalarValue::Boolean(b)), _)
                        if right.get_type(schema)? == DataType::Boolean =>
                    {
                        match b {
                            Some(false) | None => right,
                            Some(true) => Expr::Not(Box::new(right)),
                        }
                    }
                    (_, Expr::Literal(ScalarValue::Boolean(b)))
                        if left.get_type(schema)? == DataType::Boolean =>
                    {
                        match b {
                            Some(false) | None => left,
                            Some(true) => Expr::Not(Box::new(left)),
                        }
                    }
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
        Expr::Not(expr) => match &**expr {
            Expr::Not(inner) => optimize_expr(&inner, schema)?,
            _ => Expr::Not(Box::new(optimize_expr(&expr, schema)?)),
        },
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
                        .map(|(when, then)| {
                            Ok((Box::new(optimize_expr(when, schema)?), then.clone()))
                        })
                        .collect::<Result<_>>()?,
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
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical_plan::{col, lit, DFField, DFSchema, LogicalPlanBuilder};

    use arrow::datatypes::*;

    fn test_table_scan() -> Result<LogicalPlan> {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Boolean, false),
            Field::new("b", DataType::Boolean, false),
            Field::new("c", DataType::Boolean, false),
            Field::new("d", DataType::UInt32, false),
        ]);
        LogicalPlanBuilder::scan_empty("test", &schema, None)?.build()
    }

    fn expr_test_schema() -> DFSchemaRef {
        Arc::new(
            DFSchema::new(vec![
                DFField::new(None, "c1", DataType::Utf8, true),
                DFField::new(None, "c2", DataType::Boolean, true),
            ])
            .unwrap(),
        )
    }

    #[test]
    fn optimize_expr_not_not() -> Result<()> {
        let schema = expr_test_schema();
        assert_eq!(
            optimize_expr(
                &Expr::Not(Box::new(Expr::Not(Box::new(Expr::Not(Box::new(col(
                    "c2"
                ))))))),
                &schema
            )?,
            col("c2").not(),
        );

        Ok(())
    }

    #[test]
    fn optimize_expr_eq() -> Result<()> {
        let schema = expr_test_schema();
        assert_eq!(col("c2").get_type(&schema)?, DataType::Boolean);

        assert_eq!(
            optimize_expr(
                &Expr::BinaryExpr {
                    left: Box::new(lit(true)),
                    op: Operator::Eq,
                    right: Box::new(lit(true)),
                },
                &schema
            )?,
            lit(true),
        );

        assert_eq!(
            optimize_expr(
                &Expr::BinaryExpr {
                    left: Box::new(lit(true)),
                    op: Operator::Eq,
                    right: Box::new(lit(false)),
                },
                &schema
            )?,
            lit(false),
        );

        assert_eq!(
            optimize_expr(
                &Expr::BinaryExpr {
                    left: Box::new(col("c2")),
                    op: Operator::Eq,
                    right: Box::new(lit(true)),
                },
                &schema
            )?,
            col("c2"),
        );

        assert_eq!(
            optimize_expr(
                &Expr::BinaryExpr {
                    left: Box::new(col("c2")),
                    op: Operator::Eq,
                    right: Box::new(lit(false)),
                },
                &schema
            )?,
            col("c2").not(),
        );

        Ok(())
    }

    #[test]
    fn optimize_expr_eq_skip_nonboolean_type() -> Result<()> {
        let schema = expr_test_schema();

        // when one of the operand is not of boolean type, folding the other boolean constant will
        // change return type of expression to non-boolean.
        assert_eq!(col("c1").get_type(&schema)?, DataType::Utf8);

        assert_eq!(
            optimize_expr(
                &Expr::BinaryExpr {
                    left: Box::new(col("c1")),
                    op: Operator::Eq,
                    right: Box::new(lit(true)),
                },
                &schema
            )?,
            Expr::BinaryExpr {
                left: Box::new(col("c1")),
                op: Operator::Eq,
                right: Box::new(lit(true)),
            },
        );

        assert_eq!(
            optimize_expr(
                &Expr::BinaryExpr {
                    left: Box::new(col("c1")),
                    op: Operator::Eq,
                    right: Box::new(lit(false)),
                },
                &schema
            )?,
            Expr::BinaryExpr {
                left: Box::new(col("c1")),
                op: Operator::Eq,
                right: Box::new(lit(false)),
            },
        );

        // test constant operands
        assert_eq!(
            optimize_expr(
                &Expr::BinaryExpr {
                    left: Box::new(lit(1)),
                    op: Operator::Eq,
                    right: Box::new(lit(true)),
                },
                &schema
            )?,
            Expr::BinaryExpr {
                left: Box::new(lit(1)),
                op: Operator::Eq,
                right: Box::new(lit(true)),
            },
        );

        assert_eq!(
            optimize_expr(
                &Expr::BinaryExpr {
                    left: Box::new(lit("a")),
                    op: Operator::Eq,
                    right: Box::new(lit(false)),
                },
                &schema
            )?,
            Expr::BinaryExpr {
                left: Box::new(lit("a")),
                op: Operator::Eq,
                right: Box::new(lit(false)),
            },
        );

        Ok(())
    }

    #[test]
    fn optimize_expr_not_eq() -> Result<()> {
        let schema = expr_test_schema();
        assert_eq!(col("c2").get_type(&schema)?, DataType::Boolean);

        assert_eq!(
            optimize_expr(
                &Expr::BinaryExpr {
                    left: Box::new(col("c2")),
                    op: Operator::NotEq,
                    right: Box::new(lit(true)),
                },
                &schema
            )?,
            col("c2").not(),
        );

        assert_eq!(
            optimize_expr(
                &Expr::BinaryExpr {
                    left: Box::new(col("c2")),
                    op: Operator::NotEq,
                    right: Box::new(lit(false)),
                },
                &schema
            )?,
            col("c2"),
        );

        // test constant
        assert_eq!(
            optimize_expr(
                &Expr::BinaryExpr {
                    left: Box::new(lit(true)),
                    op: Operator::NotEq,
                    right: Box::new(lit(true)),
                },
                &schema
            )?,
            lit(false),
        );

        assert_eq!(
            optimize_expr(
                &Expr::BinaryExpr {
                    left: Box::new(lit(true)),
                    op: Operator::NotEq,
                    right: Box::new(lit(false)),
                },
                &schema
            )?,
            lit(true),
        );

        Ok(())
    }

    #[test]
    fn optimize_expr_not_eq_skip_nonboolean_type() -> Result<()> {
        let schema = expr_test_schema();

        // when one of the operand is not of boolean type, folding the other boolean constant will
        // change return type of expression to non-boolean.
        assert_eq!(col("c1").get_type(&schema)?, DataType::Utf8);

        assert_eq!(
            optimize_expr(
                &Expr::BinaryExpr {
                    left: Box::new(col("c1")),
                    op: Operator::NotEq,
                    right: Box::new(lit(true)),
                },
                &schema
            )?,
            Expr::BinaryExpr {
                left: Box::new(col("c1")),
                op: Operator::NotEq,
                right: Box::new(lit(true)),
            },
        );

        assert_eq!(
            optimize_expr(
                &Expr::BinaryExpr {
                    left: Box::new(col("c1")),
                    op: Operator::NotEq,
                    right: Box::new(lit(false)),
                },
                &schema
            )?,
            Expr::BinaryExpr {
                left: Box::new(col("c1")),
                op: Operator::NotEq,
                right: Box::new(lit(false)),
            },
        );

        // test constants
        assert_eq!(
            optimize_expr(
                &Expr::Not(Box::new(Expr::BinaryExpr {
                    left: Box::new(lit(1)),
                    op: Operator::NotEq,
                    right: Box::new(lit(true)),
                })),
                &schema
            )?,
            Expr::Not(Box::new(Expr::BinaryExpr {
                left: Box::new(lit(1)),
                op: Operator::NotEq,
                right: Box::new(lit(true)),
            })),
        );

        assert_eq!(
            optimize_expr(
                &Expr::Not(Box::new(Expr::BinaryExpr {
                    left: Box::new(lit("a")),
                    op: Operator::NotEq,
                    right: Box::new(lit(false)),
                })),
                &schema
            )?,
            Expr::Not(Box::new(Expr::BinaryExpr {
                left: Box::new(lit("a")),
                op: Operator::NotEq,
                right: Box::new(lit(false)),
            })),
        );

        Ok(())
    }

    #[test]
    fn optimize_expr_case_when_then_else() -> Result<()> {
        let schema = expr_test_schema();

        assert_eq!(
            optimize_expr(
                &Box::new(Expr::Case {
                    expr: None,
                    when_then_expr: vec![(
                        Box::new(Expr::BinaryExpr {
                            left: Box::new(col("c2")),
                            op: Operator::NotEq,
                            right: Box::new(lit(false)),
                        }),
                        Box::new(lit("ok")),
                    )],
                    else_expr: Some(Box::new(lit("not ok"))),
                }),
                &schema
            )?,
            Expr::Case {
                expr: None,
                when_then_expr: vec![(Box::new(col("c2")), Box::new(lit("ok")))],
                else_expr: Some(Box::new(lit("not ok"))),
            }
        );

        Ok(())
    }

    fn assert_optimized_plan_eq(plan: &LogicalPlan, expected: &str) {
        let rule = ConstantFolding::new();
        let optimized_plan = rule.optimize(plan).expect("failed to optimize plan");
        let formatted_plan = format!("{:?}", optimized_plan);
        assert_eq!(formatted_plan, expected);
    }

    #[test]
    fn optimize_plan_eq_expr() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(&table_scan)
            .filter(col("b").eq(lit(true)))?
            .filter(col("c").eq(lit(false)))?
            .project(&[col("a")])?
            .build()?;

        let expected = "\
        Projection: #a\
        \n  Filter: NOT #c\
        \n    Filter: #b\
        \n      TableScan: test projection=None";

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    #[test]
    fn optimize_plan_not_eq_expr() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(&table_scan)
            .filter(col("b").not_eq(lit(true)))?
            .filter(col("c").not_eq(lit(false)))?
            .limit(1)?
            .project(&[col("a")])?
            .build()?;

        let expected = "\
        Projection: #a\
        \n  Limit: 1\
        \n    Filter: #c\
        \n      Filter: NOT #b\
        \n        TableScan: test projection=None";

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    #[test]
    fn optimize_plan_and_expr() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(&table_scan)
            .filter(col("b").not_eq(lit(true)).and(col("c").eq(lit(true))))?
            .project(&[col("a")])?
            .build()?;

        let expected = "\
        Projection: #a\
        \n  Filter: NOT #b And #c\
        \n    TableScan: test projection=None";

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    #[test]
    fn optimize_plan_or_expr() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(&table_scan)
            .filter(col("b").not_eq(lit(true)).or(col("c").eq(lit(false))))?
            .project(&[col("a")])?
            .build()?;

        let expected = "\
        Projection: #a\
        \n  Filter: NOT #b Or NOT #c\
        \n    TableScan: test projection=None";

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    #[test]
    fn optimize_plan_not_expr() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(&table_scan)
            .filter(col("b").eq(lit(false)).not())?
            .project(&[col("a")])?
            .build()?;

        let expected = "\
        Projection: #a\
        \n  Filter: NOT NOT #b\
        \n    TableScan: test projection=None";

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }
}
