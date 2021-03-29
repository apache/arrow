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
use crate::logical_plan::{DFSchemaRef, Expr, ExprRewriter, LogicalPlan, Operator};
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
/// * `expr = null` and `expr != null` to `null`
pub struct ConstantFolding {}

impl ConstantFolding {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for ConstantFolding {
    fn optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        // We need to pass down the all schemas within the plan tree to `optimize_expr` in order to
        // to evaluate expression types. For example, a projection plan's schema will only include
        // projected columns. With just the projected schema, it's not possible to infer types for
        // expressions that references non-projected columns within the same project plan or its
        // children plans.
        let mut rewriter = ConstantRewriter {
            schemas: plan.all_schemas(),
        };

        match plan {
            LogicalPlan::Filter { predicate, input } => Ok(LogicalPlan::Filter {
                predicate: predicate.clone().rewrite(&mut rewriter)?,
                input: Arc::new(self.optimize(input)?),
            }),
            // Rest: recurse into plan, apply optimization where possible
            LogicalPlan::Projection { .. }
            | LogicalPlan::Aggregate { .. }
            | LogicalPlan::Repartition { .. }
            | LogicalPlan::CreateExternalTable { .. }
            | LogicalPlan::Extension { .. }
            | LogicalPlan::Sort { .. }
            | LogicalPlan::Explain { .. }
            | LogicalPlan::Limit { .. }
            | LogicalPlan::Union { .. }
            | LogicalPlan::Join { .. } => {
                // apply the optimization to all inputs of the plan
                let inputs = plan.inputs();
                let new_inputs = inputs
                    .iter()
                    .map(|plan| self.optimize(plan))
                    .collect::<Result<Vec<_>>>()?;

                let expr = plan
                    .expressions()
                    .into_iter()
                    .map(|e| e.rewrite(&mut rewriter))
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

struct ConstantRewriter<'a> {
    /// input schemas
    schemas: Vec<&'a DFSchemaRef>,
}

impl<'a> ConstantRewriter<'a> {
    fn is_boolean_type(&self, expr: &Expr) -> bool {
        for schema in &self.schemas {
            if let Ok(DataType::Boolean) = expr.get_type(schema) {
                return true;
            }
        }

        false
    }
}

impl<'a> ExprRewriter for ConstantRewriter<'a> {
    /// rewrite the expression simplifying any constant expressions
    fn mutate(&mut self, expr: Expr) -> Result<Expr> {
        let new_expr = match expr {
            Expr::BinaryExpr { left, op, right } => match op {
                Operator::Eq => match (left.as_ref(), right.as_ref()) {
                    (
                        Expr::Literal(ScalarValue::Boolean(l)),
                        Expr::Literal(ScalarValue::Boolean(r)),
                    ) => match (l, r) {
                        (Some(l), Some(r)) => {
                            Expr::Literal(ScalarValue::Boolean(Some(l == r)))
                        }
                        _ => Expr::Literal(ScalarValue::Boolean(None)),
                    },
                    (Expr::Literal(ScalarValue::Boolean(b)), _)
                        if self.is_boolean_type(&right) =>
                    {
                        match b {
                            Some(true) => *right,
                            Some(false) => Expr::Not(right),
                            None => Expr::Literal(ScalarValue::Boolean(None)),
                        }
                    }
                    (_, Expr::Literal(ScalarValue::Boolean(b)))
                        if self.is_boolean_type(&left) =>
                    {
                        match b {
                            Some(true) => *left,
                            Some(false) => Expr::Not(left),
                            None => Expr::Literal(ScalarValue::Boolean(None)),
                        }
                    }
                    _ => Expr::BinaryExpr {
                        left,
                        op: Operator::Eq,
                        right,
                    },
                },
                Operator::NotEq => match (left.as_ref(), right.as_ref()) {
                    (
                        Expr::Literal(ScalarValue::Boolean(l)),
                        Expr::Literal(ScalarValue::Boolean(r)),
                    ) => match (l, r) {
                        (Some(l), Some(r)) => {
                            Expr::Literal(ScalarValue::Boolean(Some(l != r)))
                        }
                        _ => Expr::Literal(ScalarValue::Boolean(None)),
                    },
                    (Expr::Literal(ScalarValue::Boolean(b)), _)
                        if self.is_boolean_type(&right) =>
                    {
                        match b {
                            Some(true) => Expr::Not(right),
                            Some(false) => *right,
                            None => Expr::Literal(ScalarValue::Boolean(None)),
                        }
                    }
                    (_, Expr::Literal(ScalarValue::Boolean(b)))
                        if self.is_boolean_type(&left) =>
                    {
                        match b {
                            Some(true) => Expr::Not(left),
                            Some(false) => *left,
                            None => Expr::Literal(ScalarValue::Boolean(None)),
                        }
                    }
                    _ => Expr::BinaryExpr {
                        left,
                        op: Operator::NotEq,
                        right,
                    },
                },
                _ => Expr::BinaryExpr { left, op, right },
            },
            Expr::Not(inner) => {
                // Not(Not(expr)) --> expr
                if let Expr::Not(negated_inner) = *inner {
                    *negated_inner
                } else {
                    Expr::Not(inner)
                }
            }
            expr => {
                // no rewrite possible
                expr
            }
        };
        Ok(new_expr)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical_plan::{
        col, lit, max, min, DFField, DFSchema, LogicalPlanBuilder,
    };

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
        let mut rewriter = ConstantRewriter {
            schemas: vec![&schema],
        };

        assert_eq!(
            (col("c2").not().not().not()).rewrite(&mut rewriter)?,
            col("c2").not(),
        );

        Ok(())
    }

    #[test]
    fn optimize_expr_null_comparision() -> Result<()> {
        let schema = expr_test_schema();
        let mut rewriter = ConstantRewriter {
            schemas: vec![&schema],
        };

        // x = null is always null
        assert_eq!(
            (lit(true).eq(lit(ScalarValue::Boolean(None)))).rewrite(&mut rewriter)?,
            lit(ScalarValue::Boolean(None)),
        );

        // null != null is always null
        assert_eq!(
            (lit(ScalarValue::Boolean(None)).not_eq(lit(ScalarValue::Boolean(None))))
                .rewrite(&mut rewriter)?,
            lit(ScalarValue::Boolean(None)),
        );

        // x != null is always null
        assert_eq!(
            (col("c2").not_eq(lit(ScalarValue::Boolean(None)))).rewrite(&mut rewriter)?,
            lit(ScalarValue::Boolean(None)),
        );

        // null = x is always null
        assert_eq!(
            (lit(ScalarValue::Boolean(None)).eq(col("c2"))).rewrite(&mut rewriter)?,
            lit(ScalarValue::Boolean(None)),
        );

        Ok(())
    }

    #[test]
    fn optimize_expr_eq() -> Result<()> {
        let schema = expr_test_schema();
        let mut rewriter = ConstantRewriter {
            schemas: vec![&schema],
        };

        assert_eq!(col("c2").get_type(&schema)?, DataType::Boolean);

        // true = ture -> true
        assert_eq!((lit(true).eq(lit(true))).rewrite(&mut rewriter)?, lit(true),);

        // true = false -> false
        assert_eq!(
            (lit(true).eq(lit(false))).rewrite(&mut rewriter)?,
            lit(false),
        );

        // c2 = true -> c2
        assert_eq!((col("c2").eq(lit(true))).rewrite(&mut rewriter)?, col("c2"),);

        // c2 = false => !c2
        assert_eq!(
            (col("c2").eq(lit(false))).rewrite(&mut rewriter)?,
            col("c2").not(),
        );

        Ok(())
    }

    #[test]
    fn optimize_expr_eq_skip_nonboolean_type() -> Result<()> {
        let schema = expr_test_schema();
        let mut rewriter = ConstantRewriter {
            schemas: vec![&schema],
        };

        // When one of the operand is not of boolean type, folding the other boolean constant will
        // change return type of expression to non-boolean.
        //
        // Make sure c1 column to be used in tests is not boolean type
        assert_eq!(col("c1").get_type(&schema)?, DataType::Utf8);

        // don't fold c1 = true
        assert_eq!(
            (col("c1").eq(lit(true))).rewrite(&mut rewriter)?,
            col("c1").eq(lit(true)),
        );

        // don't fold c1 = false
        assert_eq!(
            (col("c1").eq(lit(false))).rewrite(&mut rewriter)?,
            col("c1").eq(lit(false)),
        );

        // test constant operands
        assert_eq!(
            (lit(1).eq(lit(true))).rewrite(&mut rewriter)?,
            lit(1).eq(lit(true)),
        );

        assert_eq!(
            (lit("a").eq(lit(false))).rewrite(&mut rewriter)?,
            lit("a").eq(lit(false)),
        );

        Ok(())
    }

    #[test]
    fn optimize_expr_not_eq() -> Result<()> {
        let schema = expr_test_schema();
        let mut rewriter = ConstantRewriter {
            schemas: vec![&schema],
        };

        assert_eq!(col("c2").get_type(&schema)?, DataType::Boolean);

        // c2 != true -> !c2
        assert_eq!(
            (col("c2").not_eq(lit(true))).rewrite(&mut rewriter)?,
            col("c2").not(),
        );

        // c2 != false -> c2
        assert_eq!(
            (col("c2").not_eq(lit(false))).rewrite(&mut rewriter)?,
            col("c2"),
        );

        // test constant
        assert_eq!(
            (lit(true).not_eq(lit(true))).rewrite(&mut rewriter)?,
            lit(false),
        );

        assert_eq!(
            (lit(true).not_eq(lit(false))).rewrite(&mut rewriter)?,
            lit(true),
        );

        Ok(())
    }

    #[test]
    fn optimize_expr_not_eq_skip_nonboolean_type() -> Result<()> {
        let schema = expr_test_schema();
        let mut rewriter = ConstantRewriter {
            schemas: vec![&schema],
        };

        // when one of the operand is not of boolean type, folding the other boolean constant will
        // change return type of expression to non-boolean.
        assert_eq!(col("c1").get_type(&schema)?, DataType::Utf8);

        assert_eq!(
            (col("c1").not_eq(lit(true))).rewrite(&mut rewriter)?,
            col("c1").not_eq(lit(true)),
        );

        assert_eq!(
            (col("c1").not_eq(lit(false))).rewrite(&mut rewriter)?,
            col("c1").not_eq(lit(false)),
        );

        // test constants
        assert_eq!(
            (lit(1).not_eq(lit(true))).rewrite(&mut rewriter)?,
            lit(1).not_eq(lit(true)),
        );

        assert_eq!(
            (lit("a").not_eq(lit(false))).rewrite(&mut rewriter)?,
            lit("a").not_eq(lit(false)),
        );

        Ok(())
    }

    #[test]
    fn optimize_expr_case_when_then_else() -> Result<()> {
        let schema = expr_test_schema();
        let mut rewriter = ConstantRewriter {
            schemas: vec![&schema],
        };

        assert_eq!(
            (Box::new(Expr::Case {
                expr: None,
                when_then_expr: vec![(
                    Box::new(col("c2").not_eq(lit(false))),
                    Box::new(lit("ok").eq(lit(true))),
                )],
                else_expr: Some(Box::new(col("c2").eq(lit(true)))),
            }))
            .rewrite(&mut rewriter)?,
            Expr::Case {
                expr: None,
                when_then_expr: vec![(
                    Box::new(col("c2")),
                    Box::new(lit("ok").eq(lit(true)))
                )],
                else_expr: Some(Box::new(col("c2"))),
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
            .project(vec![col("a")])?
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
            .project(vec![col("a")])?
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
            .project(vec![col("a")])?
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
            .project(vec![col("a")])?
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
            .project(vec![col("a")])?
            .build()?;

        let expected = "\
        Projection: #a\
        \n  Filter: #b\
        \n    TableScan: test projection=None";

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    #[test]
    fn optimize_plan_support_projection() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(&table_scan)
            .project(vec![col("a"), col("d"), col("b").eq(lit(false))])?
            .build()?;

        let expected = "\
        Projection: #a, #d, NOT #b\
        \n  TableScan: test projection=None";

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    #[test]
    fn optimize_plan_support_aggregate() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(&table_scan)
            .project(vec![col("a"), col("c"), col("b")])?
            .aggregate(
                vec![col("a"), col("c")],
                vec![max(col("b").eq(lit(true))), min(col("b"))],
            )?
            .build()?;

        let expected = "\
        Aggregate: groupBy=[[#a, #c]], aggr=[[MAX(#b), MIN(#b)]]\
        \n  Projection: #a, #c, #b\
        \n    TableScan: test projection=None";

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }
}
