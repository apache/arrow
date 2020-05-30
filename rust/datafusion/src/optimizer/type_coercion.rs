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

//! The type_coercion optimizer rule ensures that all operators are operating on
//! compatible types by adding explicit cast operations to expressions. For example,
//! the operation `c_float + c_int` would be rewritten as `c_float + CAST(c_int AS
//! float)`. This keeps the runtime query execution code much simpler.

use std::collections::HashMap;

use arrow::datatypes::Schema;

use crate::error::{ExecutionError, Result};
use crate::execution::physical_plan::udf::ScalarFunction;
use crate::logicalplan::LogicalPlan;
use crate::logicalplan::{Expr, LogicalPlanBuilder};
use crate::optimizer::optimizer::OptimizerRule;
use crate::optimizer::utils;

/// Implementation of type coercion optimizer rule
pub struct TypeCoercionRule<'a> {
    scalar_functions: &'a HashMap<String, Box<ScalarFunction>>,
}

impl<'a> TypeCoercionRule<'a> {
    /// Create a new type coercion optimizer rule using meta-data about registered
    /// scalar functions
    pub fn new(scalar_functions: &'a HashMap<String, Box<ScalarFunction>>) -> Self {
        Self { scalar_functions }
    }

    /// Rewrite an expression list to include explicit CAST operations when required
    fn rewrite_expr_list(&self, expr: &[Expr], schema: &Schema) -> Result<Vec<Expr>> {
        Ok(expr
            .iter()
            .map(|e| self.rewrite_expr(e, schema))
            .collect::<Result<Vec<_>>>()?)
    }

    /// Rewrite an expression to include explicit CAST operations when required
    fn rewrite_expr(&self, expr: &Expr, schema: &Schema) -> Result<Expr> {
        match expr {
            Expr::BinaryExpr { left, op, right } => {
                let left = self.rewrite_expr(left, schema)?;
                let right = self.rewrite_expr(right, schema)?;
                let left_type = left.get_type(schema)?;
                let right_type = right.get_type(schema)?;
                if left_type == right_type {
                    Ok(Expr::BinaryExpr {
                        left: Box::new(left),
                        op: op.clone(),
                        right: Box::new(right),
                    })
                } else {
                    let super_type = utils::get_supertype(&left_type, &right_type)?;
                    Ok(Expr::BinaryExpr {
                        left: Box::new(left.cast_to(&super_type, schema)?),
                        op: op.clone(),
                        right: Box::new(right.cast_to(&super_type, schema)?),
                    })
                }
            }
            Expr::IsNull(e) => Ok(Expr::IsNull(Box::new(self.rewrite_expr(e, schema)?))),
            Expr::IsNotNull(e) => {
                Ok(Expr::IsNotNull(Box::new(self.rewrite_expr(e, schema)?)))
            }
            Expr::ScalarFunction {
                name,
                args,
                return_type,
            } => {
                // cast the inputs of scalar functions to the appropriate type where possible
                match self.scalar_functions.get(name) {
                    Some(func_meta) => {
                        let mut func_args = Vec::with_capacity(args.len());
                        for i in 0..args.len() {
                            let field = &func_meta.args[i];
                            let expr = self.rewrite_expr(&args[i], schema)?;
                            let actual_type = expr.get_type(schema)?;
                            let required_type = field.data_type();
                            if &actual_type == required_type {
                                func_args.push(expr)
                            } else {
                                let super_type =
                                    utils::get_supertype(&actual_type, required_type)?;
                                func_args.push(expr.cast_to(&super_type, schema)?);
                            }
                        }

                        Ok(Expr::ScalarFunction {
                            name: name.clone(),
                            args: func_args,
                            return_type: return_type.clone(),
                        })
                    }
                    _ => Err(ExecutionError::General(format!(
                        "Invalid scalar function {}",
                        name
                    ))),
                }
            }
            Expr::AggregateFunction {
                name,
                args,
                return_type,
            } => Ok(Expr::AggregateFunction {
                name: name.clone(),
                args: args
                    .iter()
                    .map(|a| self.rewrite_expr(a, schema))
                    .collect::<Result<Vec<_>>>()?,
                return_type: return_type.clone(),
            }),
            Expr::Cast { .. } => Ok(expr.clone()),
            Expr::Column(_) => Ok(expr.clone()),
            Expr::Alias(expr, alias) => Ok(Expr::Alias(
                Box::new(self.rewrite_expr(expr, schema)?),
                alias.to_owned(),
            )),
            Expr::Literal(_) => Ok(expr.clone()),
            Expr::UnresolvedColumn(_) => Ok(expr.clone()),
            Expr::Not(_) => Ok(expr.clone()),
            Expr::Sort { .. } => Ok(expr.clone()),
            Expr::Wildcard { .. } => Err(ExecutionError::General(
                "Wildcard expressions are not valid in a logical query plan".to_owned(),
            )),
        }
    }
}

impl<'a> OptimizerRule for TypeCoercionRule<'a> {
    fn optimize(&mut self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::Projection { expr, input, .. } => {
                LogicalPlanBuilder::from(&self.optimize(input)?)
                    .project(self.rewrite_expr_list(expr, input.schema())?)?
                    .build()
            }
            LogicalPlan::Selection { expr, input, .. } => {
                LogicalPlanBuilder::from(&self.optimize(input)?)
                    .filter(self.rewrite_expr(expr, input.schema())?)?
                    .build()
            }
            LogicalPlan::Aggregate {
                input,
                group_expr,
                aggr_expr,
                ..
            } => LogicalPlanBuilder::from(&self.optimize(input)?)
                .aggregate(
                    self.rewrite_expr_list(group_expr, input.schema())?,
                    self.rewrite_expr_list(aggr_expr, input.schema())?,
                )?
                .build(),
            LogicalPlan::TableScan { .. } => Ok(plan.clone()),
            LogicalPlan::InMemoryScan { .. } => Ok(plan.clone()),
            LogicalPlan::ParquetScan { .. } => Ok(plan.clone()),
            LogicalPlan::CsvScan { .. } => Ok(plan.clone()),
            LogicalPlan::EmptyRelation { .. } => Ok(plan.clone()),
            LogicalPlan::Limit { .. } => Ok(plan.clone()),
            LogicalPlan::CreateExternalTable { .. } => Ok(plan.clone()),
            other => Err(ExecutionError::NotImplemented(format!(
                "Type coercion optimizer rule does not support relation: {:?}",
                other
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::context::ExecutionContext;
    use crate::execution::physical_plan::csv::CsvReadOptions;
    use crate::logicalplan::Expr::*;
    use crate::logicalplan::{col, Operator};
    use crate::test::arrow_testdata_path;
    use arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn test_with_csv_plan() -> Result<()> {
        let testdata = arrow_testdata_path();
        let path = format!("{}/csv/aggregate_test_100.csv", testdata);

        let options = CsvReadOptions::new().schema_infer_max_records(100);
        let plan = LogicalPlanBuilder::scan_csv(&path, options, None)?
            .filter(col("c7").lt(&col("c12")))?
            .build()?;

        let scalar_functions = HashMap::new();
        let mut rule = TypeCoercionRule::new(&scalar_functions);
        let plan = rule.optimize(&plan)?;

        assert!(
            format!("{:?}", plan).starts_with("Selection: CAST(#c7 AS Float64) Lt #c12")
        );

        Ok(())
    }

    #[test]
    fn test_add_i32_i64() {
        binary_cast_test(
            DataType::Int32,
            DataType::Int64,
            "CAST(#0 AS Int64) Plus #1",
        );
        binary_cast_test(
            DataType::Int64,
            DataType::Int32,
            "#0 Plus CAST(#1 AS Int64)",
        );
    }

    #[test]
    fn test_add_f32_f64() {
        binary_cast_test(
            DataType::Float32,
            DataType::Float64,
            "CAST(#0 AS Float64) Plus #1",
        );
        binary_cast_test(
            DataType::Float64,
            DataType::Float32,
            "#0 Plus CAST(#1 AS Float64)",
        );
    }

    #[test]
    fn test_add_i32_f32() {
        binary_cast_test(
            DataType::Int32,
            DataType::Float32,
            "CAST(#0 AS Float32) Plus #1",
        );
        binary_cast_test(
            DataType::Float32,
            DataType::Int32,
            "#0 Plus CAST(#1 AS Float32)",
        );
    }

    #[test]
    fn test_add_u32_i64() {
        binary_cast_test(
            DataType::UInt32,
            DataType::Int64,
            "CAST(#0 AS Int64) Plus #1",
        );
        binary_cast_test(
            DataType::Int64,
            DataType::UInt32,
            "#0 Plus CAST(#1 AS Int64)",
        );
    }

    fn binary_cast_test(left_type: DataType, right_type: DataType, expected: &str) {
        let schema = Schema::new(vec![
            Field::new("c0", left_type, true),
            Field::new("c1", right_type, true),
        ]);

        let expr = Expr::BinaryExpr {
            left: Box::new(Column(0)),
            op: Operator::Plus,
            right: Box::new(Column(1)),
        };

        let ctx = ExecutionContext::new();
        let rule = TypeCoercionRule::new(ctx.scalar_functions());

        let expr2 = rule.rewrite_expr(&expr, &schema).unwrap();

        assert_eq!(expected, format!("{:?}", expr2));
    }
}
