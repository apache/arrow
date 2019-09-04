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

//! The type_coercion optimizer rule ensures that all binary operators are operating on
//! compatible types by adding explicit cast operations to expressions. For example,
//! the operation `c_float + c_int` would be rewritten as `c_float + CAST(c_int AS
//! float)`. This keeps the runtime query execution code much simpler.

use std::sync::Arc;

use arrow::datatypes::Schema;

use crate::error::{ExecutionError, Result};
use crate::logicalplan::Expr;
use crate::logicalplan::LogicalPlan;
use crate::optimizer::optimizer::OptimizerRule;
use crate::optimizer::utils;

/// Implementation of type coercion optimizer rule
pub struct TypeCoercionRule {}

impl OptimizerRule for TypeCoercionRule {
    fn optimize(&mut self, plan: &LogicalPlan) -> Result<Arc<LogicalPlan>> {
        match plan {
            LogicalPlan::Projection {
                expr,
                input,
                schema,
            } => Ok(Arc::new(LogicalPlan::Projection {
                expr: expr
                    .iter()
                    .map(|e| rewrite_expr(e, &schema))
                    .collect::<Result<Vec<_>>>()?,
                input: self.optimize(input)?,
                schema: schema.clone(),
            })),
            LogicalPlan::Selection { expr, input } => {
                Ok(Arc::new(LogicalPlan::Selection {
                    expr: rewrite_expr(expr, input.schema())?,
                    input: self.optimize(input)?,
                }))
            }
            LogicalPlan::Aggregate {
                input,
                group_expr,
                aggr_expr,
                schema,
            } => Ok(Arc::new(LogicalPlan::Aggregate {
                group_expr: group_expr
                    .iter()
                    .map(|e| rewrite_expr(e, &schema))
                    .collect::<Result<Vec<_>>>()?,
                aggr_expr: aggr_expr
                    .iter()
                    .map(|e| rewrite_expr(e, &schema))
                    .collect::<Result<Vec<_>>>()?,
                input: self.optimize(input)?,
                schema: schema.clone(),
            })),
            LogicalPlan::TableScan { .. } => Ok(Arc::new(plan.clone())),
            LogicalPlan::EmptyRelation { .. } => Ok(Arc::new(plan.clone())),
            LogicalPlan::Limit { .. } => Ok(Arc::new(plan.clone())),
            LogicalPlan::CreateExternalTable { .. } => Ok(Arc::new(plan.clone())),
            other => Err(ExecutionError::NotImplemented(format!(
                "Type coercion optimizer rule does not support relation: {:?}",
                other
            ))),
        }
    }
}

impl TypeCoercionRule {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

/// Rewrite an expression to include explicit CAST operations when required
fn rewrite_expr(expr: &Expr, schema: &Schema) -> Result<Expr> {
    match expr {
        Expr::BinaryExpr { left, op, right } => {
            let left = rewrite_expr(left, schema)?;
            let right = rewrite_expr(right, schema)?;
            let left_type = left.get_type(schema);
            let right_type = right.get_type(schema);
            if left_type == right_type {
                Ok(Expr::BinaryExpr {
                    left: Arc::new(left),
                    op: op.clone(),
                    right: Arc::new(right),
                })
            } else {
                let super_type = utils::get_supertype(&left_type, &right_type)?;
                Ok(Expr::BinaryExpr {
                    left: Arc::new(left.cast_to(&super_type, schema)?),
                    op: op.clone(),
                    right: Arc::new(right.cast_to(&super_type, schema)?),
                })
            }
        }
        Expr::IsNull(e) => Ok(Expr::IsNull(Arc::new(rewrite_expr(e, schema)?))),
        Expr::IsNotNull(e) => Ok(Expr::IsNotNull(Arc::new(rewrite_expr(e, schema)?))),
        Expr::ScalarFunction {
            name,
            args,
            return_type,
        } => Ok(Expr::ScalarFunction {
            name: name.clone(),
            args: args
                .iter()
                .map(|a| rewrite_expr(a, schema))
                .collect::<Result<Vec<_>>>()?,
            return_type: return_type.clone(),
        }),
        Expr::AggregateFunction {
            name,
            args,
            return_type,
        } => Ok(Expr::AggregateFunction {
            name: name.clone(),
            args: args
                .iter()
                .map(|a| rewrite_expr(a, schema))
                .collect::<Result<Vec<_>>>()?,
            return_type: return_type.clone(),
        }),
        Expr::Cast { .. } => Ok(expr.clone()),
        Expr::Column(_) => Ok(expr.clone()),
        Expr::Literal(_) => Ok(expr.clone()),
        other => Err(ExecutionError::NotImplemented(format!(
            "Type coercion optimizer rule does not support expression: {:?}",
            other
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logicalplan::Expr::*;
    use crate::logicalplan::Operator;
    use arrow::datatypes::{DataType, Field, Schema};

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
            left: Arc::new(Column(0)),
            op: Operator::Plus,
            right: Arc::new(Column(1)),
        };

        let expr2 = rewrite_expr(&expr, &schema).unwrap();

        assert_eq!(expected, format!("{:?}", expr2));
    }
}
