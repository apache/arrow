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

use arrow::datatypes::Schema;

use crate::error::{ExecutionError, Result};
use crate::logical_plan::Expr;
use crate::logical_plan::LogicalPlan;
use crate::optimizer::optimizer::OptimizerRule;
use crate::optimizer::utils;
use crate::physical_plan::{
    expressions::numerical_coercion, udf::ScalarFunctionRegistry,
};
use utils::optimize_explain;

/// Optimizer that applies coercion rules to expressions in the logical plan.
///
/// This optimizer does not alter the structure of the plan, it only changes expressions on it.
pub struct TypeCoercionRule<'a, P>
where
    P: ScalarFunctionRegistry,
{
    scalar_functions: &'a P,
}

impl<'a, P> TypeCoercionRule<'a, P>
where
    P: ScalarFunctionRegistry,
{
    /// Create a new type coercion optimizer rule using meta-data about registered
    /// scalar functions
    pub fn new(scalar_functions: &'a P) -> Self {
        Self { scalar_functions }
    }

    /// Rewrite an expression to include explicit CAST operations when required
    fn rewrite_expr(&self, expr: &Expr, schema: &Schema) -> Result<Expr> {
        let expressions = utils::expr_sub_expressions(expr)?;

        // recurse of the re-write
        let mut expressions = expressions
            .iter()
            .map(|e| self.rewrite_expr(e, schema))
            .collect::<Result<Vec<_>>>()?;

        // modify `expressions` by introducing casts when necessary
        match expr {
            Expr::ScalarUDF { name, .. } => {
                // cast the inputs of scalar functions to the appropriate type where possible
                match self.scalar_functions.lookup(name) {
                    Some(func_meta) => {
                        for i in 0..expressions.len() {
                            let actual_type = expressions[i].get_type(schema)?;
                            let required_type = &func_meta.arg_types[i];
                            if &actual_type != required_type {
                                // attempt to coerce using numerical coercion
                                // todo: also try string coercion.
                                if let Some(cast_to_type) =
                                    numerical_coercion(&actual_type, required_type)
                                {
                                    expressions[i] =
                                        expressions[i].cast_to(&cast_to_type, schema)?
                                };
                                // not possible: do nothing and let the plan fail with a clear error message
                            };
                        }
                    }
                    _ => {
                        return Err(ExecutionError::General(format!(
                            "Invalid scalar function {}",
                            name
                        )))
                    }
                }
            }
            _ => {}
        };
        utils::rewrite_expression(expr, &expressions)
    }
}

impl<'a, P> OptimizerRule for TypeCoercionRule<'a, P>
where
    P: ScalarFunctionRegistry,
{
    fn optimize(&mut self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::Explain {
                verbose,
                plan,
                stringified_plans,
                schema,
            } => optimize_explain(self, *verbose, &*plan, stringified_plans, &*schema),
            _ => {
                let inputs = utils::inputs(plan);
                let expressions = utils::expressions(plan);

                // apply the optimization to all inputs of the plan
                let new_inputs = inputs
                    .iter()
                    .map(|plan| self.optimize(*plan))
                    .collect::<Result<Vec<_>>>()?;
                // re-write all expressions on this plan.
                // This assumes a single input, [0]. It wont work for join, subqueries and union operations with more than one input.
                // It is currently not an issue as we do not have any plan with more than one input.
                assert!(
                    expressions.len() == 0 || inputs.len() > 0,
                    "Assume that all plan nodes with expressions have inputs"
                );

                let new_expressions = expressions
                    .iter()
                    .map(|expr| self.rewrite_expr(expr, inputs[0].schema()))
                    .collect::<Result<Vec<_>>>()?;

                utils::from_plan(plan, &new_expressions, &new_inputs)
            }
        }
    }

    fn name(&self) -> &str {
        return "type_coercion";
    }
}
