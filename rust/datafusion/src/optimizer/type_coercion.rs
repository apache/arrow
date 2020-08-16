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
use std::sync::{Arc, Mutex};

use arrow::datatypes::Schema;

use crate::error::{ExecutionError, Result};
use crate::execution::physical_plan::udf::ScalarFunction;
use crate::logicalplan::Expr;
use crate::logicalplan::LogicalPlan;
use crate::optimizer::optimizer::OptimizerRule;
use crate::optimizer::utils;
use utils::optimize_explain;

/// Optimizer that applies coercion rules to expressions in the logical plan.
///
/// This optimizer does not alter the structure of the plan, it only changes expressions on it.
pub struct TypeCoercionRule {
    scalar_functions: Arc<Mutex<HashMap<String, Box<ScalarFunction>>>>,
}

impl TypeCoercionRule {
    /// Create a new type coercion optimizer rule using meta-data about registered
    /// scalar functions
    pub fn new(
        scalar_functions: Arc<Mutex<HashMap<String, Box<ScalarFunction>>>>,
    ) -> Self {
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
            Expr::BinaryExpr { .. } => {
                let left_type = expressions[0].get_type(schema)?;
                let right_type = expressions[1].get_type(schema)?;
                if left_type != right_type {
                    let super_type = utils::get_supertype(&left_type, &right_type)?;

                    expressions[0] = expressions[0].cast_to(&super_type, schema)?;
                    expressions[1] = expressions[1].cast_to(&super_type, schema)?;
                }
            }
            Expr::ScalarFunction { name, .. } => {
                // cast the inputs of scalar functions to the appropriate type where possible
                match self
                    .scalar_functions
                    .lock()
                    .expect("failed to lock mutex")
                    .get(name)
                {
                    Some(func_meta) => {
                        for i in 0..expressions.len() {
                            let field = &func_meta.args[i];
                            let actual_type = expressions[i].get_type(schema)?;
                            let required_type = field.data_type();
                            if &actual_type != required_type {
                                let super_type =
                                    utils::get_supertype(&actual_type, required_type)?;
                                expressions[i] =
                                    expressions[i].cast_to(&super_type, schema)?
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

impl OptimizerRule for TypeCoercionRule {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::context::ExecutionContext;
    use crate::execution::physical_plan::csv::CsvReadOptions;
    use crate::logicalplan::{aggregate_expr, col, lit, LogicalPlanBuilder, Operator};
    use crate::test::arrow_testdata_path;
    use arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn test_all_operators() -> Result<()> {
        let testdata = arrow_testdata_path();
        let path = format!("{}/csv/aggregate_test_100.csv", testdata);

        let options = CsvReadOptions::new().schema_infer_max_records(100);
        let plan = LogicalPlanBuilder::scan_csv(&path, options, None)?
            // filter clause needs the type coercion rule applied
            .filter(col("c7").lt(lit(5_u8)))?
            .project(vec![col("c1"), col("c2")])?
            .aggregate(
                vec![col("c1")],
                vec![aggregate_expr("SUM", col("c2"), DataType::Int64)],
            )?
            .sort(vec![col("c1")])?
            .limit(10)?
            .build()?;

        let scalar_functions = HashMap::new();
        let mut rule = TypeCoercionRule::new(Arc::new(Mutex::new(scalar_functions)));
        let plan = rule.optimize(&plan)?;

        // check that the filter had a cast added
        let plan_str = format!("{:?}", plan);
        println!("{}", plan_str);
        let expected_plan_str = "Limit: 10
  Sort: #c1
    Aggregate: groupBy=[[#c1]], aggr=[[SUM(#c2)]]
      Projection: #c1, #c2
        Selection: #c7 Lt CAST(UInt8(5) AS Int64)";
        assert!(plan_str.starts_with(expected_plan_str));

        Ok(())
    }

    #[test]
    fn test_with_csv_plan() -> Result<()> {
        let testdata = arrow_testdata_path();
        let path = format!("{}/csv/aggregate_test_100.csv", testdata);

        let options = CsvReadOptions::new().schema_infer_max_records(100);
        let plan = LogicalPlanBuilder::scan_csv(&path, options, None)?
            .filter(col("c7").lt(col("c12")))?
            .build()?;

        let scalar_functions = HashMap::new();
        let mut rule = TypeCoercionRule::new(Arc::new(Mutex::new(scalar_functions)));
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
            "CAST(#c0 AS Int64) Plus #c1",
        );
        binary_cast_test(
            DataType::Int64,
            DataType::Int32,
            "#c0 Plus CAST(#c1 AS Int64)",
        );
    }

    #[test]
    fn test_add_f32_f64() {
        binary_cast_test(
            DataType::Float32,
            DataType::Float64,
            "CAST(#c0 AS Float64) Plus #c1",
        );
        binary_cast_test(
            DataType::Float64,
            DataType::Float32,
            "#c0 Plus CAST(#c1 AS Float64)",
        );
    }

    #[test]
    fn test_add_i32_f32() {
        binary_cast_test(
            DataType::Int32,
            DataType::Float32,
            "CAST(#c0 AS Float32) Plus #c1",
        );
        binary_cast_test(
            DataType::Float32,
            DataType::Int32,
            "#c0 Plus CAST(#c1 AS Float32)",
        );
    }

    #[test]
    fn test_add_u32_i64() {
        binary_cast_test(
            DataType::UInt32,
            DataType::Int64,
            "CAST(#c0 AS Int64) Plus #c1",
        );
        binary_cast_test(
            DataType::Int64,
            DataType::UInt32,
            "#c0 Plus CAST(#c1 AS Int64)",
        );
    }

    fn binary_cast_test(left_type: DataType, right_type: DataType, expected: &str) {
        let schema = Schema::new(vec![
            Field::new("c0", left_type, true),
            Field::new("c1", right_type, true),
        ]);

        let expr = Expr::BinaryExpr {
            left: Box::new(col("c0")),
            op: Operator::Plus,
            right: Box::new(col("c1")),
        };

        let ctx = ExecutionContext::new();
        let rule = TypeCoercionRule::new(ctx.scalar_functions());

        let expr2 = rule.rewrite_expr(&expr, &schema).unwrap();

        assert_eq!(expected, format!("{:?}", expr2));
    }
}
