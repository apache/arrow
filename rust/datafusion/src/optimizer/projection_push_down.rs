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

//! Projection Push Down optimizer rule ensures that only referenced columns are
//! loaded into memory

use crate::error::{ExecutionError, Result};
use crate::logicalplan::Expr;
use crate::logicalplan::LogicalPlan;
use crate::optimizer::optimizer::OptimizerRule;
use crate::optimizer::utils;
use arrow::datatypes::{Field, Schema};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Projection Push Down optimizer rule ensures that only referenced columns are
/// loaded into memory
pub struct ProjectionPushDown {}

impl OptimizerRule for ProjectionPushDown {
    fn optimize(&mut self, plan: &LogicalPlan) -> Result<Arc<LogicalPlan>> {
        let mut accum: HashSet<usize> = HashSet::new();
        let mut mapping: HashMap<usize, usize> = HashMap::new();
        self.optimize_plan(plan, &mut accum, &mut mapping)
    }
}

impl ProjectionPushDown {
    pub fn new() -> Self {
        Self {}
    }

    fn optimize_plan(
        &self,
        plan: &LogicalPlan,
        accum: &mut HashSet<usize>,
        mapping: &mut HashMap<usize, usize>,
    ) -> Result<Arc<LogicalPlan>> {
        match plan {
            LogicalPlan::Projection {
                expr,
                input,
                schema,
            } => {
                // collect all columns referenced by projection expressions
                utils::exprlist_to_column_indices(&expr, accum);

                // push projection down
                let input = self.optimize_plan(&input, accum, mapping)?;

                // rewrite projection expressions to use new column indexes
                let new_expr = self.rewrite_exprs(expr, mapping)?;

                Ok(Arc::new(LogicalPlan::Projection {
                    expr: new_expr,
                    input,
                    schema: schema.clone(),
                }))
            }
            LogicalPlan::Selection { expr, input } => {
                // collect all columns referenced by filter expression
                utils::expr_to_column_indices(expr, accum);

                // push projection down
                let input = self.optimize_plan(&input, accum, mapping)?;

                // rewrite filter expression to use new column indexes
                let new_expr = self.rewrite_expr(expr, mapping)?;

                Ok(Arc::new(LogicalPlan::Selection {
                    expr: new_expr,
                    input,
                }))
            }
            LogicalPlan::Aggregate {
                input,
                group_expr,
                aggr_expr,
                schema,
            } => {
                // collect all columns referenced by grouping and aggregate expressions
                utils::exprlist_to_column_indices(&group_expr, accum);
                utils::exprlist_to_column_indices(&aggr_expr, accum);

                // push projection down
                let input = self.optimize_plan(&input, accum, mapping)?;

                // rewrite expressions to use new column indexes
                let new_group_expr = self.rewrite_exprs(group_expr, mapping)?;
                let new_aggr_expr = self.rewrite_exprs(aggr_expr, mapping)?;

                Ok(Arc::new(LogicalPlan::Aggregate {
                    input,
                    group_expr: new_group_expr,
                    aggr_expr: new_aggr_expr,
                    schema: schema.clone(),
                }))
            }
            LogicalPlan::Sort {
                expr,
                input,
                schema,
            } => {
                // collect all columns referenced by sort expressions
                utils::exprlist_to_column_indices(&expr, accum);

                // push projection down
                let input = self.optimize_plan(&input, accum, mapping)?;

                // rewrite sort expressions to use new column indexes
                let new_expr = self.rewrite_exprs(expr, mapping)?;

                Ok(Arc::new(LogicalPlan::Sort {
                    expr: new_expr,
                    input,
                    schema: schema.clone(),
                }))
            }
            LogicalPlan::EmptyRelation { schema } => {
                Ok(Arc::new(LogicalPlan::EmptyRelation {
                    schema: schema.clone(),
                }))
            }
            LogicalPlan::TableScan {
                schema_name,
                table_name,
                schema,
                ..
            } => {
                // once we reach the table scan, we can use the accumulated set of column indexes as
                // the projection in the table scan
                let mut projection: Vec<usize> = Vec::with_capacity(accum.len());
                accum.iter().for_each(|i| projection.push(*i));

                // sort the projection otherwise we get non-deterministic behavior
                projection.sort();

                // create the projected schema
                let mut projected_fields: Vec<Field> =
                    Vec::with_capacity(projection.len());
                for i in 0..projection.len() {
                    projected_fields.push(schema.fields()[i].clone());
                }
                let projected_schema = Schema::new(projected_fields);

                // now that the table scan is returning a different schema we need to create a
                // mapping from the original column index to the new column index so that we
                // can rewrite expressions as we walk back up the tree

                if mapping.len() != 0 {
                    return Err(ExecutionError::InternalError(
                        "illegal state".to_string(),
                    ));
                }

                for i in 0..schema.fields().len() {
                    if let Some(n) = projection.iter().position(|v| *v == i) {
                        mapping.insert(i, n);
                    }
                }

                // return the table scan with projection
                Ok(Arc::new(LogicalPlan::TableScan {
                    schema_name: schema_name.to_string(),
                    table_name: table_name.to_string(),
                    schema: Arc::new(projected_schema),
                    projection: Some(projection),
                }))
            }
            LogicalPlan::Limit {
                expr,
                input,
                schema,
            } => Ok(Arc::new(LogicalPlan::Limit {
                expr: expr.clone(),
                input: input.clone(),
                schema: schema.clone(),
            })),
        }
    }

    fn rewrite_exprs(
        &self,
        expr: &Vec<Expr>,
        mapping: &HashMap<usize, usize>,
    ) -> Result<Vec<Expr>> {
        Ok(expr
            .iter()
            .map(|e| self.rewrite_expr(e, mapping))
            .collect::<Result<Vec<Expr>>>()?)
    }

    fn rewrite_expr(&self, expr: &Expr, mapping: &HashMap<usize, usize>) -> Result<Expr> {
        match expr {
            Expr::Column(i) => Ok(Expr::Column(self.new_index(mapping, i)?)),
            Expr::Literal(_) => Ok(expr.clone()),
            Expr::IsNull(e) => Ok(Expr::IsNull(Arc::new(self.rewrite_expr(e, mapping)?))),
            Expr::IsNotNull(e) => {
                Ok(Expr::IsNotNull(Arc::new(self.rewrite_expr(e, mapping)?)))
            }
            Expr::BinaryExpr { left, op, right } => Ok(Expr::BinaryExpr {
                left: Arc::new(self.rewrite_expr(left, mapping)?),
                op: op.clone(),
                right: Arc::new(self.rewrite_expr(right, mapping)?),
            }),
            Expr::Cast { expr, data_type } => Ok(Expr::Cast {
                expr: Arc::new(self.rewrite_expr(expr, mapping)?),
                data_type: data_type.clone(),
            }),
            Expr::Sort { expr, asc } => Ok(Expr::Sort {
                expr: Arc::new(self.rewrite_expr(expr, mapping)?),
                asc: *asc,
            }),
            Expr::AggregateFunction {
                name,
                args,
                return_type,
            } => Ok(Expr::AggregateFunction {
                name: name.to_string(),
                args: self.rewrite_exprs(args, mapping)?,
                return_type: return_type.clone(),
            }),
            Expr::ScalarFunction {
                name,
                args,
                return_type,
            } => Ok(Expr::ScalarFunction {
                name: name.to_string(),
                args: self.rewrite_exprs(args, mapping)?,
                return_type: return_type.clone(),
            }),
        }
    }

    fn new_index(&self, mapping: &HashMap<usize, usize>, i: &usize) -> Result<usize> {
        match mapping.get(i) {
            Some(j) => Ok(*j),
            _ => Err(ExecutionError::InternalError(
                "Internal error computing new column index".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::logicalplan::Expr::*;
    use crate::logicalplan::LogicalPlan::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::borrow::Borrow;
    use std::sync::Arc;

    #[test]
    fn aggregate_no_group_by() {
        let table_scan = test_table_scan();

        let aggregate = Aggregate {
            group_expr: vec![],
            aggr_expr: vec![Column(1)],
            schema: Arc::new(Schema::new(vec![Field::new(
                "MAX(b)",
                DataType::UInt32,
                false,
            )])),
            input: Arc::new(table_scan),
        };

        assert_optimized_plan_eq(&aggregate, "Aggregate: groupBy=[[]], aggr=[[#0]]\n  TableScan: test projection=Some([1])");
    }

    #[test]
    fn aggregate_group_by() {
        let table_scan = test_table_scan();

        let aggregate = Aggregate {
            group_expr: vec![Column(2)],
            aggr_expr: vec![Column(1)],
            schema: Arc::new(Schema::new(vec![
                Field::new("c", DataType::UInt32, false),
                Field::new("MAX(b)", DataType::UInt32, false),
            ])),
            input: Arc::new(table_scan),
        };

        assert_optimized_plan_eq(&aggregate, "Aggregate: groupBy=[[#1]], aggr=[[#0]]\n  TableScan: test projection=Some([1, 2])");
    }

    #[test]
    fn aggregate_no_group_by_with_selection() {
        let table_scan = test_table_scan();

        let selection = Selection {
            expr: Column(2),
            input: Arc::new(table_scan),
        };

        let aggregate = Aggregate {
            group_expr: vec![],
            aggr_expr: vec![Column(1)],
            schema: Arc::new(Schema::new(vec![Field::new(
                "MAX(b)",
                DataType::UInt32,
                false,
            )])),
            input: Arc::new(selection),
        };

        assert_optimized_plan_eq(&aggregate, "Aggregate: groupBy=[[]], aggr=[[#0]]\n  Selection: #1\n    TableScan: test projection=Some([1, 2])");
    }

    #[test]
    fn cast() {
        let table_scan = test_table_scan();

        let projection = Projection {
            expr: vec![Cast {
                expr: Arc::new(Column(2)),
                data_type: DataType::Float64,
            }],
            input: Arc::new(table_scan),
            schema: Arc::new(Schema::new(vec![Field::new(
                "CAST(c AS float)",
                DataType::Float64,
                false,
            )])),
        };

        assert_optimized_plan_eq(
            &projection,
            "Projection: CAST(#0 AS Float64)\n  TableScan: test projection=Some([2])",
        );
    }

    #[test]
    fn table_scan_projected_schema() {
        let table_scan = test_table_scan();
        assert_eq!(3, table_scan.schema().fields().len());

        let projection = Projection {
            expr: vec![Column(0), Column(1)],
            input: Arc::new(table_scan),
            schema: Arc::new(Schema::new(vec![
                Field::new("a", DataType::UInt32, false),
                Field::new("b", DataType::UInt32, false),
            ])),
        };

        let optimized_plan = optimize(&projection);

        // check that table scan schema now contains 2 columns
        match optimized_plan.as_ref().borrow() {
            LogicalPlan::Projection { input, .. } => match input.as_ref().borrow() {
                LogicalPlan::TableScan { ref schema, .. } => {
                    assert_eq!(2, schema.fields().len());
                }
                _ => assert!(false),
            },
            _ => assert!(false),
        }
    }

    fn assert_optimized_plan_eq(plan: &LogicalPlan, expected: &str) {
        let optimized_plan = optimize(plan);
        let formatted_plan = format!("{:?}", optimized_plan);
        assert_eq!(formatted_plan, expected);
    }

    fn optimize(plan: &LogicalPlan) -> Arc<LogicalPlan> {
        let mut rule = ProjectionPushDown::new();
        rule.optimize(plan).unwrap()
    }

    /// all tests share a common table
    fn test_table_scan() -> LogicalPlan {
        TableScan {
            schema_name: "default".to_string(),
            table_name: "test".to_string(),
            schema: Arc::new(Schema::new(vec![
                Field::new("a", DataType::UInt32, false),
                Field::new("b", DataType::UInt32, false),
                Field::new("c", DataType::UInt32, false),
            ])),
            projection: None,
        }
    }
}
