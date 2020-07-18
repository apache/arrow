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
use crate::logicalplan::LogicalPlan;
use crate::logicalplan::{Expr, LogicalPlanBuilder};
use crate::optimizer::optimizer::OptimizerRule;
use crate::optimizer::utils;
use arrow::datatypes::{Field, Schema};
use std::collections::{HashMap, HashSet};

/// Projection Push Down optimizer rule ensures that only referenced columns are
/// loaded into memory
pub struct ProjectionPushDown {}

impl OptimizerRule for ProjectionPushDown {
    fn optimize(&mut self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        let mut accum: HashSet<usize> = HashSet::new();
        let mut mapping: HashMap<usize, usize> = HashMap::new();
        self.optimize_plan(plan, &mut accum, &mut mapping, false)
    }
}

impl ProjectionPushDown {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }

    fn optimize_plan(
        &self,
        plan: &LogicalPlan,
        accum: &mut HashSet<usize>,
        mapping: &mut HashMap<usize, usize>,
        has_projection: bool,
    ) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::Projection { expr, input, .. } => {
                // collect all columns referenced by projection expressions
                utils::exprlist_to_column_indices(&expr, accum)?;

                LogicalPlanBuilder::from(
                    &self.optimize_plan(&input, accum, mapping, true)?,
                )
                .project(self.rewrite_expr_list(expr, mapping)?)?
                .build()
            }
            LogicalPlan::Selection { expr, input } => {
                // collect all columns referenced by filter expression
                utils::expr_to_column_indices(expr, accum)?;

                LogicalPlanBuilder::from(&self.optimize_plan(
                    &input,
                    accum,
                    mapping,
                    has_projection,
                )?)
                .filter(self.rewrite_expr(expr, mapping)?)?
                .build()
            }
            LogicalPlan::Aggregate {
                input,
                group_expr,
                aggr_expr,
                ..
            } => {
                // collect all columns referenced by grouping and aggregate expressions
                utils::exprlist_to_column_indices(&group_expr, accum)?;
                utils::exprlist_to_column_indices(&aggr_expr, accum)?;

                LogicalPlanBuilder::from(&self.optimize_plan(
                    &input,
                    accum,
                    mapping,
                    has_projection,
                )?)
                .aggregate(
                    self.rewrite_expr_list(group_expr, mapping)?,
                    self.rewrite_expr_list(aggr_expr, mapping)?,
                )?
                .build()
            }
            LogicalPlan::Sort { expr, input, .. } => {
                // collect all columns referenced by sort expressions
                utils::exprlist_to_column_indices(&expr, accum)?;

                LogicalPlanBuilder::from(&self.optimize_plan(
                    &input,
                    accum,
                    mapping,
                    has_projection,
                )?)
                .sort(self.rewrite_expr_list(expr, mapping)?)?
                .build()
            }
            LogicalPlan::EmptyRelation { schema } => Ok(LogicalPlan::EmptyRelation {
                schema: schema.clone(),
            }),
            LogicalPlan::TableScan {
                schema_name,
                table_name,
                table_schema,
                projection,
                ..
            } => {
                let (projection, projected_schema) = get_projected_schema(
                    &table_schema,
                    projection,
                    accum,
                    mapping,
                    has_projection,
                )?;

                // return the table scan with projection
                Ok(LogicalPlan::TableScan {
                    schema_name: schema_name.to_string(),
                    table_name: table_name.to_string(),
                    table_schema: table_schema.clone(),
                    projected_schema: Box::new(projected_schema),
                    projection: Some(projection),
                })
            }
            LogicalPlan::InMemoryScan {
                data,
                schema,
                projection,
                ..
            } => {
                let (projection, projected_schema) = get_projected_schema(
                    &schema,
                    projection,
                    accum,
                    mapping,
                    has_projection,
                )?;
                Ok(LogicalPlan::InMemoryScan {
                    data: data.clone(),
                    schema: schema.clone(),
                    projection: Some(projection),
                    projected_schema: Box::new(projected_schema),
                })
            }
            LogicalPlan::CsvScan {
                path,
                has_header,
                delimiter,
                schema,
                projection,
                ..
            } => {
                let (projection, projected_schema) = get_projected_schema(
                    &schema,
                    projection,
                    accum,
                    mapping,
                    has_projection,
                )?;

                Ok(LogicalPlan::CsvScan {
                    path: path.to_owned(),
                    has_header: *has_header,
                    schema: schema.clone(),
                    delimiter: *delimiter,
                    projection: Some(projection),
                    projected_schema: Box::new(projected_schema),
                })
            }
            LogicalPlan::ParquetScan {
                path,
                schema,
                projection,
                ..
            } => {
                let (projection, projected_schema) = get_projected_schema(
                    &schema,
                    projection,
                    accum,
                    mapping,
                    has_projection,
                )?;

                Ok(LogicalPlan::ParquetScan {
                    path: path.to_owned(),
                    schema: schema.clone(),
                    projection: Some(projection),
                    projected_schema: Box::new(projected_schema),
                })
            }
            LogicalPlan::Limit { n, input, .. } => LogicalPlanBuilder::from(
                &self.optimize_plan(&input, accum, mapping, has_projection)?,
            )
            .limit(*n)?
            .build(),
            LogicalPlan::CreateExternalTable {
                schema,
                name,
                location,
                file_type,
                has_header,
            } => Ok(LogicalPlan::CreateExternalTable {
                schema: schema.clone(),
                name: name.to_string(),
                location: location.to_string(),
                file_type: file_type.clone(),
                has_header: *has_header,
            }),
        }
    }

    fn rewrite_expr_list(
        &self,
        expr: &[Expr],
        mapping: &HashMap<usize, usize>,
    ) -> Result<Vec<Expr>> {
        Ok(expr
            .iter()
            .map(|e| self.rewrite_expr(e, mapping))
            .collect::<Result<Vec<Expr>>>()?)
    }

    fn rewrite_expr(&self, expr: &Expr, mapping: &HashMap<usize, usize>) -> Result<Expr> {
        match expr {
            Expr::Alias(expr, name) => Ok(Expr::Alias(
                Box::new(self.rewrite_expr(expr, mapping)?),
                name.clone(),
            )),
            Expr::Column(i) => Ok(Expr::Column(self.new_index(mapping, i)?)),
            Expr::UnresolvedColumn(_) => Err(ExecutionError::ExecutionError(
                "Columns need to be resolved before projection push down rule can run"
                    .to_owned(),
            )),
            Expr::Literal(_) => Ok(expr.clone()),
            Expr::Not(e) => Ok(Expr::Not(Box::new(self.rewrite_expr(e, mapping)?))),
            Expr::IsNull(e) => Ok(Expr::IsNull(Box::new(self.rewrite_expr(e, mapping)?))),
            Expr::IsNotNull(e) => {
                Ok(Expr::IsNotNull(Box::new(self.rewrite_expr(e, mapping)?)))
            }
            Expr::BinaryExpr { left, op, right } => Ok(Expr::BinaryExpr {
                left: Box::new(self.rewrite_expr(left, mapping)?),
                op: op.clone(),
                right: Box::new(self.rewrite_expr(right, mapping)?),
            }),
            Expr::Cast { expr, data_type } => Ok(Expr::Cast {
                expr: Box::new(self.rewrite_expr(expr, mapping)?),
                data_type: data_type.clone(),
            }),
            Expr::Sort {
                expr,
                asc,
                nulls_first,
            } => Ok(Expr::Sort {
                expr: Box::new(self.rewrite_expr(expr, mapping)?),
                asc: *asc,
                nulls_first: *nulls_first,
            }),
            Expr::AggregateFunction {
                name,
                args,
                return_type,
            } => Ok(Expr::AggregateFunction {
                name: name.to_string(),
                args: self.rewrite_expr_list(args, mapping)?,
                return_type: return_type.clone(),
            }),
            Expr::ScalarFunction {
                name,
                args,
                return_type,
            } => Ok(Expr::ScalarFunction {
                name: name.to_string(),
                args: self.rewrite_expr_list(args, mapping)?,
                return_type: return_type.clone(),
            }),
            Expr::Wildcard => Err(ExecutionError::General(
                "Wildcard expressions are not valid in a logical query plan".to_owned(),
            )),
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

fn get_projected_schema(
    table_schema: &Schema,
    projection: &Option<Vec<usize>>,
    accum: &HashSet<usize>,
    mapping: &mut HashMap<usize, usize>,
    has_projection: bool,
) -> Result<(Vec<usize>, Schema)> {
    if projection.is_some() {
        return Err(ExecutionError::General(
            "Cannot run projection push-down rule more than once".to_string(),
        ));
    }

    // once we reach the table scan, we can use the accumulated set of column
    // indexes as the projection in the table scan
    let mut projection = accum.iter().map(|i| *i).collect::<Vec<usize>>();

    if projection.is_empty() {
        if has_projection {
            // Ensure that we are reading at least one column from the table in case the query
            // does not reference any columns directly such as "SELECT COUNT(1) FROM table"
            projection.push(0);
        } else {
            // for table scan without projection, we default to return all columns
            projection = table_schema
                .fields()
                .iter()
                .enumerate()
                .map(|(i, _)| i)
                .collect::<Vec<usize>>();
        }
    }

    // sort the projection otherwise we get non-deterministic behavior
    projection.sort();

    // create the projected schema
    let mut projected_fields: Vec<Field> = Vec::with_capacity(projection.len());
    for i in &projection {
        projected_fields.push(table_schema.fields()[*i].clone());
    }

    // now that the table scan is returning a different schema we need to
    // create a mapping from the original column index to the
    // new column index so that we can rewrite expressions as
    // we walk back up the tree

    if mapping.len() != 0 {
        return Err(ExecutionError::InternalError("illegal state".to_string()));
    }

    for i in 0..table_schema.fields().len() {
        if let Some(n) = projection.iter().position(|v| *v == i) {
            mapping.insert(i, n);
        }
    }

    Ok((projection, Schema::new(projected_fields)))
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::logicalplan::Expr::*;
    use crate::logicalplan::ScalarValue;
    use crate::test::*;
    use arrow::datatypes::DataType;

    #[test]
    fn aggregate_no_group_by() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(&table_scan)
            .aggregate(vec![], vec![max(Column(1))])?
            .build()?;

        let expected = "Aggregate: groupBy=[[]], aggr=[[MAX(#0)]]\
        \n  TableScan: test projection=Some([1])";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn aggregate_group_by() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(&table_scan)
            .aggregate(vec![Column(2)], vec![max(Column(1))])?
            .build()?;

        let expected = "Aggregate: groupBy=[[#1]], aggr=[[MAX(#0)]]\
        \n  TableScan: test projection=Some([1, 2])";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn aggregate_no_group_by_with_selection() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(&table_scan)
            .filter(Column(2))?
            .aggregate(vec![], vec![max(Column(1))])?
            .build()?;

        let expected = "Aggregate: groupBy=[[]], aggr=[[MAX(#0)]]\
        \n  Selection: #1\
        \n    TableScan: test projection=Some([1, 2])";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn cast() -> Result<()> {
        let table_scan = test_table_scan()?;

        let projection = LogicalPlanBuilder::from(&table_scan)
            .project(vec![Cast {
                expr: Box::new(Column(2)),
                data_type: DataType::Float64,
            }])?
            .build()?;

        let expected = "Projection: CAST(#0 AS Float64)\
        \n  TableScan: test projection=Some([2])";

        assert_optimized_plan_eq(&projection, expected);

        Ok(())
    }

    #[test]
    fn table_scan_projected_schema() -> Result<()> {
        let table_scan = test_table_scan()?;
        assert_eq!(3, table_scan.schema().fields().len());
        assert_fields_eq(&table_scan, vec!["a", "b", "c"]);

        let plan = LogicalPlanBuilder::from(&table_scan)
            .project(vec![Column(0), Column(1)])?
            .build()?;

        assert_fields_eq(&plan, vec!["a", "b"]);

        let expected = "Projection: #0, #1\
        \n  TableScan: test projection=Some([0, 1])";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn table_limit() -> Result<()> {
        let table_scan = test_table_scan()?;
        assert_eq!(3, table_scan.schema().fields().len());
        assert_fields_eq(&table_scan, vec!["a", "b", "c"]);

        let plan = LogicalPlanBuilder::from(&table_scan)
            .project(vec![Column(2), Column(0)])?
            .limit(5)?
            .build()?;

        assert_fields_eq(&plan, vec!["c", "a"]);

        let expected = "Limit: 5\
        \n  Projection: #1, #0\
        \n    TableScan: test projection=Some([0, 2])";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn table_scan_without_projection() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(&table_scan).build()?;
        // should expand projection to all columns without projection
        let expected = "TableScan: test projection=Some([0, 1, 2])";
        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    #[test]
    fn table_scan_with_literal_projection() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(&table_scan)
            .project(vec![
                Expr::Literal(ScalarValue::Int64(1)),
                Expr::Literal(ScalarValue::Int64(2)),
            ])?
            .build()?;
        let expected = "Projection: Int64(1), Int64(2)\
                      \n  TableScan: test projection=Some([0])";
        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    fn assert_optimized_plan_eq(plan: &LogicalPlan, expected: &str) {
        let optimized_plan = optimize(plan).expect("failed to optimize plan");
        let formatted_plan = format!("{:?}", optimized_plan);
        assert_eq!(formatted_plan, expected);
    }

    fn optimize(plan: &LogicalPlan) -> Result<LogicalPlan> {
        let mut rule = ProjectionPushDown::new();
        rule.optimize(plan)
    }
}
