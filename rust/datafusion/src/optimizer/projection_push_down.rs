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
        // set of all columns refered from a scan.
        let mut accum: HashSet<String> = HashSet::new();
        // mapping
        let mut mapping: HashMap<String, String> = HashMap::new();
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
        accum: &mut HashSet<String>,
        mapping: &mut HashMap<String, String>,
        has_projection: bool,
    ) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::Projection { expr, input, .. } => {
                // collect all columns referenced by projection expressions
                utils::exprlist_to_column_names(&expr, accum)?;

                LogicalPlanBuilder::from(
                    &self.optimize_plan(&input, accum, mapping, true)?,
                )
                .project(self.rewrite_expr_list(expr, mapping)?)?
                .build()
            }
            LogicalPlan::Selection { expr, input } => {
                // collect all columns referenced by filter expression
                utils::expr_to_column_names(expr, accum)?;

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
                utils::exprlist_to_column_names(&group_expr, accum)?;
                utils::exprlist_to_column_names(&aggr_expr, accum)?;

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
                utils::exprlist_to_column_names(&expr, accum)?;

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
                let (projection, projected_schema) =
                    get_projected_schema(&schema, projection, accum, has_projection)?;
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
                let (projection, projected_schema) =
                    get_projected_schema(&schema, projection, accum, has_projection)?;

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
                let (projection, projected_schema) =
                    get_projected_schema(&schema, projection, accum, has_projection)?;

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
        mapping: &HashMap<String, String>,
    ) -> Result<Vec<Expr>> {
        Ok(expr
            .iter()
            .map(|e| self.rewrite_expr(e, mapping))
            .collect::<Result<Vec<Expr>>>()?)
    }

    fn rewrite_expr(
        &self,
        expr: &Expr,
        mapping: &HashMap<String, String>,
    ) -> Result<Expr> {
        match expr {
            Expr::Alias(expr, name) => Ok(Expr::Alias(
                Box::new(self.rewrite_expr(expr, mapping)?),
                name.clone(),
            )),
            Expr::Column(_) => Ok(expr.clone()),
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
}

fn get_projected_schema(
    table_schema: &Schema,
    projection: &Option<Vec<usize>>,
    accum: &HashSet<String>,
    has_projection: bool,
) -> Result<(Vec<usize>, Schema)> {
    if projection.is_some() {
        return Err(ExecutionError::General(
            "Cannot run projection push-down rule more than once".to_string(),
        ));
    }

    // once we reach the table scan, we can use the accumulated set of column
    // names to construct the set of column indexes in the scan
    let mut projection: Vec<usize> = accum
        .iter()
        .map(|name| table_schema.index_of(name).unwrap())
        .collect();

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

    Ok((projection, Schema::new(projected_fields)))
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::logicalplan::Expr::*;
    use crate::logicalplan::{col, lit};
    use crate::test::*;
    use arrow::datatypes::DataType;

    #[test]
    fn aggregate_no_group_by() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(&table_scan)
            .aggregate(vec![], vec![max(col("b"))])?
            .build()?;

        let expected = "Aggregate: groupBy=[[]], aggr=[[MAX(#b)]]\
        \n  TableScan: test projection=Some([1])";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn aggregate_group_by() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(&table_scan)
            .aggregate(vec![col("c")], vec![max(col("b"))])?
            .build()?;

        let expected = "Aggregate: groupBy=[[#c]], aggr=[[MAX(#b)]]\
        \n  TableScan: test projection=Some([1, 2])";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn aggregate_no_group_by_with_selection() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(&table_scan)
            .filter(col("c"))?
            .aggregate(vec![], vec![max(col("b"))])?
            .build()?;

        let expected = "Aggregate: groupBy=[[]], aggr=[[MAX(#b)]]\
        \n  Selection: #c\
        \n    TableScan: test projection=Some([1, 2])";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn cast() -> Result<()> {
        let table_scan = test_table_scan()?;

        let projection = LogicalPlanBuilder::from(&table_scan)
            .project(vec![Cast {
                expr: Box::new(col("c")),
                data_type: DataType::Float64,
            }])?
            .build()?;

        let expected = "Projection: CAST(#c AS Float64)\
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
            .project(vec![col("a"), col("b")])?
            .build()?;

        assert_fields_eq(&plan, vec!["a", "b"]);

        let expected = "Projection: #a, #b\
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
            .project(vec![col("c"), col("a")])?
            .limit(5)?
            .build()?;

        assert_fields_eq(&plan, vec!["c", "a"]);

        let expected = "Limit: 5\
        \n  Projection: #c, #a\
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
            .project(vec![lit(1_i64), lit(2_i64)])?
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
