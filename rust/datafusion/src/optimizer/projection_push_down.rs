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
use crate::optimizer::optimizer::OptimizerRule;
use crate::optimizer::utils;
use arrow::datatypes::{Field, Schema};
use arrow::error::Result as ArrowResult;
use std::collections::HashSet;

/// Projection Push Down optimizer rule ensures that only referenced columns are
/// loaded into memory
pub struct ProjectionPushDown {}

impl OptimizerRule for ProjectionPushDown {
    fn optimize(&mut self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        // set of all columns refered from a scan.
        let mut accum: HashSet<String> = HashSet::new();
        self.optimize_plan(plan, &mut accum, false)
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
        has_projection: bool,
    ) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::Projection {
                expr,
                input,
                schema,
            } => {
                // collect all columns referenced by projection expressions
                utils::exprlist_to_column_names(&expr, accum)?;

                Ok(LogicalPlan::Projection {
                    expr: expr.clone(),
                    input: Box::new(self.optimize_plan(&input, accum, true)?),
                    schema: schema.clone(),
                })
            }
            LogicalPlan::Selection { expr, input } => {
                // collect all columns referenced by filter expression
                utils::expr_to_column_names(expr, accum)?;

                Ok(LogicalPlan::Selection {
                    expr: expr.clone(),
                    input: Box::new(self.optimize_plan(&input, accum, has_projection)?),
                })
            }
            LogicalPlan::Aggregate {
                input,
                group_expr,
                aggr_expr,
                schema,
            } => {
                // collect all columns referenced by grouping and aggregate expressions
                utils::exprlist_to_column_names(&group_expr, accum)?;
                utils::exprlist_to_column_names(&aggr_expr, accum)?;

                Ok(LogicalPlan::Aggregate {
                    input: Box::new(self.optimize_plan(&input, accum, has_projection)?),
                    group_expr: group_expr.clone(),
                    aggr_expr: aggr_expr.clone(),
                    schema: schema.clone(),
                })
            }
            LogicalPlan::Sort {
                expr,
                input,
                schema,
            } => {
                // collect all columns referenced by sort expressions
                utils::exprlist_to_column_names(&expr, accum)?;

                Ok(LogicalPlan::Sort {
                    expr: expr.clone(),
                    input: Box::new(self.optimize_plan(&input, accum, has_projection)?),
                    schema: schema.clone(),
                })
            }
            LogicalPlan::Limit { n, input, schema } => Ok(LogicalPlan::Limit {
                n: n.clone(),
                input: Box::new(self.optimize_plan(&input, accum, has_projection)?),
                schema: schema.clone(),
            }),
            LogicalPlan::EmptyRelation { .. } => Ok(plan.clone()),
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
                    projection: Some(projection),
                    projected_schema: Box::new(projected_schema),
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
            LogicalPlan::CreateExternalTable { .. } => Ok(plan.clone()),
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
    //
    // we discard non-existing columns because some column names are not part of the schema,
    // e.g. when the column derives from an aggregation
    let mut projection: Vec<usize> = accum
        .iter()
        .map(|name| table_schema.index_of(name))
        .filter_map(ArrowResult::ok)
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
    use crate::logicalplan::{col, lit};
    use crate::logicalplan::{Expr, LogicalPlanBuilder};
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
            .project(vec![Expr::Cast {
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
