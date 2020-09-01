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

//! Physical query planner

use std::sync::Arc;

use super::{expressions::binary, functions};
use crate::error::{ExecutionError, Result};
use crate::execution::context::ExecutionContextState;
use crate::execution::physical_plan::csv::{CsvExec, CsvReadOptions};
use crate::execution::physical_plan::explain::ExplainExec;
use crate::execution::physical_plan::expressions::{
    Avg, Column, Count, Literal, Max, Min, PhysicalSortExpr, Sum,
};
use crate::execution::physical_plan::filter::FilterExec;
use crate::execution::physical_plan::hash_aggregate::{AggregateMode, HashAggregateExec};
use crate::execution::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use crate::execution::physical_plan::memory::MemoryExec;
use crate::execution::physical_plan::merge::MergeExec;
use crate::execution::physical_plan::parquet::ParquetExec;
use crate::execution::physical_plan::projection::ProjectionExec;
use crate::execution::physical_plan::sort::SortExec;
use crate::execution::physical_plan::udf::ScalarFunctionExpr;
use crate::execution::physical_plan::{expressions, Distribution};
use crate::execution::physical_plan::{
    AggregateExpr, ExecutionPlan, PhysicalExpr, PhysicalPlanner,
};
use crate::logicalplan::{Expr, LogicalPlan, PlanType, StringifiedPlan};
use arrow::compute::SortOptions;
use arrow::datatypes::Schema;

/// Default single node physical query planner that converts a
/// `LogicalPlan` to an `ExecutionPlan` suitable for execution.
pub struct DefaultPhysicalPlanner {}

impl Default for DefaultPhysicalPlanner {
    /// Create an implementation of the physical planner
    fn default() -> Self {
        Self {}
    }
}

impl PhysicalPlanner for DefaultPhysicalPlanner {
    /// Create a physical plan from a logical plan
    fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        ctx_state: &ExecutionContextState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let plan = self.create_initial_plan(logical_plan, ctx_state)?;
        self.optimize_plan(plan, ctx_state)
    }
}

impl DefaultPhysicalPlanner {
    /// Create a physical plan from a logical plan
    fn optimize_plan(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        ctx_state: &ExecutionContextState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let children = plan
            .children()
            .iter()
            .map(|child| self.optimize_plan(child.clone(), ctx_state))
            .collect::<Result<Vec<_>>>()?;

        if children.len() == 0 {
            // leaf node, children cannot be replaced
            Ok(plan.clone())
        } else {
            match plan.required_child_distribution() {
                Distribution::UnspecifiedDistribution => plan.with_new_children(children),
                Distribution::SinglePartition => plan.with_new_children(
                    children
                        .iter()
                        .map(|child| {
                            if child.output_partitioning().partition_count() == 1 {
                                child.clone()
                            } else {
                                Arc::new(MergeExec::new(
                                    child.clone(),
                                    ctx_state.config.concurrency,
                                ))
                            }
                        })
                        .collect(),
                ),
            }
        }
    }

    /// Create a physical plan from a logical plan
    fn create_initial_plan(
        &self,
        logical_plan: &LogicalPlan,
        ctx_state: &ExecutionContextState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let batch_size = ctx_state.config.batch_size;

        match logical_plan {
            LogicalPlan::TableScan {
                table_name,
                projection,
                ..
            } => match ctx_state.datasources.get(table_name) {
                Some(provider) => provider.scan(projection, batch_size),
                _ => Err(ExecutionError::General(format!(
                    "No table named {}",
                    table_name
                ))),
            },
            LogicalPlan::InMemoryScan {
                data,
                projection,
                projected_schema,
                ..
            } => Ok(Arc::new(MemoryExec::try_new(
                data,
                Arc::new(projected_schema.as_ref().to_owned()),
                projection.to_owned(),
            )?)),
            LogicalPlan::CsvScan {
                path,
                schema,
                has_header,
                delimiter,
                projection,
                ..
            } => Ok(Arc::new(CsvExec::try_new(
                path,
                CsvReadOptions::new()
                    .schema(schema.as_ref())
                    .delimiter_option(*delimiter)
                    .has_header(*has_header),
                projection.to_owned(),
                batch_size,
            )?)),
            LogicalPlan::ParquetScan {
                path, projection, ..
            } => Ok(Arc::new(ParquetExec::try_new(
                path,
                projection.to_owned(),
                batch_size,
            )?)),
            LogicalPlan::Projection { input, expr, .. } => {
                let input = self.create_physical_plan(input, ctx_state)?;
                let input_schema = input.as_ref().schema().clone();
                let runtime_expr = expr
                    .iter()
                    .map(|e| {
                        tuple_err((
                            self.create_physical_expr(e, &input_schema, &ctx_state),
                            e.name(&input_schema),
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(Arc::new(ProjectionExec::try_new(runtime_expr, input)?))
            }
            LogicalPlan::Aggregate {
                input,
                group_expr,
                aggr_expr,
                ..
            } => {
                // Initially need to perform the aggregate and then merge the partitions
                let input = self.create_physical_plan(input, ctx_state)?;
                let input_schema = input.as_ref().schema().clone();

                let groups = group_expr
                    .iter()
                    .map(|e| {
                        tuple_err((
                            self.create_physical_expr(e, &input_schema, ctx_state),
                            e.name(&input_schema),
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let aggregates = aggr_expr
                    .iter()
                    .map(|e| {
                        tuple_err((
                            self.create_aggregate_expr(e, &input_schema, ctx_state),
                            e.name(&input_schema),
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;

                let initial_aggr = HashAggregateExec::try_new(
                    AggregateMode::Partial,
                    groups.clone(),
                    aggregates.clone(),
                    input,
                )?;

                if initial_aggr.output_partitioning().partition_count() == 1 {
                    return Ok(Arc::new(initial_aggr));
                }

                let partial_aggr = Arc::new(initial_aggr);

                // construct the expressions for the final aggregation
                let (final_group, final_aggr) = partial_aggr.make_final_expr(
                    groups.iter().map(|x| x.1.clone()).collect(),
                    aggregates.iter().map(|x| x.1.clone()).collect(),
                );

                // construct a second aggregation, keeping the final column name equal to the first aggregation
                // and the expressions corresponding to the respective aggregate
                Ok(Arc::new(HashAggregateExec::try_new(
                    AggregateMode::Final,
                    final_group
                        .iter()
                        .enumerate()
                        .map(|(i, expr)| (expr.clone(), groups[i].1.clone()))
                        .collect(),
                    final_aggr
                        .iter()
                        .enumerate()
                        .map(|(i, expr)| (expr.clone(), aggregates[i].1.clone()))
                        .collect(),
                    partial_aggr,
                )?))
            }
            LogicalPlan::Filter {
                input, predicate, ..
            } => {
                let input = self.create_physical_plan(input, ctx_state)?;
                let input_schema = input.as_ref().schema().clone();
                let runtime_expr =
                    self.create_physical_expr(predicate, &input_schema, ctx_state)?;
                Ok(Arc::new(FilterExec::try_new(runtime_expr, input)?))
            }
            LogicalPlan::Sort { expr, input, .. } => {
                let input = self.create_physical_plan(input, ctx_state)?;
                let input_schema = input.as_ref().schema().clone();

                let sort_expr = expr
                    .iter()
                    .map(|e| match e {
                        Expr::Sort {
                            expr,
                            asc,
                            nulls_first,
                        } => self.create_physical_sort_expr(
                            expr,
                            &input_schema,
                            SortOptions {
                                descending: !*asc,
                                nulls_first: *nulls_first,
                            },
                            ctx_state,
                        ),
                        _ => Err(ExecutionError::ExecutionError(
                            "Sort only accepts sort expressions".to_string(),
                        )),
                    })
                    .collect::<Result<Vec<_>>>()?;

                Ok(Arc::new(SortExec::try_new(
                    sort_expr,
                    input,
                    ctx_state.config.concurrency,
                )?))
            }
            LogicalPlan::Limit { input, n, .. } => {
                let limit = *n;
                let input = self.create_physical_plan(input, ctx_state)?;

                // GlobalLimitExec requires a single partition for input
                let input = if input.output_partitioning().partition_count() == 1 {
                    input
                } else {
                    // Apply a LocalLimitExec to each partition. The optimizer will also insert
                    // a MergeExec between the GlobalLimitExec and LocalLimitExec
                    Arc::new(LocalLimitExec::new(input, limit))
                };

                Ok(Arc::new(GlobalLimitExec::new(
                    input,
                    limit,
                    ctx_state.config.concurrency,
                )))
            }
            LogicalPlan::Explain {
                verbose,
                plan,
                stringified_plans,
                schema,
            } => {
                let input = self.create_physical_plan(plan, ctx_state)?;

                let mut stringified_plans = stringified_plans
                    .iter()
                    .filter(|s| s.should_display(*verbose))
                    .map(|s| s.clone())
                    .collect::<Vec<_>>();

                // add in the physical plan if requested
                if *verbose {
                    stringified_plans.push(StringifiedPlan::new(
                        PlanType::PhysicalPlan,
                        format!("{:#?}", input),
                    ));
                }
                let schema_ref = Arc::new((**schema).clone());
                Ok(Arc::new(ExplainExec::new(schema_ref, stringified_plans)))
            }
            _ => Err(ExecutionError::General(
                "Unsupported logical plan variant".to_string(),
            )),
        }
    }

    /// Create a physical expression from a logical expression
    pub fn create_physical_expr(
        &self,
        e: &Expr,
        input_schema: &Schema,
        ctx_state: &ExecutionContextState,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        match e {
            Expr::Alias(expr, ..) => {
                Ok(self.create_physical_expr(expr, input_schema, ctx_state)?)
            }
            Expr::Column(name) => {
                // check that name exists
                input_schema.field_with_name(&name)?;
                Ok(Arc::new(Column::new(name)))
            }
            Expr::Literal(value) => Ok(Arc::new(Literal::new(value.clone()))),
            Expr::BinaryExpr { left, op, right } => {
                let lhs =
                    self.create_physical_expr(left, input_schema, ctx_state.clone())?;
                let rhs =
                    self.create_physical_expr(right, input_schema, ctx_state.clone())?;
                binary(lhs, op.clone(), rhs, input_schema)
            }
            Expr::Cast { expr, data_type } => expressions::cast(
                self.create_physical_expr(expr, input_schema, ctx_state.clone())?,
                input_schema,
                data_type.clone(),
            ),
            Expr::ScalarFunction { fun, args } => {
                let physical_args = args
                    .iter()
                    .map(|e| {
                        self.create_physical_expr(e, input_schema, ctx_state.clone())
                    })
                    .collect::<Result<Vec<_>>>()?;
                functions::create_physical_expr(fun, &physical_args, input_schema)
            }
            Expr::ScalarUDF {
                name,
                args,
                return_type,
            } => match ctx_state.scalar_functions.get(name) {
                Some(f) => {
                    let mut physical_args = vec![];
                    for e in args {
                        physical_args.push(self.create_physical_expr(
                            e,
                            input_schema,
                            ctx_state.clone(),
                        )?);
                    }
                    Ok(Arc::new(ScalarFunctionExpr::new(
                        name,
                        f.fun.clone(),
                        physical_args,
                        return_type,
                    )))
                }
                _ => Err(ExecutionError::General(format!(
                    "Invalid scalar function '{:?}'",
                    name
                ))),
            },
            other => Err(ExecutionError::NotImplemented(format!(
                "Physical plan does not support logical expression {:?}",
                other
            ))),
        }
    }

    /// Create an aggregate expression from a logical expression
    pub fn create_aggregate_expr(
        &self,
        e: &Expr,
        input_schema: &Schema,
        ctx_state: &ExecutionContextState,
    ) -> Result<Arc<dyn AggregateExpr>> {
        match e {
            Expr::AggregateFunction { name, args, .. } => {
                match name.to_lowercase().as_ref() {
                    "sum" => Ok(Arc::new(Sum::new(self.create_physical_expr(
                        &args[0],
                        input_schema,
                        ctx_state,
                    )?))),
                    "avg" => Ok(Arc::new(Avg::new(self.create_physical_expr(
                        &args[0],
                        input_schema,
                        ctx_state,
                    )?))),
                    "max" => Ok(Arc::new(Max::new(self.create_physical_expr(
                        &args[0],
                        input_schema,
                        ctx_state,
                    )?))),
                    "min" => Ok(Arc::new(Min::new(self.create_physical_expr(
                        &args[0],
                        input_schema,
                        ctx_state,
                    )?))),
                    "count" => Ok(Arc::new(Count::new(self.create_physical_expr(
                        &args[0],
                        input_schema,
                        ctx_state,
                    )?))),
                    other => Err(ExecutionError::NotImplemented(format!(
                        "Unsupported aggregate function '{}'",
                        other
                    ))),
                }
            }
            other => Err(ExecutionError::General(format!(
                "Invalid aggregate expression '{:?}'",
                other
            ))),
        }
    }

    /// Create an aggregate expression from a logical expression
    pub fn create_physical_sort_expr(
        &self,
        e: &Expr,
        input_schema: &Schema,
        options: SortOptions,
        ctx_state: &ExecutionContextState,
    ) -> Result<PhysicalSortExpr> {
        Ok(PhysicalSortExpr {
            expr: self.create_physical_expr(e, input_schema, ctx_state)?,
            options: options,
        })
    }
}

fn tuple_err<T, R>(value: (Result<T>, Result<R>)) -> Result<(T, R)> {
    match value {
        (Ok(e), Ok(e1)) => Ok((e, e1)),
        (Err(e), Ok(_)) => Err(e),
        (Ok(_), Err(e1)) => Err(e1),
        (Err(e), Err(_)) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::physical_plan::csv::CsvReadOptions;
    use crate::logicalplan::{aggregate_expr, col, lit, LogicalPlanBuilder};
    use crate::{prelude::ExecutionConfig, test::arrow_testdata_path};
    use std::collections::HashMap;

    fn plan(logical_plan: &LogicalPlan) -> Result<Arc<dyn ExecutionPlan>> {
        let ctx_state = ExecutionContextState {
            datasources: HashMap::new(),
            scalar_functions: HashMap::new(),
            config: ExecutionConfig::new(),
        };

        let planer = DefaultPhysicalPlanner {};
        planer.create_physical_plan(logical_plan, &ctx_state)
    }

    #[test]
    fn test_all_operators() -> Result<()> {
        let testdata = arrow_testdata_path();
        let path = format!("{}/csv/aggregate_test_100.csv", testdata);

        let options = CsvReadOptions::new().schema_infer_max_records(100);
        let logical_plan = LogicalPlanBuilder::scan_csv(&path, options, None)?
            // filter clause needs the type coercion rule applied
            .filter(col("c7").lt(lit(5_u8)))?
            .project(vec![col("c1"), col("c2")])?
            .aggregate(vec![col("c1")], vec![aggregate_expr("SUM", col("c2"))])?
            .sort(vec![col("c1").sort(true, true)])?
            .limit(10)?
            .build()?;

        let plan = plan(&logical_plan)?;

        // verify that the plan correctly casts u8 to i64
        let expected = "BinaryExpr { left: Column { name: \"c7\" }, op: Lt, right: CastExpr { expr: Literal { value: UInt8(5) }, cast_type: Int64 } }";
        assert!(format!("{:?}", plan).contains(expected));

        Ok(())
    }

    #[test]
    fn test_with_csv_plan() -> Result<()> {
        let testdata = arrow_testdata_path();
        let path = format!("{}/csv/aggregate_test_100.csv", testdata);

        let options = CsvReadOptions::new().schema_infer_max_records(100);
        let logical_plan = LogicalPlanBuilder::scan_csv(&path, options, None)?
            .filter(col("c7").lt(col("c12")))?
            .build()?;

        let plan = plan(&logical_plan)?;

        // c12 is f64, c7 is u8 -> cast c7 to f64
        let expected = "predicate: BinaryExpr { left: CastExpr { expr: Column { name: \"c7\" }, cast_type: Float64 }, op: Lt, right: Column { name: \"c12\" } }";
        assert!(format!("{:?}", plan).contains(expected));
        Ok(())
    }

    #[test]
    fn errors() -> Result<()> {
        let testdata = arrow_testdata_path();
        let path = format!("{}/csv/aggregate_test_100.csv", testdata);
        let options = CsvReadOptions::new().schema_infer_max_records(100);

        let bool_expr = col("c1").eq(col("c1"));
        let cases = vec![
            // utf8 < u32
            col("c1").lt(col("c2")),
            // utf8 AND utf8
            col("c1").and(col("c1")),
            // u8 AND u8
            col("c3").and(col("c3")),
            // utf8 = u32
            col("c1").eq(col("c2")),
            // utf8 = bool
            col("c1").eq(bool_expr.clone()),
            // u32 AND bool
            col("c2").and(bool_expr),
            // utf8 LIKE u32
            col("c1").like(col("c2")),
        ];
        for case in cases {
            let logical_plan = LogicalPlanBuilder::scan_csv(&path, options, None)?
                .project(vec![case.clone()]);
            if let Ok(_) = logical_plan {
                return Err(ExecutionError::General(format!(
                    "Expression {:?} expected to error due to impossible coercion",
                    case
                )));
            };
        }
        Ok(())
    }
}
