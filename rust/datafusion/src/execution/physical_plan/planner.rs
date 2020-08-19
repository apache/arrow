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

use std::sync::{Arc, Mutex};

use crate::error::{ExecutionError, Result};
use crate::execution::context::ExecutionContextState;
use crate::execution::physical_plan::csv::{CsvExec, CsvReadOptions};
use crate::execution::physical_plan::datasource::DatasourceExec;
use crate::execution::physical_plan::explain::ExplainExec;
use crate::execution::physical_plan::expressions::{
    Avg, BinaryExpr, CastExpr, Column, Count, Literal, Max, Min, PhysicalSortExpr, Sum,
};
use crate::execution::physical_plan::filter::FilterExec;
use crate::execution::physical_plan::hash_aggregate::HashAggregateExec;
use crate::execution::physical_plan::limit::GlobalLimitExec;
use crate::execution::physical_plan::memory::MemoryExec;
use crate::execution::physical_plan::merge::MergeExec;
use crate::execution::physical_plan::parquet::ParquetExec;
use crate::execution::physical_plan::projection::ProjectionExec;
use crate::execution::physical_plan::sort::SortExec;
use crate::execution::physical_plan::udf::ScalarFunctionExpr;
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
        ctx_state: Arc<Mutex<ExecutionContextState>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let batch_size = ctx_state
            .lock()
            .expect("failed to lock mutex")
            .config
            .batch_size;

        match logical_plan {
            LogicalPlan::TableScan {
                table_name,
                projection,
                ..
            } => match ctx_state
                .lock()
                .expect("failed to lock mutex")
                .datasources
                .lock()
                .expect("failed to lock mutex")
                .get(table_name)
            {
                Some(provider) => {
                    let partitions = provider.scan(projection, batch_size)?;
                    if partitions.is_empty() {
                        Err(ExecutionError::General(
                            "Table provider returned no partitions".to_string(),
                        ))
                    } else {
                        let schema = match projection {
                            None => provider.schema().clone(),
                            Some(p) => Arc::new(Schema::new(
                                p.iter()
                                    .map(|i| provider.schema().field(*i).clone())
                                    .collect(),
                            )),
                        };

                        let exec = DatasourceExec::new(schema, partitions.clone());
                        Ok(Arc::new(exec))
                    }
                }
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
                let input = self.create_physical_plan(input, ctx_state.clone())?;
                let input_schema = input.as_ref().schema().clone();
                let runtime_expr = expr
                    .iter()
                    .map(|e| {
                        tuple_err((
                            self.create_physical_expr(
                                e,
                                &input_schema,
                                ctx_state.clone(),
                            ),
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
                let input = self.create_physical_plan(input, ctx_state.clone())?;
                let input_schema = input.as_ref().schema().clone();

                let groups = group_expr
                    .iter()
                    .map(|e| {
                        tuple_err((
                            self.create_physical_expr(
                                e,
                                &input_schema,
                                ctx_state.clone(),
                            ),
                            e.name(&input_schema),
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let aggregates = aggr_expr
                    .iter()
                    .map(|e| {
                        tuple_err((
                            self.create_aggregate_expr(
                                e,
                                &input_schema,
                                ctx_state.clone(),
                            ),
                            e.name(&input_schema),
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;

                let initial_aggr = HashAggregateExec::try_new(
                    groups.clone(),
                    aggregates.clone(),
                    input,
                )?;

                let schema = initial_aggr.schema();
                let partitions = initial_aggr.partitions()?;

                if partitions.len() == 1 {
                    return Ok(Arc::new(initial_aggr));
                }

                let merge = Arc::new(MergeExec::new(
                    schema.clone(),
                    partitions,
                    ctx_state
                        .lock()
                        .expect("failed to lock mutex")
                        .config
                        .concurrency,
                ));

                // construct the expressions for the final aggregation
                let (final_group, final_aggr) = initial_aggr.make_final_expr(
                    groups.iter().map(|x| x.1.clone()).collect(),
                    aggregates.iter().map(|x| x.1.clone()).collect(),
                );

                // construct a second aggregation, keeping the final column name equal to the first aggregation
                // and the expressions corresponding to the respective aggregate
                Ok(Arc::new(HashAggregateExec::try_new(
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
                    merge,
                )?))
            }
            LogicalPlan::Filter {
                input, predicate, ..
            } => {
                let input = self.create_physical_plan(input, ctx_state.clone())?;
                let input_schema = input.as_ref().schema().clone();
                let runtime_expr = self.create_physical_expr(
                    predicate,
                    &input_schema,
                    ctx_state.clone(),
                )?;
                Ok(Arc::new(FilterExec::try_new(runtime_expr, input)?))
            }
            LogicalPlan::Sort { expr, input, .. } => {
                let input = self.create_physical_plan(input, ctx_state.clone())?;
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
                            ctx_state.clone(),
                        ),
                        _ => Err(ExecutionError::ExecutionError(
                            "Sort only accepts sort expressions".to_string(),
                        )),
                    })
                    .collect::<Result<Vec<_>>>()?;

                Ok(Arc::new(SortExec::try_new(
                    sort_expr,
                    input,
                    ctx_state
                        .lock()
                        .expect("failed to lock mutex")
                        .config
                        .concurrency,
                )?))
            }
            LogicalPlan::Limit { input, n, .. } => {
                let input = self.create_physical_plan(input, ctx_state.clone())?;
                let input_schema = input.as_ref().schema().clone();

                Ok(Arc::new(GlobalLimitExec::new(
                    input_schema.clone(),
                    input.partitions()?,
                    *n,
                    ctx_state
                        .lock()
                        .expect("failed to lock mutex")
                        .config
                        .concurrency,
                )))
            }
            LogicalPlan::Explain {
                verbose,
                plan,
                stringified_plans,
                schema,
            } => {
                let input = self.create_physical_plan(plan, ctx_state.clone())?;

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
}

impl DefaultPhysicalPlanner {
    /// Create a physical expression from a logical expression
    pub fn create_physical_expr(
        &self,
        e: &Expr,
        input_schema: &Schema,
        ctx_state: Arc<Mutex<ExecutionContextState>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        match e {
            Expr::Alias(expr, ..) => {
                Ok(self.create_physical_expr(expr, input_schema, ctx_state.clone())?)
            }
            Expr::Column(name) => {
                // check that name exists
                input_schema.field_with_name(&name)?;
                Ok(Arc::new(Column::new(name)))
            }
            Expr::Literal(value) => Ok(Arc::new(Literal::new(value.clone()))),
            Expr::BinaryExpr { left, op, right } => Ok(Arc::new(BinaryExpr::new(
                self.create_physical_expr(left, input_schema, ctx_state.clone())?,
                op.clone(),
                self.create_physical_expr(right, input_schema, ctx_state.clone())?,
            ))),
            Expr::Cast { expr, data_type } => Ok(Arc::new(CastExpr::try_new(
                self.create_physical_expr(expr, input_schema, ctx_state.clone())?,
                input_schema,
                data_type.clone(),
            )?)),
            Expr::ScalarFunction {
                name,
                args,
                return_type,
            } => match ctx_state
                .lock()
                .expect("failed to lock mutex")
                .scalar_functions
                .lock()
                .expect("failed to lock mutex")
                .get(name)
            {
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
                        Box::new(f.fun.clone()),
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
        ctx_state: Arc<Mutex<ExecutionContextState>>,
    ) -> Result<Arc<dyn AggregateExpr>> {
        match e {
            Expr::AggregateFunction { name, args, .. } => {
                match name.to_lowercase().as_ref() {
                    "sum" => Ok(Arc::new(Sum::new(self.create_physical_expr(
                        &args[0],
                        input_schema,
                        ctx_state.clone(),
                    )?))),
                    "avg" => Ok(Arc::new(Avg::new(self.create_physical_expr(
                        &args[0],
                        input_schema,
                        ctx_state.clone(),
                    )?))),
                    "max" => Ok(Arc::new(Max::new(self.create_physical_expr(
                        &args[0],
                        input_schema,
                        ctx_state.clone(),
                    )?))),
                    "min" => Ok(Arc::new(Min::new(self.create_physical_expr(
                        &args[0],
                        input_schema,
                        ctx_state.clone(),
                    )?))),
                    "count" => Ok(Arc::new(Count::new(self.create_physical_expr(
                        &args[0],
                        input_schema,
                        ctx_state.clone(),
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
        ctx_state: Arc<Mutex<ExecutionContextState>>,
    ) -> Result<PhysicalSortExpr> {
        Ok(PhysicalSortExpr {
            expr: self.create_physical_expr(e, input_schema, ctx_state.clone())?,
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
