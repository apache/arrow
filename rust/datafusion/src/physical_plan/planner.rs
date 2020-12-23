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

use super::{aggregates, empty::EmptyExec, expressions::binary, functions, udaf};
use crate::error::{DataFusionError, Result};
use crate::execution::context::ExecutionContextState;
use crate::logical_plan::{
    DFSchema, Expr, LogicalPlan, Operator, Partitioning as LogicalPartitioning, PlanType,
    StringifiedPlan, UserDefinedLogicalNode,
};
use crate::physical_plan::explain::ExplainExec;
use crate::physical_plan::expressions::{CaseExpr, Column, Literal, PhysicalSortExpr};
use crate::physical_plan::filter::FilterExec;
use crate::physical_plan::hash_aggregate::{AggregateMode, HashAggregateExec};
use crate::physical_plan::hash_join::HashJoinExec;
use crate::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use crate::physical_plan::merge::MergeExec;
use crate::physical_plan::projection::ProjectionExec;
use crate::physical_plan::repartition::RepartitionExec;
use crate::physical_plan::sort::SortExec;
use crate::physical_plan::udf;
use crate::physical_plan::{expressions, Distribution};
use crate::physical_plan::{hash_utils, Partitioning};
use crate::physical_plan::{AggregateExpr, ExecutionPlan, PhysicalExpr, PhysicalPlanner};
use crate::prelude::JoinType;
use crate::variable::VarType;
use arrow::compute::SortOptions;
use arrow::datatypes::{Schema, SchemaRef};
use expressions::col;

/// This trait permits the `DefaultPhysicalPlanner` to create plans for
/// user defined `ExtensionPlanNode`s
pub trait ExtensionPlanner {
    /// Create a physical plan for an extension node
    fn plan_extension(
        &self,
        node: &dyn UserDefinedLogicalNode,
        inputs: Vec<Arc<dyn ExecutionPlan>>,
        ctx_state: &ExecutionContextState,
    ) -> Result<Arc<dyn ExecutionPlan>>;
}

/// Default single node physical query planner that converts a
/// `LogicalPlan` to an `ExecutionPlan` suitable for execution.
pub struct DefaultPhysicalPlanner {
    extension_planner: Arc<dyn ExtensionPlanner + Send + Sync>,
}

impl Default for DefaultPhysicalPlanner {
    /// Create an implementation of the default physical planner
    fn default() -> Self {
        Self {
            extension_planner: Arc::new(DefaultExtensionPlanner {}),
        }
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
    /// Create a physical planner that uses `extension_planner` to
    /// plan extension nodes.
    pub fn with_extension_planner(
        extension_planner: Arc<dyn ExtensionPlanner + Send + Sync>,
    ) -> Self {
        Self { extension_planner }
    }

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

        if children.is_empty() {
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
                                Arc::new(MergeExec::new(child.clone()))
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
                source,
                projection,
                filters,
                ..
            } => source.scan(projection, batch_size, filters),
            LogicalPlan::Aggregate {
                input,
                group_expr,
                aggr_expr,
                ..
            } => {
                // Initially need to perform the aggregate and then merge the partitions
                let input_exec = self.create_physical_plan(input, ctx_state)?;
                let physical_input_schema = input_exec.as_ref().schema();
                let logical_input_schema = input.as_ref().schema();

                let groups = group_expr
                    .iter()
                    .map(|e| {
                        tuple_err((
                            self.create_physical_expr(
                                e,
                                &physical_input_schema,
                                ctx_state,
                            ),
                            e.name(&logical_input_schema),
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let aggregates = aggr_expr
                    .iter()
                    .map(|e| {
                        self.create_aggregate_expr(
                            e,
                            &logical_input_schema,
                            &physical_input_schema,
                            ctx_state,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;

                let initial_aggr = Arc::new(HashAggregateExec::try_new(
                    AggregateMode::Partial,
                    groups.clone(),
                    aggregates.clone(),
                    input_exec,
                )?);

                let final_group: Vec<Arc<dyn PhysicalExpr>> =
                    (0..groups.len()).map(|i| col(&groups[i].1)).collect();

                // construct a second aggregation, keeping the final column name equal to the first aggregation
                // and the expressions corresponding to the respective aggregate
                Ok(Arc::new(HashAggregateExec::try_new(
                    AggregateMode::Final,
                    final_group
                        .iter()
                        .enumerate()
                        .map(|(i, expr)| (expr.clone(), groups[i].1.clone()))
                        .collect(),
                    aggregates,
                    initial_aggr,
                )?))
            }
            LogicalPlan::Projection { input, expr, .. } => {
                let input_exec = self.create_physical_plan(input, ctx_state)?;
                let input_schema = input.as_ref().schema();
                let runtime_expr = expr
                    .iter()
                    .map(|e| {
                        tuple_err((
                            self.create_physical_expr(
                                e,
                                &input_exec.schema(),
                                &ctx_state,
                            ),
                            e.name(&input_schema),
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(Arc::new(ProjectionExec::try_new(runtime_expr, input_exec)?))
            }
            LogicalPlan::Filter {
                input, predicate, ..
            } => {
                let input = self.create_physical_plan(input, ctx_state)?;
                let input_schema = input.as_ref().schema();
                let runtime_expr =
                    self.create_physical_expr(predicate, &input_schema, ctx_state)?;
                Ok(Arc::new(FilterExec::try_new(runtime_expr, input)?))
            }
            LogicalPlan::Repartition {
                input,
                partitioning_scheme,
            } => {
                let input = self.create_physical_plan(input, ctx_state)?;
                let input_schema = input.schema();
                let physical_partitioning = match partitioning_scheme {
                    LogicalPartitioning::RoundRobinBatch(n) => {
                        Partitioning::RoundRobinBatch(*n)
                    }
                    LogicalPartitioning::Hash(expr, n) => {
                        let runtime_expr = expr
                            .iter()
                            .map(|e| {
                                self.create_physical_expr(e, &input_schema, &ctx_state)
                            })
                            .collect::<Result<Vec<_>>>()?;
                        Partitioning::Hash(runtime_expr, *n)
                    }
                };
                Ok(Arc::new(RepartitionExec::try_new(
                    input,
                    physical_partitioning,
                )?))
            }
            LogicalPlan::Sort { expr, input, .. } => {
                let input = self.create_physical_plan(input, ctx_state)?;
                let input_schema = input.as_ref().schema();

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
                        _ => Err(DataFusionError::Plan(
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
            LogicalPlan::Join {
                left,
                right,
                on: keys,
                join_type,
                ..
            } => {
                let left = self.create_physical_plan(left, ctx_state)?;
                let right = self.create_physical_plan(right, ctx_state)?;
                let physical_join_type = match join_type {
                    JoinType::Inner => hash_utils::JoinType::Inner,
                    JoinType::Left => hash_utils::JoinType::Left,
                    JoinType::Right => hash_utils::JoinType::Right,
                };

                Ok(Arc::new(HashJoinExec::try_new(
                    left,
                    right,
                    &keys,
                    &physical_join_type,
                )?))
            }
            LogicalPlan::EmptyRelation {
                produce_one_row,
                schema,
            } => Ok(Arc::new(EmptyExec::new(
                *produce_one_row,
                SchemaRef::new(schema.as_ref().to_owned().into()),
            ))),
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
            LogicalPlan::CreateExternalTable { .. } => {
                // There is no default plan for "CREATE EXTERNAL
                // TABLE" -- it must be handled at a higher level (so
                // that the appropriate table can be registered with
                // the context)
                Err(DataFusionError::Internal(
                    "Unsupported logical plan: CreateExternalTable".to_string(),
                ))
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
                    .cloned()
                    .collect::<Vec<_>>();

                // add in the physical plan if requested
                if *verbose {
                    stringified_plans.push(StringifiedPlan::new(
                        PlanType::PhysicalPlan,
                        format!("{:#?}", input),
                    ));
                }
                Ok(Arc::new(ExplainExec::new(
                    SchemaRef::new(schema.as_ref().to_owned().into()),
                    stringified_plans,
                )))
            }
            LogicalPlan::Extension { node } => {
                let inputs = node
                    .inputs()
                    .into_iter()
                    .map(|input_plan| self.create_physical_plan(input_plan, ctx_state))
                    .collect::<Result<Vec<_>>>()?;

                let plan = self.extension_planner.plan_extension(
                    node.as_ref(),
                    inputs,
                    ctx_state,
                )?;

                // Ensure the ExecutionPlan's  schema matches the
                // declared logical schema to catch and warn about
                // logic errors when creating user defined plans.
                if plan.schema() != node.schema().as_ref().to_owned().into() {
                    Err(DataFusionError::Plan(format!(
                        "Extension planner for {:?} created an ExecutionPlan with mismatched schema. \
                         LogicalPlan schema: {:?}, ExecutionPlan schema: {:?}",
                        node, node.schema(), plan.schema()
                    )))
                } else {
                    Ok(plan)
                }
            }
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
            Expr::ScalarVariable(variable_names) => {
                if &variable_names[0][0..2] == "@@" {
                    match ctx_state.var_provider.get(&VarType::System) {
                        Some(provider) => {
                            let scalar_value =
                                provider.get_value(variable_names.clone())?;
                            Ok(Arc::new(Literal::new(scalar_value)))
                        }
                        _ => Err(DataFusionError::Plan(
                            "No system variable provider found".to_string(),
                        )),
                    }
                } else {
                    match ctx_state.var_provider.get(&VarType::UserDefined) {
                        Some(provider) => {
                            let scalar_value =
                                provider.get_value(variable_names.clone())?;
                            Ok(Arc::new(Literal::new(scalar_value)))
                        }
                        _ => Err(DataFusionError::Plan(
                            "No user defined variable provider found".to_string(),
                        )),
                    }
                }
            }
            Expr::BinaryExpr { left, op, right } => {
                let lhs = self.create_physical_expr(left, input_schema, ctx_state)?;
                let rhs = self.create_physical_expr(right, input_schema, ctx_state)?;
                binary(lhs, op.clone(), rhs, input_schema)
            }
            Expr::Case {
                expr,
                when_then_expr,
                else_expr,
                ..
            } => {
                let expr: Option<Arc<dyn PhysicalExpr>> = if let Some(e) = expr {
                    Some(self.create_physical_expr(
                        e.as_ref(),
                        input_schema,
                        ctx_state,
                    )?)
                } else {
                    None
                };
                let when_expr = when_then_expr
                    .iter()
                    .map(|(w, _)| {
                        self.create_physical_expr(w.as_ref(), input_schema, ctx_state)
                    })
                    .collect::<Result<Vec<_>>>()?;
                let then_expr = when_then_expr
                    .iter()
                    .map(|(_, t)| {
                        self.create_physical_expr(t.as_ref(), input_schema, ctx_state)
                    })
                    .collect::<Result<Vec<_>>>()?;
                let when_then_expr: Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)> =
                    when_expr
                        .iter()
                        .zip(then_expr.iter())
                        .map(|(w, t)| (w.clone(), t.clone()))
                        .collect();
                let else_expr: Option<Arc<dyn PhysicalExpr>> = if let Some(e) = else_expr
                {
                    Some(self.create_physical_expr(
                        e.as_ref(),
                        input_schema,
                        ctx_state,
                    )?)
                } else {
                    None
                };
                Ok(Arc::new(CaseExpr::try_new(
                    expr,
                    &when_then_expr,
                    else_expr,
                )?))
            }
            Expr::Cast { expr, data_type } => expressions::cast(
                self.create_physical_expr(expr, input_schema, ctx_state)?,
                input_schema,
                data_type.clone(),
            ),
            Expr::Not(expr) => expressions::not(
                self.create_physical_expr(expr, input_schema, ctx_state)?,
                input_schema,
            ),
            Expr::Negative(expr) => expressions::negative(
                self.create_physical_expr(expr, input_schema, ctx_state)?,
                input_schema,
            ),
            Expr::IsNull(expr) => expressions::is_null(self.create_physical_expr(
                expr,
                input_schema,
                ctx_state,
            )?),
            Expr::IsNotNull(expr) => expressions::is_not_null(
                self.create_physical_expr(expr, input_schema, ctx_state)?,
            ),
            Expr::ScalarFunction { fun, args } => {
                let physical_args = args
                    .iter()
                    .map(|e| self.create_physical_expr(e, input_schema, ctx_state))
                    .collect::<Result<Vec<_>>>()?;
                functions::create_physical_expr(fun, &physical_args, input_schema)
            }
            Expr::ScalarUDF { fun, args } => {
                let mut physical_args = vec![];
                for e in args {
                    physical_args.push(self.create_physical_expr(
                        e,
                        input_schema,
                        ctx_state,
                    )?);
                }

                udf::create_physical_expr(
                    fun.clone().as_ref(),
                    &physical_args,
                    input_schema,
                )
            }
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                let value_expr =
                    self.create_physical_expr(expr, input_schema, ctx_state)?;
                let low_expr = self.create_physical_expr(low, input_schema, ctx_state)?;
                let high_expr =
                    self.create_physical_expr(high, input_schema, ctx_state)?;

                // rewrite the between into the two binary operators
                let binary_expr = binary(
                    binary(value_expr.clone(), Operator::GtEq, low_expr, input_schema)?,
                    Operator::And,
                    binary(value_expr.clone(), Operator::LtEq, high_expr, input_schema)?,
                    input_schema,
                );

                if *negated {
                    expressions::not(binary_expr?, input_schema)
                } else {
                    binary_expr
                }
            }
            other => Err(DataFusionError::NotImplemented(format!(
                "Physical plan does not support logical expression {:?}",
                other
            ))),
        }
    }

    /// Create an aggregate expression from a logical expression
    pub fn create_aggregate_expr(
        &self,
        e: &Expr,
        logical_input_schema: &DFSchema,
        physical_input_schema: &Schema,
        ctx_state: &ExecutionContextState,
    ) -> Result<Arc<dyn AggregateExpr>> {
        // unpack aliased logical expressions, e.g. "sum(col) as total"
        let (name, e) = match e {
            Expr::Alias(sub_expr, alias) => (alias.clone(), sub_expr.as_ref()),
            _ => (e.name(logical_input_schema)?, e),
        };

        match e {
            Expr::AggregateFunction {
                fun,
                distinct,
                args,
                ..
            } => {
                let args = args
                    .iter()
                    .map(|e| {
                        self.create_physical_expr(e, physical_input_schema, ctx_state)
                    })
                    .collect::<Result<Vec<_>>>()?;
                aggregates::create_aggregate_expr(
                    fun,
                    *distinct,
                    &args,
                    physical_input_schema,
                    name,
                )
            }
            Expr::AggregateUDF { fun, args, .. } => {
                let args = args
                    .iter()
                    .map(|e| {
                        self.create_physical_expr(e, physical_input_schema, ctx_state)
                    })
                    .collect::<Result<Vec<_>>>()?;

                udaf::create_aggregate_expr(fun, &args, physical_input_schema, name)
            }
            other => Err(DataFusionError::Internal(format!(
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
            options,
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

struct DefaultExtensionPlanner {}

impl ExtensionPlanner for DefaultExtensionPlanner {
    fn plan_extension(
        &self,
        node: &dyn UserDefinedLogicalNode,
        _inputs: Vec<Arc<dyn ExecutionPlan>>,
        _ctx_state: &ExecutionContextState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::NotImplemented(format!(
            "DefaultPhysicalPlanner does not know how to plan {:?}. \
                     Provide a custom ExtensionPlanNodePlanner that does",
            node
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical_plan::{DFField, DFSchema, DFSchemaRef};
    use crate::physical_plan::{csv::CsvReadOptions, expressions, Partitioning};
    use crate::prelude::ExecutionConfig;
    use crate::{
        logical_plan::{col, lit, sum, LogicalPlanBuilder},
        physical_plan::SendableRecordBatchStream,
    };
    use arrow::datatypes::{DataType, Field, SchemaRef};
    use async_trait::async_trait;
    use fmt::Debug;
    use std::{any::Any, collections::HashMap, fmt};

    fn make_ctx_state() -> ExecutionContextState {
        ExecutionContextState {
            datasources: HashMap::new(),
            scalar_functions: HashMap::new(),
            var_provider: HashMap::new(),
            aggregate_functions: HashMap::new(),
            config: ExecutionConfig::new(),
        }
    }

    fn plan(logical_plan: &LogicalPlan) -> Result<Arc<dyn ExecutionPlan>> {
        let ctx_state = make_ctx_state();
        let planner = DefaultPhysicalPlanner::default();
        planner.create_physical_plan(logical_plan, &ctx_state)
    }

    #[test]
    fn test_all_operators() -> Result<()> {
        let testdata = arrow::util::test_util::arrow_test_data();
        let path = format!("{}/csv/aggregate_test_100.csv", testdata);

        let options = CsvReadOptions::new().schema_infer_max_records(100);
        let logical_plan = LogicalPlanBuilder::scan_csv(&path, options, None)?
            // filter clause needs the type coercion rule applied
            .filter(col("c7").lt(lit(5_u8)))?
            .project(vec![col("c1"), col("c2")])?
            .aggregate(vec![col("c1")], vec![sum(col("c2"))])?
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
    fn test_create_not() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Boolean, true)]);

        let planner = DefaultPhysicalPlanner::default();

        let expr =
            planner.create_physical_expr(&col("a").not(), &schema, &make_ctx_state())?;
        let expected = expressions::not(expressions::col("a"), &schema)?;

        assert_eq!(format!("{:?}", expr), format!("{:?}", expected));

        Ok(())
    }

    #[test]
    fn test_with_csv_plan() -> Result<()> {
        let testdata = arrow::util::test_util::arrow_test_data();
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
        let testdata = arrow::util::test_util::arrow_test_data();
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
            let message = format!(
                "Expression {:?} expected to error due to impossible coercion",
                case
            );
            assert!(logical_plan.is_err(), message);
        }
        Ok(())
    }

    #[test]
    fn default_extension_planner() -> Result<()> {
        let ctx_state = make_ctx_state();
        let planner = DefaultPhysicalPlanner::default();
        let logical_plan = LogicalPlan::Extension {
            node: Arc::new(NoOpExtensionNode::default()),
        };
        let plan = planner.create_physical_plan(&logical_plan, &ctx_state);

        let expected_error = "DefaultPhysicalPlanner does not know how to plan NoOp";
        match plan {
            Ok(_) => panic!("Expected planning failure"),
            Err(e) => assert!(
                e.to_string().contains(expected_error),
                "Error '{}' did not contain expected error '{}'",
                e.to_string(),
                expected_error
            ),
        }
        Ok(())
    }

    #[test]
    fn bad_extension_planner() -> Result<()> {
        // Test that creating an execution plan whose schema doesn't
        // match the logical plan's schema generates an error.
        let ctx_state = make_ctx_state();
        let planner = DefaultPhysicalPlanner::with_extension_planner(Arc::new(
            BadExtensionPlanner {},
        ));

        let logical_plan = LogicalPlan::Extension {
            node: Arc::new(NoOpExtensionNode::default()),
        };
        let plan = planner.create_physical_plan(&logical_plan, &ctx_state);

        let expected_error: &str = "Error during planning: \
        Extension planner for NoOp created an ExecutionPlan with mismatched schema. \
        LogicalPlan schema: DFSchema { fields: [\
            DFField { qualifier: None, field: Field { \
                name: \"a\", \
                data_type: Int32, \
                nullable: false, \
                dict_id: 0, \
                dict_is_ordered: false } }\
        ] }, \
        ExecutionPlan schema: Schema { fields: [\
            Field { \
                name: \"b\", \
                data_type: Int32, \
                nullable: false, \
                dict_id: 0, \
                dict_is_ordered: false }\
        ], metadata: {} }";
        match plan {
            Ok(_) => panic!("Expected planning failure"),
            Err(e) => assert!(
                e.to_string().contains(expected_error),
                "Error '{}' did not contain expected error '{}'",
                e.to_string(),
                expected_error
            ),
        }
        Ok(())
    }

    /// An example extension node that doesn't do anything
    struct NoOpExtensionNode {
        schema: DFSchemaRef,
    }

    impl Default for NoOpExtensionNode {
        fn default() -> Self {
            Self {
                schema: DFSchemaRef::new(
                    DFSchema::new(vec![DFField::new(None, "a", DataType::Int32, false)])
                        .unwrap(),
                ),
            }
        }
    }

    impl Debug for NoOpExtensionNode {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "NoOp")
        }
    }

    impl UserDefinedLogicalNode for NoOpExtensionNode {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn inputs(&self) -> Vec<&LogicalPlan> {
            vec![]
        }

        fn schema(&self) -> &DFSchemaRef {
            &self.schema
        }

        fn expressions(&self) -> Vec<Expr> {
            vec![]
        }

        fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "NoOp")
        }

        fn from_template(
            &self,
            _exprs: &Vec<Expr>,
            _inputs: &Vec<LogicalPlan>,
        ) -> Arc<dyn UserDefinedLogicalNode + Send + Sync> {
            unimplemented!("NoOp");
        }
    }

    #[derive(Debug)]
    struct NoOpExecutionPlan {
        schema: SchemaRef,
    }

    #[async_trait]
    impl ExecutionPlan for NoOpExecutionPlan {
        /// Return a reference to Any that can be used for downcasting
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn schema(&self) -> SchemaRef {
            self.schema.clone()
        }

        fn output_partitioning(&self) -> Partitioning {
            Partitioning::UnknownPartitioning(1)
        }

        fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
            vec![]
        }

        fn with_new_children(
            &self,
            _children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            unimplemented!("NoOpExecutionPlan::with_new_children");
        }

        async fn execute(&self, _partition: usize) -> Result<SendableRecordBatchStream> {
            unimplemented!("NoOpExecutionPlan::execute");
        }
    }

    //  Produces an execution plan where the schema is mismatched from
    //  the logical plan node.
    struct BadExtensionPlanner {}

    impl ExtensionPlanner for BadExtensionPlanner {
        /// Create a physical plan for an extension node
        fn plan_extension(
            &self,
            _node: &dyn UserDefinedLogicalNode,
            _inputs: Vec<Arc<dyn ExecutionPlan>>,
            _ctx_state: &ExecutionContextState,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            Ok(Arc::new(NoOpExecutionPlan {
                schema: SchemaRef::new(Schema::new(vec![Field::new(
                    "b",
                    DataType::Int32,
                    false,
                )])),
            }))
        }
    }
}
