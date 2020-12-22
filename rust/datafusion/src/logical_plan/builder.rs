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

//! This module provides a builder for creating LogicalPlans

use std::{collections::HashMap, sync::Arc};

use arrow::{
    datatypes::{Schema, SchemaRef},
    record_batch::RecordBatch,
};

use crate::datasource::TableProvider;
use crate::error::{DataFusionError, Result};
use crate::{
    datasource::{empty::EmptyTable, parquet::ParquetTable, CsvFile, MemTable},
    prelude::CsvReadOptions,
};

use super::dfschema::ToDFSchema;
use super::{
    col, exprlist_to_fields, Expr, JoinType, LogicalPlan, PlanType, StringifiedPlan,
};
use crate::logical_plan::{DFField, DFSchema, DFSchemaRef, Partitioning};
use std::collections::HashSet;

/// Builder for logical plans
pub struct LogicalPlanBuilder {
    plan: LogicalPlan,
}

impl LogicalPlanBuilder {
    /// Create a builder from an existing plan
    pub fn from(plan: &LogicalPlan) -> Self {
        Self { plan: plan.clone() }
    }

    /// Create an empty relation.
    ///
    /// `produce_one_row` set to true means this empty node needs to produce a placeholder row.
    pub fn empty(produce_one_row: bool) -> Self {
        Self::from(&LogicalPlan::EmptyRelation {
            produce_one_row,
            schema: DFSchemaRef::new(DFSchema::empty()),
        })
    }

    /// Scan a memory data source
    pub fn scan_memory(
        partitions: Vec<Vec<RecordBatch>>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
    ) -> Result<Self> {
        let provider = Arc::new(MemTable::try_new(schema, partitions)?);
        Self::scan("", provider, projection)
    }

    /// Scan a CSV data source
    pub fn scan_csv(
        path: &str,
        options: CsvReadOptions,
        projection: Option<Vec<usize>>,
    ) -> Result<Self> {
        let provider = Arc::new(CsvFile::try_new(path, options)?);
        Self::scan("", provider, projection)
    }

    /// Scan a Parquet data source
    pub fn scan_parquet(path: &str, projection: Option<Vec<usize>>) -> Result<Self> {
        let provider = Arc::new(ParquetTable::try_new(path)?);
        Self::scan("", provider, projection)
    }

    /// Scan an empty data source, mainly used in tests
    pub fn scan_empty(
        name: &str,
        table_schema: &Schema,
        projection: Option<Vec<usize>>,
    ) -> Result<Self> {
        let table_schema = Arc::new(table_schema.clone());
        let provider = Arc::new(EmptyTable::new(table_schema));
        Self::scan(name, provider, projection)
    }

    /// Convert a table provider into a builder with a TableScan
    pub fn scan(
        name: &str,
        provider: Arc<dyn TableProvider + Send + Sync>,
        projection: Option<Vec<usize>>,
    ) -> Result<Self> {
        let schema = provider.schema();

        let projected_schema = projection
            .as_ref()
            .map(|p| Schema::new(p.iter().map(|i| schema.field(*i).clone()).collect()))
            .map_or(schema, SchemaRef::new)
            .to_dfschema_ref()?;

        let table_scan = LogicalPlan::TableScan {
            table_name: name.to_string(),
            source: provider,
            projected_schema,
            projection,
            filters: vec![],
        };

        Ok(Self::from(&table_scan))
    }

    /// Apply a projection.
    ///
    /// # Errors
    /// This function errors under any of the following conditions:
    /// * Two or more expressions have the same name
    /// * An invalid expression is used (e.g. a `sort` expression)
    pub fn project(&self, expr: Vec<Expr>) -> Result<Self> {
        let input_schema = self.plan.schema();
        let mut projected_expr = vec![];
        (0..expr.len()).for_each(|i| match &expr[i] {
            Expr::Wildcard => {
                (0..input_schema.fields().len())
                    .for_each(|i| projected_expr.push(col(input_schema.field(i).name())));
            }
            _ => projected_expr.push(expr[i].clone()),
        });

        validate_unique_names("Projections", &projected_expr, input_schema)?;

        let schema = DFSchema::new(exprlist_to_fields(&projected_expr, input_schema)?)?;

        Ok(Self::from(&LogicalPlan::Projection {
            expr: projected_expr,
            input: Arc::new(self.plan.clone()),
            schema: DFSchemaRef::new(schema),
        }))
    }

    /// Apply a filter
    pub fn filter(&self, expr: Expr) -> Result<Self> {
        Ok(Self::from(&LogicalPlan::Filter {
            predicate: expr,
            input: Arc::new(self.plan.clone()),
        }))
    }

    /// Apply a limit
    pub fn limit(&self, n: usize) -> Result<Self> {
        Ok(Self::from(&LogicalPlan::Limit {
            n,
            input: Arc::new(self.plan.clone()),
        }))
    }

    /// Apply a sort
    pub fn sort(&self, expr: Vec<Expr>) -> Result<Self> {
        Ok(Self::from(&LogicalPlan::Sort {
            expr,
            input: Arc::new(self.plan.clone()),
        }))
    }

    /// Apply a join
    pub fn join(
        &self,
        right: &LogicalPlan,
        join_type: JoinType,
        left_keys: &[&str],
        right_keys: &[&str],
    ) -> Result<Self> {
        if left_keys.len() != right_keys.len() {
            Err(DataFusionError::Plan(
                "left_keys and right_keys were not the same length".to_string(),
            ))
        } else {
            let on: Vec<_> = left_keys
                .iter()
                .zip(right_keys.iter())
                .map(|(x, y)| (x.to_string(), y.to_string()))
                .collect::<Vec<_>>();
            let join_schema =
                build_join_schema(self.plan.schema(), right.schema(), &on, &join_type)?;
            Ok(Self::from(&LogicalPlan::Join {
                left: Arc::new(self.plan.clone()),
                right: Arc::new(right.clone()),
                on: left_keys
                    .iter()
                    .zip(right_keys.iter())
                    .map(|(l, r)| (l.to_string(), r.to_string()))
                    .collect(),
                join_type,
                schema: DFSchemaRef::new(join_schema),
            }))
        }
    }

    /// Repartition
    pub fn repartition(&self, partitioning_scheme: Partitioning) -> Result<Self> {
        Ok(Self::from(&LogicalPlan::Repartition {
            input: Arc::new(self.plan.clone()),
            partitioning_scheme,
        }))
    }

    /// Apply an aggregate
    pub fn aggregate(&self, group_expr: Vec<Expr>, aggr_expr: Vec<Expr>) -> Result<Self> {
        let mut all_expr: Vec<Expr> = group_expr.clone();
        aggr_expr.iter().for_each(|x| all_expr.push(x.clone()));

        validate_unique_names("Aggregations", &all_expr, self.plan.schema())?;

        let aggr_schema =
            DFSchema::new(exprlist_to_fields(&all_expr, self.plan.schema())?)?;

        Ok(Self::from(&LogicalPlan::Aggregate {
            input: Arc::new(self.plan.clone()),
            group_expr,
            aggr_expr,
            schema: DFSchemaRef::new(aggr_schema),
        }))
    }

    /// Create an expression to represent the explanation of the plan
    pub fn explain(&self, verbose: bool) -> Result<Self> {
        let stringified_plans = vec![StringifiedPlan::new(
            PlanType::LogicalPlan,
            format!("{:#?}", self.plan.clone()),
        )];

        let schema = LogicalPlan::explain_schema();

        Ok(Self::from(&LogicalPlan::Explain {
            verbose,
            plan: Arc::new(self.plan.clone()),
            stringified_plans,
            schema: schema.to_dfschema_ref()?,
        }))
    }

    /// Build the plan
    pub fn build(&self) -> Result<LogicalPlan> {
        Ok(self.plan.clone())
    }
}

/// Creates a schema for a join operation.
/// The fields from the left side are first
fn build_join_schema(
    left: &DFSchema,
    right: &DFSchema,
    on: &[(String, String)],
    join_type: &JoinType,
) -> Result<DFSchema> {
    let fields: Vec<DFField> = match join_type {
        JoinType::Inner | JoinType::Left => {
            // remove right-side join keys if they have the same names as the left-side
            let duplicate_keys = &on
                .iter()
                .filter(|(l, r)| l == r)
                .map(|on| on.1.to_string())
                .collect::<HashSet<_>>();

            let left_fields = left.fields().iter();

            let right_fields = right
                .fields()
                .iter()
                .filter(|f| !duplicate_keys.contains(f.name()));

            // left then right
            left_fields.chain(right_fields).cloned().collect()
        }
        JoinType::Right => {
            // remove left-side join keys if they have the same names as the right-side
            let duplicate_keys = &on
                .iter()
                .filter(|(l, r)| l == r)
                .map(|on| on.1.to_string())
                .collect::<HashSet<_>>();

            let left_fields = left
                .fields()
                .iter()
                .filter(|f| !duplicate_keys.contains(f.name()));

            let right_fields = right.fields().iter();

            // left then right
            left_fields.chain(right_fields).cloned().collect()
        }
    };
    DFSchema::new(fields)
}

/// Errors if one or more expressions have equal names.
fn validate_unique_names(
    node_name: &str,
    expressions: &[Expr],
    input_schema: &DFSchema,
) -> Result<()> {
    let mut unique_names = HashMap::new();
    expressions.iter().enumerate().try_for_each(|(position, expr)| {
        let name = expr.name(input_schema)?;
        match unique_names.get(&name) {
            None => {
                unique_names.insert(name, (position, expr));
                Ok(())
            },
            Some((existing_position, existing_expr)) => {
                Err(DataFusionError::Plan(
                    format!("{} require unique expression names \
                             but the expression \"{:?}\" at position {} and \"{:?}\" \
                             at position {} have the same name. Consider aliasing (\"AS\") one of them.",
                             node_name, existing_expr, existing_position, expr, position,
                            )
                ))
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field};

    use super::super::{lit, sum};
    use super::*;

    #[test]
    fn plan_builder_simple() -> Result<()> {
        let plan = LogicalPlanBuilder::scan_empty(
            "employee.csv",
            &employee_schema(),
            Some(vec![0, 3]),
        )?
        .filter(col("state").eq(lit("CO")))?
        .project(vec![col("id")])?
        .build()?;

        let expected = "Projection: #id\
        \n  Filter: #state Eq Utf8(\"CO\")\
        \n    TableScan: employee.csv projection=Some([0, 3])";

        assert_eq!(expected, format!("{:?}", plan));

        Ok(())
    }

    #[test]
    fn plan_builder_aggregate() -> Result<()> {
        let plan = LogicalPlanBuilder::scan_empty(
            "employee.csv",
            &employee_schema(),
            Some(vec![3, 4]),
        )?
        .aggregate(
            vec![col("state")],
            vec![sum(col("salary")).alias("total_salary")],
        )?
        .project(vec![col("state"), col("total_salary")])?
        .build()?;

        let expected = "Projection: #state, #total_salary\
        \n  Aggregate: groupBy=[[#state]], aggr=[[SUM(#salary) AS total_salary]]\
        \n    TableScan: employee.csv projection=Some([3, 4])";

        assert_eq!(expected, format!("{:?}", plan));

        Ok(())
    }

    #[test]
    fn plan_builder_sort() -> Result<()> {
        let plan = LogicalPlanBuilder::scan_empty(
            "employee.csv",
            &employee_schema(),
            Some(vec![3, 4]),
        )?
        .sort(vec![
            Expr::Sort {
                expr: Box::new(col("state")),
                asc: true,
                nulls_first: true,
            },
            Expr::Sort {
                expr: Box::new(col("total_salary")),
                asc: false,
                nulls_first: false,
            },
        ])?
        .build()?;

        let expected = "Sort: #state ASC NULLS FIRST, #total_salary DESC NULLS LAST\
        \n  TableScan: employee.csv projection=Some([3, 4])";

        assert_eq!(expected, format!("{:?}", plan));

        Ok(())
    }

    #[test]
    fn projection_non_unique_names() -> Result<()> {
        let plan = LogicalPlanBuilder::scan_empty(
            "employee.csv",
            &employee_schema(),
            Some(vec![0, 3]),
        )?
        // two columns with the same name => error
        .project(vec![col("id"), col("first_name").alias("id")]);

        match plan {
            Err(DataFusionError::Plan(e)) => {
                assert_eq!(e, "Projections require unique expression names \
                    but the expression \"#id\" at position 0 and \"#first_name AS id\" at \
                    position 1 have the same name. Consider aliasing (\"AS\") one of them.");
                Ok(())
            }
            _ => Err(DataFusionError::Plan(
                "Plan should have returned an DataFusionError::Plan".to_string(),
            )),
        }
    }

    #[test]
    fn aggregate_non_unique_names() -> Result<()> {
        let plan = LogicalPlanBuilder::scan_empty(
            "employee.csv",
            &employee_schema(),
            Some(vec![0, 3]),
        )?
        // two columns with the same name => error
        .aggregate(vec![col("state")], vec![sum(col("salary")).alias("state")]);

        match plan {
            Err(DataFusionError::Plan(e)) => {
                assert_eq!(e, "Aggregations require unique expression names \
                    but the expression \"#state\" at position 0 and \"SUM(#salary) AS state\" at \
                    position 1 have the same name. Consider aliasing (\"AS\") one of them.");
                Ok(())
            }
            _ => Err(DataFusionError::Plan(
                "Plan should have returned an DataFusionError::Plan".to_string(),
            )),
        }
    }

    fn employee_schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("first_name", DataType::Utf8, false),
            Field::new("last_name", DataType::Utf8, false),
            Field::new("state", DataType::Utf8, false),
            Field::new("salary", DataType::Int32, false),
        ])
    }

    #[test]
    fn stringified_plan() -> Result<()> {
        let stringified_plan =
            StringifiedPlan::new(PlanType::LogicalPlan, "...the plan...");
        assert!(stringified_plan.should_display(true));
        assert!(stringified_plan.should_display(false)); // display in non verbose mode too

        let stringified_plan =
            StringifiedPlan::new(PlanType::PhysicalPlan, "...the plan...");
        assert!(stringified_plan.should_display(true));
        assert!(!stringified_plan.should_display(false));

        let stringified_plan = StringifiedPlan::new(
            PlanType::OptimizedLogicalPlan {
                optimizer_name: "random opt pass".into(),
            },
            "...the plan...",
        );
        assert!(stringified_plan.should_display(true));
        assert!(!stringified_plan.should_display(false));

        Ok(())
    }
}
