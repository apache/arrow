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

use arrow::datatypes::{Schema, SchemaRef};

use crate::datasource::csv::{CsvFile, CsvReadOptions};
use crate::datasource::parquet::ParquetTable;
use crate::datasource::TableProvider;
use crate::error::{DataFusionError, Result};

use super::{
    col, exprlist_to_fields, Expr, JoinType, LogicalPlan, PlanType, StringifiedPlan,
    TableSource,
};
use crate::physical_plan::hash_utils;

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
            schema: SchemaRef::new(Schema::empty()),
        })
    }

    /// Scan a CSV data source
    pub fn scan_csv(
        path: &str,
        options: CsvReadOptions,
        projection: Option<Vec<usize>>,
    ) -> Result<Self> {
        let has_header = options.has_header;
        let delimiter = options.delimiter;
        let schema: Schema = match options.schema {
            Some(s) => s.to_owned(),
            None => CsvFile::try_new(path, options)?
                .schema()
                .as_ref()
                .to_owned(),
        };

        let projected_schema = SchemaRef::new(
            projection
                .clone()
                .map(|p| {
                    Schema::new(p.iter().map(|i| schema.field(*i).clone()).collect())
                })
                .or(Some(schema.clone()))
                .unwrap(),
        );

        Ok(Self::from(&LogicalPlan::CsvScan {
            path: path.to_owned(),
            schema: SchemaRef::new(schema),
            has_header,
            delimiter: Some(delimiter),
            projection,
            projected_schema,
        }))
    }

    /// Scan a Parquet data source
    pub fn scan_parquet(path: &str, projection: Option<Vec<usize>>) -> Result<Self> {
        let p = ParquetTable::try_new(path)?;
        let schema = p.schema();

        let projected_schema = projection
            .clone()
            .map(|p| Schema::new(p.iter().map(|i| schema.field(*i).clone()).collect()));
        let projected_schema =
            projected_schema.map_or(schema.clone(), |s| SchemaRef::new(s));

        Ok(Self::from(&LogicalPlan::ParquetScan {
            path: path.to_owned(),
            schema,
            projection,
            projected_schema,
        }))
    }

    /// Scan a data source
    pub fn scan(
        schema_name: &str,
        table_name: &str,
        table_schema: &Schema,
        projection: Option<Vec<usize>>,
    ) -> Result<Self> {
        let table_schema = SchemaRef::new(table_schema.clone());
        let projected_schema = projection.clone().map(|p| {
            Schema::new(p.iter().map(|i| table_schema.field(*i).clone()).collect())
        });
        let projected_schema =
            projected_schema.map_or(table_schema.clone(), |s| SchemaRef::new(s));

        Ok(Self::from(&LogicalPlan::TableScan {
            schema_name: schema_name.to_owned(),
            source: TableSource::FromContext(table_name.to_owned()),
            table_schema,
            projected_schema,
            projection,
        }))
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

        let schema = Schema::new(exprlist_to_fields(&projected_expr, input_schema)?);

        Ok(Self::from(&LogicalPlan::Projection {
            expr: projected_expr,
            input: Arc::new(self.plan.clone()),
            schema: SchemaRef::new(schema),
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
            let physical_join_type = match join_type {
                JoinType::Inner => hash_utils::JoinType::Inner,
            };
            let physical_schema = hash_utils::build_join_schema(
                self.plan.schema(),
                right.schema(),
                &on,
                &physical_join_type,
            );
            Ok(Self::from(&LogicalPlan::Join {
                left: Arc::new(self.plan.clone()),
                right: Arc::new(right.clone()),
                on: left_keys
                    .iter()
                    .zip(right_keys.iter())
                    .map(|(l, r)| (l.to_string(), r.to_string()))
                    .collect(),
                join_type,
                schema: Arc::new(physical_schema),
            }))
        }
    }

    /// Apply an aggregate
    pub fn aggregate(&self, group_expr: Vec<Expr>, aggr_expr: Vec<Expr>) -> Result<Self> {
        let mut all_expr: Vec<Expr> = group_expr.clone();
        aggr_expr.iter().for_each(|x| all_expr.push(x.clone()));

        validate_unique_names("Aggregations", &all_expr, self.plan.schema())?;

        let aggr_schema = Schema::new(exprlist_to_fields(&all_expr, self.plan.schema())?);

        Ok(Self::from(&LogicalPlan::Aggregate {
            input: Arc::new(self.plan.clone()),
            group_expr,
            aggr_expr,
            schema: SchemaRef::new(aggr_schema),
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
            schema,
        }))
    }

    /// Build the plan
    pub fn build(&self) -> Result<LogicalPlan> {
        Ok(self.plan.clone())
    }
}

/// Errors if one or more expressions have equal names.
fn validate_unique_names(
    node_name: &str,
    expressions: &[Expr],
    input_schema: &Schema,
) -> Result<()> {
    let mut unique_names = HashMap::new();
    expressions.iter().enumerate().map(|(position, expr)| {
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
    }).collect::<Result<()>>()
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field};

    use super::super::{lit, sum};
    use super::*;

    #[test]
    fn plan_builder_simple() -> Result<()> {
        let plan = LogicalPlanBuilder::scan(
            "default",
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
    fn plan_builder_csv() -> Result<()> {
        let plan = LogicalPlanBuilder::scan_csv(
            "employee.csv",
            CsvReadOptions::new().schema(&employee_schema()),
            Some(vec![0, 3]),
        )?
        .filter(col("state").eq(lit("CO")))?
        .project(vec![col("id")])?
        .build()?;

        let expected = "Projection: #id\
        \n  Filter: #state Eq Utf8(\"CO\")\
        \n    CsvScan: employee.csv projection=Some([0, 3])";

        assert_eq!(expected, format!("{:?}", plan));

        Ok(())
    }

    #[test]
    fn plan_builder_aggregate() -> Result<()> {
        let plan = LogicalPlanBuilder::scan(
            "default",
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
        let plan = LogicalPlanBuilder::scan(
            "default",
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
        let plan = LogicalPlanBuilder::scan(
            "default",
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
        let plan = LogicalPlanBuilder::scan(
            "default",
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
