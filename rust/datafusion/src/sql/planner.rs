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

//! SQL Query Planner (produces logical plan from SQL AST)

use std::convert::TryInto;
use std::str::FromStr;
use std::sync::Arc;

use crate::catalog::TableReference;
use crate::datasource::TableProvider;
use crate::logical_plan::Expr::Alias;
use crate::logical_plan::{
    and, lit, DFSchema, Expr, LogicalPlan, LogicalPlanBuilder, Operator, PlanType,
    StringifiedPlan, ToDFSchema,
};
use crate::scalar::ScalarValue;
use crate::{
    error::{DataFusionError, Result},
    physical_plan::udaf::AggregateUDF,
};
use crate::{
    physical_plan::udf::ScalarUDF,
    physical_plan::{aggregates, functions},
    sql::parser::{CreateExternalTable, FileType, Statement as DFStatement},
};

use arrow::datatypes::*;
use hashbrown::HashMap;

use crate::prelude::JoinType;
use sqlparser::ast::{
    BinaryOperator, DataType as SQLDataType, DateTimeField, Expr as SQLExpr, FunctionArg,
    Ident, Join, JoinConstraint, JoinOperator, ObjectName, Query, Select, SelectItem,
    SetExpr, SetOperator, ShowStatementFilter, TableFactor, TableWithJoins,
    UnaryOperator, Value,
};
use sqlparser::ast::{ColumnDef as SQLColumnDef, ColumnOption};
use sqlparser::ast::{OrderByExpr, Statement};
use sqlparser::parser::ParserError::ParserError;

use super::{
    parser::DFParser,
    utils::{
        can_columns_satisfy_exprs, expand_wildcard, expr_as_column_expr, extract_aliases,
        find_aggregate_exprs, find_column_exprs, rebase_expr, resolve_aliases_to_exprs,
    },
};

/// The ContextProvider trait allows the query planner to obtain meta-data about tables and
/// functions referenced in SQL statements
pub trait ContextProvider {
    /// Getter for a datasource
    fn get_table_provider(&self, name: TableReference) -> Option<Arc<dyn TableProvider>>;
    /// Getter for a UDF description
    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>>;
    /// Getter for a UDAF description
    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>>;
}

/// SQL query planner
pub struct SqlToRel<'a, S: ContextProvider> {
    schema_provider: &'a S,
}

impl<'a, S: ContextProvider> SqlToRel<'a, S> {
    /// Create a new query planner
    pub fn new(schema_provider: &'a S) -> Self {
        SqlToRel { schema_provider }
    }

    /// Generate a logical plan from an DataFusion SQL statement
    pub fn statement_to_plan(&self, statement: &DFStatement) -> Result<LogicalPlan> {
        match statement {
            DFStatement::CreateExternalTable(s) => self.external_table_to_plan(&s),
            DFStatement::Statement(s) => self.sql_statement_to_plan(&s),
        }
    }

    /// Generate a logical plan from an SQL statement
    pub fn sql_statement_to_plan(&self, sql: &Statement) -> Result<LogicalPlan> {
        match sql {
            Statement::Explain {
                verbose,
                statement,
                analyze: _,
            } => self.explain_statement_to_plan(*verbose, &statement),
            Statement::Query(query) => self.query_to_plan(&query),
            Statement::ShowVariable { variable } => self.show_variable_to_plan(&variable),
            Statement::ShowColumns {
                extended,
                full,
                table_name,
                filter,
            } => self.show_columns_to_plan(*extended, *full, table_name, filter.as_ref()),
            _ => Err(DataFusionError::NotImplemented(
                "Only SELECT statements are implemented".to_string(),
            )),
        }
    }

    /// Generate a logic plan from an SQL query
    pub fn query_to_plan(&self, query: &Query) -> Result<LogicalPlan> {
        self.query_to_plan_with_alias(query, None, &mut HashMap::new())
    }

    /// Generate a logic plan from an SQL query with optional alias
    pub fn query_to_plan_with_alias(
        &self,
        query: &Query,
        alias: Option<String>,
        ctes: &mut HashMap<String, LogicalPlan>,
    ) -> Result<LogicalPlan> {
        let set_expr = &query.body;
        if let Some(with) = &query.with {
            // Process CTEs from top to bottom
            // do not allow self-references
            for cte in &with.cte_tables {
                // create logical plan & pass backreferencing CTEs
                let logical_plan = self.query_to_plan_with_alias(
                    &cte.query,
                    Some(cte.alias.name.value.clone()),
                    &mut ctes.clone(),
                )?;
                ctes.insert(cte.alias.name.value.clone(), logical_plan);
            }
        }
        let plan = self.set_expr_to_plan(set_expr, alias, ctes)?;

        let plan = self.order_by(&plan, &query.order_by)?;

        self.limit(&plan, &query.limit)
    }

    fn set_expr_to_plan(
        &self,
        set_expr: &SetExpr,
        alias: Option<String>,
        ctes: &mut HashMap<String, LogicalPlan>,
    ) -> Result<LogicalPlan> {
        match set_expr {
            SetExpr::Select(s) => self.select_to_plan(s.as_ref(), ctes),
            SetExpr::SetOperation {
                op,
                left,
                right,
                all,
            } => match (op, all) {
                (SetOperator::Union, true) => {
                    let left_plan = self.set_expr_to_plan(left.as_ref(), None, ctes)?;
                    let right_plan = self.set_expr_to_plan(right.as_ref(), None, ctes)?;
                    let inputs = vec![left_plan, right_plan]
                        .into_iter()
                        .flat_map(|p| match p {
                            LogicalPlan::Union { inputs, .. } => inputs,
                            x => vec![x],
                        })
                        .collect::<Vec<_>>();
                    if inputs.is_empty() {
                        return Err(DataFusionError::Plan(format!(
                            "Empty UNION: {}",
                            set_expr
                        )));
                    }
                    if !inputs.iter().all(|s| s.schema() == inputs[0].schema()) {
                        return Err(DataFusionError::Plan(
                            "UNION ALL schemas are expected to be the same".to_string(),
                        ));
                    }
                    Ok(LogicalPlan::Union {
                        schema: inputs[0].schema().clone(),
                        inputs,
                        alias,
                    })
                }
                _ => Err(DataFusionError::NotImplemented(format!(
                    "Only UNION ALL is supported, found {}",
                    op
                ))),
            },
            _ => Err(DataFusionError::NotImplemented(format!(
                "Query {} not implemented yet",
                set_expr
            ))),
        }
    }

    /// Generate a logical plan from a CREATE EXTERNAL TABLE statement
    pub fn external_table_to_plan(
        &self,
        statement: &CreateExternalTable,
    ) -> Result<LogicalPlan> {
        let CreateExternalTable {
            name,
            columns,
            file_type,
            has_header,
            location,
        } = statement;

        // semantic checks
        match *file_type {
            FileType::CSV => {
                if columns.is_empty() {
                    return Err(DataFusionError::Plan(
                        "Column definitions required for CSV files. None found".into(),
                    ));
                }
            }
            FileType::Parquet => {
                if !columns.is_empty() {
                    return Err(DataFusionError::Plan(
                        "Column definitions can not be specified for PARQUET files."
                            .into(),
                    ));
                }
            }
            FileType::NdJson => {}
        };

        let schema = self.build_schema(&columns)?;

        Ok(LogicalPlan::CreateExternalTable {
            schema: schema.to_dfschema_ref()?,
            name: name.clone(),
            location: location.clone(),
            file_type: *file_type,
            has_header: *has_header,
        })
    }

    /// Generate a plan for EXPLAIN ... that will print out a plan
    ///
    pub fn explain_statement_to_plan(
        &self,
        verbose: bool,
        statement: &Statement,
    ) -> Result<LogicalPlan> {
        let plan = self.sql_statement_to_plan(&statement)?;

        let stringified_plans = vec![StringifiedPlan::new(
            PlanType::LogicalPlan,
            format!("{:#?}", plan),
        )];

        let schema = LogicalPlan::explain_schema();
        let plan = Arc::new(plan);

        Ok(LogicalPlan::Explain {
            verbose,
            plan,
            stringified_plans,
            schema: schema.to_dfschema_ref()?,
        })
    }

    fn build_schema(&self, columns: &[SQLColumnDef]) -> Result<Schema> {
        let mut fields = Vec::new();

        for column in columns {
            let data_type = self.make_data_type(&column.data_type)?;
            let allow_null = column
                .options
                .iter()
                .any(|x| x.option == ColumnOption::Null);
            fields.push(Field::new(&column.name.value, data_type, allow_null));
        }

        Ok(Schema::new(fields))
    }

    /// Maps the SQL type to the corresponding Arrow `DataType`
    fn make_data_type(&self, sql_type: &SQLDataType) -> Result<DataType> {
        match sql_type {
            SQLDataType::BigInt => Ok(DataType::Int64),
            SQLDataType::Int => Ok(DataType::Int32),
            SQLDataType::SmallInt => Ok(DataType::Int16),
            SQLDataType::Char(_) | SQLDataType::Varchar(_) | SQLDataType::Text => {
                Ok(DataType::Utf8)
            }
            SQLDataType::Decimal(_, _) => Ok(DataType::Float64),
            SQLDataType::Float(_) => Ok(DataType::Float32),
            SQLDataType::Real | SQLDataType::Double => Ok(DataType::Float64),
            SQLDataType::Boolean => Ok(DataType::Boolean),
            SQLDataType::Date => Ok(DataType::Date32),
            SQLDataType::Time => Ok(DataType::Time64(TimeUnit::Millisecond)),
            SQLDataType::Timestamp => Ok(DataType::Timestamp(TimeUnit::Nanosecond, None)),
            _ => Err(DataFusionError::NotImplemented(format!(
                "The SQL data type {:?} is not implemented",
                sql_type
            ))),
        }
    }

    fn plan_from_tables(
        &self,
        from: &[TableWithJoins],
        ctes: &mut HashMap<String, LogicalPlan>,
    ) -> Result<Vec<LogicalPlan>> {
        match from.len() {
            0 => Ok(vec![LogicalPlanBuilder::empty(true).build()?]),
            _ => from
                .iter()
                .map(|t| self.plan_table_with_joins(t, ctes))
                .collect::<Result<Vec<_>>>(),
        }
    }

    fn plan_table_with_joins(
        &self,
        t: &TableWithJoins,
        ctes: &mut HashMap<String, LogicalPlan>,
    ) -> Result<LogicalPlan> {
        let left = self.create_relation(&t.relation, ctes)?;
        match t.joins.len() {
            0 => Ok(left),
            n => {
                let mut left = self.parse_relation_join(&left, &t.joins[0], ctes)?;
                for i in 1..n {
                    left = self.parse_relation_join(&left, &t.joins[i], ctes)?;
                }
                Ok(left)
            }
        }
    }

    fn parse_relation_join(
        &self,
        left: &LogicalPlan,
        join: &Join,
        ctes: &mut HashMap<String, LogicalPlan>,
    ) -> Result<LogicalPlan> {
        let right = self.create_relation(&join.relation, ctes)?;
        match &join.join_operator {
            JoinOperator::LeftOuter(constraint) => {
                self.parse_join(left, &right, constraint, JoinType::Left)
            }
            JoinOperator::RightOuter(constraint) => {
                self.parse_join(left, &right, constraint, JoinType::Right)
            }
            JoinOperator::Inner(constraint) => {
                self.parse_join(left, &right, constraint, JoinType::Inner)
            }
            other => Err(DataFusionError::NotImplemented(format!(
                "Unsupported JOIN operator {:?}",
                other
            ))),
        }
    }

    fn parse_join(
        &self,
        left: &LogicalPlan,
        right: &LogicalPlan,
        constraint: &JoinConstraint,
        join_type: JoinType,
    ) -> Result<LogicalPlan> {
        match constraint {
            JoinConstraint::On(sql_expr) => {
                let mut keys: Vec<(String, String)> = vec![];
                let join_schema = left.schema().join(&right.schema())?;

                // parse ON expression
                let expr = self.sql_to_rex(sql_expr, &join_schema)?;

                // extract join keys
                extract_join_keys(&expr, &mut keys)?;
                let left_keys: Vec<&str> =
                    keys.iter().map(|pair| pair.0.as_str()).collect();
                let right_keys: Vec<&str> =
                    keys.iter().map(|pair| pair.1.as_str()).collect();

                // return the logical plan representing the join
                LogicalPlanBuilder::from(&left)
                    .join(&right, join_type, &left_keys, &right_keys)?
                    .build()
            }
            JoinConstraint::Using(idents) => {
                let keys: Vec<&str> = idents.iter().map(|x| x.value.as_str()).collect();
                LogicalPlanBuilder::from(&left)
                    .join(&right, join_type, &keys, &keys)?
                    .build()
            }
            JoinConstraint::Natural => {
                // https://issues.apache.org/jira/browse/ARROW-10727
                Err(DataFusionError::NotImplemented(
                    "NATURAL JOIN is not supported (https://issues.apache.org/jira/browse/ARROW-10727)".to_string(),
                ))
            }
            JoinConstraint::None => Err(DataFusionError::NotImplemented(
                "NONE contraint is not supported".to_string(),
            )),
        }
    }

    fn create_relation(
        &self,
        relation: &TableFactor,
        ctes: &mut HashMap<String, LogicalPlan>,
    ) -> Result<LogicalPlan> {
        match relation {
            TableFactor::Table { name, .. } => {
                let table_name = name.to_string();
                let cte = ctes.get(&table_name);
                match (
                    cte,
                    self.schema_provider.get_table_provider(name.try_into()?),
                ) {
                    (Some(cte_plan), _) => Ok(cte_plan.clone()),
                    (_, Some(provider)) => {
                        LogicalPlanBuilder::scan(&table_name, provider, None)?.build()
                    }
                    (_, None) => Err(DataFusionError::Plan(format!(
                        "Table or CTE with name '{}' not found",
                        name
                    ))),
                }
            }
            TableFactor::Derived {
                subquery, alias, ..
            } => self.query_to_plan_with_alias(
                subquery,
                alias.as_ref().map(|a| a.name.value.to_string()),
                ctes,
            ),
            TableFactor::NestedJoin(table_with_joins) => {
                self.plan_table_with_joins(table_with_joins, ctes)
            }
            // @todo Support TableFactory::TableFunction?
            _ => Err(DataFusionError::NotImplemented(format!(
                "Unsupported ast node {:?} in create_relation",
                relation
            ))),
        }
    }

    /// Generate a logic plan from an SQL select
    fn select_to_plan(
        &self,
        select: &Select,
        ctes: &mut HashMap<String, LogicalPlan>,
    ) -> Result<LogicalPlan> {
        let plans = self.plan_from_tables(&select.from, ctes)?;

        let plan = match &select.selection {
            Some(predicate_expr) => {
                // build join schema
                let mut fields = vec![];
                for plan in &plans {
                    fields.extend_from_slice(&plan.schema().fields());
                }
                let join_schema = DFSchema::new(fields)?;

                let filter_expr = self.sql_to_rex(predicate_expr, &join_schema)?;

                // look for expressions of the form `<column> = <column>`
                let mut possible_join_keys = vec![];
                extract_possible_join_keys(&filter_expr, &mut possible_join_keys)?;

                let mut all_join_keys = vec![];
                let mut left = plans[0].clone();
                for right in plans.iter().skip(1) {
                    let left_schema = left.schema();
                    let right_schema = right.schema();
                    let mut join_keys = vec![];
                    for (l, r) in &possible_join_keys {
                        if left_schema.field_with_unqualified_name(l).is_ok()
                            && right_schema.field_with_unqualified_name(r).is_ok()
                        {
                            join_keys.push((l.as_str(), r.as_str()));
                        } else if left_schema.field_with_unqualified_name(r).is_ok()
                            && right_schema.field_with_unqualified_name(l).is_ok()
                        {
                            join_keys.push((r.as_str(), l.as_str()));
                        }
                    }
                    if join_keys.is_empty() {
                        return Err(DataFusionError::NotImplemented(
                            "Cartesian joins are not supported".to_string(),
                        ));
                    } else {
                        let left_keys: Vec<_> =
                            join_keys.iter().map(|(l, _)| *l).collect();
                        let right_keys: Vec<_> =
                            join_keys.iter().map(|(_, r)| *r).collect();
                        let builder = LogicalPlanBuilder::from(&left);
                        left = builder
                            .join(right, JoinType::Inner, &left_keys, &right_keys)?
                            .build()?;
                    }
                    all_join_keys.extend_from_slice(&join_keys);
                }

                // remove join expressions from filter
                match remove_join_expressions(&filter_expr, &all_join_keys)? {
                    Some(filter_expr) => {
                        LogicalPlanBuilder::from(&left).filter(filter_expr)?.build()
                    }
                    _ => Ok(left),
                }
            }
            None => {
                if plans.len() == 1 {
                    Ok(plans[0].clone())
                } else {
                    Err(DataFusionError::NotImplemented(
                        "Cartesian joins are not supported".to_string(),
                    ))
                }
            }
        };
        let plan = plan?;

        // The SELECT expressions, with wildcards expanded.
        let select_exprs = self.prepare_select_exprs(&plan, &select.projection)?;

        // Optionally the HAVING expression.
        let having_expr_opt = select
            .having
            .as_ref()
            .map::<Result<Expr>, _>(|having_expr| {
                let having_expr = self.sql_expr_to_logical_expr(having_expr)?;

                // This step "dereferences" any aliases in the HAVING clause.
                //
                // This is how we support queries with HAVING expressions that
                // refer to aliased columns.
                //
                // For example:
                //
                //   SELECT c1 AS m FROM t HAVING m > 10;
                //   SELECT c1, MAX(c2) AS m FROM t GROUP BY c1 HAVING m > 10;
                //
                // are rewritten as, respectively:
                //
                //   SELECT c1 AS m FROM t HAVING c1 > 10;
                //   SELECT c1, MAX(c2) AS m FROM t GROUP BY c1 HAVING MAX(c2) > 10;
                //
                let having_expr = resolve_aliases_to_exprs(
                    &having_expr,
                    &extract_aliases(&select_exprs),
                )?;

                Ok(having_expr)
            })
            .transpose()?;

        // The outer expressions we will search through for
        // aggregates. Aggregates may be sourced from the SELECT...
        let mut aggr_expr_haystack = select_exprs.clone();

        // ... or from the HAVING.
        if let Some(having_expr) = &having_expr_opt {
            aggr_expr_haystack.push(having_expr.clone());
        }

        // All of the aggregate expressions (deduplicated).
        let aggr_exprs = find_aggregate_exprs(&aggr_expr_haystack);

        let (plan, select_exprs_post_aggr, having_expr_post_aggr_opt) =
            if !select.group_by.is_empty() || !aggr_exprs.is_empty() {
                self.aggregate(
                    &plan,
                    &select_exprs,
                    &having_expr_opt,
                    &select.group_by,
                    aggr_exprs,
                )?
            } else {
                if let Some(having_expr) = &having_expr_opt {
                    let available_columns = select_exprs
                        .iter()
                        .map(|expr| expr_as_column_expr(expr, &plan))
                        .collect::<Result<Vec<Expr>>>()?;

                    // Ensure the HAVING expression is using only columns
                    // provided by the SELECT.
                    if !can_columns_satisfy_exprs(
                        &available_columns,
                        &[having_expr.clone()],
                    )? {
                        return Err(DataFusionError::Plan(
                            "Having references column(s) not provided by the select"
                                .to_owned(),
                        ));
                    }
                }

                (plan, select_exprs, having_expr_opt)
            };

        let plan = if let Some(having_expr_post_aggr) = having_expr_post_aggr_opt {
            LogicalPlanBuilder::from(&plan)
                .filter(having_expr_post_aggr)?
                .build()?
        } else {
            plan
        };

        self.project(&plan, select_exprs_post_aggr, false)
    }

    /// Returns the `Expr`'s corresponding to a SQL query's SELECT expressions.
    ///
    /// Wildcards are expanded into the concrete list of columns.
    fn prepare_select_exprs(
        &self,
        plan: &LogicalPlan,
        projection: &[SelectItem],
    ) -> Result<Vec<Expr>> {
        let input_schema = plan.schema();

        Ok(projection
            .iter()
            .map(|expr| self.sql_select_to_rex(&expr, &input_schema))
            .collect::<Result<Vec<Expr>>>()?
            .iter()
            .flat_map(|expr| expand_wildcard(&expr, &input_schema))
            .collect::<Vec<Expr>>())
    }

    /// Wrap a plan in a projection
    ///
    /// If the `force` argument is `false`, the projection is applied only when
    /// necessary, i.e., when the input fields are different than the
    /// projection. Note that if the input fields are the same, but out of
    /// order, the projection will be applied.
    fn project(
        &self,
        input: &LogicalPlan,
        expr: Vec<Expr>,
        force: bool,
    ) -> Result<LogicalPlan> {
        self.validate_schema_satisfies_exprs(&input.schema(), &expr)?;
        let plan = LogicalPlanBuilder::from(input).project(expr)?.build()?;

        let project = force
            || match input {
                LogicalPlan::TableScan { .. } => true,
                _ => plan.schema().fields() != input.schema().fields(),
            };

        if project {
            Ok(plan)
        } else {
            Ok(input.clone())
        }
    }

    fn aggregate(
        &self,
        input: &LogicalPlan,
        select_exprs: &[Expr],
        having_expr_opt: &Option<Expr>,
        group_by: &[SQLExpr],
        aggr_exprs: Vec<Expr>,
    ) -> Result<(LogicalPlan, Vec<Expr>, Option<Expr>)> {
        let group_by_exprs = group_by
            .iter()
            .map(|e| self.sql_to_rex(e, &input.schema()))
            .collect::<Result<Vec<Expr>>>()?;

        let aggr_projection_exprs = group_by_exprs
            .iter()
            .chain(aggr_exprs.iter())
            .cloned()
            .collect::<Vec<Expr>>();

        let plan = LogicalPlanBuilder::from(&input)
            .aggregate(group_by_exprs, aggr_exprs)?
            .build()?;

        // After aggregation, these are all of the columns that will be
        // available to next phases of planning.
        let column_exprs_post_aggr = aggr_projection_exprs
            .iter()
            .map(|expr| expr_as_column_expr(expr, input))
            .collect::<Result<Vec<Expr>>>()?;

        // Rewrite the SELECT expression to use the columns produced by the
        // aggregation.
        let select_exprs_post_aggr = select_exprs
            .iter()
            .map(|expr| rebase_expr(expr, &aggr_projection_exprs, input))
            .collect::<Result<Vec<Expr>>>()?;

        if !can_columns_satisfy_exprs(&column_exprs_post_aggr, &select_exprs_post_aggr)? {
            return Err(DataFusionError::Plan(
                "Projection references non-aggregate values".to_owned(),
            ));
        }

        // Rewrite the HAVING expression to use the columns produced by the
        // aggregation.
        let having_expr_post_aggr_opt = if let Some(having_expr) = having_expr_opt {
            let having_expr_post_aggr =
                rebase_expr(having_expr, &aggr_projection_exprs, input)?;

            if !can_columns_satisfy_exprs(
                &column_exprs_post_aggr,
                &[having_expr_post_aggr.clone()],
            )? {
                return Err(DataFusionError::Plan(
                    "Having references non-aggregate values".to_owned(),
                ));
            }

            Some(having_expr_post_aggr)
        } else {
            None
        };

        Ok((plan, select_exprs_post_aggr, having_expr_post_aggr_opt))
    }

    /// Wrap a plan in a limit
    fn limit(&self, input: &LogicalPlan, limit: &Option<SQLExpr>) -> Result<LogicalPlan> {
        match *limit {
            Some(ref limit_expr) => {
                let n = match self.sql_to_rex(&limit_expr, &input.schema())? {
                    Expr::Literal(ScalarValue::Int64(Some(n))) => Ok(n as usize),
                    _ => Err(DataFusionError::Plan(
                        "Unexpected expression for LIMIT clause".to_string(),
                    )),
                }?;

                LogicalPlanBuilder::from(&input).limit(n)?.build()
            }
            _ => Ok(input.clone()),
        }
    }

    /// Wrap the logical in a sort
    fn order_by(
        &self,
        plan: &LogicalPlan,
        order_by: &[OrderByExpr],
    ) -> Result<LogicalPlan> {
        if order_by.is_empty() {
            return Ok(plan.clone());
        }

        let input_schema = plan.schema();
        let order_by_rex: Result<Vec<Expr>> = order_by
            .iter()
            .map(|e| {
                Ok(Expr::Sort {
                    expr: Box::new(self.sql_to_rex(&e.expr, &input_schema)?),
                    // by default asc
                    asc: e.asc.unwrap_or(true),
                    // by default nulls first to be consistent with spark
                    nulls_first: e.nulls_first.unwrap_or(true),
                })
            })
            .collect();

        LogicalPlanBuilder::from(&plan).sort(order_by_rex?)?.build()
    }

    /// Validate the schema provides all of the columns referenced in the expressions.
    fn validate_schema_satisfies_exprs(
        &self,
        schema: &DFSchema,
        exprs: &[Expr],
    ) -> Result<()> {
        find_column_exprs(exprs)
            .iter()
            .try_for_each(|col| match col {
                Expr::Column(name) => {
                    schema.field_with_unqualified_name(&name).map_err(|_| {
                        DataFusionError::Plan(format!(
                            "Invalid identifier '{}' for schema {}",
                            name,
                            schema.to_string()
                        ))
                    })?;
                    Ok(())
                }
                _ => Err(DataFusionError::Internal("Not a column".to_string())),
            })
    }

    /// Generate a relational expression from a select SQL expression
    fn sql_select_to_rex(&self, sql: &SelectItem, schema: &DFSchema) -> Result<Expr> {
        match sql {
            SelectItem::UnnamedExpr(expr) => self.sql_to_rex(expr, schema),
            SelectItem::ExprWithAlias { expr, alias } => Ok(Alias(
                Box::new(self.sql_to_rex(&expr, schema)?),
                alias.value.clone(),
            )),
            SelectItem::Wildcard => Ok(Expr::Wildcard),
            SelectItem::QualifiedWildcard(_) => Err(DataFusionError::NotImplemented(
                "Qualified wildcards are not supported".to_string(),
            )),
        }
    }

    /// Generate a relational expression from a SQL expression
    pub fn sql_to_rex(&self, sql: &SQLExpr, schema: &DFSchema) -> Result<Expr> {
        let expr = self.sql_expr_to_logical_expr(sql)?;
        self.validate_schema_satisfies_exprs(schema, &[expr.clone()])?;
        Ok(expr)
    }

    fn sql_fn_arg_to_logical_expr(&self, sql: &FunctionArg) -> Result<Expr> {
        match sql {
            FunctionArg::Named { name: _, arg } => self.sql_expr_to_logical_expr(arg),
            FunctionArg::Unnamed(value) => self.sql_expr_to_logical_expr(value),
        }
    }

    fn sql_expr_to_logical_expr(&self, sql: &SQLExpr) -> Result<Expr> {
        match sql {
            SQLExpr::Value(Value::Number(n, _)) => match n.parse::<i64>() {
                Ok(n) => Ok(lit(n)),
                Err(_) => Ok(lit(n.parse::<f64>().unwrap())),
            },
            SQLExpr::Value(Value::SingleQuotedString(ref s)) => Ok(lit(s.clone())),

            SQLExpr::Value(Value::Boolean(n)) => Ok(lit(*n)),

            SQLExpr::Value(Value::Null) => Ok(Expr::Literal(ScalarValue::Utf8(None))),
            SQLExpr::Extract { field, expr } => Ok(Expr::ScalarFunction {
                fun: functions::BuiltinScalarFunction::DatePart,
                args: vec![
                    Expr::Literal(ScalarValue::Utf8(Some(format!("{}", field)))),
                    self.sql_expr_to_logical_expr(expr)?,
                ],
            }),

            SQLExpr::Value(Value::Interval {
                value,
                leading_field,
                leading_precision,
                last_field,
                fractional_seconds_precision,
            }) => self.sql_interval_to_literal(
                value,
                leading_field,
                leading_precision,
                last_field,
                fractional_seconds_precision,
            ),

            SQLExpr::Identifier(ref id) => {
                if &id.value[0..1] == "@" {
                    let var_names = vec![id.value.clone()];
                    Ok(Expr::ScalarVariable(var_names))
                } else {
                    Ok(Expr::Column(id.value.to_string()))
                }
            }

            SQLExpr::CompoundIdentifier(ids) => {
                let mut var_names = vec![];
                for id in ids {
                    var_names.push(id.value.clone());
                }
                if &var_names[0][0..1] == "@" {
                    Ok(Expr::ScalarVariable(var_names))
                } else {
                    Err(DataFusionError::NotImplemented(format!(
                        "Unsupported compound identifier '{:?}'",
                        var_names,
                    )))
                }
            }

            SQLExpr::Wildcard => Ok(Expr::Wildcard),

            SQLExpr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                let expr = if let Some(e) = operand {
                    Some(Box::new(self.sql_expr_to_logical_expr(e)?))
                } else {
                    None
                };
                let when_expr = conditions
                    .iter()
                    .map(|e| self.sql_expr_to_logical_expr(e))
                    .collect::<Result<Vec<_>>>()?;
                let then_expr = results
                    .iter()
                    .map(|e| self.sql_expr_to_logical_expr(e))
                    .collect::<Result<Vec<_>>>()?;
                let else_expr = if let Some(e) = else_result {
                    Some(Box::new(self.sql_expr_to_logical_expr(e)?))
                } else {
                    None
                };

                Ok(Expr::Case {
                    expr,
                    when_then_expr: when_expr
                        .iter()
                        .zip(then_expr.iter())
                        .map(|(w, t)| (Box::new(w.to_owned()), Box::new(t.to_owned())))
                        .collect(),
                    else_expr,
                })
            }

            SQLExpr::Cast {
                ref expr,
                ref data_type,
            } => Ok(Expr::Cast {
                expr: Box::new(self.sql_expr_to_logical_expr(&expr)?),
                data_type: convert_data_type(data_type)?,
            }),

            SQLExpr::TryCast {
                ref expr,
                ref data_type,
            } => Ok(Expr::TryCast {
                expr: Box::new(self.sql_expr_to_logical_expr(&expr)?),
                data_type: convert_data_type(data_type)?,
            }),

            SQLExpr::TypedString {
                ref data_type,
                ref value,
            } => Ok(Expr::Cast {
                expr: Box::new(lit(&**value)),
                data_type: convert_data_type(data_type)?,
            }),

            SQLExpr::IsNull(ref expr) => {
                Ok(Expr::IsNull(Box::new(self.sql_expr_to_logical_expr(expr)?)))
            }

            SQLExpr::IsNotNull(ref expr) => Ok(Expr::IsNotNull(Box::new(
                self.sql_expr_to_logical_expr(expr)?,
            ))),

            SQLExpr::UnaryOp { ref op, ref expr } => match op {
                UnaryOperator::Not => {
                    Ok(Expr::Not(Box::new(self.sql_expr_to_logical_expr(expr)?)))
                }
                UnaryOperator::Plus => Ok(self.sql_expr_to_logical_expr(expr)?),
                UnaryOperator::Minus => {
                    match expr.as_ref() {
                        // optimization: if it's a number literal, we applly the negative operator
                        // here directly to calculate the new literal.
                        SQLExpr::Value(Value::Number(n,_)) => match n.parse::<i64>() {
                            Ok(n) => Ok(lit(-n)),
                            Err(_) => Ok(lit(-n
                                .parse::<f64>()
                                .map_err(|_e| {
                                    DataFusionError::Internal(format!(
                                        "negative operator can be only applied to integer and float operands, got: {}",
                                    n))
                                })?)),
                        },
                        // not a literal, apply negative operator on expression
                        _ => Ok(Expr::Negative(Box::new(self.sql_expr_to_logical_expr(expr)?))),
                    }
                }
                _ => Err(DataFusionError::NotImplemented(format!(
                    "Unsupported SQL unary operator {:?}",
                    op
                ))),
            },

            SQLExpr::Between {
                ref expr,
                ref negated,
                ref low,
                ref high,
            } => Ok(Expr::Between {
                expr: Box::new(self.sql_expr_to_logical_expr(&expr)?),
                negated: *negated,
                low: Box::new(self.sql_expr_to_logical_expr(&low)?),
                high: Box::new(self.sql_expr_to_logical_expr(&high)?),
            }),

            SQLExpr::InList {
                ref expr,
                ref list,
                ref negated,
            } => {
                let list_expr = list
                    .iter()
                    .map(|e| self.sql_expr_to_logical_expr(e))
                    .collect::<Result<Vec<_>>>()?;

                Ok(Expr::InList {
                    expr: Box::new(self.sql_expr_to_logical_expr(&expr)?),
                    list: list_expr,
                    negated: *negated,
                })
            }

            SQLExpr::BinaryOp {
                ref left,
                ref op,
                ref right,
            } => {
                let operator = match *op {
                    BinaryOperator::Gt => Ok(Operator::Gt),
                    BinaryOperator::GtEq => Ok(Operator::GtEq),
                    BinaryOperator::Lt => Ok(Operator::Lt),
                    BinaryOperator::LtEq => Ok(Operator::LtEq),
                    BinaryOperator::Eq => Ok(Operator::Eq),
                    BinaryOperator::NotEq => Ok(Operator::NotEq),
                    BinaryOperator::Plus => Ok(Operator::Plus),
                    BinaryOperator::Minus => Ok(Operator::Minus),
                    BinaryOperator::Multiply => Ok(Operator::Multiply),
                    BinaryOperator::Divide => Ok(Operator::Divide),
                    BinaryOperator::Modulus => Ok(Operator::Modulus),
                    BinaryOperator::And => Ok(Operator::And),
                    BinaryOperator::Or => Ok(Operator::Or),
                    BinaryOperator::Like => Ok(Operator::Like),
                    BinaryOperator::NotLike => Ok(Operator::NotLike),
                    _ => Err(DataFusionError::NotImplemented(format!(
                        "Unsupported SQL binary operator {:?}",
                        op
                    ))),
                }?;

                Ok(Expr::BinaryExpr {
                    left: Box::new(self.sql_expr_to_logical_expr(&left)?),
                    op: operator,
                    right: Box::new(self.sql_expr_to_logical_expr(&right)?),
                })
            }

            SQLExpr::Function(function) => {
                let name = if function.name.0.len() > 1 {
                    // DF doesn't handle compound identifiers
                    // (e.g. "foo.bar") for function names yet
                    function.name.to_string()
                } else {
                    // if there is a quote style, then don't normalize
                    // the name, otherwise normalize to lowercase
                    let ident = &function.name.0[0];
                    match ident.quote_style {
                        Some(_) => ident.value.clone(),
                        None => ident.value.to_ascii_lowercase(),
                    }
                };

                // first, scalar built-in
                if let Ok(fun) = functions::BuiltinScalarFunction::from_str(&name) {
                    let args = function
                        .args
                        .iter()
                        .map(|a| self.sql_fn_arg_to_logical_expr(a))
                        .collect::<Result<Vec<Expr>>>()?;

                    return Ok(Expr::ScalarFunction { fun, args });
                };

                // next, aggregate built-ins
                if let Ok(fun) = aggregates::AggregateFunction::from_str(&name) {
                    let args = if fun == aggregates::AggregateFunction::Count {
                        function
                            .args
                            .iter()
                            .map(|a| match a {
                                FunctionArg::Unnamed(SQLExpr::Value(Value::Number(
                                    _,
                                    _,
                                ))) => Ok(lit(1_u8)),
                                FunctionArg::Unnamed(SQLExpr::Wildcard) => Ok(lit(1_u8)),
                                _ => self.sql_fn_arg_to_logical_expr(a),
                            })
                            .collect::<Result<Vec<Expr>>>()?
                    } else {
                        function
                            .args
                            .iter()
                            .map(|a| self.sql_fn_arg_to_logical_expr(a))
                            .collect::<Result<Vec<Expr>>>()?
                    };

                    return Ok(Expr::AggregateFunction {
                        fun,
                        distinct: function.distinct,
                        args,
                    });
                };

                // finally, user-defined functions (UDF) and UDAF
                match self.schema_provider.get_function_meta(&name) {
                    Some(fm) => {
                        let args = function
                            .args
                            .iter()
                            .map(|a| self.sql_fn_arg_to_logical_expr(a))
                            .collect::<Result<Vec<Expr>>>()?;

                        Ok(Expr::ScalarUDF { fun: fm, args })
                    }
                    None => match self.schema_provider.get_aggregate_meta(&name) {
                        Some(fm) => {
                            let args = function
                                .args
                                .iter()
                                .map(|a| self.sql_fn_arg_to_logical_expr(a))
                                .collect::<Result<Vec<Expr>>>()?;

                            Ok(Expr::AggregateUDF { fun: fm, args })
                        }
                        _ => Err(DataFusionError::Plan(format!(
                            "Invalid function '{}'",
                            name
                        ))),
                    },
                }
            }

            SQLExpr::Nested(e) => self.sql_expr_to_logical_expr(&e),

            _ => Err(DataFusionError::NotImplemented(format!(
                "Unsupported ast node {:?} in sqltorel",
                sql
            ))),
        }
    }

    fn sql_interval_to_literal(
        &self,
        value: &str,
        leading_field: &Option<DateTimeField>,
        leading_precision: &Option<u64>,
        last_field: &Option<DateTimeField>,
        fractional_seconds_precision: &Option<u64>,
    ) -> Result<Expr> {
        if leading_field.is_some() {
            return Err(DataFusionError::NotImplemented(format!(
                "Unsupported Interval Expression with leading_field {:?}",
                leading_field
            )));
        }

        if leading_precision.is_some() {
            return Err(DataFusionError::NotImplemented(format!(
                "Unsupported Interval Expression with leading_precision {:?}",
                leading_precision
            )));
        }

        if last_field.is_some() {
            return Err(DataFusionError::NotImplemented(format!(
                "Unsupported Interval Expression with last_field {:?}",
                last_field
            )));
        }

        if fractional_seconds_precision.is_some() {
            return Err(DataFusionError::NotImplemented(format!(
                "Unsupported Interval Expression with fractional_seconds_precision {:?}",
                fractional_seconds_precision
            )));
        }

        const SECONDS_PER_HOUR: f32 = 3_600_f32;
        const MILLIS_PER_SECOND: f32 = 1_000_f32;

        // We are storing parts as integers, it's why we need to align parts fractional
        // INTERVAL '0.5 MONTH' = 15 days, INTERVAL '1.5 MONTH' = 1 month 15 days
        // INTERVAL '0.5 DAY' = 12 hours, INTERVAL '1.5 DAY' = 1 day 12 hours
        let align_interval_parts = |month_part: f32,
                                    mut day_part: f32,
                                    mut milles_part: f32|
         -> (i32, i32, f32) {
            // Convert fractional month to days, It's not supported by Arrow types, but anyway
            day_part += (month_part - (month_part as i32) as f32) * 30_f32;

            // Convert fractional days to hours
            milles_part += (day_part - ((day_part as i32) as f32))
                * 24_f32
                * SECONDS_PER_HOUR
                * MILLIS_PER_SECOND;

            (month_part as i32, day_part as i32, milles_part)
        };

        let calculate_from_part = |interval_period_str: &str,
                                   interval_type: &str|
         -> Result<(i32, i32, f32)> {
            // @todo It's better to use Decimal in order to protect rounding errors
            // Wait https://github.com/apache/arrow/pull/9232
            let interval_period = match f32::from_str(interval_period_str) {
                Ok(n) => n,
                Err(_) => {
                    return Err(DataFusionError::SQL(ParserError(format!(
                        "Unsupported Interval Expression with value {:?}",
                        value
                    ))))
                }
            };

            if interval_period > (i32::MAX as f32) {
                return Err(DataFusionError::NotImplemented(format!(
                    "Interval field value out of range: {:?}",
                    value
                )));
            }

            match interval_type.to_lowercase().as_str() {
                "year" => Ok(align_interval_parts(interval_period * 12_f32, 0.0, 0.0)),
                "month" => Ok(align_interval_parts(interval_period, 0.0, 0.0)),
                "day" | "days" => Ok(align_interval_parts(0.0, interval_period, 0.0)),
                "hour" | "hours" => {
                    Ok((0, 0, interval_period * SECONDS_PER_HOUR * MILLIS_PER_SECOND))
                }
                "minutes" | "minute" => {
                    Ok((0, 0, interval_period * 60_f32 * MILLIS_PER_SECOND))
                }
                "seconds" | "second" => Ok((0, 0, interval_period * MILLIS_PER_SECOND)),
                "milliseconds" | "millisecond" => Ok((0, 0, interval_period)),
                _ => Err(DataFusionError::NotImplemented(format!(
                    "Invalid input syntax for type interval: {:?}",
                    value
                ))),
            }
        };

        let mut result_month: i64 = 0;
        let mut result_days: i64 = 0;
        let mut result_millis: i64 = 0;

        let mut parts = value.split_whitespace();

        loop {
            let interval_period_str = parts.next();
            if interval_period_str.is_none() {
                break;
            }

            let (diff_month, diff_days, diff_millis) = calculate_from_part(
                interval_period_str.unwrap(),
                parts.next().unwrap_or("second"),
            )?;

            result_month += diff_month as i64;

            if result_month > (i32::MAX as i64) {
                return Err(DataFusionError::NotImplemented(format!(
                    "Interval field value out of range: {:?}",
                    value
                )));
            }

            result_days += diff_days as i64;

            if result_days > (i32::MAX as i64) {
                return Err(DataFusionError::NotImplemented(format!(
                    "Interval field value out of range: {:?}",
                    value
                )));
            }

            result_millis += diff_millis as i64;

            if result_millis > (i32::MAX as i64) {
                return Err(DataFusionError::NotImplemented(format!(
                    "Interval field value out of range: {:?}",
                    value
                )));
            }
        }

        // Interval is tricky thing
        // 1 day is not 24 hours because timezones, 1 year != 365/364! 30 days != 1 month
        // The true way to store and calculate intervals is to store it as it defined
        // Due the fact that Arrow supports only two types YearMonth (month) and DayTime (day, time)
        // It's not possible to store complex intervals
        // It's possible to do select (NOW() + INTERVAL '1 year') + INTERVAL '1 day'; as workaround
        if result_month != 0 && (result_days != 0 || result_millis != 0) {
            return Err(DataFusionError::NotImplemented(format!(
                "DF does not support intervals that have both a Year/Month part as well as Days/Hours/Mins/Seconds: {:?}. Hint: try breaking the interval into two parts, one with Year/Month and the other with Days/Hours/Mins/Seconds - e.g. (NOW() + INTERVAL '1 year') + INTERVAL '1 day'",
                value
            )));
        }

        if result_month != 0 {
            return Ok(Expr::Literal(ScalarValue::IntervalYearMonth(Some(
                result_month as i32,
            ))));
        }

        let result: i64 = (result_days << 32) | result_millis;
        Ok(Expr::Literal(ScalarValue::IntervalDayTime(Some(result))))
    }

    fn show_variable_to_plan(&self, variable: &[Ident]) -> Result<LogicalPlan> {
        // Special case SHOW TABLES
        let variable = ObjectName(variable.to_vec()).to_string();
        if variable.as_str().eq_ignore_ascii_case("tables") {
            if self.has_table("information_schema", "tables") {
                let rewrite =
                    DFParser::parse_sql("SELECT * FROM information_schema.tables;")?;
                self.statement_to_plan(&rewrite[0])
            } else {
                Err(DataFusionError::Plan(
                    "SHOW TABLES is not supported unless information_schema is enabled"
                        .to_string(),
                ))
            }
        } else {
            Err(DataFusionError::NotImplemented(format!(
                "SHOW {} not implemented. Supported syntax: SHOW <TABLES>",
                variable
            )))
        }
    }

    fn show_columns_to_plan(
        &self,
        extended: bool,
        full: bool,
        table_name: &ObjectName,
        filter: Option<&ShowStatementFilter>,
    ) -> Result<LogicalPlan> {
        if filter.is_some() {
            return Err(DataFusionError::Plan(
                "SHOW COLUMNS with WHERE or LIKE is not supported".to_string(),
            ));
        }

        if !self.has_table("information_schema", "columns") {
            return Err(DataFusionError::Plan(
                "SHOW COLUMNS is not supported unless information_schema is enabled"
                    .to_string(),
            ));
        }

        if self
            .schema_provider
            .get_table_provider(table_name.try_into()?)
            .is_none()
        {
            return Err(DataFusionError::Plan(format!(
                "Unknown relation for SHOW COLUMNS: {}",
                table_name
            )));
        }

        // Figure out the where clause
        let columns = vec!["table_name", "table_schema", "table_catalog"].into_iter();
        let where_clause = table_name
            .0
            .iter()
            .rev()
            .zip(columns)
            .map(|(ident, column_name)| {
                format!(r#"{} = '{}'"#, column_name, ident.to_string())
            })
            .collect::<Vec<_>>()
            .join(" AND ");

        // treat both FULL and EXTENDED as the same
        let select_list = if full || extended {
            "*"
        } else {
            "table_catalog, table_schema, table_name, column_name, data_type, is_nullable"
        };

        let query = format!(
            "SELECT {} FROM information_schema.columns WHERE {}",
            select_list, where_clause
        );

        let rewrite = DFParser::parse_sql(&query)?;
        self.statement_to_plan(&rewrite[0])
    }

    /// Return true if there is a table provider available for "schema.table"
    fn has_table(&self, schema: &str, table: &str) -> bool {
        let tables_reference = TableReference::Partial { schema, table };
        self.schema_provider
            .get_table_provider(tables_reference)
            .is_some()
    }
}

/// Remove join expressions from a filter expression
fn remove_join_expressions(
    expr: &Expr,
    join_columns: &[(&str, &str)],
) -> Result<Option<Expr>> {
    match expr {
        Expr::BinaryExpr { left, op, right } => match op {
            Operator::Eq => match (left.as_ref(), right.as_ref()) {
                (Expr::Column(l), Expr::Column(r)) => {
                    if join_columns.contains(&(l, r)) || join_columns.contains(&(r, l)) {
                        Ok(None)
                    } else {
                        Ok(Some(expr.clone()))
                    }
                }
                _ => Ok(Some(expr.clone())),
            },
            Operator::And => {
                let l = remove_join_expressions(left, join_columns)?;
                let r = remove_join_expressions(right, join_columns)?;
                match (l, r) {
                    (Some(ll), Some(rr)) => Ok(Some(and(ll, rr))),
                    (Some(ll), _) => Ok(Some(ll)),
                    (_, Some(rr)) => Ok(Some(rr)),
                    _ => Ok(None),
                }
            }
            _ => Ok(Some(expr.clone())),
        },
        _ => Ok(Some(expr.clone())),
    }
}

/// Parse equijoin ON condition which could be a single Eq or multiple conjunctive Eqs
///
/// Examples
///
/// foo = bar
/// foo = bar AND bar = baz AND ...
///
fn extract_join_keys(expr: &Expr, accum: &mut Vec<(String, String)>) -> Result<()> {
    match expr {
        Expr::BinaryExpr { left, op, right } => match op {
            Operator::Eq => match (left.as_ref(), right.as_ref()) {
                (Expr::Column(l), Expr::Column(r)) => {
                    accum.push((l.to_owned(), r.to_owned()));
                    Ok(())
                }
                other => Err(DataFusionError::SQL(ParserError(format!(
                    "Unsupported expression '{:?}' in JOIN condition",
                    other
                )))),
            },
            Operator::And => {
                extract_join_keys(left, accum)?;
                extract_join_keys(right, accum)
            }
            other => Err(DataFusionError::SQL(ParserError(format!(
                "Unsupported expression '{:?}' in JOIN condition",
                other
            )))),
        },
        other => Err(DataFusionError::SQL(ParserError(format!(
            "Unsupported expression '{:?}' in JOIN condition",
            other
        )))),
    }
}

/// Extract join keys from a WHERE clause
fn extract_possible_join_keys(
    expr: &Expr,
    accum: &mut Vec<(String, String)>,
) -> Result<()> {
    match expr {
        Expr::BinaryExpr { left, op, right } => match op {
            Operator::Eq => match (left.as_ref(), right.as_ref()) {
                (Expr::Column(l), Expr::Column(r)) => {
                    accum.push((l.to_owned(), r.to_owned()));
                    Ok(())
                }
                _ => Ok(()),
            },
            Operator::And => {
                extract_possible_join_keys(left, accum)?;
                extract_possible_join_keys(right, accum)
            }
            _ => Ok(()),
        },
        _ => Ok(()),
    }
}

/// Convert SQL data type to relational representation of data type
pub fn convert_data_type(sql: &SQLDataType) -> Result<DataType> {
    match sql {
        SQLDataType::Boolean => Ok(DataType::Boolean),
        SQLDataType::SmallInt => Ok(DataType::Int16),
        SQLDataType::Int => Ok(DataType::Int32),
        SQLDataType::BigInt => Ok(DataType::Int64),
        SQLDataType::Float(_) | SQLDataType::Real => Ok(DataType::Float64),
        SQLDataType::Double => Ok(DataType::Float64),
        SQLDataType::Char(_) | SQLDataType::Varchar(_) => Ok(DataType::Utf8),
        SQLDataType::Timestamp => Ok(DataType::Timestamp(TimeUnit::Nanosecond, None)),
        SQLDataType::Date => Ok(DataType::Date32),
        other => Err(DataFusionError::NotImplemented(format!(
            "Unsupported SQL type {:?}",
            other
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datasource::empty::EmptyTable;
    use crate::{logical_plan::create_udf, sql::parser::DFParser};
    use functions::ScalarFunctionImplementation;

    const PERSON_COLUMN_NAMES: &str =
        "id, first_name, last_name, age, state, salary, birth_date";

    #[test]
    fn select_no_relation() {
        quick_test(
            "SELECT 1",
            "Projection: Int64(1)\
             \n  EmptyRelation",
        );
    }

    #[test]
    fn select_column_does_not_exist() {
        let sql = "SELECT doesnotexist FROM person";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            format!(
                "Plan(\"Invalid identifier \\\'doesnotexist\\\' for schema {}\")",
                PERSON_COLUMN_NAMES
            ),
            format!("{:?}", err)
        );
    }

    #[test]
    fn select_repeated_column() {
        let sql = "SELECT age, age FROM person";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            "Plan(\"Projections require unique expression names but the expression \\\"#age\\\" at position 0 and \\\"#age\\\" at position 1 have the same name. Consider aliasing (\\\"AS\\\") one of them.\")",
            format!("{:?}", err)
        );
    }

    #[test]
    fn select_wildcard_with_repeated_column() {
        let sql = "SELECT *, age FROM person";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            "Plan(\"Projections require unique expression names but the expression \\\"#age\\\" at position 3 and \\\"#age\\\" at position 7 have the same name. Consider aliasing (\\\"AS\\\") one of them.\")",
            format!("{:?}", err)
        );
    }

    #[test]
    fn select_wildcard_with_repeated_column_but_is_aliased() {
        quick_test(
            "SELECT *, first_name AS fn from person",
            "Projection: #id, #first_name, #last_name, #age, #state, #salary, #birth_date, #first_name AS fn\
            \n  TableScan: person projection=None",
        );
    }

    #[test]
    fn select_scalar_func_with_literal_no_relation() {
        quick_test(
            "SELECT sqrt(9)",
            "Projection: sqrt(Int64(9))\
             \n  EmptyRelation",
        );
    }

    #[test]
    fn select_simple_filter() {
        let sql = "SELECT id, first_name, last_name \
                   FROM person WHERE state = 'CO'";
        let expected = "Projection: #id, #first_name, #last_name\
                        \n  Filter: #state Eq Utf8(\"CO\")\
                        \n    TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_filter_column_does_not_exist() {
        let sql = "SELECT first_name FROM person WHERE doesnotexist = 'A'";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            format!(
                "Plan(\"Invalid identifier \\\'doesnotexist\\\' for schema {}\")",
                PERSON_COLUMN_NAMES
            ),
            format!("{:?}", err)
        );
    }

    #[test]
    fn select_filter_cannot_use_alias() {
        let sql = "SELECT first_name AS x FROM person WHERE x = 'A'";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            format!(
                "Plan(\"Invalid identifier \\\'x\\\' for schema {}\")",
                PERSON_COLUMN_NAMES
            ),
            format!("{:?}", err)
        );
    }

    #[test]
    fn select_neg_filter() {
        let sql = "SELECT id, first_name, last_name \
                   FROM person WHERE NOT state";
        let expected = "Projection: #id, #first_name, #last_name\
                        \n  Filter: NOT #state\
                        \n    TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_compound_filter() {
        let sql = "SELECT id, first_name, last_name \
                   FROM person WHERE state = 'CO' AND age >= 21 AND age <= 65";
        let expected = "Projection: #id, #first_name, #last_name\
            \n  Filter: #state Eq Utf8(\"CO\") And #age GtEq Int64(21) And #age LtEq Int64(65)\
            \n    TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn test_timestamp_filter() {
        let sql =
            "SELECT state FROM person WHERE birth_date < CAST (158412331400600000 as timestamp)";

        let expected = "Projection: #state\
            \n  Filter: #birth_date Lt CAST(Int64(158412331400600000) AS Timestamp(Nanosecond, None))\
            \n    TableScan: person projection=None";

        quick_test(sql, expected);
    }

    #[test]
    fn test_date_filter() {
        let sql =
            "SELECT state FROM person WHERE birth_date < CAST ('2020-01-01' as date)";

        let expected = "Projection: #state\
            \n  Filter: #birth_date Lt CAST(Utf8(\"2020-01-01\") AS Date32)\
            \n    TableScan: person projection=None";

        quick_test(sql, expected);
    }

    #[test]
    fn select_all_boolean_operators() {
        let sql = "SELECT age, first_name, last_name \
                   FROM person \
                   WHERE age = 21 \
                   AND age != 21 \
                   AND age > 21 \
                   AND age >= 21 \
                   AND age < 65 \
                   AND age <= 65";
        let expected = "Projection: #age, #first_name, #last_name\
                        \n  Filter: #age Eq Int64(21) \
                        And #age NotEq Int64(21) \
                        And #age Gt Int64(21) \
                        And #age GtEq Int64(21) \
                        And #age Lt Int64(65) \
                        And #age LtEq Int64(65)\
                        \n    TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_between() {
        let sql = "SELECT state FROM person WHERE age BETWEEN 21 AND 65";
        let expected = "Projection: #state\
            \n  Filter: #age BETWEEN Int64(21) AND Int64(65)\
            \n    TableScan: person projection=None";

        quick_test(sql, expected);
    }

    #[test]
    fn select_between_negated() {
        let sql = "SELECT state FROM person WHERE age NOT BETWEEN 21 AND 65";
        let expected = "Projection: #state\
            \n  Filter: #age NOT BETWEEN Int64(21) AND Int64(65)\
            \n    TableScan: person projection=None";

        quick_test(sql, expected);
    }

    #[test]
    fn select_nested() {
        let sql = "SELECT fn2, last_name
                   FROM (
                     SELECT fn1 as fn2, last_name, birth_date
                     FROM (
                       SELECT first_name AS fn1, last_name, birth_date, age
                       FROM person
                     )
                   )";
        let expected = "Projection: #fn2, #last_name\
                        \n  Projection: #fn1 AS fn2, #last_name, #birth_date\
                        \n    Projection: #first_name AS fn1, #last_name, #birth_date, #age\
                        \n      TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_nested_with_filters() {
        let sql = "SELECT fn1, age
                   FROM (
                     SELECT first_name AS fn1, age
                     FROM person
                     WHERE age > 20
                   )
                   WHERE fn1 = 'X' AND age < 30";

        let expected = "Filter: #fn1 Eq Utf8(\"X\") And #age Lt Int64(30)\
                        \n  Projection: #first_name AS fn1, #age\
                        \n    Filter: #age Gt Int64(20)\
                        \n      TableScan: person projection=None";

        quick_test(sql, expected);
    }

    #[test]
    fn select_with_having() {
        let sql = "SELECT id, age
                   FROM person
                   HAVING age > 100 AND age < 200";
        let expected = "Projection: #id, #age\
                        \n  Filter: #age Gt Int64(100) And #age Lt Int64(200)\
                        \n    TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_with_having_referencing_column_not_in_select() {
        let sql = "SELECT id, age
                   FROM person
                   HAVING first_name = 'M'";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            "Plan(\"Having references column(s) not provided by the select\")",
            format!("{:?}", err)
        );
    }

    #[test]
    fn select_with_having_referencing_column_nested_in_select_expression() {
        let sql = "SELECT id, age + 1
                   FROM person
                   HAVING age > 100";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            "Plan(\"Having references column(s) not provided by the select\")",
            format!("{:?}", err)
        );
    }

    #[test]
    fn select_with_having_with_aggregate_not_in_select() {
        let sql = "SELECT first_name
                   FROM person
                   HAVING MAX(age) > 100";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            "Plan(\"Projection references non-aggregate values\")",
            format!("{:?}", err)
        );
    }

    #[test]
    fn select_aggregate_with_having_that_reuses_aggregate() {
        let sql = "SELECT MAX(age)
                   FROM person
                   HAVING MAX(age) < 30";
        let expected = "Filter: #MAX(age) Lt Int64(30)\
                        \n  Aggregate: groupBy=[[]], aggr=[[MAX(#age)]]\
                        \n    TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_aggregate_with_having_with_aggregate_not_in_select() {
        let sql = "SELECT MAX(age)
                   FROM person
                   HAVING MAX(first_name) > 'M'";
        let expected = "Projection: #MAX(age)\
                        \n  Filter: #MAX(first_name) Gt Utf8(\"M\")\
                        \n    Aggregate: groupBy=[[]], aggr=[[MAX(#age), MAX(#first_name)]]\
                        \n      TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_aggregate_with_having_referencing_column_not_in_select() {
        let sql = "SELECT COUNT(*)
                   FROM person
                   HAVING first_name = 'M'";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            "Plan(\"Having references non-aggregate values\")",
            format!("{:?}", err)
        );
    }

    #[test]
    fn select_aggregate_aliased_with_having_referencing_aggregate_by_its_alias() {
        let sql = "SELECT MAX(age) as max_age
                   FROM person
                   HAVING max_age < 30";
        let expected = "Projection: #MAX(age) AS max_age\
                        \n  Filter: #MAX(age) Lt Int64(30)\
                        \n    Aggregate: groupBy=[[]], aggr=[[MAX(#age)]]\
                        \n      TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_aggregate_aliased_with_having_that_reuses_aggregate_but_not_by_its_alias() {
        let sql = "SELECT MAX(age) as max_age
                   FROM person
                   HAVING MAX(age) < 30";
        let expected = "Projection: #MAX(age) AS max_age\
                        \n  Filter: #MAX(age) Lt Int64(30)\
                        \n    Aggregate: groupBy=[[]], aggr=[[MAX(#age)]]\
                        \n      TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_aggregate_with_group_by_with_having() {
        let sql = "SELECT first_name, MAX(age)
                   FROM person
                   GROUP BY first_name
                   HAVING first_name = 'M'";
        let expected = "Filter: #first_name Eq Utf8(\"M\")\
                        \n  Aggregate: groupBy=[[#first_name]], aggr=[[MAX(#age)]]\
                        \n    TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_aggregate_with_group_by_with_having_and_where() {
        let sql = "SELECT first_name, MAX(age)
                   FROM person
                   WHERE id > 5
                   GROUP BY first_name
                   HAVING MAX(age) < 100";
        let expected = "Filter: #MAX(age) Lt Int64(100)\
                        \n  Aggregate: groupBy=[[#first_name]], aggr=[[MAX(#age)]]\
                        \n    Filter: #id Gt Int64(5)\
                        \n      TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_aggregate_with_group_by_with_having_and_where_filtering_on_aggregate_column(
    ) {
        let sql = "SELECT first_name, MAX(age)
                   FROM person
                   WHERE id > 5 AND age > 18
                   GROUP BY first_name
                   HAVING MAX(age) < 100";
        let expected = "Filter: #MAX(age) Lt Int64(100)\
                        \n  Aggregate: groupBy=[[#first_name]], aggr=[[MAX(#age)]]\
                        \n    Filter: #id Gt Int64(5) And #age Gt Int64(18)\
                        \n      TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_aggregate_with_group_by_with_having_using_column_by_alias() {
        let sql = "SELECT first_name AS fn, MAX(age)
                   FROM person
                   GROUP BY first_name
                   HAVING MAX(age) > 2 AND fn = 'M'";
        let expected = "Projection: #first_name AS fn, #MAX(age)\
                        \n  Filter: #MAX(age) Gt Int64(2) And #first_name Eq Utf8(\"M\")\
                        \n    Aggregate: groupBy=[[#first_name]], aggr=[[MAX(#age)]]\
                        \n      TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_aggregate_with_group_by_with_having_using_columns_with_and_without_their_aliases(
    ) {
        let sql = "SELECT first_name AS fn, MAX(age) AS max_age
                   FROM person
                   GROUP BY first_name
                   HAVING MAX(age) > 2 AND max_age < 5 AND first_name = 'M' AND fn = 'N'";
        let expected = "Projection: #first_name AS fn, #MAX(age) AS max_age\
                        \n  Filter: #MAX(age) Gt Int64(2) And #MAX(age) Lt Int64(5) And #first_name Eq Utf8(\"M\") And #first_name Eq Utf8(\"N\")\
                        \n    Aggregate: groupBy=[[#first_name]], aggr=[[MAX(#age)]]\
                        \n      TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_aggregate_with_group_by_with_having_that_reuses_aggregate() {
        let sql = "SELECT first_name, MAX(age)
                   FROM person
                   GROUP BY first_name
                   HAVING MAX(age) > 100";
        let expected = "Filter: #MAX(age) Gt Int64(100)\
                        \n  Aggregate: groupBy=[[#first_name]], aggr=[[MAX(#age)]]\
                        \n    TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_aggregate_with_group_by_with_having_referencing_column_not_in_group_by() {
        let sql = "SELECT first_name, MAX(age)
                   FROM person
                   GROUP BY first_name
                   HAVING MAX(age) > 10 AND last_name = 'M'";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            "Plan(\"Having references non-aggregate values\")",
            format!("{:?}", err)
        );
    }

    #[test]
    fn select_aggregate_with_group_by_with_having_that_reuses_aggregate_multiple_times() {
        let sql = "SELECT first_name, MAX(age)
                   FROM person
                   GROUP BY first_name
                   HAVING MAX(age) > 100 AND MAX(age) < 200";
        let expected = "Filter: #MAX(age) Gt Int64(100) And #MAX(age) Lt Int64(200)\
                        \n  Aggregate: groupBy=[[#first_name]], aggr=[[MAX(#age)]]\
                        \n    TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_aggregate_with_group_by_with_having_using_aggreagate_not_in_select() {
        let sql = "SELECT first_name, MAX(age)
                   FROM person
                   GROUP BY first_name
                   HAVING MAX(age) > 100 AND MIN(id) < 50";
        let expected = "Projection: #first_name, #MAX(age)\
                        \n  Filter: #MAX(age) Gt Int64(100) And #MIN(id) Lt Int64(50)\
                        \n    Aggregate: groupBy=[[#first_name]], aggr=[[MAX(#age), MIN(#id)]]\
                        \n      TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_aggregate_aliased_with_group_by_with_having_referencing_aggregate_by_its_alias(
    ) {
        let sql = "SELECT first_name, MAX(age) AS max_age
                   FROM person
                   GROUP BY first_name
                   HAVING max_age > 100";
        let expected = "Projection: #first_name, #MAX(age) AS max_age\
                        \n  Filter: #MAX(age) Gt Int64(100)\
                        \n    Aggregate: groupBy=[[#first_name]], aggr=[[MAX(#age)]]\
                        \n      TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_aggregate_compound_aliased_with_group_by_with_having_referencing_compound_aggregate_by_its_alias(
    ) {
        let sql = "SELECT first_name, MAX(age) + 1 AS max_age_plus_one
                   FROM person
                   GROUP BY first_name
                   HAVING max_age_plus_one > 100";
        let expected =
            "Projection: #first_name, #MAX(age) Plus Int64(1) AS max_age_plus_one\
                        \n  Filter: #MAX(age) Plus Int64(1) Gt Int64(100)\
                        \n    Aggregate: groupBy=[[#first_name]], aggr=[[MAX(#age)]]\
                        \n      TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_aggregate_with_group_by_with_having_using_derived_column_aggreagate_not_in_select(
    ) {
        let sql = "SELECT first_name, MAX(age)
                   FROM person
                   GROUP BY first_name
                   HAVING MAX(age) > 100 AND MIN(id - 2) < 50";
        let expected = "Projection: #first_name, #MAX(age)\
                        \n  Filter: #MAX(age) Gt Int64(100) And #MIN(id Minus Int64(2)) Lt Int64(50)\
                        \n    Aggregate: groupBy=[[#first_name]], aggr=[[MAX(#age), MIN(#id Minus Int64(2))]]\
                        \n      TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_aggregate_with_group_by_with_having_using_count_star_not_in_select() {
        let sql = "SELECT first_name, MAX(age)
                   FROM person
                   GROUP BY first_name
                   HAVING MAX(age) > 100 AND COUNT(*) < 50";
        let expected = "Projection: #first_name, #MAX(age)\
                        \n  Filter: #MAX(age) Gt Int64(100) And #COUNT(UInt8(1)) Lt Int64(50)\
                        \n    Aggregate: groupBy=[[#first_name]], aggr=[[MAX(#age), COUNT(UInt8(1))]]\
                        \n      TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_binary_expr() {
        let sql = "SELECT age + salary from person";
        let expected = "Projection: #age Plus #salary\
                        \n  TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_binary_expr_nested() {
        let sql = "SELECT (age + salary)/2 from person";
        let expected = "Projection: #age Plus #salary Divide Int64(2)\
                        \n  TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_wildcard_with_groupby() {
        quick_test(
            "SELECT * FROM person GROUP BY id, first_name, last_name, age, state, salary, birth_date",
            "Aggregate: groupBy=[[#id, #first_name, #last_name, #age, #state, #salary, #birth_date]], aggr=[[]]\
             \n  TableScan: person projection=None",
        );
        quick_test(
            "SELECT * FROM (SELECT first_name, last_name FROM person) GROUP BY first_name, last_name",
            "Aggregate: groupBy=[[#first_name, #last_name]], aggr=[[]]\
             \n  Projection: #first_name, #last_name\
             \n    TableScan: person projection=None",
        );
    }

    #[test]
    fn select_simple_aggregate() {
        quick_test(
            "SELECT MIN(age) FROM person",
            "Aggregate: groupBy=[[]], aggr=[[MIN(#age)]]\
             \n  TableScan: person projection=None",
        );
    }

    #[test]
    fn test_sum_aggregate() {
        quick_test(
            "SELECT SUM(age) from person",
            "Aggregate: groupBy=[[]], aggr=[[SUM(#age)]]\
             \n  TableScan: person projection=None",
        );
    }

    #[test]
    fn select_simple_aggregate_column_does_not_exist() {
        let sql = "SELECT MIN(doesnotexist) FROM person";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            format!(
                "Plan(\"Invalid identifier \\\'doesnotexist\\\' for schema {}\")",
                PERSON_COLUMN_NAMES
            ),
            format!("{:?}", err)
        );
    }

    #[test]
    fn select_simple_aggregate_repeated_aggregate() {
        let sql = "SELECT MIN(age), MIN(age) FROM person";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            "Plan(\"Projections require unique expression names but the expression \\\"#MIN(age)\\\" at position 0 and \\\"#MIN(age)\\\" at position 1 have the same name. Consider aliasing (\\\"AS\\\") one of them.\")",
            format!("{:?}", err)
        );
    }

    #[test]
    fn select_simple_aggregate_repeated_aggregate_with_single_alias() {
        quick_test(
            "SELECT MIN(age), MIN(age) AS a FROM person",
            "Projection: #MIN(age), #MIN(age) AS a\
             \n  Aggregate: groupBy=[[]], aggr=[[MIN(#age)]]\
             \n    TableScan: person projection=None",
        );
    }

    #[test]
    fn select_simple_aggregate_repeated_aggregate_with_unique_aliases() {
        quick_test(
            "SELECT MIN(age) AS a, MIN(age) AS b FROM person",
            "Projection: #MIN(age) AS a, #MIN(age) AS b\
             \n  Aggregate: groupBy=[[]], aggr=[[MIN(#age)]]\
             \n    TableScan: person projection=None",
        );
    }

    #[test]
    fn select_simple_aggregate_repeated_aggregate_with_repeated_aliases() {
        let sql = "SELECT MIN(age) AS a, MIN(age) AS a FROM person";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            "Plan(\"Projections require unique expression names but the expression \\\"#MIN(age) AS a\\\" at position 0 and \\\"#MIN(age) AS a\\\" at position 1 have the same name. Consider aliasing (\\\"AS\\\") one of them.\")",
            format!("{:?}", err)
        );
    }

    #[test]
    fn select_simple_aggregate_with_groupby() {
        quick_test(
            "SELECT state, MIN(age), MAX(age) FROM person GROUP BY state",
            "Aggregate: groupBy=[[#state]], aggr=[[MIN(#age), MAX(#age)]]\
             \n  TableScan: person projection=None",
        );
    }

    #[test]
    fn select_simple_aggregate_with_groupby_with_aliases() {
        quick_test(
            "SELECT state AS a, MIN(age) AS b FROM person GROUP BY state",
            "Projection: #state AS a, #MIN(age) AS b\
             \n  Aggregate: groupBy=[[#state]], aggr=[[MIN(#age)]]\
             \n    TableScan: person projection=None",
        );
    }

    #[test]
    fn select_simple_aggregate_with_groupby_with_aliases_repeated() {
        let sql = "SELECT state AS a, MIN(age) AS a FROM person GROUP BY state";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            "Plan(\"Projections require unique expression names but the expression \\\"#state AS a\\\" at position 0 and \\\"#MIN(age) AS a\\\" at position 1 have the same name. Consider aliasing (\\\"AS\\\") one of them.\")",
            format!("{:?}", err)
        );
    }

    #[test]
    fn select_simple_aggregate_with_groupby_column_unselected() {
        quick_test(
            "SELECT MIN(age), MAX(age) FROM person GROUP BY state",
            "Projection: #MIN(age), #MAX(age)\
             \n  Aggregate: groupBy=[[#state]], aggr=[[MIN(#age), MAX(#age)]]\
             \n    TableScan: person projection=None",
        );
    }

    #[test]
    fn select_simple_aggregate_with_groupby_and_column_in_group_by_does_not_exist() {
        let sql = "SELECT SUM(age) FROM person GROUP BY doesnotexist";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            format!(
                "Plan(\"Invalid identifier \\\'doesnotexist\\\' for schema {}\")",
                PERSON_COLUMN_NAMES
            ),
            format!("{:?}", err)
        );
    }

    #[test]
    fn select_simple_aggregate_with_groupby_and_column_in_aggregate_does_not_exist() {
        let sql = "SELECT SUM(doesnotexist) FROM person GROUP BY first_name";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            format!(
                "Plan(\"Invalid identifier \\\'doesnotexist\\\' for schema {}\")",
                PERSON_COLUMN_NAMES
            ),
            format!("{:?}", err)
        );
    }

    #[test]
    fn select_interval_out_of_range() {
        let sql = "SELECT INTERVAL '100000000000000000 day'";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            "NotImplemented(\"Interval field value out of range: \\\"100000000000000000 day\\\"\")",
            format!("{:?}", err)
        );
    }

    #[test]
    fn select_unsupported_complex_interval() {
        let sql = "SELECT INTERVAL '1 year 1 day'";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            "NotImplemented(\"DF does not support intervals that have both a Year/Month part as well as Days/Hours/Mins/Seconds: \\\"1 year 1 day\\\". Hint: try breaking the interval into two parts, one with Year/Month and the other with Days/Hours/Mins/Seconds - e.g. (NOW() + INTERVAL \\\'1 year\\\') + INTERVAL \\\'1 day\\\'\")",
            format!("{:?}", err)
        );
    }

    #[test]
    fn select_simple_aggregate_with_groupby_and_column_is_in_aggregate_and_groupby() {
        quick_test(
            "SELECT MAX(first_name) FROM person GROUP BY first_name",
            "Projection: #MAX(first_name)\
             \n  Aggregate: groupBy=[[#first_name]], aggr=[[MAX(#first_name)]]\
             \n    TableScan: person projection=None",
        );
    }

    #[test]
    fn select_simple_aggregate_with_groupby_cannot_use_alias() {
        let sql = "SELECT state AS x, MAX(age) FROM person GROUP BY x";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            format!(
                "Plan(\"Invalid identifier \\\'x\\\' for schema {}\")",
                PERSON_COLUMN_NAMES
            ),
            format!("{:?}", err)
        );
    }

    #[test]
    fn select_simple_aggregate_with_groupby_aggregate_repeated() {
        let sql = "SELECT state, MIN(age), MIN(age) FROM person GROUP BY state";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            "Plan(\"Projections require unique expression names but the expression \\\"#MIN(age)\\\" at position 1 and \\\"#MIN(age)\\\" at position 2 have the same name. Consider aliasing (\\\"AS\\\") one of them.\")",
            format!("{:?}", err)
        );
    }

    #[test]
    fn select_simple_aggregate_with_groupby_aggregate_repeated_and_one_has_alias() {
        quick_test(
            "SELECT state, MIN(age), MIN(age) AS ma FROM person GROUP BY state",
            "Projection: #state, #MIN(age), #MIN(age) AS ma\
             \n  Aggregate: groupBy=[[#state]], aggr=[[MIN(#age)]]\
             \n    TableScan: person projection=None",
        )
    }
    #[test]
    fn select_simple_aggregate_with_groupby_non_column_expression_unselected() {
        quick_test(
            "SELECT MIN(first_name) FROM person GROUP BY age + 1",
            "Projection: #MIN(first_name)\
             \n  Aggregate: groupBy=[[#age Plus Int64(1)]], aggr=[[MIN(#first_name)]]\
             \n    TableScan: person projection=None",
        );
    }

    #[test]
    fn select_simple_aggregate_with_groupby_non_column_expression_selected_and_resolvable(
    ) {
        quick_test(
            "SELECT age + 1, MIN(first_name) FROM person GROUP BY age + 1",
            "Aggregate: groupBy=[[#age Plus Int64(1)]], aggr=[[MIN(#first_name)]]\
             \n  TableScan: person projection=None",
        );
        quick_test(
            "SELECT MIN(first_name), age + 1 FROM person GROUP BY age + 1",
            "Projection: #MIN(first_name), #age Plus Int64(1)\
             \n  Aggregate: groupBy=[[#age Plus Int64(1)]], aggr=[[MIN(#first_name)]]\
             \n    TableScan: person projection=None",
        );
    }

    #[test]
    fn select_simple_aggregate_with_groupby_non_column_expression_nested_and_resolvable()
    {
        quick_test(
            "SELECT ((age + 1) / 2) * (age + 1), MIN(first_name) FROM person GROUP BY age + 1",
            "Projection: #age Plus Int64(1) Divide Int64(2) Multiply #age Plus Int64(1), #MIN(first_name)\
             \n  Aggregate: groupBy=[[#age Plus Int64(1)]], aggr=[[MIN(#first_name)]]\
             \n    TableScan: person projection=None",
        );
    }

    #[test]
    fn select_simple_aggregate_with_groupby_non_column_expression_nested_and_not_resolvable(
    ) {
        // The query should fail, because age + 9 is not in the group by.
        let sql =
            "SELECT ((age + 1) / 2) * (age + 9), MIN(first_name) FROM person GROUP BY age + 1";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            "Plan(\"Projection references non-aggregate values\")",
            format!("{:?}", err)
        );
    }

    #[test]
    fn select_simple_aggregate_with_groupby_non_column_expression_and_its_column_selected(
    ) {
        let sql = "SELECT age, MIN(first_name) FROM person GROUP BY age + 1";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            "Plan(\"Projection references non-aggregate values\")",
            format!("{:?}", err)
        );
    }

    #[test]
    fn select_simple_aggregate_nested_in_binary_expr_with_groupby() {
        quick_test(
            "SELECT state, MIN(age) < 10 FROM person GROUP BY state",
            "Projection: #state, #MIN(age) Lt Int64(10)\
             \n  Aggregate: groupBy=[[#state]], aggr=[[MIN(#age)]]\
             \n    TableScan: person projection=None",
        );
    }

    #[test]
    fn select_simple_aggregate_and_nested_groupby_column() {
        quick_test(
            "SELECT age + 1, MAX(first_name) FROM person GROUP BY age",
            "Projection: #age Plus Int64(1), #MAX(first_name)\
             \n  Aggregate: groupBy=[[#age]], aggr=[[MAX(#first_name)]]\
             \n    TableScan: person projection=None",
        );
    }

    #[test]
    fn select_aggregate_compounded_with_groupby_column() {
        quick_test(
            "SELECT age + MIN(salary) FROM person GROUP BY age",
            "Projection: #age Plus #MIN(salary)\
             \n  Aggregate: groupBy=[[#age]], aggr=[[MIN(#salary)]]\
             \n    TableScan: person projection=None",
        );
    }

    #[test]
    fn select_aggregate_with_non_column_inner_expression_with_groupby() {
        quick_test(
            "SELECT state, MIN(age + 1) FROM person GROUP BY state",
            "Aggregate: groupBy=[[#state]], aggr=[[MIN(#age Plus Int64(1))]]\
             \n  TableScan: person projection=None",
        );
    }

    #[test]
    fn test_wildcard() {
        quick_test(
            "SELECT * from person",
            "Projection: #id, #first_name, #last_name, #age, #state, #salary, #birth_date\
            \n  TableScan: person projection=None",
        );
    }

    #[test]
    fn select_count_one() {
        let sql = "SELECT COUNT(1) FROM person";
        let expected = "Aggregate: groupBy=[[]], aggr=[[COUNT(UInt8(1))]]\
                        \n  TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_count_column() {
        let sql = "SELECT COUNT(id) FROM person";
        let expected = "Aggregate: groupBy=[[]], aggr=[[COUNT(#id)]]\
                        \n  TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_scalar_func() {
        let sql = "SELECT sqrt(age) FROM person";
        let expected = "Projection: sqrt(#age)\
                        \n  TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_aliased_scalar_func() {
        let sql = "SELECT sqrt(age) AS square_people FROM person";
        let expected = "Projection: sqrt(#age) AS square_people\
                        \n  TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_where_nullif_division() {
        let sql = "SELECT c3/(c4+c5) \
                   FROM aggregate_test_100 WHERE c3/nullif(c4+c5, 0) > 0.1";
        let expected = "Projection: #c3 Divide #c4 Plus #c5\
            \n  Filter: #c3 Divide nullif(#c4 Plus #c5, Int64(0)) Gt Float64(0.1)\
            \n    TableScan: aggregate_test_100 projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_where_with_negative_operator() {
        let sql = "SELECT c3 FROM aggregate_test_100 WHERE c3 > -0.1 AND -c4 > 0";
        let expected = "Projection: #c3\
            \n  Filter: #c3 Gt Float64(-0.1) And (- #c4) Gt Int64(0)\
            \n    TableScan: aggregate_test_100 projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_where_with_positive_operator() {
        let sql = "SELECT c3 FROM aggregate_test_100 WHERE c3 > +0.1 AND +c4 > 0";
        let expected = "Projection: #c3\
            \n  Filter: #c3 Gt Float64(0.1) And #c4 Gt Int64(0)\
            \n    TableScan: aggregate_test_100 projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_order_by() {
        let sql = "SELECT id FROM person ORDER BY id";
        let expected = "Sort: #id ASC NULLS FIRST\
                        \n  Projection: #id\
                        \n    TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_order_by_desc() {
        let sql = "SELECT id FROM person ORDER BY id DESC";
        let expected = "Sort: #id DESC NULLS FIRST\
                        \n  Projection: #id\
                        \n    TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_order_by_nulls_last() {
        quick_test(
            "SELECT id FROM person ORDER BY id DESC NULLS LAST",
            "Sort: #id DESC NULLS LAST\
            \n  Projection: #id\
            \n    TableScan: person projection=None",
        );

        quick_test(
            "SELECT id FROM person ORDER BY id NULLS LAST",
            "Sort: #id ASC NULLS LAST\
            \n  Projection: #id\
            \n    TableScan: person projection=None",
        );
    }

    #[test]
    fn select_group_by() {
        let sql = "SELECT state FROM person GROUP BY state";
        let expected = "Aggregate: groupBy=[[#state]], aggr=[[]]\
                        \n  TableScan: person projection=None";

        quick_test(sql, expected);
    }

    #[test]
    fn select_group_by_columns_not_in_select() {
        let sql = "SELECT MAX(age) FROM person GROUP BY state";
        let expected = "Projection: #MAX(age)\
                        \n  Aggregate: groupBy=[[#state]], aggr=[[MAX(#age)]]\
                        \n    TableScan: person projection=None";

        quick_test(sql, expected);
    }

    #[test]
    fn select_group_by_count_star() {
        let sql = "SELECT state, COUNT(*) FROM person GROUP BY state";
        let expected = "Aggregate: groupBy=[[#state]], aggr=[[COUNT(UInt8(1))]]\
                        \n  TableScan: person projection=None";

        quick_test(sql, expected);
    }

    #[test]
    fn select_group_by_needs_projection() {
        let sql = "SELECT COUNT(state), state FROM person GROUP BY state";
        let expected = "\
        Projection: #COUNT(state), #state\
        \n  Aggregate: groupBy=[[#state]], aggr=[[COUNT(#state)]]\
        \n    TableScan: person projection=None";

        quick_test(sql, expected);
    }

    #[test]
    fn select_7480_1() {
        let sql = "SELECT c1, MIN(c12) FROM aggregate_test_100 GROUP BY c1, c13";
        let expected = "Projection: #c1, #MIN(c12)\
                       \n  Aggregate: groupBy=[[#c1, #c13]], aggr=[[MIN(#c12)]]\
                       \n    TableScan: aggregate_test_100 projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_7480_2() {
        let sql = "SELECT c1, c13, MIN(c12) FROM aggregate_test_100 GROUP BY c1";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            "Plan(\"Projection references non-aggregate values\")",
            format!("{:?}", err)
        );
    }

    #[test]
    fn create_external_table_csv() {
        let sql = "CREATE EXTERNAL TABLE t(c1 int) STORED AS CSV LOCATION 'foo.csv'";
        let expected = "CreateExternalTable: \"t\"";
        quick_test(sql, expected);
    }

    #[test]
    fn create_external_table_csv_no_schema() {
        let sql = "CREATE EXTERNAL TABLE t STORED AS CSV LOCATION 'foo.csv'";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            "Plan(\"Column definitions required for CSV files. None found\")",
            format!("{:?}", err)
        );
    }

    #[test]
    fn create_external_table_parquet() {
        let sql =
            "CREATE EXTERNAL TABLE t(c1 int) STORED AS PARQUET LOCATION 'foo.parquet'";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            "Plan(\"Column definitions can not be specified for PARQUET files.\")",
            format!("{:?}", err)
        );
    }

    #[test]
    fn create_external_table_parquet_no_schema() {
        let sql = "CREATE EXTERNAL TABLE t STORED AS PARQUET LOCATION 'foo.parquet'";
        let expected = "CreateExternalTable: \"t\"";
        quick_test(sql, expected);
    }

    #[test]
    fn equijoin_explicit_syntax() {
        let sql = "SELECT id, order_id \
            FROM person \
            JOIN orders \
            ON id = customer_id";
        let expected = "Projection: #id, #order_id\
        \n  Join: id = customer_id\
        \n    TableScan: person projection=None\
        \n    TableScan: orders projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn equijoin_explicit_syntax_3_tables() {
        let sql = "SELECT id, order_id, l_description \
            FROM person \
            JOIN orders ON id = customer_id \
            JOIN lineitem ON o_item_id = l_item_id";
        let expected = "Projection: #id, #order_id, #l_description\
            \n  Join: o_item_id = l_item_id\
            \n    Join: id = customer_id\
            \n      TableScan: person projection=None\
            \n      TableScan: orders projection=None\
            \n    TableScan: lineitem projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn boolean_literal_in_condition_expression() {
        let sql = "SELECT order_id \
        FROM orders \
        WHERE delivered = false OR delivered = true";
        let expected = "Projection: #order_id\
            \n  Filter: #delivered Eq Boolean(false) Or #delivered Eq Boolean(true)\
            \n    TableScan: orders projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn union() {
        let sql = "SELECT order_id from orders UNION ALL SELECT order_id FROM orders";
        let expected = "Union\
            \n  Projection: #order_id\
            \n    TableScan: orders projection=None\
            \n  Projection: #order_id\
            \n    TableScan: orders projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn union_4_combined_in_one() {
        let sql = "SELECT order_id from orders
                    UNION ALL SELECT order_id FROM orders
                    UNION ALL SELECT order_id FROM orders
                    UNION ALL SELECT order_id FROM orders";
        let expected = "Union\
            \n  Projection: #order_id\
            \n    TableScan: orders projection=None\
            \n  Projection: #order_id\
            \n    TableScan: orders projection=None\
            \n  Projection: #order_id\
            \n    TableScan: orders projection=None\
            \n  Projection: #order_id\
            \n    TableScan: orders projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn union_schemas_should_be_same() {
        let sql = "SELECT order_id from orders UNION ALL SELECT customer_id FROM orders";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            "Plan(\"UNION ALL schemas are expected to be the same\")",
            format!("{:?}", err)
        );
    }

    #[test]
    fn only_union_all_supported() {
        let sql = "SELECT order_id from orders EXCEPT SELECT order_id FROM orders";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            "NotImplemented(\"Only UNION ALL is supported, found EXCEPT\")",
            format!("{:?}", err)
        );
    }

    #[test]
    fn select_typedstring() {
        let sql = "SELECT date '2020-12-10' AS date FROM person";
        let expected = "Projection: CAST(Utf8(\"2020-12-10\") AS Date32) AS date\
            \n  TableScan: person projection=None";
        quick_test(sql, expected);
    }

    fn logical_plan(sql: &str) -> Result<LogicalPlan> {
        let planner = SqlToRel::new(&MockContextProvider {});
        let result = DFParser::parse_sql(&sql);
        let ast = result.unwrap();
        planner.statement_to_plan(&ast[0])
    }

    /// Create logical plan, write with formatter, compare to expected output
    fn quick_test(sql: &str, expected: &str) {
        let plan = logical_plan(sql).unwrap();
        assert_eq!(expected, format!("{:?}", plan));
    }

    struct MockContextProvider {}

    impl ContextProvider for MockContextProvider {
        fn get_table_provider(
            &self,
            name: TableReference,
        ) -> Option<Arc<dyn TableProvider>> {
            let schema = match name.table() {
                "person" => Some(Schema::new(vec![
                    Field::new("id", DataType::UInt32, false),
                    Field::new("first_name", DataType::Utf8, false),
                    Field::new("last_name", DataType::Utf8, false),
                    Field::new("age", DataType::Int32, false),
                    Field::new("state", DataType::Utf8, false),
                    Field::new("salary", DataType::Float64, false),
                    Field::new(
                        "birth_date",
                        DataType::Timestamp(TimeUnit::Nanosecond, None),
                        false,
                    ),
                ])),
                "orders" => Some(Schema::new(vec![
                    Field::new("order_id", DataType::UInt32, false),
                    Field::new("customer_id", DataType::UInt32, false),
                    Field::new("o_item_id", DataType::Utf8, false),
                    Field::new("qty", DataType::Int32, false),
                    Field::new("price", DataType::Float64, false),
                    Field::new("delivered", DataType::Boolean, false),
                ])),
                "lineitem" => Some(Schema::new(vec![
                    Field::new("l_item_id", DataType::UInt32, false),
                    Field::new("l_description", DataType::Utf8, false),
                ])),
                "aggregate_test_100" => Some(Schema::new(vec![
                    Field::new("c1", DataType::Utf8, false),
                    Field::new("c2", DataType::UInt32, false),
                    Field::new("c3", DataType::Int8, false),
                    Field::new("c4", DataType::Int16, false),
                    Field::new("c5", DataType::Int32, false),
                    Field::new("c6", DataType::Int64, false),
                    Field::new("c7", DataType::UInt8, false),
                    Field::new("c8", DataType::UInt16, false),
                    Field::new("c9", DataType::UInt32, false),
                    Field::new("c10", DataType::UInt64, false),
                    Field::new("c11", DataType::Float32, false),
                    Field::new("c12", DataType::Float64, false),
                    Field::new("c13", DataType::Utf8, false),
                ])),
                _ => None,
            };
            schema.map(|s| -> Arc<dyn TableProvider> {
                Arc::new(EmptyTable::new(Arc::new(s)))
            })
        }

        fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
            let f: ScalarFunctionImplementation =
                Arc::new(|_| Err(DataFusionError::NotImplemented("".to_string())));
            match name {
                "my_sqrt" => Some(Arc::new(create_udf(
                    "my_sqrt",
                    vec![DataType::Float64],
                    Arc::new(DataType::Float64),
                    f,
                ))),
                _ => None,
            }
        }

        fn get_aggregate_meta(&self, _name: &str) -> Option<Arc<AggregateUDF>> {
            unimplemented!()
        }
    }
}
