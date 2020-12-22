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

use std::str::FromStr;
use std::sync::Arc;

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

use super::parser::ExplainPlan;
use crate::prelude::JoinType;
use sqlparser::ast::{
    BinaryOperator, DataType as SQLDataType, Expr as SQLExpr, Join, JoinConstraint,
    JoinOperator, Query, Select, SelectItem, SetExpr, TableFactor, TableWithJoins,
    UnaryOperator, Value,
};
use sqlparser::ast::{ColumnDef as SQLColumnDef, ColumnOption};
use sqlparser::ast::{OrderByExpr, Statement};
use sqlparser::parser::ParserError::ParserError;

use super::utils::{
    can_columns_satisfy_exprs, expand_wildcard, expr_as_column_expr,
    find_aggregate_exprs, find_column_exprs, rebase_expr,
};

/// The ContextProvider trait allows the query planner to obtain meta-data about tables and
/// functions referenced in SQL statements
pub trait ContextProvider {
    /// Getter for a datasource
    fn get_table_provider(
        &self,
        name: &str,
    ) -> Option<Arc<dyn TableProvider + Send + Sync>>;
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
            DFStatement::Explain(s) => self.explain_statement_to_plan(&(*s)),
        }
    }

    /// Generate a logical plan from an SQL statement
    pub fn sql_statement_to_plan(&self, sql: &Statement) -> Result<LogicalPlan> {
        match sql {
            Statement::Query(query) => self.query_to_plan(&query),
            _ => Err(DataFusionError::NotImplemented(
                "Only SELECT statements are implemented".to_string(),
            )),
        }
    }

    /// Generate a logic plan from an SQL query
    pub fn query_to_plan(&self, query: &Query) -> Result<LogicalPlan> {
        let plan = match &query.body {
            SetExpr::Select(s) => self.select_to_plan(s.as_ref()),
            _ => Err(DataFusionError::NotImplemented(format!(
                "Query {} not implemented yet",
                query.body
            ))),
        }?;

        let plan = self.order_by(&plan, &query.order_by)?;

        self.limit(&plan, &query.limit)
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
        explain_plan: &ExplainPlan,
    ) -> Result<LogicalPlan> {
        let verbose = explain_plan.verbose;
        let plan = self.statement_to_plan(&explain_plan.statement)?;

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

    fn build_schema(&self, columns: &Vec<SQLColumnDef>) -> Result<Schema> {
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
            SQLDataType::Date => Ok(DataType::Date32(DateUnit::Day)),
            SQLDataType::Time => Ok(DataType::Time64(TimeUnit::Millisecond)),
            SQLDataType::Timestamp => Ok(DataType::Date64(DateUnit::Millisecond)),
            _ => Err(DataFusionError::NotImplemented(format!(
                "The SQL data type {:?} is not implemented",
                sql_type
            ))),
        }
    }

    fn plan_from_tables(&self, from: &Vec<TableWithJoins>) -> Result<Vec<LogicalPlan>> {
        match from.len() {
            0 => Ok(vec![LogicalPlanBuilder::empty(true).build()?]),
            _ => from
                .iter()
                .map(|t| self.plan_table_with_joins(t))
                .collect::<Result<Vec<_>>>(),
        }
    }

    fn plan_table_with_joins(&self, t: &TableWithJoins) -> Result<LogicalPlan> {
        let left = self.create_relation(&t.relation)?;
        match t.joins.len() {
            0 => Ok(left),
            n => {
                let mut left = self.parse_relation_join(&left, &t.joins[0])?;
                for i in 1..n {
                    left = self.parse_relation_join(&left, &t.joins[i])?;
                }
                Ok(left)
            }
        }
    }

    fn parse_relation_join(
        &self,
        left: &LogicalPlan,
        join: &Join,
    ) -> Result<LogicalPlan> {
        let right = self.create_relation(&join.relation)?;
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
        }
    }

    fn create_relation(&self, relation: &TableFactor) -> Result<LogicalPlan> {
        match relation {
            TableFactor::Table { name, .. } => {
                let table_name = name.to_string();
                match self.schema_provider.get_table_provider(&table_name) {
                    Some(provider) => {
                        LogicalPlanBuilder::scan(&table_name, provider, None)?.build()
                    }
                    None => Err(DataFusionError::Plan(format!(
                        "no provider found for table {}",
                        name
                    ))),
                }
            }
            TableFactor::Derived { subquery, .. } => self.query_to_plan(subquery),
            TableFactor::NestedJoin(table_with_joins) => {
                self.plan_table_with_joins(table_with_joins)
            }
        }
    }

    /// Generate a logic plan from an SQL select
    fn select_to_plan(&self, select: &Select) -> Result<LogicalPlan> {
        if select.having.is_some() {
            return Err(DataFusionError::NotImplemented(
                "HAVING is not implemented yet".to_string(),
            ));
        }

        let plans = self.plan_from_tables(&select.from)?;

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
                for i in 1..plans.len() {
                    let right = &plans[i];
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

        // All of the aggregate expressions (deduplicated).
        let aggr_exprs = find_aggregate_exprs(&select_exprs);

        let (plan, select_exprs_post_aggr) =
            if !select.group_by.is_empty() || !aggr_exprs.is_empty() {
                self.aggregate(&plan, &select_exprs, &select.group_by, &aggr_exprs)?
            } else {
                (plan, select_exprs)
            };

        self.project(&plan, select_exprs_post_aggr, false)
    }

    /// Returns the `Expr`'s corresponding to a SQL query's SELECT expressions.
    ///
    /// Wildcards are expanded into the concrete list of columns.
    fn prepare_select_exprs(
        &self,
        plan: &LogicalPlan,
        projection: &Vec<SelectItem>,
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
        select_exprs: &Vec<Expr>,
        group_by: &Vec<SQLExpr>,
        aggr_exprs: &Vec<Expr>,
    ) -> Result<(LogicalPlan, Vec<Expr>)> {
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
            .aggregate(group_by_exprs, aggr_exprs.clone())?
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

        Ok((plan, select_exprs_post_aggr))
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
        order_by: &Vec<OrderByExpr>,
    ) -> Result<LogicalPlan> {
        if order_by.is_empty() {
            return Ok(plan.clone());
        }

        let input_schema = plan.schema();
        let order_by_rex: Result<Vec<Expr>> = order_by
            .iter()
            .map(|e| {
                Ok(Expr::Sort {
                    expr: Box::new(self.sql_to_rex(&e.expr, &input_schema).unwrap()),
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
        exprs: &Vec<Expr>,
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
        self.validate_schema_satisfies_exprs(schema, &vec![expr.clone()])?;
        Ok(expr)
    }

    fn sql_expr_to_logical_expr(&self, sql: &SQLExpr) -> Result<Expr> {
        match sql {
            SQLExpr::Value(Value::Number(n)) => match n.parse::<i64>() {
                Ok(n) => Ok(lit(n)),
                Err(_) => Ok(lit(n.parse::<f64>().unwrap())),
            },
            SQLExpr::Value(Value::SingleQuotedString(ref s)) => Ok(lit(s.clone())),

            SQLExpr::Value(Value::Null) => Ok(Expr::Literal(ScalarValue::Utf8(None))),

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
                for i in 0..ids.len() {
                    let id = ids[i].clone();
                    var_names.push(id.value);
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
                        SQLExpr::Value(Value::Number(n)) => match n.parse::<i64>() {
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
                let name: String = function.name.to_string();

                // first, scalar built-in
                if let Ok(fun) = functions::BuiltinScalarFunction::from_str(&name) {
                    let args = function
                        .args
                        .iter()
                        .map(|a| self.sql_expr_to_logical_expr(a))
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
                                SQLExpr::Value(Value::Number(_)) => Ok(lit(1_u8)),
                                SQLExpr::Wildcard => Ok(lit(1_u8)),
                                _ => self.sql_expr_to_logical_expr(a),
                            })
                            .collect::<Result<Vec<Expr>>>()?
                    } else {
                        function
                            .args
                            .iter()
                            .map(|a| self.sql_expr_to_logical_expr(a))
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
                            .map(|a| self.sql_expr_to_logical_expr(a))
                            .collect::<Result<Vec<Expr>>>()?;

                        Ok(Expr::ScalarUDF { fun: fm, args })
                    }
                    None => match self.schema_provider.get_aggregate_meta(&name) {
                        Some(fm) => {
                            let args = function
                                .args
                                .iter()
                                .map(|a| self.sql_expr_to_logical_expr(a))
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
        SQLDataType::Date => Ok(DataType::Date32(DateUnit::Day)),
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
        let sql = "SELECT state FROM person WHERE birth_date < CAST (158412331400600000 as timestamp)";

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
            \n  Filter: #birth_date Lt CAST(Utf8(\"2020-01-01\") AS Date32(Day))\
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
        let sql = "SELECT ((age + 1) / 2) * (age + 9), MIN(first_name) FROM person GROUP BY age + 1";
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
    fn select_typedstring() {
        let sql = "SELECT date '2020-12-10' AS date FROM person";
        let expected = "Projection: CAST(Utf8(\"2020-12-10\") AS Date32(Day)) AS date\
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
            name: &str,
        ) -> Option<Arc<dyn TableProvider + Send + Sync>> {
            let schema = match name {
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
            schema.map(|s| -> Arc<dyn TableProvider + Send + Sync> {
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
