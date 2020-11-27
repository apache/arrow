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

use crate::logical_plan::Expr::Alias;
use crate::logical_plan::{
    and, lit, Expr, LogicalPlan, LogicalPlanBuilder, Operator, PlanType, StringifiedPlan,
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

/// The SchemaProvider trait allows the query planner to obtain meta-data about tables and
/// functions referenced in SQL statements
pub trait SchemaProvider {
    /// Getter for a field description
    fn get_table_meta(&self, name: &str) -> Option<SchemaRef>;
    /// Getter for a UDF description
    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>>;
    /// Getter for a UDAF description
    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>>;
}

/// SQL query planner
pub struct SqlToRel<'a, S: SchemaProvider> {
    schema_provider: &'a S,
}

impl<'a, S: SchemaProvider> SqlToRel<'a, S> {
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

        let schema = SchemaRef::new(self.build_schema(&columns)?);

        Ok(LogicalPlan::CreateExternalTable {
            schema,
            name: name.clone(),
            location: location.clone(),
            file_type: file_type.clone(),
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
            schema,
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
            SQLDataType::Date => Ok(DataType::Date64(DateUnit::Day)),
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
            JoinOperator::Inner(constraint) => {
                match constraint {
                    JoinConstraint::On(sql_expr) => {
                        let mut keys: Vec<(String, String)> = vec![];
                        let join_schema =
                            create_join_schema(left.schema(), &right.schema())?;

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
                            .join(&right, JoinType::Inner, &left_keys, &right_keys)?
                            .build()
                    }
                    JoinConstraint::Using(_) => {
                        // https://issues.apache.org/jira/browse/ARROW-10728
                        Err(DataFusionError::NotImplemented(
                            "JOIN with USING is not supported (https://issues.apache.org/jira/browse/ARROW-10728)".to_string(),
                        ))
                    }
                    JoinConstraint::Natural => {
                        // https://issues.apache.org/jira/browse/ARROW-10727
                        Err(DataFusionError::NotImplemented(
                            "NATURAL JOIN is not supported (https://issues.apache.org/jira/browse/ARROW-10727)".to_string(),
                        ))
                    }
                }
            }
            other => Err(DataFusionError::NotImplemented(format!(
                "Unsupported JOIN operator {:?}",
                other
            ))),
        }
    }

    fn create_relation(&self, relation: &TableFactor) -> Result<LogicalPlan> {
        match relation {
            TableFactor::Table { name, .. } => {
                let table_name = name.to_string();
                match self.schema_provider.get_table_meta(&table_name) {
                    Some(schema) => LogicalPlanBuilder::scan(
                        "default",
                        &table_name,
                        schema.as_ref(),
                        None,
                    )?
                    .build(),
                    None => Err(DataFusionError::Plan(format!(
                        "no schema found for table {}",
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
                    let schema = plan.schema();
                    for field in schema.fields() {
                        fields.push(field.clone());
                    }
                }
                let join_schema = Schema::new(fields);

                let filter_expr = self.sql_to_rex(predicate_expr, &join_schema)?;

                // look for expressions of the form `<column> = <column>`
                let mut possible_join_keys = vec![];
                extract_possible_join_keys(&filter_expr, &mut possible_join_keys)?;

                let mut join_keys = vec![];

                let mut left = plans[0].clone();
                for i in 1..plans.len() {
                    let right = &plans[i];
                    let left_schema = left.schema();
                    let right_schema = right.schema();
                    let mut left_keys = vec![];
                    let mut right_keys = vec![];
                    for (l, r) in &possible_join_keys {
                        if left_schema.field_with_name(l).is_ok()
                            && right_schema.field_with_name(r).is_ok()
                        {
                            left_keys.push(l.as_str());
                            right_keys.push(r.as_str());
                            join_keys.push((l, r));
                        } else if left_schema.field_with_name(r).is_ok()
                            && right_schema.field_with_name(l).is_ok()
                        {
                            left_keys.push(r.as_str());
                            right_keys.push(l.as_str());
                            join_keys.push((r, l));
                        }
                    }
                    if left_keys.len() == 0 {
                        return Err(DataFusionError::NotImplemented(
                            "Cartesian joins are not supported".to_string(),
                        ));
                    } else {
                        let builder = LogicalPlanBuilder::from(&left);
                        left = builder
                            .join(right, JoinType::Inner, &left_keys, &right_keys)?
                            .build()?;
                    }
                }

                // remove join expressions from filter
                match remove_join_expressions(&filter_expr, &join_keys)? {
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

        let projection_expr: Vec<Expr> = select
            .projection
            .iter()
            .map(|e| self.sql_select_to_rex(&e, &plan.schema()))
            .collect::<Result<Vec<Expr>>>()?;

        let aggr_expr: Vec<Expr> = projection_expr
            .iter()
            .filter(|e| is_aggregate_expr(e))
            .map(|e| e.clone())
            .collect();

        // apply projection or aggregate
        let plan = if (select.group_by.len() > 0) | (aggr_expr.len() > 0) {
            self.aggregate(&plan, projection_expr, &select.group_by, aggr_expr)?
        } else {
            self.project(&plan, projection_expr)?
        };
        Ok(plan)
    }

    /// Wrap a plan in a projection
    fn project(&self, input: &LogicalPlan, expr: Vec<Expr>) -> Result<LogicalPlan> {
        LogicalPlanBuilder::from(input).project(expr)?.build()
    }

    /// Wrap a plan in an aggregate
    fn aggregate(
        &self,
        input: &LogicalPlan,
        projection_expr: Vec<Expr>,
        group_by: &Vec<SQLExpr>,
        aggr_expr: Vec<Expr>,
    ) -> Result<LogicalPlan> {
        let group_expr: Vec<Expr> = group_by
            .iter()
            .map(|e| self.sql_to_rex(&e, &input.schema()))
            .collect::<Result<Vec<Expr>>>()?;

        let group_by_count = group_expr.len();
        let aggr_count = aggr_expr.len();

        if group_by_count + aggr_count != projection_expr.len() {
            return Err(DataFusionError::Plan(
                "Projection references non-aggregate values".to_owned(),
            ));
        }

        let plan = LogicalPlanBuilder::from(&input)
            .aggregate(group_expr, aggr_expr)?
            .build()?;

        // optionally wrap in projection to preserve final order of fields
        let expected_columns: Vec<String> = projection_expr
            .iter()
            .map(|e| e.name(input.schema()))
            .collect::<Result<Vec<_>>>()?;
        let columns: Vec<String> = plan
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect::<Vec<_>>();
        if expected_columns != columns {
            self.project(
                &plan,
                expected_columns
                    .iter()
                    .map(|c| Expr::Column(c.clone()))
                    .collect(),
            )
        } else {
            Ok(plan)
        }
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
        if order_by.len() == 0 {
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

    /// Generate a relational expression from a select SQL expression
    fn sql_select_to_rex(&self, sql: &SelectItem, schema: &Schema) -> Result<Expr> {
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
    pub fn sql_to_rex(&self, sql: &SQLExpr, schema: &Schema) -> Result<Expr> {
        match sql {
            SQLExpr::Value(Value::Number(n)) => match n.parse::<i64>() {
                Ok(n) => Ok(lit(n)),
                Err(_) => Ok(lit(n.parse::<f64>().unwrap())),
            },
            SQLExpr::Value(Value::SingleQuotedString(ref s)) => Ok(lit(s.clone())),

            SQLExpr::Identifier(ref id) => {
                if &id.value[0..1] == "@" {
                    let var_names = vec![id.value.clone()];
                    Ok(Expr::ScalarVariable(var_names))
                } else {
                    match schema.field_with_name(&id.value) {
                        Ok(field) => Ok(Expr::Column(field.name().clone())),
                        Err(_) => Err(DataFusionError::Plan(format!(
                            "Invalid identifier '{}' for schema {}",
                            id,
                            schema.to_string()
                        ))),
                    }
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
                    Err(DataFusionError::Plan(format!(
                        "Invalid compound identifier '{:?}' for schema {}",
                        var_names,
                        schema.to_string()
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
                    Some(Box::new(self.sql_to_rex(e, schema)?))
                } else {
                    None
                };
                let when_expr = conditions
                    .iter()
                    .map(|e| self.sql_to_rex(e, schema))
                    .collect::<Result<Vec<_>>>()?;
                let then_expr = results
                    .iter()
                    .map(|e| self.sql_to_rex(e, schema))
                    .collect::<Result<Vec<_>>>()?;
                let else_expr = if let Some(e) = else_result {
                    Some(Box::new(self.sql_to_rex(e, schema)?))
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
                expr: Box::new(self.sql_to_rex(&expr, schema)?),
                data_type: convert_data_type(data_type)?,
            }),

            SQLExpr::IsNull(ref expr) => {
                Ok(Expr::IsNull(Box::new(self.sql_to_rex(expr, schema)?)))
            }

            SQLExpr::IsNotNull(ref expr) => {
                Ok(Expr::IsNotNull(Box::new(self.sql_to_rex(expr, schema)?)))
            }

            SQLExpr::UnaryOp { ref op, ref expr } => match *op {
                UnaryOperator::Not => {
                    Ok(Expr::Not(Box::new(self.sql_to_rex(expr, schema)?)))
                }
                _ => Err(DataFusionError::Internal(format!(
                    "SQL binary operator cannot be interpreted as a unary operator"
                ))),
            },

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
                    left: Box::new(self.sql_to_rex(&left, &schema)?),
                    op: operator,
                    right: Box::new(self.sql_to_rex(&right, &schema)?),
                })
            }

            SQLExpr::Function(function) => {
                let name: String = function.name.to_string();

                // first, scalar built-in
                if let Ok(fun) = functions::BuiltinScalarFunction::from_str(&name) {
                    let args = function
                        .args
                        .iter()
                        .map(|a| self.sql_to_rex(a, schema))
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
                                _ => self.sql_to_rex(a, schema),
                            })
                            .collect::<Result<Vec<Expr>>>()?
                    } else {
                        function
                            .args
                            .iter()
                            .map(|a| self.sql_to_rex(a, schema))
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
                            .map(|a| self.sql_to_rex(a, schema))
                            .collect::<Result<Vec<Expr>>>()?;

                        Ok(Expr::ScalarUDF { fun: fm, args })
                    }
                    None => match self.schema_provider.get_aggregate_meta(&name) {
                        Some(fm) => {
                            let args = function
                                .args
                                .iter()
                                .map(|a| self.sql_to_rex(a, schema))
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

            SQLExpr::Nested(e) => self.sql_to_rex(&e, &schema),

            _ => Err(DataFusionError::NotImplemented(format!(
                "Unsupported ast node {:?} in sqltorel",
                sql
            ))),
        }
    }
}

fn create_join_schema(left: &SchemaRef, right: &SchemaRef) -> Result<Schema> {
    let mut fields = vec![];
    for field in left.fields() {
        fields.push(field.clone());
    }
    for field in right.fields() {
        fields.push(field.clone());
    }
    Ok(Schema::new(fields))
}

/// Remove join expressions from a filter expression
fn remove_join_expressions(
    expr: &Expr,
    join_columns: &[(&String, &String)],
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
                    (Some(ll), _) => Ok(Some(ll.clone())),
                    (_, Some(rr)) => Ok(Some(rr.clone())),
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

/// Determine if an expression is an aggregate expression or not
fn is_aggregate_expr(e: &Expr) -> bool {
    match e {
        Expr::AggregateFunction { .. } | Expr::AggregateUDF { .. } => true,
        Expr::Alias(expr, _) => is_aggregate_expr(expr),
        _ => false,
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
        other => Err(DataFusionError::NotImplemented(format!(
            "Unsupported SQL type {:?}",
            other
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{logical_plan::create_udf, sql::parser::DFParser};
    use functions::ScalarFunctionImplementation;

    #[test]
    fn select_no_relation() {
        quick_test(
            "SELECT 1",
            "Projection: Int64(1)\
             \n  EmptyRelation",
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
        let expected = "Projection: #fn1, #age\
                        \n  Filter: #fn1 Eq Utf8(\"X\") And #age Lt Int64(30)\
                        \n    Projection: #first_name AS fn1, #age\
                        \n      Filter: #age Gt Int64(20)\
                        \n        TableScan: person projection=None";
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
    fn select_simple_aggregate_with_groupby() {
        quick_test(
            "SELECT state, MIN(age), MAX(age) FROM person GROUP BY state",
            "Aggregate: groupBy=[[#state]], aggr=[[MIN(#age), MAX(#age)]]\
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
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            "Plan(\"Projection references non-aggregate values\")",
            format!("{:?}", err)
        );
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

    fn logical_plan(sql: &str) -> Result<LogicalPlan> {
        let planner = SqlToRel::new(&MockSchemaProvider {});
        let result = DFParser::parse_sql(&sql);
        let ast = result.unwrap();
        planner.statement_to_plan(&ast[0])
    }

    /// Create logical plan, write with formatter, compare to expected output
    fn quick_test(sql: &str, expected: &str) {
        let plan = logical_plan(sql).unwrap();
        assert_eq!(expected, format!("{:?}", plan));
    }

    struct MockSchemaProvider {}

    impl SchemaProvider for MockSchemaProvider {
        fn get_table_meta(&self, name: &str) -> Option<SchemaRef> {
            match name {
                "person" => Some(Arc::new(Schema::new(vec![
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
                ]))),
                "orders" => Some(Arc::new(Schema::new(vec![
                    Field::new("order_id", DataType::UInt32, false),
                    Field::new("customer_id", DataType::UInt32, false),
                    Field::new("o_item_id", DataType::Utf8, false),
                    Field::new("qty", DataType::Int32, false),
                    Field::new("price", DataType::Float64, false),
                    Field::new(
                        "birth_date",
                        DataType::Timestamp(TimeUnit::Nanosecond, None),
                        false,
                    ),
                ]))),
                "lineitem" => Some(Arc::new(Schema::new(vec![
                    Field::new("l_item_id", DataType::UInt32, false),
                    Field::new("l_description", DataType::Utf8, false),
                ]))),
                "aggregate_test_100" => Some(Arc::new(Schema::new(vec![
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
                ]))),
                _ => None,
            }
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
