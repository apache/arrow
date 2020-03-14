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

use std::string::String;
use std::sync::Arc;

use crate::error::{ExecutionError, Result};
use crate::logicalplan::{Expr, FunctionMeta, LogicalPlan, Operator, ScalarValue};
use crate::optimizer::utils;

use arrow::datatypes::*;

use sqlparser::sqlast::*;

/// The SchemaProvider trait allows the query planner to obtain meta-data about tables and
/// functions referenced in SQL statements
pub trait SchemaProvider {
    /// Getter for a field description
    fn get_table_meta(&self, name: &str) -> Option<Arc<Schema>>;
    /// Getter for a UDF description
    fn get_function_meta(&self, name: &str) -> Option<Arc<FunctionMeta>>;
}

/// SQL query planner
pub struct SqlToRel<S: SchemaProvider> {
    schema_provider: S,
}

impl<S: SchemaProvider> SqlToRel<S> {
    /// Create a new query planner
    pub fn new(schema_provider: S) -> Self {
        SqlToRel { schema_provider }
    }

    /// Generate a logic plan from a SQL AST node
    pub fn sql_to_rel(&self, sql: &ASTNode) -> Result<Arc<LogicalPlan>> {
        match *sql {
            ASTNode::SQLSelect {
                ref projection,
                ref relation,
                ref selection,
                ref order_by,
                ref limit,
                ref group_by,
                ref having,
                ..
            } => {
                // parse the input relation so we have access to the row type
                let input = match *relation {
                    Some(ref r) => self.sql_to_rel(r)?,
                    None => Arc::new(LogicalPlan::EmptyRelation {
                        schema: Arc::new(Schema::empty()),
                    }),
                };

                let input_schema = input.schema();

                // selection first
                let selection_plan = match *selection {
                    Some(ref filter_expr) => Some(LogicalPlan::Selection {
                        expr: self.sql_to_rex(&filter_expr, &input_schema.clone())?,
                        input: input.clone(),
                    }),
                    _ => None,
                };

                let expr: Vec<Expr> = projection
                    .iter()
                    .map(|e| self.sql_to_rex(&e, &input_schema))
                    .collect::<Result<Vec<Expr>>>()?;

                // collect aggregate expressions
                let aggr_expr: Vec<Expr> = expr
                    .iter()
                    .filter(|e| is_aggregate_expr(e))
                    .map(|e| e.clone())
                    .collect();

                if aggr_expr.len() > 0 {
                    let aggregate_input: Arc<LogicalPlan> = match selection_plan {
                        Some(s) => Arc::new(s),
                        _ => input.clone(),
                    };

                    let group_expr: Vec<Expr> = match group_by {
                        Some(gbe) => gbe
                            .iter()
                            .map(|e| self.sql_to_rex(&e, &input_schema))
                            .collect::<Result<Vec<Expr>>>()?,
                        None => vec![],
                    };

                    let mut all_fields: Vec<Expr> = group_expr.clone();
                    aggr_expr.iter().for_each(|x| all_fields.push(x.clone()));

                    let aggr_schema = Schema::new(utils::exprlist_to_fields(
                        &all_fields,
                        input_schema,
                    )?);

                    let group_by_count = group_expr.len();
                    let aggr_count = aggr_expr.len();

                    let aggregate = Arc::new(LogicalPlan::Aggregate {
                        input: aggregate_input,
                        group_expr,
                        aggr_expr,
                        schema: Arc::new(aggr_schema),
                    });

                    // wrap in projection to preserve final order of fields
                    let mut projected_fields =
                        Vec::with_capacity(group_by_count + aggr_count);
                    let mut group_expr_index = 0;
                    let mut aggr_expr_index = 0;
                    for i in 0..expr.len() {
                        if is_aggregate_expr(&expr[i]) {
                            projected_fields.push(group_by_count + aggr_expr_index);
                            aggr_expr_index += 1;
                        } else {
                            projected_fields.push(group_expr_index);
                            group_expr_index += 1;
                        }
                    }

                    // determine if projection is needed or not
                    // NOTE this would be better done later in a query optimizer rule
                    let mut projection_needed = false;
                    for i in 0..projected_fields.len() {
                        if projected_fields[i] != i {
                            projection_needed = true;
                            break;
                        }
                    }

                    if projection_needed {
                        let projection = create_projection(
                            projected_fields.iter().map(|i| Expr::Column(*i)).collect(),
                            aggregate,
                        )?;
                        Ok(Arc::new(projection))
                    } else {
                        Ok(aggregate)
                    }
                } else {
                    let projection_input: Arc<LogicalPlan> = match selection_plan {
                        Some(s) => Arc::new(s),
                        _ => input.clone(),
                    };

                    let projection = create_projection(expr, projection_input)?;

                    if having.is_some() {
                        return Err(ExecutionError::General(
                            "HAVING is not implemented yet".to_string(),
                        ));
                    }

                    let group_by_plan = match *group_by {
                        Some(ref group_by_expr) => {
                            let group_by_rex: Vec<Expr> = group_by_expr
                                .iter()
                                .map(|e| self.sql_to_rex(&e, &input_schema))
                                .collect::<Result<Vec<Expr>>>()?;
                            LogicalPlan::Aggregate {
                                input: Arc::new(projection),
                                group_expr: group_by_rex,
                                aggr_expr: vec![],
                                schema: input_schema.clone(),
                            }
                        }
                        _ => projection,
                    };

                    let order_by_plan = match *order_by {
                        Some(ref order_by_expr) => {
                            let input_schema = group_by_plan.schema();
                            let order_by_rex: Result<Vec<Expr>> = order_by_expr
                                .iter()
                                .map(|e| {
                                    Ok(Expr::Sort {
                                        expr: Arc::new(
                                            self.sql_to_rex(&e.expr, &input_schema)
                                                .unwrap(),
                                        ),
                                        asc: e.asc,
                                    })
                                })
                                .collect();

                            LogicalPlan::Sort {
                                expr: order_by_rex?,
                                input: Arc::new(group_by_plan.clone()),
                                schema: input_schema.clone(),
                            }
                        }
                        _ => group_by_plan,
                    };

                    let limit_plan = match *limit {
                        Some(ref limit_expr) => {
                            let input_schema = order_by_plan.schema();

                            let limit_rex = match self
                                .sql_to_rex(&limit_expr, &input_schema.clone())?
                            {
                                Expr::Literal(ScalarValue::Int64(n)) => {
                                    Ok(Expr::Literal(ScalarValue::UInt32(n as u32)))
                                }
                                _ => Err(ExecutionError::General(
                                    "Unexpected expression for LIMIT clause".to_string(),
                                )),
                            }?;

                            LogicalPlan::Limit {
                                expr: limit_rex,
                                input: Arc::new(order_by_plan.clone()),
                                schema: input_schema.clone(),
                            }
                        }
                        _ => order_by_plan,
                    };

                    Ok(Arc::new(limit_plan))
                }
            }

            ASTNode::SQLIdentifier(ref id) => {
                match self.schema_provider.get_table_meta(id.as_ref()) {
                    Some(schema) => Ok(Arc::new(LogicalPlan::TableScan {
                        schema_name: String::from("default"),
                        table_name: id.clone(),
                        table_schema: schema.clone(),
                        projected_schema: schema.clone(),
                        projection: None,
                    })),
                    None => Err(ExecutionError::General(format!(
                        "no schema found for table {}",
                        id
                    ))),
                }
            }

            _ => Err(ExecutionError::ExecutionError(format!(
                "sql_to_rel does not support this relation: {:?}",
                sql
            ))),
        }
    }

    /// Generate a relational expression from a SQL expression
    pub fn sql_to_rex(&self, sql: &ASTNode, schema: &Schema) -> Result<Expr> {
        match *sql {
            ASTNode::SQLValue(sqlparser::sqlast::Value::Long(n)) => {
                Ok(Expr::Literal(ScalarValue::Int64(n)))
            }
            ASTNode::SQLValue(sqlparser::sqlast::Value::Double(n)) => {
                Ok(Expr::Literal(ScalarValue::Float64(n)))
            }
            ASTNode::SQLValue(sqlparser::sqlast::Value::SingleQuotedString(ref s)) => {
                Ok(Expr::Literal(ScalarValue::Utf8(Arc::new(s.clone()))))
            }

            ASTNode::SQLIdentifier(ref id) => {
                match schema.fields().iter().position(|c| c.name().eq(id)) {
                    Some(index) => Ok(Expr::Column(index)),
                    None => Err(ExecutionError::ExecutionError(format!(
                        "Invalid identifier '{}' for schema {}",
                        id,
                        schema.to_string()
                    ))),
                }
            }

            ASTNode::SQLWildcard => {
                Err(ExecutionError::NotImplemented("SQL wildcard operator is not supported in projection - please use explicit column names".to_string()))
            }

            ASTNode::SQLCast {
                ref expr,
                ref data_type,
            } => Ok(Expr::Cast {
                expr: Arc::new(self.sql_to_rex(&expr, schema)?),
                data_type: convert_data_type(data_type)?,
            }),

            ASTNode::SQLIsNull(ref expr) => {
                Ok(Expr::IsNull(Arc::new(self.sql_to_rex(expr, schema)?)))
            }

            ASTNode::SQLIsNotNull(ref expr) => {
                Ok(Expr::IsNotNull(Arc::new(self.sql_to_rex(expr, schema)?)))
            }

            ASTNode::SQLUnary{
                ref operator,
                ref expr,
            } => {
                match *operator {
                    SQLOperator::Not => Ok(Expr::Not(Arc::new(self.sql_to_rex(expr, schema)?))),
                    _ => Err(ExecutionError::InternalError(format!(
                        "SQL binary operator cannot be interpreted as a unary operator"
                    ))),
                }
            }

            ASTNode::SQLBinaryExpr {
                ref left,
                ref op,
                ref right,
            } => {
                let operator = match *op {
                    SQLOperator::Gt => Operator::Gt,
                    SQLOperator::GtEq => Operator::GtEq,
                    SQLOperator::Lt => Operator::Lt,
                    SQLOperator::LtEq => Operator::LtEq,
                    SQLOperator::Eq => Operator::Eq,
                    SQLOperator::NotEq => Operator::NotEq,
                    SQLOperator::Plus => Operator::Plus,
                    SQLOperator::Minus => Operator::Minus,
                    SQLOperator::Multiply => Operator::Multiply,
                    SQLOperator::Divide => Operator::Divide,
                    SQLOperator::Modulus => Operator::Modulus,
                    SQLOperator::And => Operator::And,
                    SQLOperator::Or => Operator::Or,
                    SQLOperator::Not => Operator::Not,
                    SQLOperator::Like => Operator::Like,
                    SQLOperator::NotLike => Operator::NotLike,
                };

                match operator {
                    Operator::Not => Err(ExecutionError::InternalError(format!(
                        "SQL unary operator \"NOT\" cannot be interpreted as a binary operator"
                    ))),
                    _ => Ok(Expr::BinaryExpr {
                        left: Arc::new(self.sql_to_rex(&left, &schema)?),
                        op: operator,
                        right: Arc::new(self.sql_to_rex(&right, &schema)?),
                    })
                }
            }

            //            &ASTNode::SQLOrderBy { ref expr, asc } => Ok(Expr::Sort {
            //                expr: Arc::new(self.sql_to_rex(&expr, &schema)?),
            //                asc,
            //            }),
            ASTNode::SQLFunction { ref id, ref args } => {
                //TODO: fix this hack
                match id.to_lowercase().as_ref() {
                    "min" | "max" | "sum" | "avg" => {
                        let rex_args = args
                            .iter()
                            .map(|a| self.sql_to_rex(a, schema))
                            .collect::<Result<Vec<Expr>>>()?;

                        // return type is same as the argument type for these aggregate
                        // functions
                        let return_type = rex_args[0].get_type(schema).clone();

                        Ok(Expr::AggregateFunction {
                            name: id.clone(),
                            args: rex_args,
                            return_type,
                        })
                    }
                    "count" => {
                        let rex_args = args
                            .iter()
                            .map(|a| match a {
                                ASTNode::SQLValue(sqlparser::sqlast::Value::Long(_)) => {
                                    Ok(Expr::Literal(ScalarValue::UInt8(1)))
                                }
                                ASTNode::SQLWildcard => {
                                    Ok(Expr::Literal(ScalarValue::UInt8(1)))
                                },
                                _ => self.sql_to_rex(a, schema),
                            })
                            .collect::<Result<Vec<Expr>>>()?;

                        Ok(Expr::AggregateFunction {
                            name: id.clone(),
                            args: rex_args,
                            return_type: DataType::UInt64,
                        })
                    }
                    _ => match self.schema_provider.get_function_meta(id) {
                        Some(fm) => {
                            let rex_args = args
                                .iter()
                                .map(|a| self.sql_to_rex(a, schema))
                                .collect::<Result<Vec<Expr>>>()?;

                            let mut safe_args: Vec<Expr> = vec![];
                            for i in 0..rex_args.len() {
                                safe_args.push(
                                    rex_args[i]
                                        .cast_to(fm.args()[i].data_type(), schema)?,
                                );
                            }

                            Ok(Expr::ScalarFunction {
                                name: id.clone(),
                                args: safe_args,
                                return_type: fm.return_type().clone(),
                            })
                        }
                        _ => Err(ExecutionError::General(format!(
                            "Invalid function '{}'",
                            id
                        ))),
                    },
                }
            }

            _ => Err(ExecutionError::General(format!(
                "Unsupported ast node {:?} in sqltorel",
                sql
            ))),
        }
    }
}

/// Create a projection
fn create_projection(expr: Vec<Expr>, input: Arc<LogicalPlan>) -> Result<LogicalPlan> {
    let input_schema = input.schema();

    let schema = Arc::new(Schema::new(utils::exprlist_to_fields(
        &expr,
        input_schema.as_ref(),
    )?));

    Ok(LogicalPlan::Projection {
        expr,
        input,
        schema,
    })
}

/// Determine if an expression is an aggregate expression or not
fn is_aggregate_expr(e: &Expr) -> bool {
    match e {
        Expr::AggregateFunction { .. } => true,
        _ => false,
    }
}

/// Convert SQL data type to relational representation of data type
pub fn convert_data_type(sql: &SQLType) -> Result<DataType> {
    match sql {
        SQLType::Boolean => Ok(DataType::Boolean),
        SQLType::SmallInt => Ok(DataType::Int16),
        SQLType::Int => Ok(DataType::Int32),
        SQLType::BigInt => Ok(DataType::Int64),
        SQLType::Float(_) | SQLType::Real => Ok(DataType::Float64),
        SQLType::Double => Ok(DataType::Float64),
        SQLType::Char(_) | SQLType::Varchar(_) => Ok(DataType::Utf8),
        SQLType::Timestamp => Ok(DataType::Timestamp(TimeUnit::Nanosecond, None)),
        other => Err(ExecutionError::NotImplemented(format!(
            "Unsupported SQL type {:?}",
            other
        ))),
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::logicalplan::FunctionType;
    use sqlparser::sqlparser::*;

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
            "Projection: sqrt(CAST(Int64(9) AS Float64))\
             \n  EmptyRelation",
        );
    }

    #[test]
    fn select_simple_selection() {
        let sql = "SELECT id, first_name, last_name \
                   FROM person WHERE state = 'CO'";
        let expected = "Projection: #0, #1, #2\
                        \n  Selection: #4 Eq Utf8(\"CO\")\
                        \n    TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_neg_selection() {
        let sql = "SELECT id, first_name, last_name \
                   FROM person WHERE NOT state";
        let expected = "Projection: #0, #1, #2\
                        \n  Selection: NOT #4\
                        \n    TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_compound_selection() {
        let sql = "SELECT id, first_name, last_name \
                   FROM person WHERE state = 'CO' AND age >= 21 AND age <= 65";
        let expected = "Projection: #0, #1, #2\
            \n  Selection: #4 Eq Utf8(\"CO\") And #3 GtEq Int64(21) And #3 LtEq Int64(65)\
            \n    TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn test_timestamp_selection() {
        let sql = "SELECT state FROM person WHERE birth_date < CAST (158412331400600000 as timestamp)";

        let expected = "Projection: #4\
            \n  Selection: #6 Lt CAST(Int64(158412331400600000) AS Timestamp(Nanosecond, None))\
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
        let expected = "Projection: #3, #1, #2\
                        \n  Selection: #3 Eq Int64(21) \
                        And #3 NotEq Int64(21) \
                        And #3 Gt Int64(21) \
                        And #3 GtEq Int64(21) \
                        And #3 Lt Int64(65) \
                        And #3 LtEq Int64(65)\
                        \n    TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_simple_aggregate() {
        quick_test(
            "SELECT MIN(age) FROM person",
            "Aggregate: groupBy=[[]], aggr=[[MIN(#3)]]\
             \n  TableScan: person projection=None",
        );
    }

    #[test]
    fn test_sum_aggregate() {
        quick_test(
            "SELECT SUM(age) from person",
            "Aggregate: groupBy=[[]], aggr=[[SUM(#3)]]\
             \n  TableScan: person projection=None",
        );
    }

    #[test]
    fn select_simple_aggregate_with_groupby() {
        quick_test(
            "SELECT state, MIN(age), MAX(age) FROM person GROUP BY state",
            "Aggregate: groupBy=[[#4]], aggr=[[MIN(#3), MAX(#3)]]\
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
        let expected = "Aggregate: groupBy=[[]], aggr=[[COUNT(#0)]]\
                        \n  TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_scalar_func() {
        let sql = "SELECT sqrt(age) FROM person";
        let expected = "Projection: sqrt(CAST(#3 AS Float64))\
                        \n  TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_order_by() {
        let sql = "SELECT id FROM person ORDER BY id";
        let expected = "Sort: #0 ASC\
                        \n  Projection: #0\
                        \n    TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_order_by_desc() {
        let sql = "SELECT id FROM person ORDER BY id DESC";
        let expected = "Sort: #0 DESC\
                        \n  Projection: #0\
                        \n    TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_group_by() {
        let sql = "SELECT state FROM person GROUP BY state";
        let expected = "Aggregate: groupBy=[[#4]], aggr=[[]]\
                        \n  Projection: #4\
                        \n    TableScan: person projection=None";

        quick_test(sql, expected);
    }

    fn logical_plan(sql: &str) -> Arc<LogicalPlan> {
        use sqlparser::dialect::*;
        let dialect = GenericSqlDialect {};
        let planner = SqlToRel::new(MockSchemaProvider {});
        let ast = Parser::parse_sql(&dialect, sql.to_string()).unwrap();
        planner.sql_to_rel(&ast).unwrap()
    }

    /// Create logical plan, write with formatter, compare to expected output
    fn quick_test(sql: &str, expected: &str) {
        let plan = logical_plan(sql);
        assert_eq!(expected, format!("{:?}", plan));
    }

    struct MockSchemaProvider {}

    impl SchemaProvider for MockSchemaProvider {
        fn get_table_meta(&self, name: &str) -> Option<Arc<Schema>> {
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
                _ => None,
            }
        }

        fn get_function_meta(&self, name: &str) -> Option<Arc<FunctionMeta>> {
            match name {
                "sqrt" => Some(Arc::new(FunctionMeta::new(
                    "sqrt".to_string(),
                    vec![Field::new("n", DataType::Float64, false)],
                    DataType::Float64,
                    FunctionType::Scalar,
                ))),
                _ => None,
            }
        }
    }
}
