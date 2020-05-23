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

//! Collection of utility functions that are leveraged by the query optimizer rules

use std::collections::HashSet;

use arrow::datatypes::{DataType, Field, Schema};

use crate::error::{ExecutionError, Result};
use crate::logicalplan::Expr;

/// Recursively walk a list of expression trees, collecting the unique set of column
/// indexes referenced in the expression
pub fn exprlist_to_column_indices(
    expr: &[Expr],
    accum: &mut HashSet<usize>,
) -> Result<()> {
    for e in expr {
        expr_to_column_indices(e, accum)?;
    }
    Ok(())
}

/// Recursively walk an expression tree, collecting the unique set of column indexes
/// referenced in the expression
pub fn expr_to_column_indices(expr: &Expr, accum: &mut HashSet<usize>) -> Result<()> {
    match expr {
        Expr::Alias(expr, _) => expr_to_column_indices(expr, accum),
        Expr::Column(i) => {
            accum.insert(*i);
            Ok(())
        }
        Expr::UnresolvedColumn(_) => Err(ExecutionError::ExecutionError(
            "Columns need to be resolved before this rule can run".to_owned(),
        )),
        Expr::Literal(_) => {
            // not needed
            Ok(())
        }
        Expr::Not(e) => expr_to_column_indices(e, accum),
        Expr::IsNull(e) => expr_to_column_indices(e, accum),
        Expr::IsNotNull(e) => expr_to_column_indices(e, accum),
        Expr::BinaryExpr { left, right, .. } => {
            expr_to_column_indices(left, accum)?;
            expr_to_column_indices(right, accum)?;
            Ok(())
        }
        Expr::Cast { expr, .. } => expr_to_column_indices(expr, accum),
        Expr::Sort { expr, .. } => expr_to_column_indices(expr, accum),
        Expr::AggregateFunction { args, .. } => exprlist_to_column_indices(args, accum),
        Expr::ScalarFunction { args, .. } => exprlist_to_column_indices(args, accum),
        Expr::Wildcard => Err(ExecutionError::General(
            "Wildcard expressions are not valid in a logical query plan".to_owned(),
        )),
    }
}

/// Create field meta-data from an expression, for use in a result set schema
pub fn expr_to_field(e: &Expr, input_schema: &Schema) -> Result<Field> {
    match e {
        Expr::Alias(expr, name) => {
            Ok(Field::new(name, expr.get_type(input_schema)?, true))
        }
        Expr::UnresolvedColumn(name) => Ok(input_schema.field_with_name(&name)?.clone()),
        Expr::Column(i) => {
            let input_schema_field_count = input_schema.fields().len();
            if *i < input_schema_field_count {
                Ok(input_schema.fields()[*i].clone())
            } else {
                Err(ExecutionError::General(format!(
                    "Column index {} out of bounds for input schema with {} field(s)",
                    *i, input_schema_field_count
                )))
            }
        }
        Expr::Literal(ref lit) => Ok(Field::new("lit", lit.get_datatype(), true)),
        Expr::ScalarFunction {
            ref name,
            ref return_type,
            ..
        } => Ok(Field::new(&name, return_type.clone(), true)),
        Expr::AggregateFunction {
            ref name,
            ref return_type,
            ..
        } => Ok(Field::new(&name, return_type.clone(), true)),
        Expr::Cast { ref data_type, .. } => {
            Ok(Field::new("cast", data_type.clone(), true))
        }
        Expr::BinaryExpr {
            ref left,
            ref right,
            ..
        } => {
            let left_type = left.get_type(input_schema)?;
            let right_type = right.get_type(input_schema)?;
            Ok(Field::new(
                "binary_expr",
                get_supertype(&left_type, &right_type).unwrap(),
                true,
            ))
        }
        _ => Err(ExecutionError::NotImplemented(format!(
            "Cannot determine schema type for expression {:?}",
            e
        ))),
    }
}

/// Create field meta-data from an expression, for use in a result set schema
pub fn exprlist_to_fields(expr: &[Expr], input_schema: &Schema) -> Result<Vec<Field>> {
    expr.iter()
        .map(|e| expr_to_field(e, input_schema))
        .collect()
}

/// Given two datatypes, determine the supertype that both types can safely be cast to
pub fn get_supertype(l: &DataType, r: &DataType) -> Result<DataType> {
    use arrow::datatypes::DataType::*;

    if l == r {
        return Ok(l.clone());
    }

    let super_type = match l {
        UInt8 => match r {
            UInt16 | UInt32 | UInt64 => Some(r.clone()),
            Int16 | Int32 | Int64 => Some(r.clone()),
            Float32 | Float64 => Some(r.clone()),
            _ => None,
        },
        UInt16 => match r {
            UInt8 => Some(l.clone()),
            UInt32 | UInt64 => Some(r.clone()),
            Int8 => Some(l.clone()),
            Int32 | Int64 => Some(r.clone()),
            Float32 | Float64 => Some(r.clone()),
            _ => None,
        },
        UInt32 => match r {
            UInt8 | UInt16 => Some(l.clone()),
            UInt64 => Some(r.clone()),
            Int8 | Int16 => Some(l.clone()),
            Int64 => Some(r.clone()),
            Float32 | Float64 => Some(r.clone()),
            _ => None,
        },
        UInt64 => match r {
            UInt8 | UInt16 | UInt32 => Some(l.clone()),
            Int8 | Int16 | Int32 => Some(l.clone()),
            Float32 | Float64 => Some(r.clone()),
            _ => None,
        },
        Int8 => match r {
            Int16 | Int32 | Int64 => Some(r.clone()),
            UInt16 | UInt32 | UInt64 => Some(r.clone()),
            Float32 | Float64 => Some(r.clone()),
            _ => None,
        },
        Int16 => match r {
            Int8 => Some(l.clone()),
            Int32 | Int64 => Some(r.clone()),
            UInt32 | UInt64 => Some(r.clone()),
            Float32 | Float64 => Some(r.clone()),
            _ => None,
        },
        Int32 => match r {
            Int8 | Int16 => Some(l.clone()),
            Int64 => Some(r.clone()),
            UInt64 => Some(r.clone()),
            Float32 | Float64 => Some(r.clone()),
            _ => None,
        },
        Int64 => match r {
            Int8 | Int16 | Int32 => Some(l.clone()),
            UInt8 | UInt16 | UInt32 => Some(l.clone()),
            Float32 | Float64 => Some(r.clone()),
            _ => None,
        },
        Float32 => match r {
            Int8 | Int16 | Int32 | Int64 => Some(Float32),
            UInt8 | UInt16 | UInt32 | UInt64 => Some(Float32),
            Float64 => Some(Float64),
            _ => None,
        },
        Float64 => match r {
            Int8 | Int16 | Int32 | Int64 => Some(Float64),
            UInt8 | UInt16 | UInt32 | UInt64 => Some(Float64),
            Float32 | Float64 => Some(Float64),
            _ => None,
        },
        _ => None,
    };

    match super_type {
        Some(dt) => Ok(dt),
        None => Err(ExecutionError::InternalError(format!(
            "Failed to determine supertype of {:?} and {:?}",
            l, r
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logicalplan::Expr;
    use arrow::datatypes::DataType;
    use std::collections::HashSet;

    #[test]
    fn test_collect_expr() -> Result<()> {
        let mut accum: HashSet<usize> = HashSet::new();
        expr_to_column_indices(
            &Expr::Cast {
                expr: Box::new(Expr::Column(3)),
                data_type: DataType::Float64,
            },
            &mut accum,
        )?;
        expr_to_column_indices(
            &Expr::Cast {
                expr: Box::new(Expr::Column(3)),
                data_type: DataType::Float64,
            },
            &mut accum,
        )?;
        assert_eq!(1, accum.len());
        assert!(accum.contains(&3));
        Ok(())
    }
}
