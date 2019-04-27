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
pub fn exprlist_to_column_indices(expr: &Vec<Expr>, accum: &mut HashSet<usize>) {
    expr.iter().for_each(|e| expr_to_column_indices(e, accum));
}

/// Recursively walk an expression tree, collecting the unique set of column indexes
/// referenced in the expression
pub fn expr_to_column_indices(expr: &Expr, accum: &mut HashSet<usize>) {
    match expr {
        Expr::Column(i) => {
            accum.insert(*i);
        }
        Expr::Literal(_) => { /* not needed */ }
        Expr::IsNull(e) => expr_to_column_indices(e, accum),
        Expr::IsNotNull(e) => expr_to_column_indices(e, accum),
        Expr::BinaryExpr { left, right, .. } => {
            expr_to_column_indices(left, accum);
            expr_to_column_indices(right, accum);
        }
        Expr::Cast { expr, .. } => expr_to_column_indices(expr, accum),
        Expr::Sort { expr, .. } => expr_to_column_indices(expr, accum),
        Expr::AggregateFunction { args, .. } => exprlist_to_column_indices(args, accum),
        Expr::ScalarFunction { args, .. } => exprlist_to_column_indices(args, accum),
    }
}

/// Create field meta-data from an expression, for use in a result set schema
pub fn expr_to_field(e: &Expr, input_schema: &Schema) -> Result<Field> {
    match e {
        Expr::Column(i) => Ok(input_schema.fields()[*i].clone()),
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
            let left_type = left.get_type(input_schema);
            let right_type = right.get_type(input_schema);
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
pub fn exprlist_to_fields(expr: &Vec<Expr>, input_schema: &Schema) -> Result<Vec<Field>> {
    expr.iter()
        .map(|e| expr_to_field(e, input_schema))
        .collect()
}

/// Given two datatypes, determine the supertype that both types can safely be cast to
pub fn get_supertype(l: &DataType, r: &DataType) -> Result<DataType> {
    match _get_supertype(l, r) {
        Some(dt) => Ok(dt),
        None => match _get_supertype(r, l) {
            Some(dt) => Ok(dt),
            None => Err(ExecutionError::InternalError(format!(
                "Failed to determine supertype of {:?} and {:?}",
                l, r
            ))),
        },
    }
}

/// Given two datatypes, determine the supertype that both types can safely be cast to
fn _get_supertype(l: &DataType, r: &DataType) -> Option<DataType> {
    use arrow::datatypes::DataType::*;
    match (l, r) {
        (UInt8, Int8) => Some(Int8),
        (UInt8, Int16) => Some(Int16),
        (UInt8, Int32) => Some(Int32),
        (UInt8, Int64) => Some(Int64),

        (UInt16, Int16) => Some(Int16),
        (UInt16, Int32) => Some(Int32),
        (UInt16, Int64) => Some(Int64),

        (UInt32, Int32) => Some(Int32),
        (UInt32, Int64) => Some(Int64),

        (UInt64, Int64) => Some(Int64),

        (Int8, UInt8) => Some(Int8),

        (Int16, UInt8) => Some(Int16),
        (Int16, UInt16) => Some(Int16),

        (Int32, UInt8) => Some(Int32),
        (Int32, UInt16) => Some(Int32),
        (Int32, UInt32) => Some(Int32),

        (Int64, UInt8) => Some(Int64),
        (Int64, UInt16) => Some(Int64),
        (Int64, UInt32) => Some(Int64),
        (Int64, UInt64) => Some(Int64),

        (UInt8, UInt8) => Some(UInt8),
        (UInt8, UInt16) => Some(UInt16),
        (UInt8, UInt32) => Some(UInt32),
        (UInt8, UInt64) => Some(UInt64),
        (UInt8, Float32) => Some(Float32),
        (UInt8, Float64) => Some(Float64),

        (UInt16, UInt8) => Some(UInt16),
        (UInt16, UInt16) => Some(UInt16),
        (UInt16, UInt32) => Some(UInt32),
        (UInt16, UInt64) => Some(UInt64),
        (UInt16, Float32) => Some(Float32),
        (UInt16, Float64) => Some(Float64),

        (UInt32, UInt8) => Some(UInt32),
        (UInt32, UInt16) => Some(UInt32),
        (UInt32, UInt32) => Some(UInt32),
        (UInt32, UInt64) => Some(UInt64),
        (UInt32, Float32) => Some(Float32),
        (UInt32, Float64) => Some(Float64),

        (UInt64, UInt8) => Some(UInt64),
        (UInt64, UInt16) => Some(UInt64),
        (UInt64, UInt32) => Some(UInt64),
        (UInt64, UInt64) => Some(UInt64),
        (UInt64, Float32) => Some(Float32),
        (UInt64, Float64) => Some(Float64),

        (Int8, Int8) => Some(Int8),
        (Int8, Int16) => Some(Int16),
        (Int8, Int32) => Some(Int32),
        (Int8, Int64) => Some(Int64),
        (Int8, Float32) => Some(Float32),
        (Int8, Float64) => Some(Float64),

        (Int16, Int8) => Some(Int16),
        (Int16, Int16) => Some(Int16),
        (Int16, Int32) => Some(Int32),
        (Int16, Int64) => Some(Int64),
        (Int16, Float32) => Some(Float32),
        (Int16, Float64) => Some(Float64),

        (Int32, Int8) => Some(Int32),
        (Int32, Int16) => Some(Int32),
        (Int32, Int32) => Some(Int32),
        (Int32, Int64) => Some(Int64),
        (Int32, Float32) => Some(Float32),
        (Int32, Float64) => Some(Float64),

        (Int64, Int8) => Some(Int64),
        (Int64, Int16) => Some(Int64),
        (Int64, Int32) => Some(Int64),
        (Int64, Int64) => Some(Int64),
        (Int64, Float32) => Some(Float32),
        (Int64, Float64) => Some(Float64),

        (Float32, Float32) => Some(Float32),
        (Float32, Float64) => Some(Float64),
        (Float64, Float32) => Some(Float64),
        (Float64, Float64) => Some(Float64),

        (Utf8, _) => Some(Utf8),
        (_, Utf8) => Some(Utf8),

        (Boolean, Boolean) => Some(Boolean),

        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logicalplan::Expr;
    use arrow::datatypes::DataType;
    use std::collections::HashSet;
    use std::sync::Arc;

    #[test]
    fn test_collect_expr() {
        let mut accum: HashSet<usize> = HashSet::new();
        expr_to_column_indices(
            &Expr::Cast {
                expr: Arc::new(Expr::Column(3)),
                data_type: DataType::Float64,
            },
            &mut accum,
        );
        expr_to_column_indices(
            &Expr::Cast {
                expr: Arc::new(Expr::Column(3)),
                data_type: DataType::Float64,
            },
            &mut accum,
        );
        assert_eq!(1, accum.len());
        assert!(accum.contains(&3));
    }
}
