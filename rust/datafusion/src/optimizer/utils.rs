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

/// Recursively walk a list of expression trees, collecting the unique set of column indexes
/// referenced in the expression
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
    use arrow::datatypes::DataType::*;
    let d = if l == r {
        Some(l.clone())
    } else if l == &Utf8 || r == &Utf8 {
        Some(Utf8)
    } else if is_signed_int(l) {
        if is_signed_int(r) {
            if bit_width(l) >= bit_width(r) {
                Some(l.clone())
            } else {
                Some(r.clone())
            }
        } else if is_unsigned_int(r) {
            match bit_width(l).max(bit_width(r)) {
                8 => Some(Int8),
                16 => Some(Int16),
                32 => Some(Int32),
                64 => Some(Int64),
                _ => None,
            }
        } else if is_floating_point(r) {
            Some(r.clone())
        } else {
            None
        }
    } else if is_unsigned_int(l) {
        if is_signed_int(r) {
            match bit_width(l).max(bit_width(r)) {
                8 => Some(Int8),
                16 => Some(Int16),
                32 => Some(Int32),
                64 => Some(Int64),
                _ => None,
            }
        } else if is_unsigned_int(r) {
            if bit_width(l) >= bit_width(r) {
                Some(l.clone())
            } else {
                Some(r.clone())
            }
        } else if is_floating_point(r) {
            Some(r.clone())
        } else {
            None
        }
    } else if is_floating_point(l) {
        if is_int(r) {
            Some(l.clone())
        } else if is_floating_point(r) {
            if bit_width(l) >= bit_width(r) {
                Some(l.clone())
            } else {
                Some(r.clone())
            }
        } else {
            None
        }
    } else {
        None
    };

    match d {
        Some(dd) => Ok(dd),
        None => Err(ExecutionError::General(format!(
            "Could not determine supertype for {:?} and {:?}",
            l, r
        ))),
    }
}

fn is_int(d: &DataType) -> bool {
    use arrow::datatypes::DataType::*;
    match d {
        Int8 | Int16 | Int32 | Int64 => true,
        UInt8 | UInt16 | UInt32 | UInt64 => true,
        _ => false,
    }
}

fn is_signed_int(d: &DataType) -> bool {
    use arrow::datatypes::DataType::*;
    match d {
        Int8 | Int16 | Int32 | Int64 => true,
        _ => false,
    }
}

fn is_unsigned_int(d: &DataType) -> bool {
    use arrow::datatypes::DataType::*;
    match d {
        UInt8 | UInt16 | UInt32 | UInt64 => true,
        _ => false,
    }
}

fn is_floating_point(d: &DataType) -> bool {
    use arrow::datatypes::DataType::*;
    match d {
        Float32 | Float64 => true,
        _ => false,
    }
}

fn bit_width(d: &DataType) -> usize {
    use arrow::datatypes::DataType::*;
    match d {
        Int8 | UInt8 => 8,
        Int16 | UInt16 => 16,
        Int32 | UInt32 | Float32 => 32,
        Int64 | UInt64 | Float64 => 64,
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logicalplan::Expr;
    use arrow::datatypes::DataType;
    use arrow::datatypes::DataType::*;
    use std::collections::HashSet;
    use std::sync::Arc;

    #[test]
    fn test_supertype_numeric_types() {
        let types = vec![
            UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32, Float64,
        ];
        let mut result = String::new();
        for l in &types {
            for r in &types {
                result.push_str(&format!(
                    "supertype of {:?} and {:?} is {:?}\n",
                    l,
                    r,
                    get_supertype(l, r).unwrap()
                ));
            }
        }
        assert_eq!("supertype of UInt8 and UInt8 is UInt8\nsupertype of UInt8 and UInt16 is UInt16\nsupertype of UInt8 and UInt32 is UInt32\nsupertype of UInt8 and UInt64 is UInt64\nsupertype of UInt8 and Int8 is Int8\nsupertype of UInt8 and Int16 is Int16\nsupertype of UInt8 and Int32 is Int32\nsupertype of UInt8 and Int64 is Int64\nsupertype of UInt8 and Float32 is Float32\nsupertype of UInt8 and Float64 is Float64\nsupertype of UInt16 and UInt8 is UInt16\nsupertype of UInt16 and UInt16 is UInt16\nsupertype of UInt16 and UInt32 is UInt32\nsupertype of UInt16 and UInt64 is UInt64\nsupertype of UInt16 and Int8 is Int16\nsupertype of UInt16 and Int16 is Int16\nsupertype of UInt16 and Int32 is Int32\nsupertype of UInt16 and Int64 is Int64\nsupertype of UInt16 and Float32 is Float32\nsupertype of UInt16 and Float64 is Float64\nsupertype of UInt32 and UInt8 is UInt32\nsupertype of UInt32 and UInt16 is UInt32\nsupertype of UInt32 and UInt32 is UInt32\nsupertype of UInt32 and UInt64 is UInt64\nsupertype of UInt32 and Int8 is Int32\nsupertype of UInt32 and Int16 is Int32\nsupertype of UInt32 and Int32 is Int32\nsupertype of UInt32 and Int64 is Int64\nsupertype of UInt32 and Float32 is Float32\nsupertype of UInt32 and Float64 is Float64\nsupertype of UInt64 and UInt8 is UInt64\nsupertype of UInt64 and UInt16 is UInt64\nsupertype of UInt64 and UInt32 is UInt64\nsupertype of UInt64 and UInt64 is UInt64\nsupertype of UInt64 and Int8 is Int64\nsupertype of UInt64 and Int16 is Int64\nsupertype of UInt64 and Int32 is Int64\nsupertype of UInt64 and Int64 is Int64\nsupertype of UInt64 and Float32 is Float32\nsupertype of UInt64 and Float64 is Float64\nsupertype of Int8 and UInt8 is Int8\nsupertype of Int8 and UInt16 is Int16\nsupertype of Int8 and UInt32 is Int32\nsupertype of Int8 and UInt64 is Int64\nsupertype of Int8 and Int8 is Int8\nsupertype of Int8 and Int16 is Int16\nsupertype of Int8 and Int32 is Int32\nsupertype of Int8 and Int64 is Int64\nsupertype of Int8 and Float32 is Float32\nsupertype of Int8 and Float64 is Float64\nsupertype of Int16 and UInt8 is Int16\nsupertype of Int16 and UInt16 is Int16\nsupertype of Int16 and UInt32 is Int32\nsupertype of Int16 and UInt64 is Int64\nsupertype of Int16 and Int8 is Int16\nsupertype of Int16 and Int16 is Int16\nsupertype of Int16 and Int32 is Int32\nsupertype of Int16 and Int64 is Int64\nsupertype of Int16 and Float32 is Float32\nsupertype of Int16 and Float64 is Float64\nsupertype of Int32 and UInt8 is Int32\nsupertype of Int32 and UInt16 is Int32\nsupertype of Int32 and UInt32 is Int32\nsupertype of Int32 and UInt64 is Int64\nsupertype of Int32 and Int8 is Int32\nsupertype of Int32 and Int16 is Int32\nsupertype of Int32 and Int32 is Int32\nsupertype of Int32 and Int64 is Int64\nsupertype of Int32 and Float32 is Float32\nsupertype of Int32 and Float64 is Float64\nsupertype of Int64 and UInt8 is Int64\nsupertype of Int64 and UInt16 is Int64\nsupertype of Int64 and UInt32 is Int64\nsupertype of Int64 and UInt64 is Int64\nsupertype of Int64 and Int8 is Int64\nsupertype of Int64 and Int16 is Int64\nsupertype of Int64 and Int32 is Int64\nsupertype of Int64 and Int64 is Int64\nsupertype of Int64 and Float32 is Float32\nsupertype of Int64 and Float64 is Float64\nsupertype of Float32 and UInt8 is Float32\nsupertype of Float32 and UInt16 is Float32\nsupertype of Float32 and UInt32 is Float32\nsupertype of Float32 and UInt64 is Float32\nsupertype of Float32 and Int8 is Float32\nsupertype of Float32 and Int16 is Float32\nsupertype of Float32 and Int32 is Float32\nsupertype of Float32 and Int64 is Float32\nsupertype of Float32 and Float32 is Float32\nsupertype of Float32 and Float64 is Float64\nsupertype of Float64 and UInt8 is Float64\nsupertype of Float64 and UInt16 is Float64\nsupertype of Float64 and UInt32 is Float64\nsupertype of Float64 and UInt64 is Float64\nsupertype of Float64 and Int8 is Float64\nsupertype of Float64 and Int16 is Float64\nsupertype of Float64 and Int32 is Float64\nsupertype of Float64 and Int64 is Float64\nsupertype of Float64 and Float32 is Float64\nsupertype of Float64 and Float64 is Float64\n", result);
    }

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
