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

//! Coercion rules used to coerce types to match existing expressions' implementations

use arrow::datatypes::DataType;
use std::cmp;
use crate::logical_plan::Operator;

/// Determine if a DataType is signed numeric or not
pub fn is_signed_numeric(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
    )
}

/// Determine if a DataType is numeric or not
pub fn is_numeric(dt: &DataType) -> bool {
    is_signed_numeric(dt)
        || match dt {
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                true
            }
            _ => false,
        }
}

/// Get next byte size of the integer number
fn next_size(size: usize) -> usize {
    if size < 8_usize {
        return size * 2;
    }
    size
}

/// Determine if a DataType is float or not
pub fn is_floating(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Float16 | DataType::Float32 | DataType::Float64
    )
}

pub fn is_integer(dt: &DataType) -> bool {
    is_numeric(dt) && !is_floating(dt)
}

pub fn numeric_byte_size(dt: &DataType) -> Option<usize> {
    match dt {
        DataType::Int8 | DataType::UInt8 => Some(1),
        DataType::Int16 | DataType::UInt16 | DataType::Float16 => Some(2),
        DataType::Int32 | DataType::UInt32 | DataType::Float32 => Some(4),
        DataType::Int64 | DataType::UInt64 | DataType::Float64 => Some(8),
        _ => None,
    }
}

pub fn construct_numeric_type(
    is_signed: bool,
    is_floating: bool,
    byte_size: usize,
) -> Option<DataType> {
    match (is_signed, is_floating, byte_size) {
        (false, false, 1) => Some(DataType::UInt8),
        (false, false, 2) => Some(DataType::UInt16),
        (false, false, 4) => Some(DataType::UInt32),
        (false, false, 8) => Some(DataType::UInt64),
        (false, true, 1) => Some(DataType::Float16),
        (false, true, 2) => Some(DataType::Float16),
        (false, true, 4) => Some(DataType::Float32),
        (false, true, 8) => Some(DataType::Float64),
        (true, false, 1) => Some(DataType::Int8),
        (true, false, 2) => Some(DataType::Int16),
        (true, false, 4) => Some(DataType::Int32),
        (true, false, 8) => Some(DataType::Int64),
        (true, true, 1) => Some(DataType::Float32),
        (true, true, 2) => Some(DataType::Float32),
        (true, true, 4) => Some(DataType::Float32),
        (true, true, 8) => Some(DataType::Float64),

        // TODO support bigint and decimal types, now we just let's overflow
        (false, false, d) if d > 8 => Some(DataType::Int64),
        (true, false, d) if d > 8 => Some(DataType::UInt64),
        (_, true, d) if d > 8 => Some(DataType::Float64),

        _ => None,
    }
}


/// Coercion rules for dictionary values (aka the type of the  dictionary itself)
fn dictionary_value_coercion(
    lhs_type: &DataType,
    rhs_type: &DataType,
) -> Option<DataType> {
    numerical_coercion(lhs_type, rhs_type).or_else(|| string_coercion(lhs_type, rhs_type))
}

/// Coercion rules for Dictionaries: the type that both lhs and rhs
/// can be casted to for the purpose of a computation.
///
/// It would likely be preferable to cast primitive values to
/// dictionaries, and thus avoid unpacking dictionary as well as doing
/// faster comparisons. However, the arrow compute kernels (e.g. eq)
/// don't have DictionaryArray support yet, so fall back to unpacking
/// the dictionaries
pub fn dictionary_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    match (lhs_type, rhs_type) {
        (
            DataType::Dictionary(_lhs_index_type, lhs_value_type),
            DataType::Dictionary(_rhs_index_type, rhs_value_type),
        ) => dictionary_value_coercion(lhs_value_type, rhs_value_type),
        (DataType::Dictionary(_index_type, value_type), _) => {
            dictionary_value_coercion(value_type, rhs_type)
        }
        (_, DataType::Dictionary(_index_type, value_type)) => {
            dictionary_value_coercion(lhs_type, value_type)
        }
        _ => None,
    }
}

/// Coercion rules for Strings: the type that both lhs and rhs can be
/// casted to for the purpose of a string computation
pub fn string_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    use arrow::datatypes::DataType::*;
    match (lhs_type, rhs_type) {
        (Utf8, Utf8) => Some(Utf8),
        (LargeUtf8, Utf8) => Some(LargeUtf8),
        (Utf8, LargeUtf8) => Some(LargeUtf8),
        (LargeUtf8, LargeUtf8) => Some(LargeUtf8),
        _ => None,
    }
}

/// Coercion rules for Temporal columns: the type that both lhs and rhs can be
/// casted to for the purpose of a date computation
pub fn temporal_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    use arrow::datatypes::DataType::*;
    match (lhs_type, rhs_type) {
        (Utf8, Date32) => Some(Date32),
        (Date32, Utf8) => Some(Date32),
        (Utf8, Date64) => Some(Date64),
        (Date64, Utf8) => Some(Date64),
        _ => None,
    }
}

pub fn numerical_arithmetic_coercion(
    op: &Operator,
    lhs_type: &DataType,
    rhs_type: &DataType,
) -> Option<DataType> {
    // error on any non-numeric type
    if !is_numeric(lhs_type) || !is_numeric(rhs_type) {
        return None;
    };

    let has_signed = is_signed_numeric(lhs_type) || is_signed_numeric(rhs_type);
    let has_float = is_floating(lhs_type) || is_floating(rhs_type);
    let max_size = cmp::max(
        numeric_byte_size(lhs_type).unwrap(),
        numeric_byte_size(rhs_type).unwrap(),
    );

    match op {
        Operator::Plus | Operator::Multiply => {
            construct_numeric_type(has_signed, has_float, next_size(max_size))
        }
        Operator::Minus => {
            construct_numeric_type(true, has_float, next_size(max_size))
        }
        Operator::Divide => Some(DataType::Float64),
        Operator::Modulus => {
            // https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/DivisionUtils.h#L113-L117
            let mut bytes_size = numeric_byte_size(rhs_type)?;
            if has_signed {
                bytes_size = next_size(bytes_size);
            }
            let type0 = construct_numeric_type(has_signed, false, bytes_size)?;
            if has_float {
                Some(DataType::Float64)
            } else {
                Some(type0)
            }
        }
        _ => None
    }
}

/// Coercion rule for numerical types: The type that both lhs and rhs
/// can be casted to for numerical calculation, while maintaining
/// maximum precision
pub fn numerical_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    let has_float = is_floating(lhs_type) || is_floating(rhs_type);
    let has_integer = is_integer(lhs_type) || is_integer(rhs_type);
    let has_signed = is_signed_numeric(lhs_type) || is_signed_numeric(rhs_type);
    let has_unsigned = !is_signed_numeric(lhs_type) || !is_signed_numeric(rhs_type);

    let size_of_lhs = numeric_byte_size(lhs_type)?;
    let size_of_rhs = numeric_byte_size(rhs_type)?;

    let max_size_of_unsigned_integer = cmp::max(
        if is_signed_numeric(lhs_type) {
            0
        } else {
            size_of_lhs
        },
        if is_signed_numeric(rhs_type) {
            0
        } else {
            size_of_rhs
        },
    );

    let max_size_of_signed_integer = cmp::max(
        if !is_signed_numeric(lhs_type) {
            0
        } else {
            size_of_lhs
        },
        if !is_signed_numeric(rhs_type) {
            0
        } else {
            size_of_rhs
        },
    );

    let max_size_of_integer = cmp::max(
        if !is_integer(lhs_type) {
            0
        } else {
            size_of_lhs
        },
        if !is_integer(rhs_type) {
            0
        } else {
            size_of_rhs
        },
    );

    let max_size_of_float = cmp::max(
        if !is_floating(lhs_type) {
            0
        } else {
            size_of_lhs
        },
        if !is_floating(rhs_type) {
            0
        } else {
            size_of_rhs
        },
    );

    let should_double = (has_float && has_integer && max_size_of_integer >= max_size_of_float)
        || (has_signed
        && has_unsigned
        && max_size_of_unsigned_integer >= max_size_of_signed_integer);

    construct_numeric_type(
        has_signed,
        has_float,
        if should_double {
            cmp::max(size_of_rhs, size_of_lhs) * 2
        } else {
            cmp::max(size_of_rhs, size_of_lhs)
        },
    )
}

// coercion rules for equality operations. This is a superset of all numerical coercion rules.
pub fn eq_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    if lhs_type == rhs_type {
        // same type => equality is possible
        return Some(lhs_type.clone());
    }
    numerical_coercion(lhs_type, rhs_type)
        .or_else(|| dictionary_coercion(lhs_type, rhs_type))
        .or_else(|| temporal_coercion(lhs_type, rhs_type))
}

// coercion rules that assume an ordered set, such as "less than".
// These are the union of all numerical coercion rules and all string coercion rules
pub fn order_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    if lhs_type == rhs_type {
        // same type => all good
        return Some(lhs_type.clone());
    }

    numerical_coercion(lhs_type, rhs_type)
        .or_else(|| string_coercion(lhs_type, rhs_type))
        .or_else(|| dictionary_coercion(lhs_type, rhs_type))
        .or_else(|| temporal_coercion(lhs_type, rhs_type))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dictionary_type_coersion() {
        use DataType::*;

        // TODO: In the future, this would ideally return Dictionary types and avoid unpacking
        let lhs_type = Dictionary(Box::new(Int8), Box::new(Int32));
        let rhs_type = Dictionary(Box::new(Int8), Box::new(Int16));
        assert_eq!(dictionary_coercion(&lhs_type, &rhs_type), Some(Int32));

        let lhs_type = Dictionary(Box::new(Int8), Box::new(Utf8));
        let rhs_type = Dictionary(Box::new(Int8), Box::new(Int16));
        assert_eq!(dictionary_coercion(&lhs_type, &rhs_type), None);

        let lhs_type = Dictionary(Box::new(Int8), Box::new(Utf8));
        let rhs_type = Utf8;
        assert_eq!(dictionary_coercion(&lhs_type, &rhs_type), Some(Utf8));

        let lhs_type = Utf8;
        let rhs_type = Dictionary(Box::new(Int8), Box::new(Utf8));
        assert_eq!(dictionary_coercion(&lhs_type, &rhs_type), Some(Utf8));
    }
}
