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

//! Defines anxiliary functions for hashing keys

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use crate::error::{ExecutionError, Result};
use crate::execution::physical_plan::Accumulator;

use arrow::array::{
    ArrayRef, BooleanArray, Int16Array, Int32Array, Int64Array, Int8Array, StringArray,
    UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::array::{
    BooleanBuilder, Int16Builder, Int32Builder, Int64Builder, Int8Builder, StringBuilder,
    UInt16Builder, UInt32Builder, UInt64Builder, UInt8Builder,
};
use arrow::datatypes::DataType;

use fnv::FnvHashMap;

/// Enumeration of types that can be used in any expression that uses an hash (all primitives except
/// for floating point numerics)
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum KeyScalar {
    /// Boolean
    Boolean(bool),
    /// 8 bits
    UInt8(u8),
    /// 16 bits
    UInt16(u16),
    /// 32 bits
    UInt32(u32),
    /// 64 bits
    UInt64(u64),
    /// 8 bits signed
    Int8(i8),
    /// 16 bits signed
    Int16(i16),
    /// 32 bits signed
    Int32(i32),
    /// 64 bits signed
    Int64(i64),
    /// string
    Utf8(String),
}

macro_rules! safe_cast {
    ($col:expr, $row:expr, $datatype:expr, $ArrayType:ty, $keyscalar: expr) => {{
        if let Some(array) = $col.as_any().downcast_ref::<$ArrayType>() {
            return Ok($keyscalar(array.value($row)));
        } else {
            return Err(ExecutionError::General(format!(
                "Failed to downcast {:?} to {:?}",
                $col, $datatype
            )));
        }
    }};
}

/// Return an KeyScalar from an ArrayRef of a given row.
pub fn create_key(col: &ArrayRef, row: usize) -> Result<KeyScalar> {
    match col.data_type() {
        DataType::Boolean => safe_cast!(
            col,
            row,
            DataType::Boolean,
            BooleanArray,
            KeyScalar::Boolean
        ),
        DataType::UInt8 => {
            safe_cast!(col, row, DataType::UInt8, UInt8Array, KeyScalar::UInt8)
        }
        DataType::UInt16 => {
            safe_cast!(col, row, DataType::UInt16, UInt16Array, KeyScalar::UInt16)
        }
        DataType::UInt32 => {
            safe_cast!(col, row, DataType::UInt32, UInt32Array, KeyScalar::UInt32)
        }
        DataType::UInt64 => {
            safe_cast!(col, row, DataType::UInt64, UInt64Array, KeyScalar::UInt64)
        }
        DataType::Int8 => {
            safe_cast!(col, row, DataType::Int8, Int8Array, KeyScalar::Int8)
        }
        DataType::Int16 => {
            safe_cast!(col, row, DataType::Int16, Int16Array, KeyScalar::Int16)
        }
        DataType::Int32 => {
            safe_cast!(col, row, DataType::Int32, Int32Array, KeyScalar::Int32)
        }
        DataType::Int64 => {
            safe_cast!(col, row, DataType::Int64, Int64Array, KeyScalar::Int64)
        }
        DataType::Utf8 => {
            if let Some(array) = col.as_any().downcast_ref::<StringArray>() {
                return Ok(KeyScalar::Utf8(String::from(array.value(row))));
            } else {
                return Err(ExecutionError::General(format!(
                    "Failed to downcast {:?} to {:?}",
                    col,
                    DataType::Utf8
                )));
            }
        }
        _ => {
            return Err(ExecutionError::ExecutionError(
                "Unsupported key data type".to_string(),
            ))
        }
    }
}

/// Create array from `key` attribute in map entry (representing a grouping scalar value)
macro_rules! key_array_from_map_entries {
    ($BUILDER:ident, $TY:ident, $MAP:expr, $COL_INDEX:expr) => {{
        let mut builder = $BUILDER::new($MAP.len());
        let mut err = false;
        for k in $MAP.keys() {
            match k[$COL_INDEX] {
                KeyScalar::$TY(n) => builder.append_value(n).unwrap(),
                _ => err = true,
            }
        }
        if err {
            Err(ExecutionError::ExecutionError(
                "unexpected type when creating grouping array from aggregate map"
                    .to_string(),
            ))
        } else {
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
    }};
}

/// A set of accumulators
pub type AccumulatorSet = Vec<Rc<RefCell<dyn Accumulator>>>;

/// Builds the array of KeyScalars from `data_type`.
pub fn create_key_array(
    i: usize,
    data_type: DataType,
    map: &FnvHashMap<Vec<KeyScalar>, Rc<AccumulatorSet>>,
) -> Result<ArrayRef> {
    let array: Result<ArrayRef> = match data_type {
        DataType::Boolean => key_array_from_map_entries!(BooleanBuilder, Boolean, map, i),
        DataType::UInt8 => key_array_from_map_entries!(UInt8Builder, UInt8, map, i),
        DataType::UInt16 => key_array_from_map_entries!(UInt16Builder, UInt16, map, i),
        DataType::UInt32 => key_array_from_map_entries!(UInt32Builder, UInt32, map, i),
        DataType::UInt64 => key_array_from_map_entries!(UInt64Builder, UInt64, map, i),
        DataType::Int8 => key_array_from_map_entries!(Int8Builder, Int8, map, i),
        DataType::Int16 => key_array_from_map_entries!(Int16Builder, Int16, map, i),
        DataType::Int32 => key_array_from_map_entries!(Int32Builder, Int32, map, i),
        DataType::Int64 => key_array_from_map_entries!(Int64Builder, Int64, map, i),
        DataType::Utf8 => {
            let mut builder = StringBuilder::new(1);
            for k in map.keys() {
                match &k[i] {
                    KeyScalar::Utf8(s) => builder.append_value(&s).unwrap(),
                    _ => {
                        return Err(ExecutionError::ExecutionError(
                            "Unexpected value for Utf8 group column".to_string(),
                        ))
                    }
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        _ => Err(ExecutionError::ExecutionError(
            "Unsupported key by expr".to_string(),
        )),
    };
    Ok(array?)
}
