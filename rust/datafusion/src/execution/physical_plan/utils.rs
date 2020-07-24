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

//! Defines utility functions for low-level operations on Arrays

use std::sync::Arc;

use crate::error::{ExecutionError, Result};
use arrow::array::{
    Array, ArrayRef, BooleanArray, BooleanBuilder, Float32Array, Float32Builder,
    Float64Array, Float64Builder, Int16Array, Int16Builder, Int32Array, Int32Builder,
    Int64Array, Int64Builder, Int8Array, Int8Builder, StringArray, StringBuilder,
    UInt16Array, UInt16Builder, UInt32Array, UInt32Builder, UInt64Array, UInt64Builder,
    UInt8Array, UInt8Builder,
};
use arrow::datatypes::DataType;

// cast, iterate over and re-build array
macro_rules! _build_array {
    ($ARRAY:expr, $INDEXES:expr, $TYPE:ty, $BUILDER:ty) => {{
        let array = match $ARRAY.as_any().downcast_ref::<$TYPE>() {
            Some(n) => Ok(n),
            None => Err(ExecutionError::InternalError(
                format!("Invalid data type for ").to_string(),
            )),
        }?;

        let mut builder = <$BUILDER>::new($INDEXES.len());
        for index in $INDEXES {
            if array.is_null(*index) {
                builder.append_null()?;
            } else {
                builder.append_value(array.value(*index))?;
            }
        }
        Ok(Arc::new(builder.finish()))
    };};
}

/// Builds and array
pub fn build_array(
    array: &ArrayRef,
    indexes: &Vec<usize>,
    datatype: &DataType,
) -> Result<ArrayRef> {
    match datatype {
        DataType::Boolean => _build_array!(array, indexes, BooleanArray, BooleanBuilder),
        DataType::Int8 => _build_array!(array, indexes, Int8Array, Int8Builder),
        DataType::Int16 => _build_array!(array, indexes, Int16Array, Int16Builder),
        DataType::Int32 => _build_array!(array, indexes, Int32Array, Int32Builder),
        DataType::Int64 => _build_array!(array, indexes, Int64Array, Int64Builder),
        DataType::UInt8 => _build_array!(array, indexes, UInt8Array, UInt8Builder),
        DataType::UInt16 => _build_array!(array, indexes, UInt16Array, UInt16Builder),
        DataType::UInt32 => _build_array!(array, indexes, UInt32Array, UInt32Builder),
        DataType::UInt64 => _build_array!(array, indexes, UInt64Array, UInt64Builder),
        DataType::Float64 => _build_array!(array, indexes, Float64Array, Float64Builder),
        DataType::Float32 => _build_array!(array, indexes, Float32Array, Float32Builder),
        DataType::Utf8 => _build_array!(array, indexes, StringArray, StringBuilder),
        _ => Err(ExecutionError::NotImplemented(
            format!(
                "Conversions for type {:?} are still not implemented",
                datatype
            )
            .to_string(),
        )),
    }
}
