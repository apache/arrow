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

//! Array expressions

use crate::error::{DataFusionError, Result};
use arrow::array::*;
use arrow::datatypes::DataType;
use std::sync::Arc;

macro_rules! downcast_vec {
    ($ARGS:expr, $ARRAY_TYPE:ident) => {{
        $ARGS
            .iter()
            .map(|e| match e.as_any().downcast_ref::<$ARRAY_TYPE>() {
                Some(array) => Ok(array),
                _ => Err(DataFusionError::Internal("failed to downcast".to_string())),
            })
    }};
}

macro_rules! array {
    ($ARGS:expr, $ARRAY_TYPE:ident, $BUILDER_TYPE:ident) => {{
        // downcast all arguments to their common format
        let args =
            downcast_vec!($ARGS, $ARRAY_TYPE).collect::<Result<Vec<&$ARRAY_TYPE>>>()?;

        let mut builder = FixedSizeListBuilder::<$BUILDER_TYPE>::new(
            <$BUILDER_TYPE>::new(args[0].len()),
            args.len() as i32,
        );
        // for each entry in the array
        for index in 0..args[0].len() {
            for arg in &args {
                if arg.is_null(index) {
                    builder.values().append_null()?;
                } else {
                    builder.values().append_value(arg.value(index))?;
                }
            }
            builder.append(true)?;
        }
        Ok(Arc::new(builder.finish()))
    }};
}

/// put values in an array.
pub fn array(args: &[ArrayRef]) -> Result<ArrayRef> {
    // do not accept 0 arguments.
    if args.is_empty() {
        return Err(DataFusionError::Internal(
            "array requires at least one argument".to_string(),
        ));
    }

    match args[0].data_type() {
        DataType::Utf8 => array!(args, StringArray, StringBuilder),
        DataType::LargeUtf8 => array!(args, LargeStringArray, LargeStringBuilder),
        DataType::Boolean => array!(args, BooleanArray, BooleanBuilder),
        DataType::Float32 => array!(args, Float32Array, Float32Builder),
        DataType::Float64 => array!(args, Float64Array, Float64Builder),
        DataType::Int8 => array!(args, Int8Array, Int8Builder),
        DataType::Int16 => array!(args, Int16Array, Int16Builder),
        DataType::Int32 => array!(args, Int32Array, Int32Builder),
        DataType::Int64 => array!(args, Int64Array, Int64Builder),
        DataType::UInt8 => array!(args, UInt8Array, UInt8Builder),
        DataType::UInt16 => array!(args, UInt16Array, UInt16Builder),
        DataType::UInt32 => array!(args, UInt32Array, UInt32Builder),
        DataType::UInt64 => array!(args, UInt64Array, UInt64Builder),
        data_type => Err(DataFusionError::NotImplemented(format!(
            "Array is not implemented for type '{:?}'.",
            data_type
        ))),
    }
}

/// Currently supported types by the array function.
/// The order of these types correspond to the order on which coercion applies
/// This should thus be from least informative to most informative
pub static SUPPORTED_ARRAY_TYPES: &[DataType] = &[
    DataType::Boolean,
    DataType::UInt8,
    DataType::UInt16,
    DataType::UInt32,
    DataType::UInt64,
    DataType::Int8,
    DataType::Int16,
    DataType::Int32,
    DataType::Int64,
    DataType::Float32,
    DataType::Float64,
    DataType::Utf8,
    DataType::LargeUtf8,
];
