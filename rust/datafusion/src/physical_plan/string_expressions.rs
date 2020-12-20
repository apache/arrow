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

//! String expressions

use crate::error::{DataFusionError, Result};
use arrow::{
    array::{
        Array, ArrayData, ArrayRef, LargeStringArray, LargeStringBuilder, StringArray,
        StringBuilder, UInt32Array,
    },
    buffer::Buffer,
    datatypes::{DataType, ToByteSlice},
};
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

/// concatenate string columns together.
pub fn concatenate(args: &[ArrayRef]) -> Result<StringArray> {
    // downcast all arguments to strings
    let args = downcast_vec!(args, StringArray).collect::<Result<Vec<&StringArray>>>()?;
    // do not accept 0 arguments.
    if args.is_empty() {
        return Err(DataFusionError::Internal(
            "Concatenate was called with 0 arguments. It requires at least one."
                .to_string(),
        ));
    }

    let mut builder = StringBuilder::new(args.len());
    // for each entry in the array
    for index in 0..args[0].len() {
        let mut owned_string: String = "".to_owned();

        // if any is null, the result is null
        let mut is_null = false;
        for arg in &args {
            if arg.is_null(index) {
                is_null = true;
                break; // short-circuit as we already know the result
            } else {
                owned_string.push_str(&arg.value(index));
            }
        }
        if is_null {
            builder.append_null()?;
        } else {
            builder.append_value(&owned_string)?;
        }
    }
    Ok(builder.finish())
}

/// character_length returns number of characters in the string
/// character_length('josé') = 4
pub fn character_length(args: &[ArrayRef]) -> Result<UInt32Array> {
    let num_rows = args[0].len();
    let string_args =
        &args[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "could not cast input to StringArray".to_string(),
                )
            })?;

    let result = (0..num_rows)
        .map(|i| {
            if string_args.is_null(i) {
                // NB: Since we use the same null bitset as the input,
                // the output for this value will be ignored, but we
                // need some value in the array we are building.
                Ok(0)
            } else {
                Ok(string_args.value(i).chars().count() as u32)
            }
        })
        .collect::<Result<Vec<_>>>()?;

    let data = ArrayData::new(
        DataType::UInt32,
        num_rows,
        Some(string_args.null_count()),
        string_args.data().null_buffer().cloned(),
        0,
        vec![Buffer::from(result.to_byte_slice())],
        vec![],
    );

    Ok(UInt32Array::from(Arc::new(data)))
}

macro_rules! compute_op {
    ($ARRAY:expr, $FUNC:ident, $TYPE:ident, $BUILDER:ident) => {{
        let mut builder = $BUILDER::new($ARRAY.len());
        for index in 0..$ARRAY.len() {
            if $ARRAY.is_null(index) {
                builder.append_null()?;
            } else {
                builder.append_value(&$ARRAY.value(index).$FUNC())?;
            }
        }
        Ok(Arc::new(builder.finish()))
    }};
}

macro_rules! downcast_compute_op {
    ($ARRAY:expr, $NAME:expr, $FUNC:ident, $TYPE:ident, $BUILDER:ident) => {{
        let n = $ARRAY.as_any().downcast_ref::<$TYPE>();
        match n {
            Some(array) => compute_op!(array, $FUNC, $TYPE, $BUILDER),
            _ => Err(DataFusionError::Internal(format!(
                "Invalid data type for {}",
                $NAME
            ))),
        }
    }};
}

macro_rules! unary_primitive_array_op {
    ($ARRAY:expr, $NAME:expr, $FUNC:ident) => {{
        match ($ARRAY).data_type() {
            DataType::Utf8 => {
                downcast_compute_op!($ARRAY, $NAME, $FUNC, StringArray, StringBuilder)
            }
            DataType::LargeUtf8 => {
                downcast_compute_op!(
                    $ARRAY,
                    $NAME,
                    $FUNC,
                    LargeStringArray,
                    LargeStringBuilder
                )
            }
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function {}",
                other, $NAME,
            ))),
        }
    }};
}

macro_rules! string_unary_function {
    ($NAME:expr, $FUNC:ident, $STRINGFUNC:ident) => {
        /// string function that accepts Utf8 or LargeUtf8 and returns StringArray or LargeStringArray
        pub fn $FUNC(args: &[ArrayRef]) -> Result<ArrayRef> {
            unary_primitive_array_op!(args[0], $NAME, $STRINGFUNC)
        }
    };
}

string_unary_function!("lower", lower, to_ascii_lowercase);
string_unary_function!("upper", upper, to_ascii_uppercase);
string_unary_function!("trim", trim, trim);
