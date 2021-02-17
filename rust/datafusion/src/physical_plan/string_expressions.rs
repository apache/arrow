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

use std::sync::Arc;

use crate::{
    error::{DataFusionError, Result},
    scalar::ScalarValue,
};
use arrow::{
    array::{
        Array, ArrayRef, GenericStringArray, PrimitiveArray, StringArray,
        StringOffsetSizeTrait,
    },
    datatypes::{ArrowNativeType, ArrowPrimitiveType, DataType},
};
use unicode_segmentation::UnicodeSegmentation;

use super::ColumnarValue;

/// applies a unary expression to `args[0]` that is expected to be downcastable to
/// a `GenericStringArray` and returns a `GenericStringArray` (which may have a different offset)
/// # Errors
/// This function errors when:
/// * the number of arguments is not 1
/// * the first argument is not castable to a `GenericStringArray`
pub(crate) fn unary_string_function<'a, T, O, F, R>(
    args: &[&'a dyn Array],
    op: F,
    name: &str,
) -> Result<GenericStringArray<O>>
where
    R: AsRef<str>,
    O: StringOffsetSizeTrait,
    T: StringOffsetSizeTrait,
    F: Fn(&'a str) -> R,
{
    if args.len() != 1 {
        return Err(DataFusionError::Internal(format!(
            "{:?} args were supplied but {} takes exactly one argument",
            args.len(),
            name,
        )));
    }

    let array = args[0]
        .as_any()
        .downcast_ref::<GenericStringArray<T>>()
        .ok_or_else(|| {
            DataFusionError::Internal("failed to downcast to string".to_string())
        })?;

    // first map is the iterator, second is for the `Option<_>`
    Ok(array.iter().map(|x| x.map(|x| op(x))).collect())
}

fn handle<'a, F, R>(args: &'a [ColumnarValue], op: F, name: &str) -> Result<ColumnarValue>
where
    R: AsRef<str>,
    F: Fn(&'a str) -> R,
{
    match &args[0] {
        ColumnarValue::Array(a) => match a.data_type() {
            DataType::Utf8 => {
                Ok(ColumnarValue::Array(Arc::new(unary_string_function::<
                    i32,
                    i32,
                    _,
                    _,
                >(
                    &[a.as_ref()], op, name
                )?)))
            }
            DataType::LargeUtf8 => {
                Ok(ColumnarValue::Array(Arc::new(unary_string_function::<
                    i64,
                    i64,
                    _,
                    _,
                >(
                    &[a.as_ref()], op, name
                )?)))
            }
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function {}",
                other, name,
            ))),
        },
        ColumnarValue::Scalar(scalar) => match scalar {
            ScalarValue::Utf8(a) => {
                let result = a.as_ref().map(|x| (op)(x).as_ref().to_string());
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result)))
            }
            ScalarValue::LargeUtf8(a) => {
                let result = a.as_ref().map(|x| (op)(x).as_ref().to_string());
                Ok(ColumnarValue::Scalar(ScalarValue::LargeUtf8(result)))
            }
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function {}",
                other, name,
            ))),
        },
    }
}

/// Returns number of characters in the string.
/// character_length('josé') = 4
pub fn character_length<T: ArrowPrimitiveType>(args: &[ArrayRef]) -> Result<ArrayRef>
where
    T::Native: StringOffsetSizeTrait,
{
    let string_array: &GenericStringArray<T::Native> = args[0]
        .as_any()
        .downcast_ref::<GenericStringArray<T::Native>>()
        .unwrap();

    let result = string_array
        .iter()
        .map(|x| {
            x.map(|x: &str| T::Native::from_usize(x.graphemes(true).count()).unwrap())
        })
        .collect::<PrimitiveArray<T>>();

    Ok(Arc::new(result) as ArrayRef)
}

/// concatenate string columns together.
pub fn concatenate(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    // downcast all arguments to strings
    //let args = downcast_vec!(args, StringArray).collect::<Result<Vec<&StringArray>>>()?;
    // do not accept 0 arguments.
    if args.is_empty() {
        return Err(DataFusionError::Internal(
            "Concatenate was called with 0 arguments. It requires at least one."
                .to_string(),
        ));
    }

    // first, decide whether to return a scalar or a vector.
    let mut return_array = args.iter().filter_map(|x| match x {
        ColumnarValue::Array(array) => Some(array.len()),
        _ => None,
    });
    if let Some(size) = return_array.next() {
        let iter = (0..size).map(|index| {
            let mut owned_string: String = "".to_owned();

            // if any is null, the result is null
            let mut is_null = false;
            for arg in args {
                match arg {
                    ColumnarValue::Scalar(ScalarValue::Utf8(maybe_value)) => {
                        if let Some(value) = maybe_value {
                            owned_string.push_str(value);
                        } else {
                            is_null = true;
                            break; // short-circuit as we already know the result
                        }
                    }
                    ColumnarValue::Array(v) => {
                        if v.is_null(index) {
                            is_null = true;
                            break; // short-circuit as we already know the result
                        } else {
                            let v = v.as_any().downcast_ref::<StringArray>().unwrap();
                            owned_string.push_str(&v.value(index));
                        }
                    }
                    _ => unreachable!(),
                }
            }
            if is_null {
                None
            } else {
                Some(owned_string)
            }
        });
        let array = iter.collect::<StringArray>();

        Ok(ColumnarValue::Array(Arc::new(array)))
    } else {
        // short avenue with only scalars
        let initial = Some("".to_string());
        let result = args.iter().fold(initial, |mut acc, rhs| {
            if let Some(ref mut inner) = acc {
                match rhs {
                    ColumnarValue::Scalar(ScalarValue::Utf8(Some(v))) => {
                        inner.push_str(v);
                    }
                    ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {
                        acc = None;
                    }
                    _ => unreachable!(""),
                };
            };
            acc
        });
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result)))
    }
}

/// lower
pub fn lower(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    handle(args, |x| x.to_ascii_lowercase(), "lower")
}

/// upper
pub fn upper(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    handle(args, |x| x.to_ascii_uppercase(), "upper")
}

/// trim
pub fn trim(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    handle(args, |x: &str| x.trim(), "trim")
}

/// ltrim
pub fn ltrim(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    handle(args, |x| x.trim_start(), "ltrim")
}

/// rtrim
pub fn rtrim(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    handle(args, |x| x.trim_end(), "rtrim")
}
