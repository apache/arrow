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

// Some of these functions reference the Postgres documentation
// or implementation to ensure compatibility and are subject to
// the Postgres license.

//! String expressions

use std::sync::Arc;

use crate::{
    error::{DataFusionError, Result},
    scalar::ScalarValue,
};
use arrow::{
    array::{
        Array, ArrayRef, GenericStringArray, Int64Array, PrimitiveArray, StringArray,
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

/// Removes the longest string containing only characters in characters (a space by default) from the start and end of string.
/// btrim('xyxtrimyyx', 'xyz') = 'trim'
pub fn btrim<T: StringOffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args.len() {
        1 => {
            let string_array: &GenericStringArray<T> = args[0]
                .as_any()
                .downcast_ref::<GenericStringArray<T>>()
                .unwrap();

            let result = string_array
                .iter()
                .map(|x| x.map(|x: &str| x.trim_start_matches(' ').trim_end_matches(' ')))
                .collect::<GenericStringArray<T>>();

            Ok(Arc::new(result) as ArrayRef)
        }
        2 => {
            let string_array: &GenericStringArray<T> = args[0]
                .as_any()
                .downcast_ref::<GenericStringArray<T>>()
                .unwrap();

            let characters_array: &GenericStringArray<T> = args[1]
                .as_any()
                .downcast_ref::<GenericStringArray<T>>()
                .unwrap();

            let result = string_array
                .iter()
                .enumerate()
                .map(|(i, x)| {
                    if characters_array.is_null(i) {
                        None
                    } else {
                        x.map(|x: &str| {
                            let chars: Vec<char> =
                                characters_array.value(i).chars().collect();
                            x.trim_start_matches(&chars[..])
                                .trim_end_matches(&chars[..])
                        })
                    }
                })
                .collect::<GenericStringArray<T>>();

            Ok(Arc::new(result) as ArrayRef)
        }
        other => Err(DataFusionError::Internal(format!(
            "btrim was called with {} arguments. It requires at most 2.",
            other
        ))),
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

/// Concatenates the text representations of all the arguments. NULL arguments are ignored.
/// concat('abcde', 2, NULL, 22) = 'abcde222'
pub fn concat(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    // do not accept 0 arguments.
    if args.is_empty() {
        return Err(DataFusionError::Internal(format!(
            "concat was called with {} arguments. It requires at least 1.",
            args.len()
        )));
    }

    // first, decide whether to return a scalar or a vector.
    let mut return_array = args.iter().filter_map(|x| match x {
        ColumnarValue::Array(array) => Some(array.len()),
        _ => None,
    });
    if let Some(size) = return_array.next() {
        let result = (0..size)
            .map(|index| {
                let mut owned_string: String = "".to_owned();
                for arg in args {
                    match arg {
                        ColumnarValue::Scalar(ScalarValue::Utf8(maybe_value)) => {
                            if let Some(value) = maybe_value {
                                owned_string.push_str(value);
                            }
                        }
                        ColumnarValue::Array(v) => {
                            if v.is_valid(index) {
                                let v = v.as_any().downcast_ref::<StringArray>().unwrap();
                                owned_string.push_str(&v.value(index));
                            }
                        }
                        _ => unreachable!(),
                    }
                }
                Some(owned_string)
            })
            .collect::<StringArray>();

        Ok(ColumnarValue::Array(Arc::new(result)))
    } else {
        // short avenue with only scalars
        let initial = Some("".to_string());
        let result = args.iter().fold(initial, |mut acc, rhs| {
            if let Some(ref mut inner) = acc {
                match rhs {
                    ColumnarValue::Scalar(ScalarValue::Utf8(Some(v))) => {
                        inner.push_str(v);
                    }
                    ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {}
                    _ => unreachable!(""),
                };
            };
            acc
        });
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result)))
    }
}

/// Concatenates all but the first argument, with separators. The first argument is used as the separator string, and should not be NULL. Other NULL arguments are ignored.
/// concat_ws(',', 'abcde', 2, NULL, 22) = 'abcde,2,22'
pub fn concat_ws(args: &[ArrayRef]) -> Result<ArrayRef> {
    // downcast all arguments to strings
    let args = downcast_vec!(args, StringArray).collect::<Result<Vec<&StringArray>>>()?;

    // do not accept 0 or 1 arguments.
    if args.len() < 2 {
        return Err(DataFusionError::Internal(format!(
            "concat_ws was called with {} arguments. It requires at least 2.",
            args.len()
        )));
    }

    // first map is the iterator, second is for the `Option<_>`
    let result = args[0]
        .iter()
        .enumerate()
        .map(|(index, x)| {
            x.map(|sep: &str| {
                let mut owned_string: String = "".to_owned();
                for arg_index in 1..args.len() {
                    let arg = &args[arg_index];
                    if !arg.is_null(index) {
                        owned_string.push_str(&arg.value(index));
                        // if not last push separator
                        if arg_index != args.len() - 1 {
                            owned_string.push_str(&sep);
                        }
                    }
                }
                owned_string
            })
        })
        .collect::<StringArray>();

    Ok(Arc::new(result) as ArrayRef)
}

/// Removes the longest string containing only characters in characters (a space by default) from the start of string.
/// ltrim('zzzytest', 'xyz') = 'test'
pub fn ltrim<T: StringOffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args.len() {
        1 => {
            let string_array: &GenericStringArray<T> = args[0]
                .as_any()
                .downcast_ref::<GenericStringArray<T>>()
                .unwrap();

            let result = string_array
                .iter()
                .map(|x| x.map(|x: &str| x.trim_start_matches(' ')))
                .collect::<GenericStringArray<T>>();

            Ok(Arc::new(result) as ArrayRef)
        }
        2 => {
            let string_array: &GenericStringArray<T> = args[0]
                .as_any()
                .downcast_ref::<GenericStringArray<T>>()
                .unwrap();

            let characters_array: &GenericStringArray<T> = args[1]
                .as_any()
                .downcast_ref::<GenericStringArray<T>>()
                .unwrap();

            let result = string_array
                .iter()
                .enumerate()
                .map(|(i, x)| {
                    if characters_array.is_null(i) {
                        None
                    } else {
                        x.map(|x: &str| {
                            let chars: Vec<char> =
                                characters_array.value(i).chars().collect();
                            x.trim_start_matches(&chars[..])
                        })
                    }
                })
                .collect::<GenericStringArray<T>>();

            Ok(Arc::new(result) as ArrayRef)
        }
        other => Err(DataFusionError::Internal(format!(
            "ltrim was called with {} arguments. It requires at most 2.",
            other
        ))),
    }
}

/// Converts the string to all lower case.
/// lower('TOM') = 'tom'
pub fn lower(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    handle(args, |x| x.to_ascii_lowercase(), "lower")
}

/// Removes the longest string containing only characters in characters (a space by default) from the end of string.
/// rtrim('testxxzx', 'xyz') = 'test'
pub fn rtrim<T: StringOffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args.len() {
        1 => {
            let string_array: &GenericStringArray<T> = args[0]
                .as_any()
                .downcast_ref::<GenericStringArray<T>>()
                .unwrap();

            let result = string_array
                .iter()
                .map(|x| x.map(|x: &str| x.trim_end_matches(' ')))
                .collect::<GenericStringArray<T>>();

            Ok(Arc::new(result) as ArrayRef)
        }
        2 => {
            let string_array: &GenericStringArray<T> = args[0]
                .as_any()
                .downcast_ref::<GenericStringArray<T>>()
                .unwrap();

            let characters_array: &GenericStringArray<T> = args[1]
                .as_any()
                .downcast_ref::<GenericStringArray<T>>()
                .unwrap();

            let result = string_array
                .iter()
                .enumerate()
                .map(|(i, x)| {
                    if characters_array.is_null(i) {
                        None
                    } else {
                        x.map(|x: &str| {
                            let chars: Vec<char> =
                                characters_array.value(i).chars().collect();
                            x.trim_end_matches(&chars[..])
                        })
                    }
                })
                .collect::<GenericStringArray<T>>();

            Ok(Arc::new(result) as ArrayRef)
        }
        other => Err(DataFusionError::Internal(format!(
            "rtrim was called with {} arguments. It requires at most two.",
            other
        ))),
    }
}

/// Extracts the substring of string starting at the start'th character, and extending for count characters if that is specified. (Same as substring(string from start for count).)
/// substr('alphabet', 3) = 'phabet'
/// substr('alphabet', 3, 2) = 'ph'
pub fn substr<T: StringOffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args.len() {
        2 => {
            let string_array: &GenericStringArray<T> = args[0]
                .as_any()
                .downcast_ref::<GenericStringArray<T>>()
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "could not cast string to StringArray".to_string(),
                    )
                })?;

            let start_array: &Int64Array = args[1]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "could not cast start to Int64Array".to_string(),
                    )
                })?;

            let result = string_array
                .iter()
                .enumerate()
                .map(|(i, x)| {
                    if start_array.is_null(i) {
                        None
                    } else {
                        x.map(|x: &str| {
                            let start: i64 = start_array.value(i);

                            if start <= 0 {
                                x.to_string()
                            } else {
                                let graphemes = x.graphemes(true).collect::<Vec<&str>>();
                                let start_pos = start as usize - 1;
                                if graphemes.len() < start_pos {
                                    "".to_string()
                                } else {
                                    graphemes[start_pos..].concat()
                                }
                            }
                        })
                    }
                })
                .collect::<GenericStringArray<T>>();

            Ok(Arc::new(result) as ArrayRef)
        }
        3 => {
            let string_array: &GenericStringArray<T> = args[0]
                .as_any()
                .downcast_ref::<GenericStringArray<T>>()
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "could not cast string to StringArray".to_string(),
                    )
                })?;

            let start_array: &Int64Array = args[1]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "could not cast start to Int64Array".to_string(),
                    )
                })?;

            let count_array: &Int64Array = args[2]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "could not cast count to Int64Array".to_string(),
                    )
                })?;

            let result = string_array
                .iter()
                .enumerate()
                .map(|(i, x)| {
                    if start_array.is_null(i) || count_array.is_null(i) {
                        Ok(None)
                    } else {
                        x.map(|x: &str| {
                            let start: i64 = start_array.value(i);
                            let count = count_array.value(i);

                            if count < 0 {
                                Err(DataFusionError::Execution(
                                    "negative substring length not allowed".to_string(),
                                ))
                            } else if start <= 0 {
                                Ok(x.to_string())
                            } else {
                                let graphemes = x.graphemes(true).collect::<Vec<&str>>();
                                let start_pos = start as usize - 1;
                                let count_usize = count as usize;
                                if graphemes.len() < start_pos {
                                    Ok("".to_string())
                                } else if graphemes.len() < start_pos + count_usize {
                                    Ok(graphemes[start_pos..].concat())
                                } else {
                                    Ok(graphemes[start_pos..start_pos + count_usize]
                                        .concat())
                                }
                            }
                        })
                        .transpose()
                    }
                })
                .collect::<Result<GenericStringArray<T>>>()?;

            Ok(Arc::new(result) as ArrayRef)
        }
        other => Err(DataFusionError::Internal(format!(
            "substr was called with {} arguments. It requires 2 or 3.",
            other
        ))),
    }
}

/// Converts the string to all upper case.
/// upper('tom') = 'TOM'
pub fn upper(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    handle(args, |x| x.to_ascii_uppercase(), "upper")
}
