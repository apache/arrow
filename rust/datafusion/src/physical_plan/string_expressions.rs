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
//
// Some of these functions reference the Postgres documentation
// or implementation to ensure compatibility and are subject to
// the Postgres license.

//! String expressions
use std::cmp::Ordering;
use std::str::from_utf8;
use std::sync::Arc;

use crate::{
    error::{DataFusionError, Result},
    scalar::ScalarValue,
};
use arrow::{
    array::{
        Array, ArrayRef, BooleanArray, GenericStringArray, Int32Array, Int64Array,
        PrimitiveArray, StringArray, StringOffsetSizeTrait,
    },
    datatypes::{ArrowNativeType, ArrowPrimitiveType, DataType},
};
use hashbrown::HashMap;
use regex::Regex;
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
    T: StringOffsetSizeTrait,
    O: StringOffsetSizeTrait,
    F: Fn(&'a str) -> R,
    R: AsRef<str>,
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
    F: Fn(&'a str) -> R,
    R: AsRef<str>,
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

/// Returns the numeric code of the first character of the argument.
/// ascii('x') = 120
pub fn ascii<T: StringOffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let string_array: &GenericStringArray<T> = args[0]
        .as_any()
        .downcast_ref::<GenericStringArray<T>>()
        .unwrap();

    // first map is the iterator, second is for the `Option<_>`
    let result = string_array
        .iter()
        .map(|x| {
            x.map(|x: &str| {
                let mut chars = x.chars();
                chars.next().map_or(0, |v| v as i32)
            })
        })
        .collect::<Int32Array>();

    Ok(Arc::new(result) as ArrayRef)
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

/// Returns the character with the given code. chr(0) is disallowed because text data types cannot store that character.
/// chr(65) = 'A'
pub fn chr(args: &[ArrayRef]) -> Result<ArrayRef> {
    let integer_array: &Int64Array =
        args[0].as_any().downcast_ref::<Int64Array>().unwrap();

    // first map is the iterator, second is for the `Option<_>`
    let result = integer_array
        .iter()
        .map(|x: Option<i64>| {
            x.map(|x| {
                if x == 0 {
                    Err(DataFusionError::Execution(
                        "null character not permitted.".to_string(),
                    ))
                } else {
                    match core::char::from_u32(x as u32) {
                        Some(x) => Ok(x.to_string()),
                        None => Err(DataFusionError::Execution(
                            "requested character too large for encoding.".to_string(),
                        )),
                    }
                }
            })
            .transpose()
        })
        .collect::<Result<StringArray>>()?;

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

/// Converts the first letter of each word to upper case and the rest to lower case. Words are sequences of alphanumeric characters separated by non-alphanumeric characters.
/// initcap('hi THOMAS') = 'Hi Thomas'
pub fn initcap<T: StringOffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let string_array: &GenericStringArray<T> = args[0]
        .as_any()
        .downcast_ref::<GenericStringArray<T>>()
        .unwrap();

    // first map is the iterator, second is for the `Option<_>`
    let result = string_array
        .iter()
        .map(|x| {
            x.map(|x: &str| {
                let mut char_vector = Vec::<char>::new();
                let mut wasalnum = false;
                for c in x.chars() {
                    if wasalnum {
                        char_vector.push(c.to_ascii_lowercase());
                    } else {
                        char_vector.push(c.to_ascii_uppercase());
                    }
                    wasalnum = ('A'..='Z').contains(&c)
                        || ('a'..='z').contains(&c)
                        || ('0'..='9').contains(&c);
                }
                char_vector.iter().collect::<String>()
            })
        })
        .collect::<GenericStringArray<T>>();

    Ok(Arc::new(result) as ArrayRef)
}

/// Returns first n characters in the string, or when n is negative, returns all but last |n| characters.
/// left('abcde', 2) = 'ab'
pub fn left<T: StringOffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let string_array: &GenericStringArray<T> = args[0]
        .as_any()
        .downcast_ref::<GenericStringArray<T>>()
        .ok_or_else(|| {
            DataFusionError::Internal("could not cast string to StringArray".to_string())
        })?;

    let n_array: &Int64Array =
        args[1]
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                DataFusionError::Internal("could not cast n to Int64Array".to_string())
            })?;

    let result = string_array
        .iter()
        .enumerate()
        .map(|(i, x)| {
            if n_array.is_null(i) {
                None
            } else {
                x.map(|x: &str| {
                    let n: i64 = n_array.value(i);
                    match n.cmp(&0) {
                        Ordering::Equal => "",
                        Ordering::Greater => x
                            .grapheme_indices(true)
                            .nth(n as usize)
                            .map_or(x, |(i, _)| &from_utf8(&x.as_bytes()[..i]).unwrap()),
                        Ordering::Less => x
                            .grapheme_indices(true)
                            .rev()
                            .nth(n.abs() as usize - 1)
                            .map_or("", |(i, _)| &from_utf8(&x.as_bytes()[..i]).unwrap()),
                    }
                })
            }
        })
        .collect::<GenericStringArray<T>>();

    Ok(Arc::new(result) as ArrayRef)
}

/// Converts the string to all lower case.
/// length('jose') = 4
pub fn lower(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    handle(args, |x| x.to_ascii_lowercase(), "lower")
}

/// Extends the string to length length by prepending the characters fill (a space by default). If the string is already longer than length then it is truncated (on the right).
/// lpad('hi', 5, 'xy') = 'xyxhi'
pub fn lpad<T: StringOffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args.len() {
        2 => {
            let string_array: &GenericStringArray<T> = args[0]
                .as_any()
                .downcast_ref::<GenericStringArray<T>>()
                .unwrap();

            let length_array: &Int64Array = args[1]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "could not cast length to Int64Array".to_string(),
                    )
                })?;

            let result = string_array
                .iter()
                .enumerate()
                .map(|(i, x)| {
                    if length_array.is_null(i) {
                        None
                    } else {
                        x.map(|x: &str| {
                            let length = length_array.value(i) as usize;
                            if length == 0 {
                                "".to_string()
                            } else {
                                let graphemes = x.graphemes(true).collect::<Vec<&str>>();
                                if length < graphemes.len() {
                                    graphemes[..length].concat()
                                } else {
                                    let mut s = x.to_string();
                                    s.insert_str(
                                        0,
                                        " ".repeat(length - graphemes.len()).as_str(),
                                    );
                                    s
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
                .unwrap();

            let length_array: &Int64Array =
                args[1].as_any().downcast_ref::<Int64Array>().unwrap();

            let fill_array: &GenericStringArray<T> = args[2]
                .as_any()
                .downcast_ref::<GenericStringArray<T>>()
                .unwrap();

            let result = string_array
                .iter()
                .enumerate()
                .map(|(i, x)| {
                    if length_array.is_null(i) || fill_array.is_null(i) {
                        None
                    } else {
                        x.map(|x: &str| {
                            let length = length_array.value(i) as usize;

                            if length == 0 {
                                "".to_string()
                            } else {
                                let graphemes = x.graphemes(true).collect::<Vec<&str>>();
                                let fill_chars =
                                    fill_array.value(i).chars().collect::<Vec<char>>();

                                if length < graphemes.len() {
                                    graphemes[..length].concat()
                                } else if fill_chars.is_empty() {
                                    x.to_string()
                                } else {
                                    let mut s = x.to_string();
                                    let mut char_vector = Vec::<char>::with_capacity(
                                        length - graphemes.len(),
                                    );
                                    for l in 0..length - graphemes.len() {
                                        char_vector.push(
                                            *fill_chars
                                                .get(l % fill_chars.len())
                                                .unwrap(),
                                        );
                                    }
                                    s.insert_str(
                                        0,
                                        char_vector.iter().collect::<String>().as_str(),
                                    );
                                    s
                                }
                            }
                        })
                    }
                })
                .collect::<GenericStringArray<T>>();

            Ok(Arc::new(result) as ArrayRef)
        }
        other => Err(DataFusionError::Internal(format!(
            "lpad was called with {} arguments. It requires at least 2 and at most 3.",
            other
        ))),
    }
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

/// replace POSIX capture groups (like \1) with Rust Regex group (like ${1})
/// used by regexp_replace
fn regex_replace_posix_groups(replacement: &str) -> String {
    lazy_static! {
        static ref CAPTURE_GROUPS_RE: Regex = Regex::new("(\\\\)(\\d*)").unwrap();
    }
    CAPTURE_GROUPS_RE
        .replace_all(replacement, "$${$2}")
        .into_owned()
}

/// Replaces substring(s) matching a POSIX regular expression
/// regexp_replace('Thomas', '.[mN]a.', 'M') = 'ThM'
pub fn regexp_replace<T: StringOffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    // creating Regex is expensive so create hashmap for memoization
    let mut patterns: HashMap<String, Regex> = HashMap::new();

    match args.len() {
        3 => {
            let string_array: &GenericStringArray<T> = args[0]
                .as_any()
                .downcast_ref::<GenericStringArray<T>>()
                .unwrap();

            let pattern_array: &StringArray = args[1]
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();

            let replacement_array: &StringArray = args[2]
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();

            let result = string_array
                .iter()
                .enumerate()
                .map(|(i, x)| {
                    if pattern_array.is_null(i) || replacement_array.is_null(i) {
                        None
                    } else {
                        x.map(|x: &str| {
                            let pattern = pattern_array.value(i).to_string();
                            let replacement = regex_replace_posix_groups(replacement_array.value(i));
                            let re = match patterns.get(pattern_array.value(i)) {
                                Some(re) => Ok(re.clone()),
                                None => {
                                    match Regex::new(pattern.as_str()) {
                                        Ok(re) => {
                                            patterns.insert(pattern, re.clone());
                                            Ok(re)
                                        },
                                        Err(err) => Err(DataFusionError::Execution(err.to_string())),
                                    }
                                }
                            };
                            re.map(|re| re.replace(x, replacement.as_str()))
                        })
                    }.transpose()
                })
                .collect::<Result<GenericStringArray<T>>>()?;

                Ok(Arc::new(result) as ArrayRef)
        }
        4 => {
            let string_array: &GenericStringArray<T> = args[0]
                .as_any()
                .downcast_ref::<GenericStringArray<T>>()
                .unwrap();

            let pattern_array: &StringArray = args[1]
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();

            let replacement_array: &StringArray = args[2]
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();

            let flags_array: &StringArray = args[3]
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();

            let result = string_array
                .iter()
                .enumerate()
                .map(|(i, x)| {
                    if pattern_array.is_null(i) || replacement_array.is_null(i) || flags_array.is_null(i) {
                        None
                    } else {
                        x.map(|x: &str| {
                            let replacement = regex_replace_posix_groups(replacement_array.value(i));

                            let flags = flags_array.value(i);
                            let (pattern, replace_all) = if flags == "g" {
                                (pattern_array.value(i).to_string(), true)
                            } else if flags.contains('g') {
                                (format!("(?{}){}", flags.to_string().replace("g", ""), pattern_array.value(i)), true)
                            } else {
                                (format!("(?{}){}", flags, pattern_array.value(i)), false)
                            };

                            let re = match patterns.get(pattern_array.value(i)) {
                                Some(re) => Ok(re.clone()),
                                None => {
                                    match Regex::new(pattern.as_str()) {
                                        Ok(re) => {
                                            patterns.insert(pattern, re.clone());
                                            Ok(re)
                                        },
                                        Err(err) => Err(DataFusionError::Execution(err.to_string())),
                                    }
                                }
                            };

                            re.map(|re| {
                                if replace_all {
                                    re.replace_all(x, replacement.as_str())
                                } else {
                                    re.replace(x, replacement.as_str())
                                }
                            })
                        })
                    }.transpose()
                })
                .collect::<Result<GenericStringArray<T>>>()?;

                Ok(Arc::new(result) as ArrayRef)
        }
        other => Err(DataFusionError::Internal(format!(
            "regexp_replace was called with {} arguments. It requires at least 3 and at most 4.",
            other
        ))),
    }
}

/// Repeats string the specified number of times.
/// repeat('Pg', 4) = 'PgPgPgPg'
pub fn repeat<T: StringOffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let string_array: &GenericStringArray<T> = args[0]
        .as_any()
        .downcast_ref::<GenericStringArray<T>>()
        .unwrap();

    let number_array: &Int64Array =
        args[1].as_any().downcast_ref::<Int64Array>().unwrap();

    let result = string_array
        .iter()
        .enumerate()
        .map(|(i, x)| {
            if number_array.is_null(i) {
                None
            } else {
                x.map(|x: &str| x.repeat(number_array.value(i) as usize))
            }
        })
        .collect::<GenericStringArray<T>>();

    Ok(Arc::new(result) as ArrayRef)
}

/// Replaces all occurrences in string of substring from with substring to.
/// replace('abcdefabcdef', 'cd', 'XX') = 'abXXefabXXef'
pub fn replace<T: StringOffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let string_array: &GenericStringArray<T> = args[0]
        .as_any()
        .downcast_ref::<GenericStringArray<T>>()
        .unwrap();

    let from_array: &GenericStringArray<T> = args[1]
        .as_any()
        .downcast_ref::<GenericStringArray<T>>()
        .unwrap();

    let to_array: &GenericStringArray<T> = args[2]
        .as_any()
        .downcast_ref::<GenericStringArray<T>>()
        .unwrap();

    let result = string_array
        .iter()
        .enumerate()
        .map(|(i, x)| {
            if from_array.is_null(i) || to_array.is_null(i) {
                None
            } else {
                x.map(|x: &str| x.replace(from_array.value(i), to_array.value(i)))
            }
        })
        .collect::<GenericStringArray<T>>();

    Ok(Arc::new(result) as ArrayRef)
}

/// Reverses the order of the characters in the string.
/// reverse('abcde') = 'edcba'
pub fn reverse<T: StringOffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let string_array: &GenericStringArray<T> = args[0]
        .as_any()
        .downcast_ref::<GenericStringArray<T>>()
        .unwrap();

    // first map is the iterator, second is for the `Option<_>`
    let result = string_array
        .iter()
        .map(|x| x.map(|x: &str| x.graphemes(true).rev().collect::<String>()))
        .collect::<GenericStringArray<T>>();

    Ok(Arc::new(result) as ArrayRef)
}

/// Returns last n characters in the string, or when n is negative, returns all but first |n| characters.
/// right('abcde', 2) = 'de'
pub fn right<T: StringOffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let string_array: &GenericStringArray<T> = args[0]
        .as_any()
        .downcast_ref::<GenericStringArray<T>>()
        .ok_or_else(|| {
            DataFusionError::Internal("could not cast string to StringArray".to_string())
        })?;

    let n_array: &Int64Array =
        args[1]
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                DataFusionError::Internal("could not cast n to Int64Array".to_string())
            })?;

    let result = string_array
        .iter()
        .enumerate()
        .map(|(i, x)| {
            if n_array.is_null(i) {
                None
            } else {
                x.map(|x: &str| {
                    let n: i64 = n_array.value(i);
                    match n.cmp(&0) {
                        Ordering::Equal => "",
                        Ordering::Greater => x
                            .grapheme_indices(true)
                            .rev()
                            .nth(n as usize - 1)
                            .map_or(x, |(i, _)| &from_utf8(&x.as_bytes()[i..]).unwrap()),
                        Ordering::Less => x
                            .grapheme_indices(true)
                            .nth(n.abs() as usize)
                            .map_or("", |(i, _)| &from_utf8(&x.as_bytes()[i..]).unwrap()),
                    }
                })
            }
        })
        .collect::<GenericStringArray<T>>();

    Ok(Arc::new(result) as ArrayRef)
}

/// Extends the string to length length by appending the characters fill (a space by default). If the string is already longer than length then it is truncated.
/// rpad('hi', 5, 'xy') = 'hixyx'
pub fn rpad<T: StringOffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args.len() {
        2 => {
            let string_array: &GenericStringArray<T> = args[0]
                .as_any()
                .downcast_ref::<GenericStringArray<T>>()
                .unwrap();

            let length_array: &Int64Array = args[1]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "could not cast length to Int64Array".to_string(),
                    )
                })?;

            let result = string_array
                .iter()
                .enumerate()
                .map(|(i, x)| {
                    if length_array.is_null(i) {
                        None
                    } else {
                        x.map(|x: &str| {
                            let length = length_array.value(i) as usize;
                            if length == 0 {
                                "".to_string()
                            } else {
                                let graphemes = x.graphemes(true).collect::<Vec<&str>>();
                                if length < graphemes.len() {
                                    graphemes[..length].concat()
                                } else {
                                    let mut s = x.to_string();
                                    s.push_str(
                                        " ".repeat(length - graphemes.len()).as_str(),
                                    );
                                    s
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
                .unwrap();

            let length_array: &Int64Array =
                args[1].as_any().downcast_ref::<Int64Array>().unwrap();

            let fill_array: &GenericStringArray<T> = args[2]
                .as_any()
                .downcast_ref::<GenericStringArray<T>>()
                .unwrap();

            let result = string_array
                .iter()
                .enumerate()
                .map(|(i, x)| {
                    if length_array.is_null(i) || fill_array.is_null(i) {
                        None
                    } else {
                        x.map(|x: &str| {
                            let length = length_array.value(i) as usize;

                            if length == 0 {
                                "".to_string()
                            } else {
                                let graphemes = x.graphemes(true).collect::<Vec<&str>>();
                                let fill_chars =
                                    fill_array.value(i).chars().collect::<Vec<char>>();

                                if length < graphemes.len() {
                                    graphemes[..length].concat()
                                } else if fill_chars.is_empty() {
                                    x.to_string()
                                } else {
                                    let mut s = x.to_string();
                                    let mut char_vector = Vec::<char>::with_capacity(
                                        length - graphemes.len(),
                                    );
                                    for l in 0..length - graphemes.len() {
                                        char_vector.push(
                                            *fill_chars
                                                .get(l % fill_chars.len())
                                                .unwrap(),
                                        );
                                    }
                                    s.push_str(
                                        char_vector.iter().collect::<String>().as_str(),
                                    );
                                    s
                                }
                            }
                        })
                    }
                })
                .collect::<GenericStringArray<T>>();

            Ok(Arc::new(result) as ArrayRef)
        }
        other => Err(DataFusionError::Internal(format!(
            "rpad was called with {} arguments. It requires at least 2 and at most 3.",
            other
        ))),
    }
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

/// Splits string at occurrences of delimiter and returns the n'th field (counting from one).
/// split_part('abc~@~def~@~ghi', '~@~', 2) = 'def'
pub fn split_part<T: StringOffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let string_array: &GenericStringArray<T> = args[0]
        .as_any()
        .downcast_ref::<GenericStringArray<T>>()
        .unwrap();

    let delimiter_array: &GenericStringArray<T> = args[1]
        .as_any()
        .downcast_ref::<GenericStringArray<T>>()
        .unwrap();

    let n_array: &Int64Array = args[2].as_any().downcast_ref::<Int64Array>().unwrap();

    let result = string_array
        .iter()
        .enumerate()
        .map(|(i, x)| {
            if delimiter_array.is_null(i) || n_array.is_null(i) {
                Ok(None)
            } else {
                x.map(|x: &str| {
                    let delimiter = delimiter_array.value(i);
                    let n = n_array.value(i);
                    if n <= 0 {
                        Err(DataFusionError::Execution(
                            "field position must be greater than zero".to_string(),
                        ))
                    } else {
                        let v: Vec<&str> = x.split(delimiter).collect();
                        match v.get(n as usize - 1) {
                            Some(s) => Ok(*s),
                            None => Ok(""),
                        }
                    }
                })
                .transpose()
            }
        })
        .collect::<Result<GenericStringArray<T>>>()?;

    Ok(Arc::new(result) as ArrayRef)
}

/// Returns true if string starts with prefix.
/// starts_with('alphabet', 'alph') = 't'
pub fn starts_with<T: StringOffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let string_array: &GenericStringArray<T> = args[0]
        .as_any()
        .downcast_ref::<GenericStringArray<T>>()
        .unwrap();

    let prefix_array: &GenericStringArray<T> = args[1]
        .as_any()
        .downcast_ref::<GenericStringArray<T>>()
        .unwrap();

    let result = string_array
        .iter()
        .enumerate()
        .map(|(i, x)| {
            if prefix_array.is_null(i) {
                None
            } else {
                x.map(|x: &str| x.starts_with(prefix_array.value(i)))
            }
        })
        .collect::<BooleanArray>();

    Ok(Arc::new(result) as ArrayRef)
}

/// Returns starting index of specified substring within string, or zero if it's not present. (Same as position(substring in string), but note the reversed argument order.)
/// strpos('high', 'ig') = 2
pub fn strpos<T: ArrowPrimitiveType>(args: &[ArrayRef]) -> Result<ArrayRef>
where
    T::Native: StringOffsetSizeTrait,
{
    let string_array: &GenericStringArray<T::Native> = args[0]
        .as_any()
        .downcast_ref::<GenericStringArray<T::Native>>()
        .ok_or_else(|| {
            DataFusionError::Internal("could not cast string to StringArray".to_string())
        })?;

    let substring_array: &GenericStringArray<T::Native> = args[1]
        .as_any()
        .downcast_ref::<GenericStringArray<T::Native>>()
        .ok_or_else(|| {
            DataFusionError::Internal(
                "could not cast substring to StringArray".to_string(),
            )
        })?;

    let result = string_array
        .iter()
        .enumerate()
        .map(|(i, x)| {
            if substring_array.is_null(i) {
                None
            } else {
                x.map(|x: &str| {
                    let substring: &str = substring_array.value(i);
                    // the rfind method returns the byte index which may or may not be the same as the character index due to UTF8 encoding
                    // this method first finds the matching byte using rfind
                    // then maps that to the character index by matching on the grapheme_index of the byte_index
                    T::Native::from_usize(x.to_string().rfind(substring).map_or(
                        0,
                        |byte_offset| {
                            x.grapheme_indices(true)
                                .collect::<Vec<(usize, &str)>>()
                                .iter()
                                .enumerate()
                                .filter(|(_, (offset, _))| *offset == byte_offset)
                                .map(|(index, _)| index)
                                .collect::<Vec<usize>>()
                                .first()
                                .unwrap()
                                .to_owned()
                                + 1
                        },
                    ))
                    .unwrap()
                })
            }
        })
        .collect::<PrimitiveArray<T>>();

    Ok(Arc::new(result) as ArrayRef)
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

/// Converts the number to its equivalent hexadecimal representation.
/// to_hex(2147483647) = '7fffffff'
pub fn to_hex<T: ArrowPrimitiveType>(args: &[ArrayRef]) -> Result<ArrayRef>
where
    T::Native: StringOffsetSizeTrait,
{
    let integer_array: &PrimitiveArray<T> = args[0]
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .unwrap();

    // first map is the iterator, second is for the `Option<_>`
    let result = integer_array
        .iter()
        .map(|x| x.map(|x| format!("{:x}", x.to_usize().unwrap())))
        .collect::<GenericStringArray<i32>>();

    Ok(Arc::new(result) as ArrayRef)
}

/// Replaces each character in string that matches a character in the from set with the corresponding character in the to set. If from is longer than to, occurrences of the extra characters in from are deleted.
/// translate('12345', '143', 'ax') = 'a2x5'
pub fn translate<T: StringOffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let string_array: &GenericStringArray<T> = args[0]
        .as_any()
        .downcast_ref::<GenericStringArray<T>>()
        .unwrap();

    let from_array: &GenericStringArray<T> = args[1]
        .as_any()
        .downcast_ref::<GenericStringArray<T>>()
        .unwrap();

    let to_array: &GenericStringArray<T> = args[2]
        .as_any()
        .downcast_ref::<GenericStringArray<T>>()
        .unwrap();

    let result = string_array
        .iter()
        .enumerate()
        .map(|(i, x)| {
            if from_array.is_null(i) || to_array.is_null(i) {
                None
            } else {
                x.map(|x: &str| {
                    let from = from_array.value(i).graphemes(true).collect::<Vec<&str>>();
                    // create a hashmap to change from O(n) to O(1) from lookup
                    let from_map: HashMap<&str, usize> = from
                        .iter()
                        .enumerate()
                        .map(|(index, c)| (c.to_owned(), index))
                        .collect();

                    let to = to_array.value(i).graphemes(true).collect::<Vec<&str>>();

                    x.graphemes(true)
                        .collect::<Vec<&str>>()
                        .iter()
                        .flat_map(|c| match from_map.get(*c) {
                            Some(n) => to.get(*n).copied(),
                            None => Some(*c),
                        })
                        .collect::<Vec<&str>>()
                        .concat()
                })
            }
        })
        .collect::<GenericStringArray<T>>();

    Ok(Arc::new(result) as ArrayRef)
}

/// Converts the string to all upper case.
/// upper('tom') = 'TOM'
pub fn upper(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    handle(args, |x| x.to_ascii_uppercase(), "upper")
}
