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

//! Unicode expressions

use std::any::type_name;
use std::cmp::Ordering;
use std::sync::Arc;

use crate::error::{DataFusionError, Result};
use arrow::{
    array::{
        ArrayRef, GenericStringArray, Int64Array, PrimitiveArray, StringOffsetSizeTrait,
    },
    datatypes::{ArrowNativeType, ArrowPrimitiveType},
};
use hashbrown::HashMap;
use unicode_segmentation::UnicodeSegmentation;

macro_rules! downcast_string_arg {
    ($ARG:expr, $NAME:expr, $T:ident) => {{
        $ARG.as_any()
            .downcast_ref::<GenericStringArray<T>>()
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "could not cast {} to {}",
                    $NAME,
                    type_name::<GenericStringArray<T>>()
                ))
            })?
    }};
}

macro_rules! downcast_arg {
    ($ARG:expr, $NAME:expr, $ARRAY_TYPE:ident) => {{
        $ARG.as_any().downcast_ref::<$ARRAY_TYPE>().ok_or_else(|| {
            DataFusionError::Internal(format!(
                "could not cast {} to {}",
                $NAME,
                type_name::<$ARRAY_TYPE>()
            ))
        })?
    }};
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
        .ok_or_else(|| {
            DataFusionError::Internal("could not cast string to StringArray".to_string())
        })?;

    let result = string_array
        .iter()
        .map(|string| {
            string.map(|string: &str| {
                T::Native::from_usize(string.graphemes(true).count()).expect(
                    "should not fail as graphemes.count will always return integer",
                )
            })
        })
        .collect::<PrimitiveArray<T>>();

    Ok(Arc::new(result) as ArrayRef)
}

/// Returns first n characters in the string, or when n is negative, returns all but last |n| characters.
/// left('abcde', 2) = 'ab'
pub fn left<T: StringOffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let string_array = downcast_string_arg!(args[0], "string", T);
    let n_array = downcast_arg!(args[1], "n", Int64Array);

    let result = string_array
        .iter()
        .zip(n_array.iter())
        .map(|(string, n)| match (string, n) {
            (Some(string), Some(n)) => match n.cmp(&0) {
                Ordering::Less => {
                    let graphemes = string.graphemes(true);
                    let len = graphemes.clone().count() as i64;
                    match n.abs().cmp(&len) {
                        Ordering::Less => {
                            Some(graphemes.take((len + n) as usize).collect::<String>())
                        }
                        Ordering::Equal => Some("".to_string()),
                        Ordering::Greater => Some("".to_string()),
                    }
                }
                Ordering::Equal => Some("".to_string()),
                Ordering::Greater => {
                    Some(string.graphemes(true).take(n as usize).collect::<String>())
                }
            },
            _ => None,
        })
        .collect::<GenericStringArray<T>>();

    Ok(Arc::new(result) as ArrayRef)
}

/// Extends the string to length 'length' by prepending the characters fill (a space by default). If the string is already longer than length then it is truncated (on the right).
/// lpad('hi', 5, 'xy') = 'xyxhi'
pub fn lpad<T: StringOffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args.len() {
        2 => {
            let string_array = downcast_string_arg!(args[0], "string", T);
            let length_array = downcast_arg!(args[1], "length", Int64Array);

            let result = string_array
                .iter()
                .zip(length_array.iter())
                .map(|(string, length)| match (string, length) {
                    (Some(string), Some(length)) => {
                        let length = length as usize;
                        if length == 0 {
                            Some("".to_string())
                        } else {
                            let graphemes = string.graphemes(true).collect::<Vec<&str>>();
                            if length < graphemes.len() {
                                Some(graphemes[..length].concat())
                            } else {
                                let mut s = string.to_string();
                                s.insert_str(
                                    0,
                                    " ".repeat(length - graphemes.len()).as_str(),
                                );
                                Some(s)
                            }
                        }
                    }
                    _ => None,
                })
                .collect::<GenericStringArray<T>>();

            Ok(Arc::new(result) as ArrayRef)
        }
        3 => {
            let string_array = downcast_string_arg!(args[0], "string", T);
            let length_array = downcast_arg!(args[1], "length", Int64Array);
            let fill_array = downcast_string_arg!(args[2], "fill", T);

            let result = string_array
                .iter()
                .zip(length_array.iter())
                .zip(fill_array.iter())
                .map(|((string, length), fill)| match (string, length, fill) {
                    (Some(string), Some(length), Some(fill)) => {
                        let length = length as usize;

                        if length == 0 {
                            Some("".to_string())
                        } else {
                            let graphemes = string.graphemes(true).collect::<Vec<&str>>();
                            let fill_chars = fill.chars().collect::<Vec<char>>();

                            if length < graphemes.len() {
                                Some(graphemes[..length].concat())
                            } else if fill_chars.is_empty() {
                                Some(string.to_string())
                            } else {
                                let mut s = string.to_string();
                                let mut char_vector =
                                    Vec::<char>::with_capacity(length - graphemes.len());
                                for l in 0..length - graphemes.len() {
                                    char_vector.push(
                                        *fill_chars.get(l % fill_chars.len()).unwrap(),
                                    );
                                }
                                s.insert_str(
                                    0,
                                    char_vector.iter().collect::<String>().as_str(),
                                );
                                Some(s)
                            }
                        }
                    }
                    _ => None,
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

/// Reverses the order of the characters in the string.
/// reverse('abcde') = 'edcba'
pub fn reverse<T: StringOffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let string_array = downcast_string_arg!(args[0], "string", T);

    let result = string_array
        .iter()
        .map(|string| {
            string.map(|string: &str| string.graphemes(true).rev().collect::<String>())
        })
        .collect::<GenericStringArray<T>>();

    Ok(Arc::new(result) as ArrayRef)
}

/// Returns last n characters in the string, or when n is negative, returns all but first |n| characters.
/// right('abcde', 2) = 'de'
pub fn right<T: StringOffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let string_array = downcast_string_arg!(args[0], "string", T);
    let n_array = downcast_arg!(args[1], "n", Int64Array);

    let result = string_array
        .iter()
        .zip(n_array.iter())
        .map(|(string, n)| match (string, n) {
            (Some(string), Some(n)) => match n.cmp(&0) {
                Ordering::Less => {
                    let graphemes = string.graphemes(true).rev();
                    let len = graphemes.clone().count() as i64;
                    match n.abs().cmp(&len) {
                        Ordering::Less => Some(
                            graphemes
                                .take((len + n) as usize)
                                .collect::<Vec<&str>>()
                                .iter()
                                .rev()
                                .copied()
                                .collect::<String>(),
                        ),
                        Ordering::Equal => Some("".to_string()),
                        Ordering::Greater => Some("".to_string()),
                    }
                }
                Ordering::Equal => Some("".to_string()),
                Ordering::Greater => Some(
                    string
                        .graphemes(true)
                        .rev()
                        .take(n as usize)
                        .collect::<Vec<&str>>()
                        .iter()
                        .rev()
                        .copied()
                        .collect::<String>(),
                ),
            },
            _ => None,
        })
        .collect::<GenericStringArray<T>>();

    Ok(Arc::new(result) as ArrayRef)
}

/// Extends the string to length 'length' by appending the characters fill (a space by default). If the string is already longer than length then it is truncated.
/// rpad('hi', 5, 'xy') = 'hixyx'
pub fn rpad<T: StringOffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args.len() {
        2 => {
            let string_array = downcast_string_arg!(args[0], "string", T);
            let length_array = downcast_arg!(args[1], "length", Int64Array);

            let result = string_array
                .iter()
                .zip(length_array.iter())
                .map(|(string, length)| match (string, length) {
                    (Some(string), Some(length)) => {
                        let length = length as usize;
                        if length == 0 {
                            Some("".to_string())
                        } else {
                            let graphemes = string.graphemes(true).collect::<Vec<&str>>();
                            if length < graphemes.len() {
                                Some(graphemes[..length].concat())
                            } else {
                                let mut s = string.to_string();
                                s.push_str(" ".repeat(length - graphemes.len()).as_str());
                                Some(s)
                            }
                        }
                    }
                    _ => None,
                })
                .collect::<GenericStringArray<T>>();

            Ok(Arc::new(result) as ArrayRef)
        }
        3 => {
            let string_array = downcast_string_arg!(args[0], "string", T);
            let length_array = downcast_arg!(args[1], "length", Int64Array);
            let fill_array = downcast_string_arg!(args[2], "fill", T);

            let result = string_array
                .iter()
                .zip(length_array.iter())
                .zip(fill_array.iter())
                .map(|((string, length), fill)| match (string, length, fill) {
                    (Some(string), Some(length), Some(fill)) => {
                        let length = length as usize;
                        let graphemes = string.graphemes(true).collect::<Vec<&str>>();
                        let fill_chars = fill.chars().collect::<Vec<char>>();

                        if length < graphemes.len() {
                            Some(graphemes[..length].concat())
                        } else if fill_chars.is_empty() {
                            Some(string.to_string())
                        } else {
                            let mut s = string.to_string();
                            let mut char_vector =
                                Vec::<char>::with_capacity(length - graphemes.len());
                            for l in 0..length - graphemes.len() {
                                char_vector
                                    .push(*fill_chars.get(l % fill_chars.len()).unwrap());
                            }
                            s.push_str(char_vector.iter().collect::<String>().as_str());
                            Some(s)
                        }
                    }
                    _ => None,
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
        .zip(substring_array.iter())
        .map(|(string, substring)| match (string, substring) {
            (Some(string), Some(substring)) => {
                // the rfind method returns the byte index of the substring which may or may not be the same as the character index due to UTF8 encoding
                // this method first finds the matching byte using rfind
                // then maps that to the character index by matching on the grapheme_index of the byte_index
                Some(
                    T::Native::from_usize(string.to_string().rfind(substring).map_or(
                        0,
                        |byte_offset| {
                            string
                                .grapheme_indices(true)
                                .collect::<Vec<(usize, &str)>>()
                                .iter()
                                .enumerate()
                                .filter(|(_, (offset, _))| *offset == byte_offset)
                                .map(|(index, _)| index)
                                .collect::<Vec<usize>>()
                                .first()
                                .expect("should not fail as grapheme_indices and byte offsets are tightly coupled")
                                .to_owned()
                                + 1
                        },
                    ))
                    .expect("should not fail due to map_or default value")
                )
            }
            _ => None,
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
            let string_array = downcast_string_arg!(args[0], "string", T);
            let start_array = downcast_arg!(args[1], "start", Int64Array);

            let result = string_array
                .iter()
                .zip(start_array.iter())
                .map(|(string, start)| match (string, start) {
                    (Some(string), Some(start)) => {
                        if start <= 0 {
                            Some(string.to_string())
                        } else {
                            let graphemes = string.graphemes(true).collect::<Vec<&str>>();
                            let start_pos = start as usize - 1;
                            if graphemes.len() < start_pos {
                                Some("".to_string())
                            } else {
                                Some(graphemes[start_pos..].concat())
                            }
                        }
                    }
                    _ => None,
                })
                .collect::<GenericStringArray<T>>();

            Ok(Arc::new(result) as ArrayRef)
        }
        3 => {
            let string_array = downcast_string_arg!(args[0], "string", T);
            let start_array = downcast_arg!(args[1], "start", Int64Array);
            let count_array = downcast_arg!(args[2], "count", Int64Array);

            let result = string_array
                .iter()
                .zip(start_array.iter())
                .zip(count_array.iter())
                .map(|((string, start), count)| match (string, start, count) {
                    (Some(string), Some(start), Some(count)) => {
                        if count < 0 {
                            Err(DataFusionError::Execution(
                                "negative substring length not allowed".to_string(),
                            ))
                        } else if start <= 0 {
                            Ok(Some(string.to_string()))
                        } else {
                            let graphemes = string.graphemes(true).collect::<Vec<&str>>();
                            let start_pos = start as usize - 1;
                            let count_usize = count as usize;
                            if graphemes.len() < start_pos {
                                Ok(Some("".to_string()))
                            } else if graphemes.len() < start_pos + count_usize {
                                Ok(Some(graphemes[start_pos..].concat()))
                            } else {
                                Ok(Some(
                                    graphemes[start_pos..start_pos + count_usize]
                                        .concat(),
                                ))
                            }
                        }
                    }
                    _ => Ok(None),
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

/// Replaces each character in string that matches a character in the from set with the corresponding character in the to set. If from is longer than to, occurrences of the extra characters in from are deleted.
/// translate('12345', '143', 'ax') = 'a2x5'
pub fn translate<T: StringOffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let string_array = downcast_string_arg!(args[0], "string", T);
    let from_array = downcast_string_arg!(args[1], "from", T);
    let to_array = downcast_string_arg!(args[2], "to", T);

    let result = string_array
        .iter()
        .zip(from_array.iter())
        .zip(to_array.iter())
        .map(|((string, from), to)| match (string, from, to) {
            (Some(string), Some(from), Some(to)) => {
                // create a hashmap of [char, index] to change from O(n) to O(1) for from list
                let from_map: HashMap<&str, usize> = from
                    .graphemes(true)
                    .collect::<Vec<&str>>()
                    .iter()
                    .enumerate()
                    .map(|(index, c)| (c.to_owned(), index))
                    .collect();

                let to = to.graphemes(true).collect::<Vec<&str>>();

                Some(
                    string
                        .graphemes(true)
                        .collect::<Vec<&str>>()
                        .iter()
                        .flat_map(|c| match from_map.get(*c) {
                            Some(n) => to.get(*n).copied(),
                            None => Some(*c),
                        })
                        .collect::<Vec<&str>>()
                        .concat(),
                )
            }
            _ => None,
        })
        .collect::<GenericStringArray<T>>();

    Ok(Arc::new(result) as ArrayRef)
}
