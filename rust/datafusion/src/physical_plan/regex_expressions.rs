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

//! Regex expressions

use std::any::type_name;
use std::sync::Arc;

use crate::error::{DataFusionError, Result};
use arrow::array::{ArrayRef, GenericStringArray, StringOffsetSizeTrait};
use arrow::compute;
use hashbrown::HashMap;
use regex::Regex;

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

/// extract a specific group from a string column, using a regular expression
pub fn regexp_match<T: StringOffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args.len() {
        2 => compute::regexp_match(downcast_string_arg!(args[0], "string", T), downcast_string_arg!(args[1], "pattern", T), None)
        .map_err(DataFusionError::ArrowError),
        3 => compute::regexp_match(downcast_string_arg!(args[0], "string", T), downcast_string_arg!(args[1], "pattern", T),  Some(downcast_string_arg!(args[1], "flags", T)))
        .map_err(DataFusionError::ArrowError),
        other => Err(DataFusionError::Internal(format!(
            "regexp_match was called with {} arguments. It requires at least 2 and at most 3.",
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

/// Replaces substring(s) matching a POSIX regular expression.
///
/// example: `regexp_replace('Thomas', '.[mN]a.', 'M') = 'ThM'`
pub fn regexp_replace<T: StringOffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    // creating Regex is expensive so create hashmap for memoization
    let mut patterns: HashMap<String, Regex> = HashMap::new();

    match args.len() {
        3 => {
            let string_array = downcast_string_arg!(args[0], "string", T);
            let pattern_array = downcast_string_arg!(args[1], "pattern", T);
            let replacement_array = downcast_string_arg!(args[2], "replacement", T);

            let result = string_array
            .iter()
            .zip(pattern_array.iter())
            .zip(replacement_array.iter())
            .map(|((string, pattern), replacement)| match (string, pattern, replacement) {
                (Some(string), Some(pattern), Some(replacement)) => {
                    let replacement = regex_replace_posix_groups(replacement);

                    // if patterns hashmap already has regexp then use else else create and return
                    let re = match patterns.get(pattern) {
                        Some(re) => Ok(re.clone()),
                        None => {
                            match Regex::new(pattern) {
                                Ok(re) => {
                                    patterns.insert(pattern.to_string(), re.clone());
                                    Ok(re)
                                },
                                Err(err) => Err(DataFusionError::Execution(err.to_string())),
                            }
                        }
                    };

                    Some(re.map(|re| re.replace(string, replacement.as_str()))).transpose()
                }
            _ => Ok(None)
            })
            .collect::<Result<GenericStringArray<T>>>()?;

            Ok(Arc::new(result) as ArrayRef)
        }
        4 => {
            let string_array = downcast_string_arg!(args[0], "string", T);
            let pattern_array = downcast_string_arg!(args[1], "pattern", T);
            let replacement_array = downcast_string_arg!(args[2], "replacement", T);
            let flags_array = downcast_string_arg!(args[3], "flags", T);

            let result = string_array
            .iter()
            .zip(pattern_array.iter())
            .zip(replacement_array.iter())
            .zip(flags_array.iter())
            .map(|(((string, pattern), replacement), flags)| match (string, pattern, replacement, flags) {
                (Some(string), Some(pattern), Some(replacement), Some(flags)) => {
                    let replacement = regex_replace_posix_groups(replacement);

                    // format flags into rust pattern
                    let (pattern, replace_all) = if flags == "g" {
                        (pattern.to_string(), true)
                    } else if flags.contains('g') {
                        (format!("(?{}){}", flags.to_string().replace("g", ""), pattern), true)
                    } else {
                        (format!("(?{}){}", flags, pattern), false)
                    };

                    // if patterns hashmap already has regexp then use else else create and return
                    let re = match patterns.get(&pattern) {
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

                    Some(re.map(|re| {
                        if replace_all {
                            re.replace_all(string, replacement.as_str())
                        } else {
                            re.replace(string, replacement.as_str())
                        }
                    })).transpose()
                }
            _ => Ok(None)
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
