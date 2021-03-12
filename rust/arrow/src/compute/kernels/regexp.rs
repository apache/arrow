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

//! Defines kernel to extract substrings based on a regular
//! expression of a \[Large\]StringArray

use crate::array::{
    Array, ArrayRef, GenericStringArray, GenericStringBuilder, LargeStringArray,
    ListBuilder, StringArray, StringOffsetSizeTrait,
};
use crate::datatypes::DataType;
use crate::error::{ArrowError, Result};
use std::collections::HashMap;

use std::sync::Arc;

use regex::Regex;

fn generic_regexp_match<OffsetSize: StringOffsetSizeTrait>(
    array: &GenericStringArray<OffsetSize>,
    regex_array: &StringArray,
    flags_array: Option<&StringArray>,
) -> Result<ArrayRef> {
    let mut patterns: HashMap<String, Regex> = HashMap::new();
    let builder: GenericStringBuilder<OffsetSize> = GenericStringBuilder::new(0);
    let mut list_builder = ListBuilder::new(builder);

    let complete_pattern = match flags_array {
        Some(flags) => Box::new(regex_array.iter().zip(flags.iter()).map(
            |(pattern, flags)| {
                pattern.map(|pattern| match flags {
                    Some(value) => format!("(?{}){}", value, pattern),
                    None => pattern.to_string(),
                })
            },
        )) as Box<dyn Iterator<Item = Option<String>>>,
        None => Box::new(
            regex_array
                .iter()
                .map(|pattern| pattern.map(|pattern| pattern.to_string())),
        ),
    };
    array
        .iter()
        .zip(complete_pattern)
        .map(|(value, pattern)| {
            match (value, pattern) {
                (Some(value), Some(pattern)) => {
                    let existing_pattern = patterns.get(&pattern);
                    let re = match existing_pattern {
                        Some(re) => re.clone(),
                        None => {
                            let re = Regex::new(pattern.as_str()).map_err(|e| {
                                ArrowError::ComputeError(format!(
                                    "Regular expression did not compile: {:?}",
                                    e
                                ))
                            })?;
                            patterns.insert(pattern, re.clone());
                            re
                        }
                    };
                    match re.captures(value) {
                        Some(caps) => {
                            for m in caps.iter().skip(1) {
                                if let Some(v) = m {
                                    list_builder.values().append_value(v.as_str())?;
                                }
                            }
                            list_builder.append(true)?
                        }
                        None => {
                            list_builder.values().append_value("")?;
                            list_builder.append(true)?
                        }
                    }
                }
                _ => list_builder.append(false)?,
            }
            Ok(())
        })
        .collect::<Result<Vec<()>>>()?;
    Ok(Arc::new(list_builder.finish()))
}

/// Extract all groups matched by a regular expression for a given String array.
pub fn regexp_match(
    array: &Array,
    pattern: &Array,
    flags: Option<&Array>,
) -> Result<ArrayRef> {
    match array.data_type() {
        DataType::LargeUtf8 => generic_regexp_match(
            array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .expect("A large string is expected"),
            pattern
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("A string is expected"),
            flags.map(|x| {
                x.as_any()
                    .downcast_ref::<StringArray>()
                    .expect("A string is expected")
            }),
        ),
        DataType::Utf8 => generic_regexp_match(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("A string is expected"),
            pattern
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("A string is expected"),
            flags.map(|x| {
                x.as_any()
                    .downcast_ref::<StringArray>()
                    .expect("A string is expected")
            }),
        ),
        _ => Err(ArrowError::ComputeError(format!(
            "regexp_match does not support type {:?}",
            array.data_type()
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::ListArray;

    #[test]
    fn match_single_group() -> Result<()> {
        let values = vec![Some("abc-005-def"), Some("X-7-5"), Some("X545"), None];
        let array = StringArray::from(values);
        let pattern = StringArray::from(vec![r".*-(\d*)-.*"; 4]);
        let actual = regexp_match(&array, &pattern, None)?;
        let elem_builder: GenericStringBuilder<i32> = GenericStringBuilder::new(0);
        let mut expected_builder = ListBuilder::new(elem_builder);
        expected_builder.values().append_value("005")?;
        expected_builder.append(true)?;
        expected_builder.values().append_value("7")?;
        expected_builder.append(true)?;
        expected_builder.values().append_value("")?;
        expected_builder.append(true)?;
        expected_builder.append(false)?;
        let expected = expected_builder.finish();
        let result = actual.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(&expected, result);
        Ok(())
    }

    #[test]
    fn match_single_group_with_flags() -> Result<()> {
        let values = vec![Some("abc-005-def"), Some("X-7-5"), Some("X545"), None];
        let array = StringArray::from(values);
        let pattern = StringArray::from(vec![r"x.*-(\d*)-.*"; 4]);
        let flags = StringArray::from(vec!["i"; 4]);
        let actual = regexp_match(&array, &pattern, Some(&flags))?;
        let elem_builder: GenericStringBuilder<i32> = GenericStringBuilder::new(0);
        let mut expected_builder = ListBuilder::new(elem_builder);
        expected_builder.values().append_value("")?;
        expected_builder.append(true)?;
        expected_builder.values().append_value("7")?;
        expected_builder.append(true)?;
        expected_builder.values().append_value("")?;
        expected_builder.append(true)?;
        expected_builder.append(false)?;
        let expected = expected_builder.finish();
        let result = actual.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(&expected, result);
        Ok(())
    }
}
