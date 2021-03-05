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

use std::sync::Arc;

use regex::Regex;

fn generic_regexp_extract<OffsetSize: StringOffsetSizeTrait>(
    array: &GenericStringArray<OffsetSize>,
    re: &Regex,
    idx: usize,
) -> Result<ArrayRef> {
    let mut builder: GenericStringBuilder<OffsetSize> = GenericStringBuilder::new(0);

    for maybe_value in array.iter() {
        match maybe_value {
            Some(value) => match re.captures(value) {
                Some(caps) => {
                    let m = caps.get(idx).ok_or_else(|| {
                        ArrowError::ComputeError(format!(
                            "Regexp has no group with index {}",
                            idx
                        ))
                    })?;
                    builder.append_value(m.as_str())?
                }
                None => builder.append_null()?,
            },
            None => builder.append_null()?,
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn generic_regexp_match<OffsetSize: StringOffsetSizeTrait>(
    array: &GenericStringArray<OffsetSize>,
    re: &Regex,
) -> Result<ArrayRef> {
    let builder: GenericStringBuilder<OffsetSize> = GenericStringBuilder::new(0);
    let mut list_builder = ListBuilder::new(builder);

    for maybe_value in array.iter() {
        match maybe_value {
            Some(value) => match re.captures(value) {
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
            },
            None => list_builder.append(false)?,
        }
    }
    Ok(Arc::new(list_builder.finish()))
}

/// Extracts a specific group matched by a regular expression for a given String array.
/// Group index 0 returns the whole match, index 1 returns the first group and so on. Please
/// refer to regex crate for details on pattern specifics.
pub fn regexp_extract(array: &Array, pattern: &str, idx: usize) -> Result<ArrayRef> {
    let re = Regex::new(pattern).map_err(|e| {
        ArrowError::ComputeError(format!("Regular expression did not compile: {:?}", e))
    })?;
    match array.data_type() {
        DataType::LargeUtf8 => generic_regexp_extract(
            array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .expect("A large string is expected"),
            &re,
            idx,
        ),
        DataType::Utf8 => generic_regexp_extract(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("A string is expected"),
            &re,
            idx,
        ),
        _ => Err(ArrowError::ComputeError(format!(
            "regexp_extract does not support type {:?}",
            array.data_type()
        ))),
    }
}

/// Extract all groups matched by a regular expression for a given String array.
pub fn regexp_match(array: &Array, pattern: &str) -> Result<ArrayRef> {
    let re = Regex::new(pattern).map_err(|e| {
        ArrowError::ComputeError(format!("Regular expression did not compile: {:?}", e))
    })?;
    match array.data_type() {
        DataType::LargeUtf8 => generic_regexp_match(
            array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .expect("A large string is expected"),
            &re,
        ),
        DataType::Utf8 => generic_regexp_match(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("A string is expected"),
            &re,
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
    fn extract_single_group() -> Result<()> {
        let values = vec!["abc-005-def", "X-7-5", "X545"];
        let array = StringArray::from(values);
        let pattern = r".*-(\d*)-.*";
        let actual = regexp_extract(&array, pattern, 1)?;
        let expected = StringArray::from(vec![Some("005"), Some("7"), None]);
        let result = actual.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(&expected, result);
        Ok(())
    }

    #[test]
    fn match_single_group() -> Result<()> {
        let values = vec![Some("abc-005-def"), Some("X-7-5"), Some("X545"), None];
        let array = StringArray::from(values);
        let pattern = r".*-(\d*)-.*";
        let actual = regexp_match(&array, pattern)?;
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
}
