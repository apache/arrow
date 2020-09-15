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

//! Defines kernel to extract a substring of a StringArray

use crate::{array::*, buffer::Buffer, datatypes::ToByteSlice};
use crate::{
    datatypes::DataType,
    error::{ArrowError, Result},
};
use std::sync::Arc;

/// Returns an ArrayRef with a substring starting from `start` and with optional length `length` of each of the elements in `array`.
/// `start` can be negative, in which case the start counts from the end of the string.
pub fn substring(array: &Array, start: i32, length: &Option<u32>) -> Result<ArrayRef> {
    match array.data_type() {
        DataType::Utf8 => {
            // compute current offsets
            let offsets = array.data_ref().clone().buffers()[0].clone();
            let offsets: &[u32] = unsafe { offsets.typed_data::<u32>() };

            // compute null bitmap (copy)
            let null_bit_buffer = array.data_ref().null_buffer().map(|e| e.clone());

            // compute values
            let values = &array.data_ref().buffers()[1];
            let data = values.data();

            let mut new_values = Vec::new(); // we have no way to estimate how much this will be.
            let mut new_offsets = Vec::with_capacity(array.len() + 1);

            let mut length_so_far = 0i32;
            new_offsets.push(length_so_far);
            (0..array.len()).for_each(|i| {
                // the length of this entry
                let lenght_i = offsets[i + 1] - offsets[i];
                // compute where we should start slicing this entry
                let start = offsets[i] as i32
                    + if start >= 0 {
                        start
                    } else {
                        lenght_i as i32 + start
                    };

                let start =
                    start.max(offsets[i] as i32).min(offsets[i + 1] as i32) as usize;
                // compute the lenght of the slice
                let length = length
                    .unwrap_or(lenght_i)
                    // .max(0) is not needed as it is guaranteed
                    .min(offsets[i + 1] - start as u32) // so we do not go beyond this entry
                    as i32;

                length_so_far += length;

                new_offsets.push(length_so_far);
                new_values.extend_from_slice(&data[start..start + length as usize]);
            });

            let data = ArrayData::new(
                DataType::Utf8,
                array.len(),
                None,
                null_bit_buffer,
                0,
                vec![
                    Buffer::from(new_offsets.to_byte_slice()),
                    Buffer::from(&new_values[..]),
                ],
                vec![],
            );
            Ok(Arc::new(StringArray::from(Arc::new(data))))
        }
        _ => Err(ArrowError::ComputeError(format!(
            "substring does not support type {:?}",
            array.data_type()
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn with_nulls() -> Result<()> {
        let cases = vec![
            // identity
            (
                vec![Some("hello"), None, Some("word")],
                0,
                None,
                vec![Some("hello"), None, Some("word")],
            ),
            // 0 length -> Nothing
            (
                vec![Some("hello"), None, Some("word")],
                0,
                Some(0),
                vec![Some(""), None, Some("")],
            ),
            // high start -> Nothing
            (
                vec![Some("hello"), None, Some("word")],
                1000,
                Some(0),
                vec![Some(""), None, Some("")],
            ),
            // high negative start -> identity
            (
                vec![Some("hello"), None, Some("word")],
                -1000,
                None,
                vec![Some("hello"), None, Some("word")],
            ),
            // high length -> identity
            (
                vec![Some("hello"), None, Some("word")],
                0,
                Some(1000),
                vec![Some("hello"), None, Some("word")],
            ),
        ];

        cases
            .into_iter()
            .map(|(array, start, length, expected)| {
                let array = StringArray::from(array);
                let result = substring(&array, start, &length)?;
                assert_eq!(array.len(), result.len());

                let result = result.as_any().downcast_ref::<StringArray>().unwrap();
                let expected = StringArray::from(expected);
                assert_eq!(&expected, result,);
                Ok(())
            })
            .collect::<Result<()>>()?;

        Ok(())
    }

    #[test]
    fn substring_non_null() -> Result<()> {
        let cases = vec![
            // increase start
            (
                vec!["hello", "", "word"],
                0,
                None,
                vec!["hello", "", "word"],
            ),
            (vec!["hello", "", "word"], 1, None, vec!["ello", "", "ord"]),
            (vec!["hello", "", "word"], 2, None, vec!["llo", "", "rd"]),
            (vec!["hello", "", "word"], 3, None, vec!["lo", "", "d"]),
            (vec!["hello", "", "word"], 10, None, vec!["", "", ""]),
            // increase start negatively
            (vec!["hello", "", "word"], -1, None, vec!["o", "", "d"]),
            (vec!["hello", "", "word"], -2, None, vec!["lo", "", "rd"]),
            (vec!["hello", "", "word"], -3, None, vec!["llo", "", "ord"]),
            (
                vec!["hello", "", "word"],
                -10,
                None,
                vec!["hello", "", "word"],
            ),
            // increase length
            (vec!["hello", "", "word"], 1, Some(1), vec!["e", "", "o"]),
            (vec!["hello", "", "word"], 1, Some(2), vec!["el", "", "or"]),
            (
                vec!["hello", "", "word"],
                1,
                Some(3),
                vec!["ell", "", "ord"],
            ),
            (
                vec!["hello", "", "word"],
                1,
                Some(4),
                vec!["ello", "", "ord"],
            ),
            (vec!["hello", "", "word"], -3, Some(1), vec!["l", "", "o"]),
            (vec!["hello", "", "word"], -3, Some(2), vec!["ll", "", "or"]),
            (
                vec!["hello", "", "word"],
                -3,
                Some(3),
                vec!["llo", "", "ord"],
            ),
            (
                vec!["hello", "", "word"],
                -3,
                Some(4),
                vec!["llo", "", "ord"],
            ),
        ];

        cases
            .into_iter()
            .map(|(array, start, length, expected)| {
                let array = StringArray::from(array);
                let result = substring(&array, start, &length)?;
                assert_eq!(array.len(), result.len());
                let result = result.as_any().downcast_ref::<StringArray>().unwrap();
                let expected = StringArray::from(expected);
                assert_eq!(&expected, result,);
                Ok(())
            })
            .collect::<Result<()>>()?;

        Ok(())
    }
}
