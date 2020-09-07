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

//! Defines kernel for length of a string array

use crate::array::*;
use crate::{
    datatypes::DataType,
    datatypes::UInt32Type,
    error::{ArrowError, Result},
};
use std::sync::Arc;

/// Returns an array of UInt32 denoting the number of characters in each string in the array.
///
/// * this only accepts StringArray
/// * length of null is null.
/// * length is in number of bytes
pub fn length(array: &Array) -> Result<UInt32Array> {
    match array.data_type() {
        DataType::Utf8 => {
            // note: offsets are stored as u8, but they can be interpreted as u32
            let offsets = array.data_ref().clone().buffers()[0].clone();
            // this is a 30% improvement over iterating over u8s and building u32, which
            // justifies the usage of `unsafe`.
            let slice: &[u32] = unsafe { offsets.typed_data::<u32>() };

            let mut builder = UInt32BufferBuilder::new(array.len());
            let lengths: Vec<u32> = slice
                .windows(2)
                .map(|offset| offset[1] - offset[0])
                .collect();
            builder.append_slice(lengths.as_slice())?;

            let null_bit_buffer = array
                .data_ref()
                .null_bitmap()
                .as_ref()
                .map(|b| b.bits.clone());

            let data = ArrayData::new(
                DataType::UInt32,
                array.len(),
                None,
                null_bit_buffer,
                0,
                vec![builder.finish()],
                vec![],
            );
            Ok(PrimitiveArray::<UInt32Type>::from(Arc::new(data)))
        }
        _ => Err(ArrowError::ComputeError(format!(
            "length not supported for {:?}",
            array.data_type()
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Tests a vector whose len is not a multiple of 4
    #[test]
    fn len_3() -> Result<()> {
        let array = StringArray::from(vec!["hello", " ", "world"]);
        let result = length(&array)?;
        assert_eq!(3, result.len());
        assert_eq!(
            vec![5, 1, 5],
            vec![result.value(0), result.value(1), result.value(2)]
        );
        Ok(())
    }

    /// Tests a vector whose len is multiple of 4
    #[test]
    fn len_4() -> Result<()> {
        let array = StringArray::from(vec!["hello", " ", "world", "!"]);
        let result = length(&array)?;
        assert_eq!(4, result.len());
        assert_eq!(
            vec![5, 1, 5, 1],
            vec![
                result.value(0),
                result.value(1),
                result.value(2),
                result.value(3)
            ]
        );
        Ok(())
    }

    /// Tests a vector with a character with more than one code point.
    #[test]
    fn special() -> Result<()> {
        let mut builder: StringBuilder = StringBuilder::new(1);
        builder.append_value("💖")?;
        let array = builder.finish();

        let result = length(&array)?;

        assert_eq!(1, result.len());

        assert_eq!(4, result.value(0));
        Ok(())
    }

    /// Tests a vector with more than 255 entries, to ensure that offsets are correctly computed beyond simple cases
    #[test]
    fn long_array() -> Result<()> {
        fn double_vec<T: Clone>(v: Vec<T>) -> Vec<T> {
            [&v[..], &v[..]].concat()
        }

        // double ["hello", " ", "world", "!"] 10 times
        let mut values = vec!["one", "on", "o", ""];
        let mut expected = vec![3, 2, 1, 0];
        for _ in 0..10 {
            values = double_vec(values);
            expected = double_vec(expected);
        }

        let a = StringArray::from(values);

        let result = length(&a)?;

        assert_eq!(4096, result.len()); // 2^12

        let expected: UInt32Array = expected.into();
        assert_eq!(expected, result);
        Ok(())
    }

    /// Tests handling of null values
    #[test]
    fn null() -> Result<()> {
        let mut builder: StringBuilder = StringBuilder::new(4);
        builder.append_value("one")?;
        builder.append_null()?;
        builder.append_value("three")?;
        builder.append_value("four")?;
        let array = builder.finish();

        let a = length(&array)?;
        assert_eq!(a.len(), array.len());

        let expected: UInt32Array = vec![Some(3), None, Some(5), Some(4)].into();

        assert_eq!(expected.data(), a.data());
        Ok(())
    }

    /// Tests that length is not valid for u64.
    #[test]
    fn wrong_type() -> Result<()> {
        let array: UInt64Array = vec![1u64].into();

        assert!(length(&array).is_err());
        Ok(())
    }
}
