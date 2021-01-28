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

use crate::{array::*, buffer::Buffer};
use crate::{
    datatypes::DataType,
    error::{ArrowError, Result},
};
use std::sync::Arc;

fn clone_null_buffer(array: &impl Array) -> Option<Buffer> {
    array
        .data_ref()
        .null_bitmap()
        .as_ref()
        .map(|b| b.bits.clone())
}

fn length_from_offsets<T: OffsetSizeTrait>(offsets: &[T]) -> Buffer {
    let lengths = offsets.windows(2).map(|offset| offset[1] - offset[0]);

    // JUSTIFICATION
    //  Benefit
    //      ~60% speedup
    //  Soundness
    //      `lengths` is `TrustedLen`
    unsafe { Buffer::from_trusted_len_iter(lengths) }
}

fn length_string<OffsetSize>(
    array: &GenericStringArray<OffsetSize>,
    data_type: DataType,
) -> ArrayRef
where
    OffsetSize: StringOffsetSizeTrait,
{
    make_array(Arc::new(ArrayData::new(
        data_type,
        array.len(),
        None,
        clone_null_buffer(array),
        0,
        vec![length_from_offsets(array.value_offsets())],
        vec![],
    )))
}

fn length_list<OffsetSize>(
    array: &GenericListArray<OffsetSize>,
    data_type: DataType,
) -> ArrayRef
where
    OffsetSize: OffsetSizeTrait,
{
    make_array(Arc::new(ArrayData::new(
        data_type,
        array.len(),
        None,
        clone_null_buffer(array),
        0,
        vec![length_from_offsets(array.value_offsets())],
        vec![],
    )))
}

fn length_binary<OffsetSize>(
    array: &GenericBinaryArray<OffsetSize>,
    data_type: DataType,
) -> ArrayRef
where
    OffsetSize: BinaryOffsetSizeTrait,
{
    make_array(Arc::new(ArrayData::new(
        data_type,
        array.len(),
        None,
        clone_null_buffer(array),
        0,
        vec![length_from_offsets(array.value_offsets())],
        vec![],
    )))
}

/// Returns an array of Int32/Int64 denoting the number of characters in each string in the array.
///
/// * this only accepts StringArray/Utf8 and LargeString/LargeUtf8
/// * length of null is null.
/// * length is in number of bytes
pub fn length(array: &Array) -> Result<ArrayRef> {
    match array.data_type() {
        DataType::Binary => {
            let array = array.as_any().downcast_ref::<BinaryArray>().unwrap();
            Ok(length_binary(array, DataType::Int32))
        }
        DataType::LargeBinary => {
            let array = array.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
            Ok(length_binary(array, DataType::Int64))
        }
        DataType::List(_) => {
            let array = array.as_any().downcast_ref::<ListArray>().unwrap();
            Ok(length_list(array, DataType::Int32))
        }
        DataType::LargeList(_) => {
            let array = array.as_any().downcast_ref::<LargeListArray>().unwrap();
            Ok(length_list(array, DataType::Int64))
        }
        DataType::Utf8 => {
            let array = array.as_any().downcast_ref::<StringArray>().unwrap();
            Ok(length_string(array, DataType::Int32))
        }
        DataType::LargeUtf8 => {
            let array = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            Ok(length_string(array, DataType::Int64))
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

    fn cases() -> Vec<(Vec<&'static str>, usize, Vec<i32>)> {
        fn double_vec<T: Clone>(v: Vec<T>) -> Vec<T> {
            [&v[..], &v[..]].concat()
        }

        // a large array
        let mut values = vec!["one", "on", "o", ""];
        let mut expected = vec![3, 2, 1, 0];
        for _ in 0..10 {
            values = double_vec(values);
            expected = double_vec(expected);
        }

        vec![
            (vec!["hello", " ", "world"], 3, vec![5, 1, 5]),
            (vec!["hello", " ", "world", "!"], 4, vec![5, 1, 5, 1]),
            (vec!["💖"], 1, vec![4]),
            (values, 4096, expected),
        ]
    }

    #[test]
    fn test_string() -> Result<()> {
        cases().into_iter().try_for_each(|(input, len, expected)| {
            let array = StringArray::from(input);
            let result = length(&array)?;
            assert_eq!(len, result.len());
            let result = result.as_any().downcast_ref::<Int32Array>().unwrap();
            expected.iter().enumerate().for_each(|(i, value)| {
                assert_eq!(*value, result.value(i));
            });
            Ok(())
        })
    }

    #[test]
    fn test_large_string() -> Result<()> {
        cases().into_iter().try_for_each(|(input, len, expected)| {
            let array = LargeStringArray::from(input);
            let result = length(&array)?;
            assert_eq!(len, result.len());
            let result = result.as_any().downcast_ref::<Int64Array>().unwrap();
            expected.iter().enumerate().for_each(|(i, value)| {
                assert_eq!(*value as i64, result.value(i));
            });
            Ok(())
        })
    }

    fn null_cases() -> Vec<(Vec<Option<&'static str>>, usize, Vec<Option<i32>>)> {
        vec![(
            vec![Some("one"), None, Some("three"), Some("four")],
            4,
            vec![Some(3), None, Some(5), Some(4)],
        )]
    }

    #[test]
    fn null_string() -> Result<()> {
        null_cases()
            .into_iter()
            .try_for_each(|(input, len, expected)| {
                let array = StringArray::from(input);
                let result = length(&array)?;
                assert_eq!(len, result.len());
                let result = result.as_any().downcast_ref::<Int32Array>().unwrap();

                let expected: Int32Array = expected.into();
                assert_eq!(expected.data(), result.data());
                Ok(())
            })
    }

    #[test]
    fn null_large_string() -> Result<()> {
        null_cases()
            .into_iter()
            .try_for_each(|(input, len, expected)| {
                let array = LargeStringArray::from(input);
                let result = length(&array)?;
                assert_eq!(len, result.len());
                let result = result.as_any().downcast_ref::<Int64Array>().unwrap();

                // convert to i64
                let expected: Int64Array = expected
                    .iter()
                    .map(|e| e.map(|e| e as i64))
                    .collect::<Vec<_>>()
                    .into();
                assert_eq!(expected.data(), result.data());
                Ok(())
            })
    }

    /// Tests that length is not valid for u64.
    #[test]
    fn wrong_type() {
        let array: UInt64Array = vec![1u64].into();

        assert!(length(&array).is_err());
    }

    /// Tests with an offset
    #[test]
    fn offsets() -> Result<()> {
        let a = StringArray::from(vec!["hello", " ", "world"]);
        let b = make_array(
            ArrayData::builder(DataType::Utf8)
                .len(2)
                .offset(1)
                .buffers(a.data_ref().buffers().to_vec())
                .build(),
        );
        let result = length(b.as_ref())?;

        let expected = Int32Array::from(vec![1, 5]);
        assert_eq!(expected.data(), result.data());

        Ok(())
    }

    #[test]
    fn test_binary() -> Result<()> {
        let data: Vec<&[u8]> = vec![b"hello", b" ", b"world"];
        let a = BinaryArray::from(data);
        let result = length(&a)?;

        let expected: &Array = &Int32Array::from(vec![5, 1, 5]);
        assert_eq!(expected, result.as_ref());

        Ok(())
    }
}
