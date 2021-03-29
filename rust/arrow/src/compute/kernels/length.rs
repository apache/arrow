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

use crate::{
    array::*,
    buffer::Buffer,
    datatypes::{ArrowNativeType, ArrowPrimitiveType},
};
use crate::{
    datatypes::{DataType, Int32Type, Int64Type},
    error::{ArrowError, Result},
};

fn unary_offsets_string<O, F>(
    array: &GenericStringArray<O>,
    data_type: DataType,
    op: F,
) -> ArrayRef
where
    O: StringOffsetSizeTrait + ArrowNativeType,
    F: Fn(O) -> O,
{
    // note: offsets are stored as u8, but they can be interpreted as OffsetSize
    let offsets = &array.data_ref().buffers()[0];
    // this is a 30% improvement over iterating over u8s and building OffsetSize, which
    // justifies the usage of `unsafe`.
    let slice: &[O] = &unsafe { offsets.typed_data::<O>() }[array.offset()..];

    let lengths = slice.windows(2).map(|offset| op(offset[1] - offset[0]));

    // JUSTIFICATION
    //  Benefit
    //      ~60% speedup
    //  Soundness
    //      `values` is an iterator with a known size.
    let buffer = unsafe { Buffer::from_trusted_len_iter(lengths) };

    let null_bit_buffer = array
        .data_ref()
        .null_bitmap()
        .as_ref()
        .map(|b| b.bits.clone());

    let data = ArrayData::new(
        data_type,
        array.len(),
        None,
        null_bit_buffer,
        0,
        vec![buffer],
        vec![],
    );
    make_array(data)
}

fn octet_length<O: StringOffsetSizeTrait, T: ArrowPrimitiveType>(
    array: &dyn Array,
) -> ArrayRef
where
    T::Native: StringOffsetSizeTrait,
{
    let array = array
        .as_any()
        .downcast_ref::<GenericStringArray<O>>()
        .unwrap();
    unary_offsets_string::<O, _>(array, T::DATA_TYPE, |x| x)
}

fn bit_length_impl<O: StringOffsetSizeTrait, T: ArrowPrimitiveType>(
    array: &dyn Array,
) -> ArrayRef
where
    T::Native: StringOffsetSizeTrait,
{
    let array = array
        .as_any()
        .downcast_ref::<GenericStringArray<O>>()
        .unwrap();
    let bits_in_bytes = O::from_usize(8).unwrap();
    unary_offsets_string::<O, _>(array, T::DATA_TYPE, |x| x * bits_in_bytes)
}

/// Returns an array of Int32/Int64 denoting the number of bytes in each string in the array.
///
/// * this only accepts StringArray/Utf8 and LargeString/LargeUtf8
/// * length of null is null.
/// * length is in number of bytes
pub fn length(array: &Array) -> Result<ArrayRef> {
    match array.data_type() {
        DataType::Utf8 => Ok(octet_length::<i32, Int32Type>(array)),
        DataType::LargeUtf8 => Ok(octet_length::<i64, Int64Type>(array)),
        _ => Err(ArrowError::ComputeError(format!(
            "length not supported for {:?}",
            array.data_type()
        ))),
    }
}

/// Returns an array of Int32/Int64 denoting the number of bits in each string in the array.
///
/// * this only accepts StringArray/Utf8 and LargeString/LargeUtf8
/// * bit_length of null is null.
/// * bit_length is in number of bits
pub fn bit_length(array: &Array) -> Result<ArrayRef> {
    match array.data_type() {
        DataType::Utf8 => Ok(bit_length_impl::<i32, Int32Type>(array)),
        DataType::LargeUtf8 => Ok(bit_length_impl::<i64, Int64Type>(array)),
        _ => Err(ArrowError::ComputeError(format!(
            "bit_length not supported for {:?}",
            array.data_type()
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn length_cases() -> Vec<(Vec<&'static str>, usize, Vec<i32>)> {
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
    fn length_test_string() -> Result<()> {
        length_cases()
            .into_iter()
            .try_for_each(|(input, len, expected)| {
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
    fn length_test_large_string() -> Result<()> {
        length_cases()
            .into_iter()
            .try_for_each(|(input, len, expected)| {
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

    fn length_null_cases() -> Vec<(Vec<Option<&'static str>>, usize, Vec<Option<i32>>)> {
        vec![(
            vec![Some("one"), None, Some("three"), Some("four")],
            4,
            vec![Some(3), None, Some(5), Some(4)],
        )]
    }

    #[test]
    fn length_null_string() -> Result<()> {
        length_null_cases()
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
    fn length_null_large_string() -> Result<()> {
        length_null_cases()
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
    fn length_wrong_type() {
        let array: UInt64Array = vec![1u64].into();

        assert!(length(&array).is_err());
    }

    /// Tests with an offset
    #[test]
    fn length_offsets() -> Result<()> {
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

    fn bit_length_cases() -> Vec<(Vec<&'static str>, usize, Vec<i32>)> {
        fn double_vec<T: Clone>(v: Vec<T>) -> Vec<T> {
            [&v[..], &v[..]].concat()
        }

        // a large array
        let mut values = vec!["one", "on", "o", ""];
        let mut expected = vec![24, 16, 8, 0];
        for _ in 0..10 {
            values = double_vec(values);
            expected = double_vec(expected);
        }

        vec![
            (vec!["hello", " ", "world", "!"], 4, vec![40, 8, 40, 8]),
            (vec!["💖"], 1, vec![32]),
            (vec!["josé"], 1, vec![40]),
            (values, 4096, expected),
        ]
    }

    #[test]
    fn bit_length_test_string() -> Result<()> {
        bit_length_cases()
            .into_iter()
            .try_for_each(|(input, len, expected)| {
                let array = StringArray::from(input);
                let result = bit_length(&array)?;
                assert_eq!(len, result.len());
                let result = result.as_any().downcast_ref::<Int32Array>().unwrap();
                expected.iter().enumerate().for_each(|(i, value)| {
                    assert_eq!(*value, result.value(i));
                });
                Ok(())
            })
    }

    #[test]
    fn bit_length_test_large_string() -> Result<()> {
        bit_length_cases()
            .into_iter()
            .try_for_each(|(input, len, expected)| {
                let array = LargeStringArray::from(input);
                let result = bit_length(&array)?;
                assert_eq!(len, result.len());
                let result = result.as_any().downcast_ref::<Int64Array>().unwrap();
                expected.iter().enumerate().for_each(|(i, value)| {
                    assert_eq!(*value as i64, result.value(i));
                });
                Ok(())
            })
    }

    fn bit_length_null_cases() -> Vec<(Vec<Option<&'static str>>, usize, Vec<Option<i32>>)>
    {
        vec![(
            vec![Some("one"), None, Some("three"), Some("four")],
            4,
            vec![Some(24), None, Some(40), Some(32)],
        )]
    }

    #[test]
    fn bit_length_null_string() -> Result<()> {
        bit_length_null_cases()
            .into_iter()
            .try_for_each(|(input, len, expected)| {
                let array = StringArray::from(input);
                let result = bit_length(&array)?;
                assert_eq!(len, result.len());
                let result = result.as_any().downcast_ref::<Int32Array>().unwrap();

                let expected: Int32Array = expected.into();
                assert_eq!(expected.data(), result.data());
                Ok(())
            })
    }

    #[test]
    fn bit_length_null_large_string() -> Result<()> {
        bit_length_null_cases()
            .into_iter()
            .try_for_each(|(input, len, expected)| {
                let array = LargeStringArray::from(input);
                let result = bit_length(&array)?;
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

    /// Tests that bit_length is not valid for u64.
    #[test]
    fn bit_length_wrong_type() {
        let array: UInt64Array = vec![1u64].into();

        assert!(bit_length(&array).is_err());
    }

    /// Tests with an offset
    #[test]
    fn bit_length_offsets() -> Result<()> {
        let a = StringArray::from(vec!["hello", " ", "world"]);
        let b = make_array(
            ArrayData::builder(DataType::Utf8)
                .len(2)
                .offset(1)
                .buffers(a.data_ref().buffers().to_vec())
                .build(),
        );
        let result = bit_length(b.as_ref())?;

        let expected = Int32Array::from(vec![8, 40]);
        assert_eq!(expected.data(), result.data());

        Ok(())
    }
}
