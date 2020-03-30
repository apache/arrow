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

//! Defines basic comparison kernels for `PrimitiveArrays`.
//!
//! These kernels can leverage SIMD if available on your system.  Currently no runtime
//! detection is provided, you should enable the specific SIMD intrinsics using
//! `RUSTFLAGS="-C target-feature=+avx2"` for example.  See the documentation
//! [here](https://doc.rust-lang.org/stable/core/arch/) for more information.

use regex::Regex;
use std::collections::HashMap;
use std::sync::Arc;

use crate::array::*;
use crate::buffer::{Buffer, MutableBuffer};
use crate::compute::util::apply_bin_op_to_option_bitmap;
use crate::datatypes::{ArrowNumericType, BooleanType, DataType};
use crate::error::{ArrowError, Result};
use crate::util::bit_util;

/// Helper function to perform boolean lambda function on values from two arrays, this
/// version does not attempt to use SIMD.
macro_rules! compare_op {
    ($left: expr, $right:expr, $op:expr) => {{
        if $left.len() != $right.len() {
            return Err(ArrowError::ComputeError(
                "Cannot perform comparison operation on arrays of different length"
                    .to_string(),
            ));
        }

        let null_bit_buffer = apply_bin_op_to_option_bitmap(
            $left.data().null_bitmap(),
            $right.data().null_bitmap(),
            |a, b| a & b,
        )?;

        let mut result = BooleanBufferBuilder::new($left.len());
        for i in 0..$left.len() {
            result.append($op($left.value(i), $right.value(i)))?;
        }

        let data = ArrayData::new(
            DataType::Boolean,
            $left.len(),
            None,
            null_bit_buffer,
            $left.offset(),
            vec![result.finish()],
            vec![],
        );
        Ok(PrimitiveArray::<BooleanType>::from(Arc::new(data)))
    }};
}

pub fn no_simd_compare_op<T, F>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
    op: F,
) -> Result<BooleanArray>
where
    T: ArrowNumericType,
    F: Fn(T::Native, T::Native) -> bool,
{
    compare_op!(left, right, op)
}

pub fn like_utf8(left: &StringArray, right: &StringArray) -> Result<BooleanArray> {
    let mut map = HashMap::new();
    if left.len() != right.len() {
        return Err(ArrowError::ComputeError(
            "Cannot perform comparison operation on arrays of different length"
                .to_string(),
        ));
    }

    let null_bit_buffer = apply_bin_op_to_option_bitmap(
        left.data().null_bitmap(),
        right.data().null_bitmap(),
        |a, b| a & b,
    )?;

    let mut result = BooleanBufferBuilder::new(left.len());
    for i in 0..left.len() {
        let haystack = left.value(i);
        let pat = right.value(i);
        let re = if let Some(ref regex) = map.get(pat) {
            regex
        } else {
            let re_pattern = pat.replace("%", ".*").replace("_", ".");
            let re = Regex::new(&re_pattern).map_err(|e| {
                ArrowError::ComputeError(format!(
                    "Unable to build regex from LIKE pattern: {}",
                    e
                ))
            })?;
            map.insert(pat, re);
            map.get(pat).unwrap()
        };

        result.append(re.is_match(haystack))?;
    }

    let data = ArrayData::new(
        DataType::Boolean,
        left.len(),
        None,
        null_bit_buffer,
        left.offset(),
        vec![result.finish()],
        vec![],
    );
    Ok(PrimitiveArray::<BooleanType>::from(Arc::new(data)))
}

pub fn nlike_utf8(left: &StringArray, right: &StringArray) -> Result<BooleanArray> {
    let mut map = HashMap::new();
    if left.len() != right.len() {
        return Err(ArrowError::ComputeError(
            "Cannot perform comparison operation on arrays of different length"
                .to_string(),
        ));
    }

    let null_bit_buffer = apply_bin_op_to_option_bitmap(
        left.data().null_bitmap(),
        right.data().null_bitmap(),
        |a, b| a & b,
    )?;

    let mut result = BooleanBufferBuilder::new(left.len());
    for i in 0..left.len() {
        let haystack = left.value(i);
        let pat = right.value(i);
        let re = if let Some(ref regex) = map.get(pat) {
            regex
        } else {
            let re_pattern = pat.replace("%", ".*").replace("_", ".");
            let re = Regex::new(&re_pattern).map_err(|e| {
                ArrowError::ComputeError(format!(
                    "Unable to build regex from LIKE pattern: {}",
                    e
                ))
            })?;
            map.insert(pat, re);
            map.get(pat).unwrap()
        };

        result.append(!re.is_match(haystack))?;
    }

    let data = ArrayData::new(
        DataType::Boolean,
        left.len(),
        None,
        null_bit_buffer,
        left.offset(),
        vec![result.finish()],
        vec![],
    );
    Ok(PrimitiveArray::<BooleanType>::from(Arc::new(data)))
}

pub fn eq_utf8(left: &StringArray, right: &StringArray) -> Result<BooleanArray> {
    compare_op!(left, right, |a, b| a == b)
}

pub fn neq_utf8(left: &StringArray, right: &StringArray) -> Result<BooleanArray> {
    compare_op!(left, right, |a, b| a != b)
}

pub fn lt_utf8(left: &StringArray, right: &StringArray) -> Result<BooleanArray> {
    compare_op!(left, right, |a, b| a < b)
}

pub fn lt_eq_utf8(left: &StringArray, right: &StringArray) -> Result<BooleanArray> {
    compare_op!(left, right, |a, b| a <= b)
}

pub fn gt_utf8(left: &StringArray, right: &StringArray) -> Result<BooleanArray> {
    compare_op!(left, right, |a, b| a > b)
}

pub fn gt_eq_utf8(left: &StringArray, right: &StringArray) -> Result<BooleanArray> {
    compare_op!(left, right, |a, b| a >= b)
}

/// Helper function to perform boolean lambda function on values from two arrays using
/// SIMD.
#[cfg(all(any(target_arch = "x86", target_arch = "x86_64"), feature = "simd"))]
fn simd_compare_op<T, F>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
    op: F,
) -> Result<BooleanArray>
where
    T: ArrowNumericType,
    F: Fn(T::Simd, T::Simd) -> T::SimdMask,
{
    if left.len() != right.len() {
        return Err(ArrowError::ComputeError(
            "Cannot perform comparison operation on arrays of different length"
                .to_string(),
        ));
    }

    let null_bit_buffer = apply_bin_op_to_option_bitmap(
        left.data().null_bitmap(),
        right.data().null_bitmap(),
        |a, b| a & b,
    )?;

    let lanes = T::lanes();
    let mut result = BooleanBufferBuilder::new(left.len());

    for i in (0..left.len()).step_by(lanes) {
        let simd_left = T::load(left.value_slice(i, lanes));
        let simd_right = T::load(right.value_slice(i, lanes));
        let simd_result = op(simd_left, simd_right);
        for i in 0..lanes {
            result.append(T::mask_get(&simd_result, i))?;
        }
    }

    let data = ArrayData::new(
        DataType::Boolean,
        left.len(),
        None,
        null_bit_buffer,
        left.offset(),
        vec![result.finish()],
        vec![],
    );
    Ok(PrimitiveArray::<BooleanType>::from(Arc::new(data)))
}

/// Perform `left == right` operation on two arrays.
pub fn eq<T>(left: &PrimitiveArray<T>, right: &PrimitiveArray<T>) -> Result<BooleanArray>
where
    T: ArrowNumericType,
{
    #[cfg(all(any(target_arch = "x86", target_arch = "x86_64"), feature = "simd"))]
    return simd_compare_op(left, right, |a, b| T::eq(a, b));

    #[cfg(any(
        not(any(target_arch = "x86", target_arch = "x86_64")),
        not(feature = "simd")
    ))]
    compare_op!(left, right, |a, b| a == b)
}

/// Perform `left != right` operation on two arrays.
pub fn neq<T>(left: &PrimitiveArray<T>, right: &PrimitiveArray<T>) -> Result<BooleanArray>
where
    T: ArrowNumericType,
{
    #[cfg(all(any(target_arch = "x86", target_arch = "x86_64"), feature = "simd"))]
    return simd_compare_op(left, right, |a, b| T::ne(a, b));

    #[cfg(any(
        not(any(target_arch = "x86", target_arch = "x86_64")),
        not(feature = "simd")
    ))]
    compare_op!(left, right, |a, b| a != b)
}

/// Perform `left < right` operation on two arrays. Null values are less than non-null
/// values.
pub fn lt<T>(left: &PrimitiveArray<T>, right: &PrimitiveArray<T>) -> Result<BooleanArray>
where
    T: ArrowNumericType,
{
    #[cfg(all(any(target_arch = "x86", target_arch = "x86_64"), feature = "simd"))]
    return simd_compare_op(left, right, |a, b| T::lt(a, b));

    #[cfg(any(
        not(any(target_arch = "x86", target_arch = "x86_64")),
        not(feature = "simd")
    ))]
    compare_op!(left, right, |a, b| a < b)
}

/// Perform `left <= right` operation on two arrays. Null values are less than non-null
/// values.
pub fn lt_eq<T>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<BooleanArray>
where
    T: ArrowNumericType,
{
    #[cfg(all(any(target_arch = "x86", target_arch = "x86_64"), feature = "simd"))]
    return simd_compare_op(left, right, |a, b| T::le(a, b));

    #[cfg(any(
        not(any(target_arch = "x86", target_arch = "x86_64")),
        not(feature = "simd")
    ))]
    compare_op!(left, right, |a, b| a <= b)
}

/// Perform `left > right` operation on two arrays. Non-null values are greater than null
/// values.
pub fn gt<T>(left: &PrimitiveArray<T>, right: &PrimitiveArray<T>) -> Result<BooleanArray>
where
    T: ArrowNumericType,
{
    #[cfg(all(any(target_arch = "x86", target_arch = "x86_64"), feature = "simd"))]
    return simd_compare_op(left, right, |a, b| T::gt(a, b));

    #[cfg(any(
        not(any(target_arch = "x86", target_arch = "x86_64")),
        not(feature = "simd")
    ))]
    compare_op!(left, right, |a, b| a > b)
}

/// Perform `left >= right` operation on two arrays. Non-null values are greater than null
/// values.
pub fn gt_eq<T>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<BooleanArray>
where
    T: ArrowNumericType,
{
    #[cfg(all(any(target_arch = "x86", target_arch = "x86_64"), feature = "simd"))]
    return simd_compare_op(left, right, |a, b| T::ge(a, b));

    #[cfg(any(
        not(any(target_arch = "x86", target_arch = "x86_64")),
        not(feature = "simd")
    ))]
    compare_op!(left, right, |a, b| a >= b)
}

// create a buffer and fill it with valid bits
fn new_all_set_buffer(len: usize) -> Buffer {
    let buffer = MutableBuffer::new(len);
    let buffer = buffer.with_bitset(len, true);
    let buffer = buffer.freeze();
    buffer
}

pub fn contains<T>(left: &PrimitiveArray<T>, right: &ListArray) -> Result<BooleanArray>
where
    T: ArrowNumericType,
{
    if left.len() != right.len() {
        return Err(ArrowError::ComputeError(
            "Cannot perform comparison operation on arrays of different length"
                .to_string(),
        ));
    }

    let not_both_null_bit_buffer = match apply_bin_op_to_option_bitmap(
        left.data().null_bitmap(),
        right.data().null_bitmap(),
        |a, b| a | b,
    ) {
        Ok(both_null_buff) => match both_null_buff {
            Some(buff) => buff,
            _ => new_all_set_buffer(left.len()),
        },
        _ => new_all_set_buffer(left.len()),
    };
    let not_both_null_bitmap = not_both_null_bit_buffer.data();

    let left_data = left.data();
    let left_null_bitmap = match left_data.null_bitmap() {
        Some(bitmap) => bitmap.clone().to_buffer(),
        _ => new_all_set_buffer(left.len()),
    };
    let left_null_bitmap = left_null_bitmap.data();

    let mut result = BooleanBufferBuilder::new(left.len());

    for i in 0..left.len() {
        let mut is_in = false;

        // contains(null, null) = false
        if bit_util::get_bit(not_both_null_bitmap, i) {
            let list = right.value(i);

            // contains(null, [null]) = true
            if !bit_util::get_bit(left_null_bitmap, i) {
                if list.null_count() > 0 {
                    is_in = true;
                }
            } else {
                let list = list.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();

                for j in 0..list.len() {
                    if list.is_valid(j) && (left.value(i) == list.value(j)) {
                        is_in = true;
                    }
                }
            }
        }
        result.append(is_in)?;
    }

    let data = ArrayData::new(
        DataType::Boolean,
        left.len(),
        None,
        None,
        left.offset(),
        vec![result.finish()],
        vec![],
    );
    Ok(PrimitiveArray::<BooleanType>::from(Arc::new(data)))
}

pub fn contains_utf8(left: &StringArray, right: &ListArray) -> Result<BooleanArray> {
    if left.len() != right.len() {
        return Err(ArrowError::ComputeError(
            "Cannot perform comparison operation on arrays of different length"
                .to_string(),
        ));
    }

    let not_both_null_bit_buffer = match apply_bin_op_to_option_bitmap(
        left.data().null_bitmap(),
        right.data().null_bitmap(),
        |a, b| a | b,
    ) {
        Ok(both_null_buff) => match both_null_buff {
            Some(buff) => buff,
            _ => new_all_set_buffer(left.len()),
        },
        _ => new_all_set_buffer(left.len()),
    };
    let not_both_null_bitmap = not_both_null_bit_buffer.data();

    let left_data = left.data();
    let left_null_bitmap = match left_data.null_bitmap() {
        Some(bitmap) => bitmap.clone().to_buffer(),
        _ => new_all_set_buffer(left.len()),
    };
    let left_null_bitmap = left_null_bitmap.data();

    let mut result = BooleanBufferBuilder::new(left.len());

    for i in 0..left.len() {
        let mut is_in = false;

        // contains(null, null) = false
        if bit_util::get_bit(not_both_null_bitmap, i) {
            let list = right.value(i);

            // contains(null, [null]) = true
            if !bit_util::get_bit(left_null_bitmap, i) {
                if list.null_count() > 0 {
                    is_in = true;
                }
            } else {
                let list = list.as_any().downcast_ref::<StringArray>().unwrap();

                for j in 0..list.len() {
                    if list.is_valid(j) && (left.value(i) == list.value(j)) {
                        is_in = true;
                    }
                }
            }
        }
        result.append(is_in)?;
    }

    let data = ArrayData::new(
        DataType::Boolean,
        left.len(),
        None,
        None,
        left.offset(),
        vec![result.finish()],
        vec![],
    );
    Ok(PrimitiveArray::<BooleanType>::from(Arc::new(data)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::Int32Array;
    use crate::buffer::Buffer;
    use crate::datatypes::ToByteSlice;
    use std::convert::TryFrom;

    #[test]
    fn test_primitive_array_eq() {
        let a = Int32Array::from(vec![8, 8, 8, 8, 8]);
        let b = Int32Array::from(vec![6, 7, 8, 9, 10]);
        let c = eq(&a, &b).unwrap();
        assert_eq!(false, c.value(0));
        assert_eq!(false, c.value(1));
        assert_eq!(true, c.value(2));
        assert_eq!(false, c.value(3));
        assert_eq!(false, c.value(4));
    }

    #[test]
    fn test_primitive_array_neq() {
        let a = Int32Array::from(vec![8, 8, 8, 8, 8]);
        let b = Int32Array::from(vec![6, 7, 8, 9, 10]);
        let c = neq(&a, &b).unwrap();
        assert_eq!(true, c.value(0));
        assert_eq!(true, c.value(1));
        assert_eq!(false, c.value(2));
        assert_eq!(true, c.value(3));
        assert_eq!(true, c.value(4));
    }

    #[test]
    fn test_primitive_array_lt() {
        let a = Int32Array::from(vec![8, 8, 8, 8, 8]);
        let b = Int32Array::from(vec![6, 7, 8, 9, 10]);
        let c = lt(&a, &b).unwrap();
        assert_eq!(false, c.value(0));
        assert_eq!(false, c.value(1));
        assert_eq!(false, c.value(2));
        assert_eq!(true, c.value(3));
        assert_eq!(true, c.value(4));
    }

    #[test]
    fn test_primitive_array_lt_nulls() {
        let a = Int32Array::from(vec![None, None, Some(1)]);
        let b = Int32Array::from(vec![None, Some(1), None]);
        let c = lt(&a, &b).unwrap();
        assert_eq!(false, c.value(0));
        assert_eq!(true, c.value(1));
        assert_eq!(false, c.value(2));
    }

    #[test]
    fn test_primitive_array_lt_eq() {
        let a = Int32Array::from(vec![8, 8, 8, 8, 8]);
        let b = Int32Array::from(vec![6, 7, 8, 9, 10]);
        let c = lt_eq(&a, &b).unwrap();
        assert_eq!(false, c.value(0));
        assert_eq!(false, c.value(1));
        assert_eq!(true, c.value(2));
        assert_eq!(true, c.value(3));
        assert_eq!(true, c.value(4));
    }

    #[test]
    fn test_primitive_array_lt_eq_nulls() {
        let a = Int32Array::from(vec![None, None, Some(1)]);
        let b = Int32Array::from(vec![None, Some(1), None]);
        let c = lt_eq(&a, &b).unwrap();
        assert_eq!(true, c.value(0));
        assert_eq!(true, c.value(1));
        assert_eq!(false, c.value(2));
    }

    #[test]
    fn test_primitive_array_gt() {
        let a = Int32Array::from(vec![8, 8, 8, 8, 8]);
        let b = Int32Array::from(vec![6, 7, 8, 9, 10]);
        let c = gt(&a, &b).unwrap();
        assert_eq!(true, c.value(0));
        assert_eq!(true, c.value(1));
        assert_eq!(false, c.value(2));
        assert_eq!(false, c.value(3));
        assert_eq!(false, c.value(4));
    }

    #[test]
    fn test_primitive_array_gt_nulls() {
        let a = Int32Array::from(vec![None, None, Some(1)]);
        let b = Int32Array::from(vec![None, Some(1), None]);
        let c = gt(&a, &b).unwrap();
        assert_eq!(false, c.value(0));
        assert_eq!(false, c.value(1));
        assert_eq!(true, c.value(2));
    }

    #[test]
    fn test_primitive_array_gt_eq() {
        let a = Int32Array::from(vec![8, 8, 8, 8, 8]);
        let b = Int32Array::from(vec![6, 7, 8, 9, 10]);
        let c = gt_eq(&a, &b).unwrap();
        assert_eq!(true, c.value(0));
        assert_eq!(true, c.value(1));
        assert_eq!(true, c.value(2));
        assert_eq!(false, c.value(3));
        assert_eq!(false, c.value(4));
    }

    #[test]
    fn test_primitive_array_gt_eq_nulls() {
        let a = Int32Array::from(vec![None, None, Some(1)]);
        let b = Int32Array::from(vec![None, Some(1), Some(1)]);
        let c = gt_eq(&a, &b).unwrap();
        assert_eq!(true, c.value(0));
        assert_eq!(false, c.value(1));
        assert_eq!(true, c.value(2));
    }

    // Expected behaviour:
    // contains(1, [1, 2, null]) = true
    // contains(3, [1, 2, null]) = false
    // contains(null, [1, 2, null]) = true
    // contains(null, null) = false
    #[test]
    fn test_contains() {
        let value_data = Int32Array::from(vec![
            Some(0),
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            Some(5),
            Some(6),
            None,
            Some(7),
        ])
        .data();
        let value_offsets = Buffer::from(&[0, 3, 6, 6, 9].to_byte_slice());
        let list_data_type = DataType::List(Box::new(DataType::Int32));
        let list_data = ArrayData::builder(list_data_type)
            .len(4)
            .add_buffer(value_offsets)
            .null_count(1)
            .add_child_data(value_data)
            .null_bit_buffer(Buffer::from([0b00001011]))
            .build();

        //  [[0, 1, 2], [3, 4, 5], null, [6, null, 7]]
        let list_array = ListArray::from(list_data);

        let nulls = Int32Array::from(vec![None, None, None, None]);
        let nulls_result = contains(&nulls, &list_array).unwrap();
        assert_eq!(
            nulls_result
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap(),
            &BooleanArray::from(vec![false, false, false, true]),
        );

        let values = Int32Array::from(vec![Some(0), Some(0), Some(0), Some(0)]);
        let values_result = contains(&values, &list_array).unwrap();
        assert_eq!(
            values_result
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap(),
            &BooleanArray::from(vec![true, false, false, false]),
        );
    }

    // Expected behaviour:
    // contains("ab", ["ab", "cd", null]) = true
    // contains("ef", ["ab", "cd", null]) = false
    // contains(null, ["ab", "cd", null]) = true
    // contains(null, null) = false
    #[test]
    fn test_contains_utf8() {
        let values_builder = StringBuilder::new(10);
        let mut builder = ListBuilder::new(values_builder);

        builder.values().append_value("Lorem").unwrap();
        builder.values().append_value("ipsum").unwrap();
        builder.values().append_null().unwrap();
        builder.append(true).unwrap();
        builder.values().append_value("sit").unwrap();
        builder.values().append_value("amet").unwrap();
        builder.values().append_value("Lorem").unwrap();
        builder.append(true).unwrap();
        builder.append(false).unwrap();
        builder.values().append_value("ipsum").unwrap();
        builder.append(true).unwrap();

        //  [["Lorem", "ipsum", null], ["sit", "amet", "Lorem"], null, ["ipsum"]]
        // value_offsets = [0, 3, 6, 6]
        let list_array = builder.finish();

        let nulls = StringArray::try_from(vec![None, None, None, None]).unwrap();
        let nulls_result = contains_utf8(&nulls, &list_array).unwrap();
        assert_eq!(
            nulls_result
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap(),
            &BooleanArray::from(vec![true, false, false, false]),
        );

        let values = StringArray::try_from(vec![
            Some("Lorem"),
            Some("Lorem"),
            Some("Lorem"),
            Some("Lorem"),
        ])
        .unwrap();
        let values_result = contains_utf8(&values, &list_array).unwrap();
        assert_eq!(
            values_result
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap(),
            &BooleanArray::from(vec![true, true, false, false]),
        );
    }

    macro_rules! test_utf8 {
        ($test_name:ident, $left:expr, $right:expr, $op:expr, $expected:expr) => {
            #[test]
            fn $test_name() {
                let left = StringArray::from($left);
                let right = StringArray::from($right);
                let res = $op(&left, &right).unwrap();
                let expected = $expected;
                assert_eq!(expected.len(), res.len());
                for i in 0..res.len() {
                    let v = res.value(i);
                    assert_eq!(v, expected[i]);
                }
            }
        };
    }

    test_utf8!(
        test_utf8_array_like,
        vec!["arrow", "arrow", "arrow", "arrow"],
        vec!["arrow", "ar%", "%ro%", "foo"],
        like_utf8,
        vec![true, true, true, false]
    );
    test_utf8!(
        test_utf8_array_nlike,
        vec!["arrow", "arrow", "arrow", "arrow"],
        vec!["arrow", "ar%", "%ro%", "foo"],
        nlike_utf8,
        vec![false, false, false, true]
    );
    test_utf8!(
        test_utf8_array_eq,
        vec!["arrow", "arrow", "arrow", "arrow"],
        vec!["arrow", "parquet", "datafusion", "flight"],
        eq_utf8,
        vec![true, false, false, false]
    );
    test_utf8!(
        test_utf8_array_new,
        vec!["arrow", "arrow", "arrow", "arrow"],
        vec!["arrow", "parquet", "datafusion", "flight"],
        neq_utf8,
        vec![false, true, true, true]
    );
    test_utf8!(
        test_utf8_array_lt,
        vec!["arrow", "datafusion", "flight", "parquet"],
        vec!["flight", "flight", "flight", "flight"],
        lt_utf8,
        vec![true, true, false, false]
    );
    test_utf8!(
        test_utf8_array_lt_eq,
        vec!["arrow", "datafusion", "flight", "parquet"],
        vec!["flight", "flight", "flight", "flight"],
        lt_eq_utf8,
        vec![true, true, true, false]
    );
    test_utf8!(
        test_utf8_array_gt,
        vec!["arrow", "datafusion", "flight", "parquet"],
        vec!["flight", "flight", "flight", "flight"],
        gt_utf8,
        vec![false, false, false, true]
    );
    test_utf8!(
        test_utf8_array_gt_eq,
        vec!["arrow", "datafusion", "flight", "parquet"],
        vec!["flight", "flight", "flight", "flight"],
        gt_eq_utf8,
        vec![false, false, true, true]
    );
}
