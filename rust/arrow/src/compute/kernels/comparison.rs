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
use crate::compute::util::combine_option_bitmap;
use crate::datatypes::{ArrowNumericType, DataType};
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

        let null_bit_buffer =
            combine_option_bitmap($left.data_ref(), $right.data_ref(), $left.len())?;

        let byte_capacity = bit_util::ceil($left.len(), 8);
        let actual_capacity = bit_util::round_upto_multiple_of_64(byte_capacity);
        let mut buffer = MutableBuffer::new(actual_capacity);
        buffer.resize(byte_capacity);
        let data = buffer.raw_data_mut();

        for i in 0..$left.len() {
            if $op($left.value(i), $right.value(i)) {
                // SAFETY: this is safe as `data` has at least $left.len() elements.
                // and `i` is bound by $left.len()
                unsafe {
                    bit_util::set_bit_raw(data, i);
                }
            }
        }

        let data = ArrayData::new(
            DataType::Boolean,
            $left.len(),
            None,
            null_bit_buffer,
            0,
            vec![buffer.freeze()],
            vec![],
        );
        Ok(BooleanArray::from(Arc::new(data)))
    }};
}

macro_rules! compare_op_scalar {
    ($left: expr, $right:expr, $op:expr) => {{
        let null_bit_buffer = $left.data().null_buffer().cloned();

        let byte_capacity = bit_util::ceil($left.len(), 8);
        let actual_capacity = bit_util::round_upto_multiple_of_64(byte_capacity);
        let mut buffer = MutableBuffer::new(actual_capacity);
        buffer.resize(byte_capacity);
        let data = buffer.raw_data_mut();

        for i in 0..$left.len() {
            if $op($left.value(i), $right) {
                // SAFETY: this is safe as `data` has at least $left.len() elements
                // and `i` is bound by $left.len()
                unsafe {
                    bit_util::set_bit_raw(data, i);
                }
            }
        }

        let data = ArrayData::new(
            DataType::Boolean,
            $left.len(),
            None,
            null_bit_buffer,
            0,
            vec![buffer.freeze()],
            vec![],
        );
        Ok(BooleanArray::from(Arc::new(data)))
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

pub fn no_simd_compare_op_scalar<T, F>(
    left: &PrimitiveArray<T>,
    right: T::Native,
    op: F,
) -> Result<BooleanArray>
where
    T: ArrowNumericType,
    F: Fn(T::Native, T::Native) -> bool,
{
    compare_op_scalar!(left, right, op)
}

pub fn like_utf8(left: &StringArray, right: &StringArray) -> Result<BooleanArray> {
    let mut map = HashMap::new();
    if left.len() != right.len() {
        return Err(ArrowError::ComputeError(
            "Cannot perform comparison operation on arrays of different length"
                .to_string(),
        ));
    }

    let null_bit_buffer =
        combine_option_bitmap(left.data_ref(), right.data_ref(), left.len())?;

    let mut result = BooleanBufferBuilder::new(left.len());
    for i in 0..left.len() {
        let haystack = left.value(i);
        let pat = right.value(i);
        let re = if let Some(ref regex) = map.get(pat) {
            regex
        } else {
            let re_pattern = pat.replace("%", ".*").replace("_", ".");
            let re = Regex::new(&format!("^{}$", re_pattern)).map_err(|e| {
                ArrowError::ComputeError(format!(
                    "Unable to build regex from LIKE pattern: {}",
                    e
                ))
            })?;
            map.insert(pat, re);
            map.get(pat).unwrap()
        };

        result.append(re.is_match(haystack));
    }

    let data = ArrayData::new(
        DataType::Boolean,
        left.len(),
        None,
        null_bit_buffer,
        0,
        vec![result.finish()],
        vec![],
    );
    Ok(BooleanArray::from(Arc::new(data)))
}

fn is_like_pattern(c: char) -> bool {
    c == '%' || c == '_'
}

pub fn like_utf8_scalar(left: &StringArray, right: &str) -> Result<BooleanArray> {
    let null_bit_buffer = left.data().null_buffer().cloned();
    let mut result = BooleanBufferBuilder::new(left.len());

    if !right.contains(is_like_pattern) {
        // fast path, can use equals
        for i in 0..left.len() {
            result.append(left.value(i) == right);
        }
    } else if right.ends_with('%') && !right[..right.len() - 1].contains(is_like_pattern)
    {
        // fast path, can use starts_with
        for i in 0..left.len() {
            result.append(left.value(i).starts_with(&right[..right.len() - 1]));
        }
    } else if right.starts_with('%') && !right[1..].contains(is_like_pattern) {
        // fast path, can use ends_with
        for i in 0..left.len() {
            result.append(left.value(i).ends_with(&right[1..]));
        }
    } else {
        let re_pattern = right.replace("%", ".*").replace("_", ".");
        let re = Regex::new(&format!("^{}$", re_pattern)).map_err(|e| {
            ArrowError::ComputeError(format!(
                "Unable to build regex from LIKE pattern: {}",
                e
            ))
        })?;

        for i in 0..left.len() {
            let haystack = left.value(i);
            result.append(re.is_match(haystack));
        }
    };

    let data = ArrayData::new(
        DataType::Boolean,
        left.len(),
        None,
        null_bit_buffer,
        0,
        vec![result.finish()],
        vec![],
    );
    Ok(BooleanArray::from(Arc::new(data)))
}

pub fn nlike_utf8(left: &StringArray, right: &StringArray) -> Result<BooleanArray> {
    let mut map = HashMap::new();
    if left.len() != right.len() {
        return Err(ArrowError::ComputeError(
            "Cannot perform comparison operation on arrays of different length"
                .to_string(),
        ));
    }

    let null_bit_buffer =
        combine_option_bitmap(left.data_ref(), right.data_ref(), left.len())?;

    let mut result = BooleanBufferBuilder::new(left.len());
    for i in 0..left.len() {
        let haystack = left.value(i);
        let pat = right.value(i);
        let re = if let Some(ref regex) = map.get(pat) {
            regex
        } else {
            let re_pattern = pat.replace("%", ".*").replace("_", ".");
            let re = Regex::new(&format!("^{}$", re_pattern)).map_err(|e| {
                ArrowError::ComputeError(format!(
                    "Unable to build regex from LIKE pattern: {}",
                    e
                ))
            })?;
            map.insert(pat, re);
            map.get(pat).unwrap()
        };

        result.append(!re.is_match(haystack));
    }

    let data = ArrayData::new(
        DataType::Boolean,
        left.len(),
        None,
        null_bit_buffer,
        0,
        vec![result.finish()],
        vec![],
    );
    Ok(BooleanArray::from(Arc::new(data)))
}

pub fn nlike_utf8_scalar(left: &StringArray, right: &str) -> Result<BooleanArray> {
    let null_bit_buffer = left.data().null_buffer().cloned();
    let mut result = BooleanBufferBuilder::new(left.len());

    if !right.contains(is_like_pattern) {
        // fast path, can use equals
        for i in 0..left.len() {
            result.append(left.value(i) != right);
        }
    } else if right.ends_with('%') && !right[..right.len() - 1].contains(is_like_pattern)
    {
        // fast path, can use ends_with
        for i in 0..left.len() {
            result.append(!left.value(i).starts_with(&right[..right.len() - 1]));
        }
    } else if right.starts_with('%') && !right[1..].contains(is_like_pattern) {
        // fast path, can use starts_with
        for i in 0..left.len() {
            result.append(!left.value(i).ends_with(&right[1..]));
        }
    } else {
        let re_pattern = right.replace("%", ".*").replace("_", ".");
        let re = Regex::new(&format!("^{}$", re_pattern)).map_err(|e| {
            ArrowError::ComputeError(format!(
                "Unable to build regex from LIKE pattern: {}",
                e
            ))
        })?;
        for i in 0..left.len() {
            let haystack = left.value(i);
            result.append(!re.is_match(haystack));
        }
    }

    let data = ArrayData::new(
        DataType::Boolean,
        left.len(),
        None,
        null_bit_buffer,
        0,
        vec![result.finish()],
        vec![],
    );
    Ok(BooleanArray::from(Arc::new(data)))
}

pub fn eq_utf8(left: &StringArray, right: &StringArray) -> Result<BooleanArray> {
    compare_op!(left, right, |a, b| a == b)
}

pub fn eq_utf8_scalar(left: &StringArray, right: &str) -> Result<BooleanArray> {
    compare_op_scalar!(left, right, |a, b| a == b)
}

pub fn neq_utf8(left: &StringArray, right: &StringArray) -> Result<BooleanArray> {
    compare_op!(left, right, |a, b| a != b)
}

pub fn neq_utf8_scalar(left: &StringArray, right: &str) -> Result<BooleanArray> {
    compare_op_scalar!(left, right, |a, b| a != b)
}

pub fn lt_utf8(left: &StringArray, right: &StringArray) -> Result<BooleanArray> {
    compare_op!(left, right, |a, b| a < b)
}

pub fn lt_utf8_scalar(left: &StringArray, right: &str) -> Result<BooleanArray> {
    compare_op_scalar!(left, right, |a, b| a < b)
}

pub fn lt_eq_utf8(left: &StringArray, right: &StringArray) -> Result<BooleanArray> {
    compare_op!(left, right, |a, b| a <= b)
}

pub fn lt_eq_utf8_scalar(left: &StringArray, right: &str) -> Result<BooleanArray> {
    compare_op_scalar!(left, right, |a, b| a <= b)
}

pub fn gt_utf8(left: &StringArray, right: &StringArray) -> Result<BooleanArray> {
    compare_op!(left, right, |a, b| a > b)
}

pub fn gt_utf8_scalar(left: &StringArray, right: &str) -> Result<BooleanArray> {
    compare_op_scalar!(left, right, |a, b| a > b)
}

pub fn gt_eq_utf8(left: &StringArray, right: &StringArray) -> Result<BooleanArray> {
    compare_op!(left, right, |a, b| a >= b)
}

pub fn gt_eq_utf8_scalar(left: &StringArray, right: &str) -> Result<BooleanArray> {
    compare_op_scalar!(left, right, |a, b| a >= b)
}

/// Helper function to perform boolean lambda function on values from two arrays using
/// SIMD.
#[cfg(simd_x86)]
fn simd_compare_op<T, F>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
    op: F,
) -> Result<BooleanArray>
where
    T: ArrowNumericType,
    F: Fn(T::Simd, T::Simd) -> T::SimdMask,
{
    use std::mem;

    let len = left.len();
    if len != right.len() {
        return Err(ArrowError::ComputeError(
            "Cannot perform comparison operation on arrays of different length"
                .to_string(),
        ));
    }

    let null_bit_buffer = combine_option_bitmap(left.data_ref(), right.data_ref(), len)?;

    let lanes = T::lanes();
    let mut result = MutableBuffer::new(left.len() * mem::size_of::<bool>());

    let rem = len % lanes;

    for i in (0..len - rem).step_by(lanes) {
        let simd_left = T::load(unsafe { left.value_slice(i, lanes) });
        let simd_right = T::load(unsafe { right.value_slice(i, lanes) });
        let simd_result = op(simd_left, simd_right);
        T::bitmask(&simd_result, |b| {
            result.extend_from_slice(b);
        });
    }

    if rem > 0 {
        //Soundness
        //	This is not sound because it can read past the end of PrimitiveArray buffer (lanes is always greater than rem), see ARROW-10990
        let simd_left = T::load(unsafe { left.value_slice(len - rem, lanes) });
        let simd_right = T::load(unsafe { right.value_slice(len - rem, lanes) });
        let simd_result = op(simd_left, simd_right);
        let rem_buffer_size = (rem as f32 / 8f32).ceil() as usize;
        T::bitmask(&simd_result, |b| {
            result.extend_from_slice(&b[0..rem_buffer_size]);
        });
    }

    let data = ArrayData::new(
        DataType::Boolean,
        left.len(),
        None,
        null_bit_buffer,
        0,
        vec![result.freeze()],
        vec![],
    );
    Ok(BooleanArray::from(Arc::new(data)))
}

/// Helper function to perform boolean lambda function on values from an array and a scalar value using
/// SIMD.
#[cfg(simd_x86)]
fn simd_compare_op_scalar<T, F>(
    left: &PrimitiveArray<T>,
    right: T::Native,
    op: F,
) -> Result<BooleanArray>
where
    T: ArrowNumericType,
    F: Fn(T::Simd, T::Simd) -> T::SimdMask,
{
    use std::mem;

    let len = left.len();
    let null_bit_buffer = left.data().null_buffer().cloned();
    let lanes = T::lanes();
    let mut result = MutableBuffer::new(left.len() * mem::size_of::<bool>());
    let simd_right = T::init(right);

    let rem = len % lanes;

    for i in (0..len - rem).step_by(lanes) {
        let simd_left = T::load(unsafe { left.value_slice(i, lanes) });
        let simd_result = op(simd_left, simd_right);
        T::bitmask(&simd_result, |b| {
            result.extend_from_slice(b);
        });
    }

    if rem > 0 {
        //Soundness
        //	This is not sound because it can read past the end of PrimitiveArray buffer (lanes is always greater than rem), see ARROW-10990
        let simd_left = T::load(unsafe { left.value_slice(len - rem, lanes) });
        let simd_result = op(simd_left, simd_right);
        let rem_buffer_size = (rem as f32 / 8f32).ceil() as usize;
        T::bitmask(&simd_result, |b| {
            result.extend_from_slice(&b[0..rem_buffer_size]);
        });
    }

    let data = ArrayData::new(
        DataType::Boolean,
        left.len(),
        None,
        null_bit_buffer,
        0,
        vec![result.freeze()],
        vec![],
    );
    Ok(BooleanArray::from(Arc::new(data)))
}

/// Perform `left == right` operation on two arrays.
pub fn eq<T>(left: &PrimitiveArray<T>, right: &PrimitiveArray<T>) -> Result<BooleanArray>
where
    T: ArrowNumericType,
{
    #[cfg(simd_x86)]
    return simd_compare_op(left, right, T::eq);
    #[cfg(not(simd_x86))]
    return compare_op!(left, right, |a, b| a == b);
}

/// Perform `left == right` operation on an array and a scalar value.
pub fn eq_scalar<T>(left: &PrimitiveArray<T>, right: T::Native) -> Result<BooleanArray>
where
    T: ArrowNumericType,
{
    #[cfg(simd_x86)]
    return simd_compare_op_scalar(left, right, T::eq);
    #[cfg(not(simd_x86))]
    return compare_op_scalar!(left, right, |a, b| a == b);
}

/// Perform `left != right` operation on two arrays.
pub fn neq<T>(left: &PrimitiveArray<T>, right: &PrimitiveArray<T>) -> Result<BooleanArray>
where
    T: ArrowNumericType,
{
    #[cfg(simd_x86)]
    return simd_compare_op(left, right, T::ne);
    #[cfg(not(simd_x86))]
    return compare_op!(left, right, |a, b| a != b);
}

/// Perform `left != right` operation on an array and a scalar value.
pub fn neq_scalar<T>(left: &PrimitiveArray<T>, right: T::Native) -> Result<BooleanArray>
where
    T: ArrowNumericType,
{
    #[cfg(simd_x86)]
    return simd_compare_op_scalar(left, right, T::ne);
    #[cfg(not(simd_x86))]
    return compare_op_scalar!(left, right, |a, b| a != b);
}

/// Perform `left < right` operation on two arrays. Null values are less than non-null
/// values.
pub fn lt<T>(left: &PrimitiveArray<T>, right: &PrimitiveArray<T>) -> Result<BooleanArray>
where
    T: ArrowNumericType,
{
    #[cfg(simd_x86)]
    return simd_compare_op(left, right, T::lt);
    #[cfg(not(simd_x86))]
    return compare_op!(left, right, |a, b| a < b);
}

/// Perform `left < right` operation on an array and a scalar value.
/// Null values are less than non-null values.
pub fn lt_scalar<T>(left: &PrimitiveArray<T>, right: T::Native) -> Result<BooleanArray>
where
    T: ArrowNumericType,
{
    #[cfg(simd_x86)]
    return simd_compare_op_scalar(left, right, T::lt);
    #[cfg(not(simd_x86))]
    return compare_op_scalar!(left, right, |a, b| a < b);
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
    #[cfg(simd_x86)]
    return simd_compare_op(left, right, T::le);
    #[cfg(not(simd_x86))]
    return compare_op!(left, right, |a, b| a <= b);
}

/// Perform `left <= right` operation on an array and a scalar value.
/// Null values are less than non-null values.
pub fn lt_eq_scalar<T>(left: &PrimitiveArray<T>, right: T::Native) -> Result<BooleanArray>
where
    T: ArrowNumericType,
{
    #[cfg(simd_x86)]
    return simd_compare_op_scalar(left, right, T::le);
    #[cfg(not(simd_x86))]
    return compare_op_scalar!(left, right, |a, b| a <= b);
}

/// Perform `left > right` operation on two arrays. Non-null values are greater than null
/// values.
pub fn gt<T>(left: &PrimitiveArray<T>, right: &PrimitiveArray<T>) -> Result<BooleanArray>
where
    T: ArrowNumericType,
{
    #[cfg(simd_x86)]
    return simd_compare_op(left, right, T::gt);
    #[cfg(not(simd_x86))]
    return compare_op!(left, right, |a, b| a > b);
}

/// Perform `left > right` operation on an array and a scalar value.
/// Non-null values are greater than null values.
pub fn gt_scalar<T>(left: &PrimitiveArray<T>, right: T::Native) -> Result<BooleanArray>
where
    T: ArrowNumericType,
{
    #[cfg(simd_x86)]
    return simd_compare_op_scalar(left, right, T::gt);
    #[cfg(not(simd_x86))]
    return compare_op_scalar!(left, right, |a, b| a > b);
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
    #[cfg(simd_x86)]
    return simd_compare_op(left, right, T::ge);
    #[cfg(not(simd_x86))]
    return compare_op!(left, right, |a, b| a >= b);
}

/// Perform `left >= right` operation on an array and a scalar value.
/// Non-null values are greater than null values.
pub fn gt_eq_scalar<T>(left: &PrimitiveArray<T>, right: T::Native) -> Result<BooleanArray>
where
    T: ArrowNumericType,
{
    #[cfg(simd_x86)]
    return simd_compare_op_scalar(left, right, T::ge);
    #[cfg(not(simd_x86))]
    return compare_op_scalar!(left, right, |a, b| a >= b);
}

/// Checks if a `GenericListArray` contains a value in the `PrimitiveArray`
pub fn contains<T, OffsetSize>(
    left: &PrimitiveArray<T>,
    right: &GenericListArray<OffsetSize>,
) -> Result<BooleanArray>
where
    T: ArrowNumericType,
    OffsetSize: OffsetSizeTrait,
{
    let left_len = left.len();
    if left_len != right.len() {
        return Err(ArrowError::ComputeError(
            "Cannot perform comparison operation on arrays of different length"
                .to_string(),
        ));
    }

    let num_bytes = bit_util::ceil(left_len, 8);

    let not_both_null_bit_buffer =
        match combine_option_bitmap(left.data_ref(), right.data_ref(), left_len)? {
            Some(buff) => buff,
            None => new_all_set_buffer(num_bytes),
        };
    let not_both_null_bitmap = not_both_null_bit_buffer.data();

    let mut bool_buf = MutableBuffer::new(num_bytes).with_bitset(num_bytes, false);
    let bool_slice = bool_buf.data_mut();

    // if both array slots are valid, check if list contains primitive
    for i in 0..left_len {
        if bit_util::get_bit(not_both_null_bitmap, i) {
            let list = right.value(i);
            let list = list.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();

            for j in 0..list.len() {
                if list.is_valid(j) && (left.value(i) == list.value(j)) {
                    bit_util::set_bit(bool_slice, i);
                    continue;
                }
            }
        }
    }

    let data = ArrayData::new(
        DataType::Boolean,
        left.len(),
        None,
        None,
        0,
        vec![bool_buf.freeze()],
        vec![],
    );
    Ok(BooleanArray::from(Arc::new(data)))
}

/// Checks if a `GenericListArray` contains a value in the `GenericStringArray`
pub fn contains_utf8<OffsetSize>(
    left: &GenericStringArray<OffsetSize>,
    right: &ListArray,
) -> Result<BooleanArray>
where
    OffsetSize: StringOffsetSizeTrait,
{
    let left_len = left.len();
    if left_len != right.len() {
        return Err(ArrowError::ComputeError(
            "Cannot perform comparison operation on arrays of different length"
                .to_string(),
        ));
    }

    let num_bytes = bit_util::ceil(left_len, 8);

    let not_both_null_bit_buffer =
        match combine_option_bitmap(left.data_ref(), right.data_ref(), left_len)? {
            Some(buff) => buff,
            None => new_all_set_buffer(num_bytes),
        };
    let not_both_null_bitmap = not_both_null_bit_buffer.data();

    let mut bool_buf = MutableBuffer::new(num_bytes).with_bitset(num_bytes, false);
    let bool_slice = bool_buf.data_mut();

    for i in 0..left_len {
        // contains(null, null) = false
        if bit_util::get_bit(not_both_null_bitmap, i) {
            let list = right.value(i);
            let list = list
                .as_any()
                .downcast_ref::<GenericStringArray<OffsetSize>>()
                .unwrap();

            for j in 0..list.len() {
                if list.is_valid(j) && (left.value(i) == list.value(j)) {
                    bit_util::set_bit(bool_slice, i);
                    continue;
                }
            }
        }
    }

    let data = ArrayData::new(
        DataType::Boolean,
        left.len(),
        None,
        None,
        0,
        vec![bool_buf.freeze()],
        vec![],
    );
    Ok(BooleanArray::from(Arc::new(data)))
}

// create a buffer and fill it with valid bits
#[inline]
fn new_all_set_buffer(len: usize) -> Buffer {
    let buffer = MutableBuffer::new(len);
    let buffer = buffer.with_bitset(len, true);

    buffer.freeze()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datatypes::{Int8Type, ToByteSlice};
    use crate::{array::Int32Array, datatypes::Field};

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
    fn test_primitive_array_eq_scalar() {
        let a = Int32Array::from(vec![6, 7, 8, 9, 10]);
        let c = eq_scalar(&a, 8).unwrap();
        assert_eq!(false, c.value(0));
        assert_eq!(false, c.value(1));
        assert_eq!(true, c.value(2));
        assert_eq!(false, c.value(3));
        assert_eq!(false, c.value(4));
    }

    #[test]
    fn test_primitive_array_eq_with_slice() {
        let a = Int32Array::from(vec![6, 7, 8, 8, 10]);
        let b = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let b_slice = b.slice(5, 5);
        let c = b_slice.as_any().downcast_ref().unwrap();
        let d = eq(&c, &a).unwrap();
        assert_eq!(true, d.value(0));
        assert_eq!(true, d.value(1));
        assert_eq!(true, d.value(2));
        assert_eq!(false, d.value(3));
        assert_eq!(true, d.value(4));
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
    fn test_primitive_array_neq_scalar() {
        let a = Int32Array::from(vec![6, 7, 8, 9, 10]);
        let c = neq_scalar(&a, 8).unwrap();
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
    fn test_primitive_array_lt_scalar() {
        let a = Int32Array::from(vec![6, 7, 8, 9, 10]);
        let c = lt_scalar(&a, 8).unwrap();
        assert_eq!(true, c.value(0));
        assert_eq!(true, c.value(1));
        assert_eq!(false, c.value(2));
        assert_eq!(false, c.value(3));
        assert_eq!(false, c.value(4));
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
    fn test_primitive_array_lt_scalar_nulls() {
        let a = Int32Array::from(vec![None, Some(1), Some(2)]);
        let c = lt_scalar(&a, 2).unwrap();
        assert_eq!(true, c.value(0));
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
    fn test_primitive_array_lt_eq_scalar() {
        let a = Int32Array::from(vec![6, 7, 8, 9, 10]);
        let c = lt_eq_scalar(&a, 8).unwrap();
        assert_eq!(true, c.value(0));
        assert_eq!(true, c.value(1));
        assert_eq!(true, c.value(2));
        assert_eq!(false, c.value(3));
        assert_eq!(false, c.value(4));
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
    fn test_primitive_array_lt_eq_scalar_nulls() {
        let a = Int32Array::from(vec![None, Some(1), Some(2)]);
        let c = lt_eq_scalar(&a, 1).unwrap();
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
    fn test_primitive_array_gt_scalar() {
        let a = Int32Array::from(vec![6, 7, 8, 9, 10]);
        let c = gt_scalar(&a, 8).unwrap();
        assert_eq!(false, c.value(0));
        assert_eq!(false, c.value(1));
        assert_eq!(false, c.value(2));
        assert_eq!(true, c.value(3));
        assert_eq!(true, c.value(4));
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
    fn test_primitive_array_gt_scalar_nulls() {
        let a = Int32Array::from(vec![None, Some(1), Some(2)]);
        let c = gt_scalar(&a, 1).unwrap();
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
    fn test_primitive_array_gt_eq_scalar() {
        let a = Int32Array::from(vec![6, 7, 8, 9, 10]);
        let c = gt_eq_scalar(&a, 8).unwrap();
        assert_eq!(false, c.value(0));
        assert_eq!(false, c.value(1));
        assert_eq!(true, c.value(2));
        assert_eq!(true, c.value(3));
        assert_eq!(true, c.value(4));
    }

    #[test]
    fn test_primitive_array_gt_eq_nulls() {
        let a = Int32Array::from(vec![None, None, Some(1)]);
        let b = Int32Array::from(vec![None, Some(1), None]);
        let c = gt_eq(&a, &b).unwrap();
        assert_eq!(true, c.value(0));
        assert_eq!(false, c.value(1));
        assert_eq!(true, c.value(2));
    }

    #[test]
    fn test_primitive_array_gt_eq_scalar_nulls() {
        let a = Int32Array::from(vec![None, Some(1), Some(2)]);
        let c = gt_eq_scalar(&a, 1).unwrap();
        assert_eq!(false, c.value(0));
        assert_eq!(true, c.value(1));
        assert_eq!(true, c.value(2));
    }

    #[test]
    fn test_length_of_result_buffer() {
        // `item_count` is chosen to not be a multiple of the number of SIMD lanes for this
        // type (`Int8Type`), 64.
        let item_count = 130;

        let select_mask: BooleanArray = vec![true; item_count].into();

        let array_a: PrimitiveArray<Int8Type> = vec![1; item_count].into();
        let array_b: PrimitiveArray<Int8Type> = vec![2; item_count].into();
        let result_mask = gt_eq(&array_a, &array_b).unwrap();

        assert_eq!(
            result_mask.data().buffers()[0].len(),
            select_mask.data().buffers()[0].len()
        );
    }

    // Expected behaviour:
    // contains(1, [1, 2, null]) = true
    // contains(3, [1, 2, null]) = false
    // contains(null, [1, 2, null]) = false
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
        let value_offsets = Buffer::from(&[0i64, 3, 6, 6, 9].to_byte_slice());
        let list_data_type =
            DataType::LargeList(Box::new(Field::new("item", DataType::Int32, true)));
        let list_data = ArrayData::builder(list_data_type)
            .len(4)
            .add_buffer(value_offsets)
            .null_count(1)
            .add_child_data(value_data)
            .null_bit_buffer(Buffer::from([0b00001011]))
            .build();

        //  [[0, 1, 2], [3, 4, 5], null, [6, null, 7]]
        let list_array = LargeListArray::from(list_data);

        let nulls = Int32Array::from(vec![None, None, None, None]);
        let nulls_result = contains(&nulls, &list_array).unwrap();
        assert_eq!(
            nulls_result
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap(),
            &BooleanArray::from(vec![false, false, false, false]),
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
    // contains(null, ["ab", "cd", null]) = false
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

        let nulls = StringArray::from(vec![None, None, None, None]);
        let nulls_result = contains_utf8(&nulls, &list_array).unwrap();
        assert_eq!(
            nulls_result
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap(),
            &BooleanArray::from(vec![false, false, false, false]),
        );

        let values = StringArray::from(vec![
            Some("Lorem"),
            Some("Lorem"),
            Some("Lorem"),
            Some("Lorem"),
        ]);
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

    macro_rules! test_utf8_scalar {
        ($test_name:ident, $left:expr, $right:expr, $op:expr, $expected:expr) => {
            #[test]
            fn $test_name() {
                let left = StringArray::from($left);
                let res = $op(&left, $right).unwrap();
                let expected = $expected;
                assert_eq!(expected.len(), res.len());
                for i in 0..res.len() {
                    let v = res.value(i);
                    assert_eq!(
                        v,
                        expected[i],
                        "unexpected result when comparing {} at position {} to {} ",
                        left.value(i),
                        i,
                        $right
                    );
                }
            }
        };
    }

    test_utf8!(
        test_utf8_array_like,
        vec!["arrow", "arrow", "arrow", "arrow", "arrow", "arrows", "arrow"],
        vec!["arrow", "ar%", "%ro%", "foo", "arr", "arrow_", "arrow_"],
        like_utf8,
        vec![true, true, true, false, false, true, false]
    );

    test_utf8_scalar!(
        test_utf8_array_like_scalar,
        vec!["arrow", "parquet", "datafusion", "flight"],
        "%ar%",
        like_utf8_scalar,
        vec![true, true, false, false]
    );
    test_utf8_scalar!(
        test_utf8_array_like_scalar_start,
        vec!["arrow", "parrow", "arrows", "arr"],
        "arrow%",
        like_utf8_scalar,
        vec![true, false, true, false]
    );

    test_utf8_scalar!(
        test_utf8_array_like_scalar_end,
        vec!["arrow", "parrow", "arrows", "arr"],
        "%arrow",
        like_utf8_scalar,
        vec![true, true, false, false]
    );

    test_utf8_scalar!(
        test_utf8_array_like_scalar_equals,
        vec!["arrow", "parrow", "arrows", "arr"],
        "arrow",
        like_utf8_scalar,
        vec![true, false, false, false]
    );

    test_utf8_scalar!(
        test_utf8_array_like_scalar_one,
        vec!["arrow", "arrows", "parrow", "arr"],
        "arrow_",
        like_utf8_scalar,
        vec![false, true, false, false]
    );

    test_utf8!(
        test_utf8_array_nlike,
        vec!["arrow", "arrow", "arrow", "arrow", "arrow", "arrows", "arrow"],
        vec!["arrow", "ar%", "%ro%", "foo", "arr", "arrow_", "arrow_"],
        nlike_utf8,
        vec![false, false, false, true, true, false, true]
    );
    test_utf8_scalar!(
        test_utf8_array_nlike_scalar,
        vec!["arrow", "parquet", "datafusion", "flight"],
        "%ar%",
        nlike_utf8_scalar,
        vec![false, false, true, true]
    );

    test_utf8!(
        test_utf8_array_eq,
        vec!["arrow", "arrow", "arrow", "arrow"],
        vec!["arrow", "parquet", "datafusion", "flight"],
        eq_utf8,
        vec![true, false, false, false]
    );
    test_utf8_scalar!(
        test_utf8_array_eq_scalar,
        vec!["arrow", "parquet", "datafusion", "flight"],
        "arrow",
        eq_utf8_scalar,
        vec![true, false, false, false]
    );

    test_utf8_scalar!(
        test_utf8_array_nlike_scalar_start,
        vec!["arrow", "parrow", "arrows", "arr"],
        "arrow%",
        nlike_utf8_scalar,
        vec![false, true, false, true]
    );

    test_utf8_scalar!(
        test_utf8_array_nlike_scalar_end,
        vec!["arrow", "parrow", "arrows", "arr"],
        "%arrow",
        nlike_utf8_scalar,
        vec![false, false, true, true]
    );

    test_utf8_scalar!(
        test_utf8_array_nlike_scalar_equals,
        vec!["arrow", "parrow", "arrows", "arr"],
        "arrow",
        nlike_utf8_scalar,
        vec![false, true, true, true]
    );

    test_utf8_scalar!(
        test_utf8_array_nlike_scalar_one,
        vec!["arrow", "arrows", "parrow", "arr"],
        "arrow_",
        nlike_utf8_scalar,
        vec![true, false, true, true]
    );

    test_utf8!(
        test_utf8_array_neq,
        vec!["arrow", "arrow", "arrow", "arrow"],
        vec!["arrow", "parquet", "datafusion", "flight"],
        neq_utf8,
        vec![false, true, true, true]
    );
    test_utf8_scalar!(
        test_utf8_array_neq_scalar,
        vec!["arrow", "parquet", "datafusion", "flight"],
        "arrow",
        neq_utf8_scalar,
        vec![false, true, true, true]
    );

    test_utf8!(
        test_utf8_array_lt,
        vec!["arrow", "datafusion", "flight", "parquet"],
        vec!["flight", "flight", "flight", "flight"],
        lt_utf8,
        vec![true, true, false, false]
    );
    test_utf8_scalar!(
        test_utf8_array_lt_scalar,
        vec!["arrow", "datafusion", "flight", "parquet"],
        "flight",
        lt_utf8_scalar,
        vec![true, true, false, false]
    );

    test_utf8!(
        test_utf8_array_lt_eq,
        vec!["arrow", "datafusion", "flight", "parquet"],
        vec!["flight", "flight", "flight", "flight"],
        lt_eq_utf8,
        vec![true, true, true, false]
    );
    test_utf8_scalar!(
        test_utf8_array_lt_eq_scalar,
        vec!["arrow", "datafusion", "flight", "parquet"],
        "flight",
        lt_eq_utf8_scalar,
        vec![true, true, true, false]
    );

    test_utf8!(
        test_utf8_array_gt,
        vec!["arrow", "datafusion", "flight", "parquet"],
        vec!["flight", "flight", "flight", "flight"],
        gt_utf8,
        vec![false, false, false, true]
    );
    test_utf8_scalar!(
        test_utf8_array_gt_scalar,
        vec!["arrow", "datafusion", "flight", "parquet"],
        "flight",
        gt_utf8_scalar,
        vec![false, false, false, true]
    );

    test_utf8!(
        test_utf8_array_gt_eq,
        vec!["arrow", "datafusion", "flight", "parquet"],
        vec!["flight", "flight", "flight", "flight"],
        gt_eq_utf8,
        vec![false, false, true, true]
    );
    test_utf8_scalar!(
        test_utf8_array_gt_eq_scalar,
        vec!["arrow", "datafusion", "flight", "parquet"],
        "flight",
        gt_eq_utf8_scalar,
        vec![false, false, true, true]
    );
}
