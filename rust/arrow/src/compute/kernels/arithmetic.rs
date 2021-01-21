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

//! Defines basic arithmetic kernels for `PrimitiveArrays`.
//!
//! These kernels can leverage SIMD if available on your system.  Currently no runtime
//! detection is provided, you should enable the specific SIMD intrinsics using
//! `RUSTFLAGS="-C target-feature=+avx2"` for example.  See the documentation
//! [here](https://doc.rust-lang.org/stable/core/arch/) for more information.

use std::ops::{Add, Div, Mul, Neg, Sub};
use std::sync::Arc;

use num::{One, Zero};

use crate::buffer::Buffer;
#[cfg(simd)]
use crate::buffer::MutableBuffer;
use crate::compute::util::combine_option_bitmap;
use crate::datatypes;
use crate::datatypes::ArrowNumericType;
use crate::error::{ArrowError, Result};
use crate::{array::*, util::bit_util};
#[cfg(simd)]
use std::borrow::BorrowMut;
#[cfg(simd)]
use std::slice::{ChunksExact, ChunksExactMut};

/// Helper function to perform math lambda function on values from single array of signed numeric
/// type. If value is null then the output value is also null, so `-null` is `null`.
pub fn signed_unary_math_op<T, F>(
    array: &PrimitiveArray<T>,
    op: F,
) -> Result<PrimitiveArray<T>>
where
    T: datatypes::ArrowSignedNumericType,
    T::Native: Neg<Output = T::Native>,
    F: Fn(T::Native) -> T::Native,
{
    let values = array.values().iter().map(|v| op(*v));
    // JUSTIFICATION
    //  Benefit
    //      ~60% speedup
    //  Soundness
    //      `values` is an iterator with a known size.
    let buffer = unsafe { Buffer::from_trusted_len_iter(values) };

    let data = ArrayData::new(
        T::DATA_TYPE,
        array.len(),
        None,
        array.data_ref().null_buffer().cloned(),
        0,
        vec![buffer],
        vec![],
    );
    Ok(PrimitiveArray::<T>::from(Arc::new(data)))
}

/// SIMD vectorized version of `signed_unary_math_op` above.
#[cfg(simd)]
fn simd_signed_unary_math_op<T, SIMD_OP, SCALAR_OP>(
    array: &PrimitiveArray<T>,
    simd_op: SIMD_OP,
    scalar_op: SCALAR_OP,
) -> Result<PrimitiveArray<T>>
where
    T: datatypes::ArrowSignedNumericType,
    SIMD_OP: Fn(T::SignedSimd) -> T::SignedSimd,
    SCALAR_OP: Fn(T::Native) -> T::Native,
{
    let lanes = T::lanes();
    let buffer_size = array.len() * std::mem::size_of::<T::Native>();
    let mut result = MutableBuffer::new(buffer_size).with_bitset(buffer_size, false);

    let mut result_chunks = result.typed_data_mut().chunks_exact_mut(lanes);
    let mut array_chunks = array.values().chunks_exact(lanes);

    result_chunks
        .borrow_mut()
        .zip(array_chunks.borrow_mut())
        .for_each(|(result_slice, input_slice)| {
            let simd_input = T::load_signed(input_slice);
            let simd_result = T::signed_unary_op(simd_input, &simd_op);
            T::write_signed(simd_result, result_slice);
        });

    let result_remainder = result_chunks.into_remainder();
    let array_remainder = array_chunks.remainder();

    result_remainder.into_iter().zip(array_remainder).for_each(
        |(scalar_result, scalar_input)| {
            *scalar_result = scalar_op(*scalar_input);
        },
    );

    let data = ArrayData::new(
        T::DATA_TYPE,
        array.len(),
        None,
        array.data_ref().null_buffer().cloned(),
        0,
        vec![result.into()],
        vec![],
    );
    Ok(PrimitiveArray::<T>::from(Arc::new(data)))
}

/// Helper function to perform math lambda function on values from two arrays. If either
/// left or right value is null then the output value is also null, so `1 + null` is
/// `null`.
///
/// # Errors
///
/// This function errors if the arrays have different lengths
pub fn math_op<T, F>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
    op: F,
) -> Result<PrimitiveArray<T>>
where
    T: ArrowNumericType,
    F: Fn(T::Native, T::Native) -> T::Native,
{
    if left.len() != right.len() {
        return Err(ArrowError::ComputeError(
            "Cannot perform math operation on arrays of different length".to_string(),
        ));
    }

    let null_bit_buffer =
        combine_option_bitmap(left.data_ref(), right.data_ref(), left.len())?;

    let values = left
        .values()
        .iter()
        .zip(right.values().iter())
        .map(|(l, r)| op(*l, *r));
    // JUSTIFICATION
    //  Benefit
    //      ~60% speedup
    //  Soundness
    //      `values` is an iterator with a known size.
    let buffer = unsafe { Buffer::from_trusted_len_iter(values) };

    let data = ArrayData::new(
        T::DATA_TYPE,
        left.len(),
        None,
        null_bit_buffer,
        0,
        vec![buffer],
        vec![],
    );
    Ok(PrimitiveArray::<T>::from(Arc::new(data)))
}

/// Helper function to divide two arrays.
///
/// # Errors
///
/// This function errors if:
/// * the arrays have different lengths
/// * a division by zero is found
fn math_divide<T>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<PrimitiveArray<T>>
where
    T: ArrowNumericType,
    T::Native: Div<Output = T::Native> + Zero,
{
    if left.len() != right.len() {
        return Err(ArrowError::ComputeError(
            "Cannot perform math operation on arrays of different length".to_string(),
        ));
    }

    let null_bit_buffer =
        combine_option_bitmap(left.data_ref(), right.data_ref(), left.len())?;

    let buffer = if let Some(b) = &null_bit_buffer {
        let values = left.values().iter().zip(right.values()).enumerate().map(
            |(i, (left, right))| {
                let is_valid = unsafe { bit_util::get_bit_raw(b.as_ptr(), i) };
                if is_valid {
                    if right.is_zero() {
                        Err(ArrowError::DivideByZero)
                    } else {
                        Ok(*left / *right)
                    }
                } else {
                    Ok(T::default_value())
                }
            },
        );
        unsafe { Buffer::try_from_trusted_len_iter(values) }
    } else {
        // no value is null
        let values = left
            .values()
            .iter()
            .zip(right.values())
            .map(|(left, right)| {
                if right.is_zero() {
                    Err(ArrowError::DivideByZero)
                } else {
                    Ok(*left / *right)
                }
            });
        unsafe { Buffer::try_from_trusted_len_iter(values) }
    }?;

    let data = ArrayData::new(
        T::DATA_TYPE,
        left.len(),
        None,
        null_bit_buffer,
        0,
        vec![buffer],
        vec![],
    );
    Ok(PrimitiveArray::<T>::from(Arc::new(data)))
}

/// SIMD vectorized version of `math_op` above.
#[cfg(simd)]
fn simd_math_op<T, SIMD_OP, SCALAR_OP>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
    simd_op: SIMD_OP,
    scalar_op: SCALAR_OP,
) -> Result<PrimitiveArray<T>>
where
    T: ArrowNumericType,
    SIMD_OP: Fn(T::Simd, T::Simd) -> T::Simd,
    SCALAR_OP: Fn(T::Native, T::Native) -> T::Native,
{
    if left.len() != right.len() {
        return Err(ArrowError::ComputeError(
            "Cannot perform math operation on arrays of different length".to_string(),
        ));
    }

    let null_bit_buffer =
        combine_option_bitmap(left.data_ref(), right.data_ref(), left.len())?;

    let lanes = T::lanes();
    let buffer_size = left.len() * std::mem::size_of::<T::Native>();
    let mut result = MutableBuffer::new(buffer_size).with_bitset(buffer_size, false);

    let mut result_chunks = result.typed_data_mut().chunks_exact_mut(lanes);
    let mut left_chunks = left.values().chunks_exact(lanes);
    let mut right_chunks = right.values().chunks_exact(lanes);

    result_chunks
        .borrow_mut()
        .zip(left_chunks.borrow_mut().zip(right_chunks.borrow_mut()))
        .for_each(|(result_slice, (left_slice, right_slice))| {
            let simd_left = T::load(left_slice);
            let simd_right = T::load(right_slice);
            let simd_result = T::bin_op(simd_left, simd_right, &simd_op);
            T::write(simd_result, result_slice);
        });

    let result_remainder = result_chunks.into_remainder();
    let left_remainder = left_chunks.remainder();
    let right_remainder = right_chunks.remainder();

    result_remainder
        .iter_mut()
        .zip(left_remainder.iter().zip(right_remainder.iter()))
        .for_each(|(scalar_result, (scalar_left, scalar_right))| {
            *scalar_result = scalar_op(*scalar_left, *scalar_right);
        });

    let data = ArrayData::new(
        T::DATA_TYPE,
        left.len(),
        None,
        null_bit_buffer,
        0,
        vec![result.into()],
        vec![],
    );
    Ok(PrimitiveArray::<T>::from(Arc::new(data)))
}

/// SIMD vectorized implementation of `left / right`.
/// If any of the lanes marked as valid in `valid_mask` are `0` then an `ArrowError::DivideByZero`
/// is returned. The contents of no-valid lanes are undefined.
#[cfg(simd)]
#[inline]
fn simd_checked_divide<T: ArrowNumericType>(
    valid_mask: Option<u64>,
    left: T::Simd,
    right: T::Simd,
) -> Result<T::Simd>
where
    T::Native: One + Zero,
{
    let zero = T::init(T::Native::zero());
    let one = T::init(T::Native::one());

    let right_no_invalid_zeros = match valid_mask {
        Some(mask) => {
            let simd_mask = T::mask_from_u64(mask);
            // select `1` for invalid lanes, which will be a no-op during division later
            T::mask_select(simd_mask, right, one)
        }
        None => right,
    };

    let zero_mask = T::eq(right_no_invalid_zeros, zero);

    if T::mask_any(zero_mask) {
        Err(ArrowError::DivideByZero)
    } else {
        Ok(T::bin_op(left, right_no_invalid_zeros, |a, b| a / b))
    }
}

/// Scalar implementation of `left / right` for the remainder elements after complete chunks have been processed using SIMD.
/// If any of the values marked as valid in `valid_mask` are `0` then an `ArrowError::DivideByZero` is returned.
#[cfg(simd)]
#[inline]
fn simd_checked_divide_remainder<T: ArrowNumericType>(
    valid_mask: Option<u64>,
    left_chunks: ChunksExact<T::Native>,
    right_chunks: ChunksExact<T::Native>,
    result_chunks: ChunksExactMut<T::Native>,
) -> Result<()>
where
    T::Native: Zero + Div<Output = T::Native>,
{
    let result_remainder = result_chunks.into_remainder();
    let left_remainder = left_chunks.remainder();
    let right_remainder = right_chunks.remainder();

    result_remainder
        .iter_mut()
        .zip(left_remainder.iter().zip(right_remainder.iter()))
        .enumerate()
        .try_for_each(|(i, (result_scalar, (left_scalar, right_scalar)))| {
            if valid_mask.map(|mask| mask & (1 << i) != 0).unwrap_or(true) {
                if *right_scalar == T::Native::zero() {
                    return Err(ArrowError::DivideByZero);
                }
                *result_scalar = *left_scalar / *right_scalar;
            }
            Ok(())
        })?;

    Ok(())
}

/// SIMD vectorized version of `divide`, the divide kernel needs it's own implementation as there
/// is a need to handle situations where a divide by `0` occurs.  This is complicated by `NULL`
/// slots and padding.
#[cfg(simd)]
fn simd_divide<T>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<PrimitiveArray<T>>
where
    T: ArrowNumericType,
    T::Native: One + Zero + Div<Output = T::Native>,
{
    if left.len() != right.len() {
        return Err(ArrowError::ComputeError(
            "Cannot perform math operation on arrays of different length".to_string(),
        ));
    }

    // Create the combined `Bitmap`
    let null_bit_buffer =
        combine_option_bitmap(left.data_ref(), right.data_ref(), left.len())?;

    let lanes = T::lanes();
    let buffer_size = left.len() * std::mem::size_of::<T::Native>();
    let mut result = MutableBuffer::new(buffer_size).with_bitset(buffer_size, false);

    match &null_bit_buffer {
        Some(b) => {
            // combine_option_bitmap returns a slice or new buffer starting at 0
            let valid_chunks = b.bit_chunks(0, left.len());

            // process data in chunks of 64 elements since we also get 64 bits of validity information at a time
            let mut result_chunks = result.typed_data_mut().chunks_exact_mut(64);
            let mut left_chunks = left.values().chunks_exact(64);
            let mut right_chunks = right.values().chunks_exact(64);

            valid_chunks
                .iter()
                .zip(
                    result_chunks
                        .borrow_mut()
                        .zip(left_chunks.borrow_mut().zip(right_chunks.borrow_mut())),
                )
                .try_for_each(
                    |(mut mask, (result_slice, (left_slice, right_slice)))| {
                        // split chunks further into slices corresponding to the vector length
                        // the compiler is able to unroll this inner loop and remove bounds checks
                        // since the outer chunk size (64) is always a multiple of the number of lanes
                        result_slice
                            .chunks_exact_mut(lanes)
                            .zip(left_slice.chunks_exact(lanes).zip(right_slice.chunks_exact(lanes)))
                            .try_for_each(|(result_slice, (left_slice, right_slice))| -> Result<()> {
                                let simd_left = T::load(left_slice);
                                let simd_right = T::load(right_slice);

                                let simd_result = simd_checked_divide::<T>(Some(mask), simd_left, simd_right)?;

                                T::write(simd_result, result_slice);

                                // skip the shift and avoid overflow for u8 type, which uses 64 lanes.
                                mask >>= T::lanes() % 64;

                                Ok(())
                            })
                    },
                )?;

            let valid_remainder = valid_chunks.remainder_bits();

            simd_checked_divide_remainder::<T>(
                Some(valid_remainder),
                left_chunks,
                right_chunks,
                result_chunks,
            )?;
        }
        None => {
            let mut result_chunks = result.typed_data_mut().chunks_exact_mut(lanes);
            let mut left_chunks = left.values().chunks_exact(lanes);
            let mut right_chunks = right.values().chunks_exact(lanes);

            result_chunks
                .borrow_mut()
                .zip(left_chunks.borrow_mut().zip(right_chunks.borrow_mut()))
                .try_for_each(
                    |(result_slice, (left_slice, right_slice))| -> Result<()> {
                        let simd_left = T::load(left_slice);
                        let simd_right = T::load(right_slice);

                        let simd_result =
                            simd_checked_divide::<T>(None, simd_left, simd_right)?;

                        T::write(simd_result, result_slice);

                        Ok(())
                    },
                )?;

            simd_checked_divide_remainder::<T>(
                None,
                left_chunks,
                right_chunks,
                result_chunks,
            )?;
        }
    }

    let data = ArrayData::new(
        T::DATA_TYPE,
        left.len(),
        None,
        null_bit_buffer,
        0,
        vec![result.into()],
        vec![],
    );
    Ok(PrimitiveArray::<T>::from(Arc::new(data)))
}

/// Perform `left + right` operation on two arrays. If either left or right value is null
/// then the result is also null.
pub fn add<T>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<PrimitiveArray<T>>
where
    T: ArrowNumericType,
    T::Native: Add<Output = T::Native>
        + Sub<Output = T::Native>
        + Mul<Output = T::Native>
        + Div<Output = T::Native>
        + Zero,
{
    #[cfg(simd)]
    return simd_math_op(&left, &right, |a, b| a + b, |a, b| a + b);
    #[cfg(not(simd))]
    return math_op(left, right, |a, b| a + b);
}

/// Perform `left - right` operation on two arrays. If either left or right value is null
/// then the result is also null.
pub fn subtract<T>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<PrimitiveArray<T>>
where
    T: datatypes::ArrowNumericType,
    T::Native: Add<Output = T::Native>
        + Sub<Output = T::Native>
        + Mul<Output = T::Native>
        + Div<Output = T::Native>
        + Zero,
{
    #[cfg(simd)]
    return simd_math_op(&left, &right, |a, b| a - b, |a, b| a - b);
    #[cfg(not(simd))]
    return math_op(left, right, |a, b| a - b);
}

/// Perform `-` operation on an array. If value is null then the result is also null.
pub fn negate<T>(array: &PrimitiveArray<T>) -> Result<PrimitiveArray<T>>
where
    T: datatypes::ArrowSignedNumericType,
    T::Native: Neg<Output = T::Native>,
{
    #[cfg(simd)]
    return simd_signed_unary_math_op(array, |x| -x, |x| -x);
    #[cfg(not(simd))]
    return signed_unary_math_op(array, |x| -x);
}

/// Perform `left * right` operation on two arrays. If either left or right value is null
/// then the result is also null.
pub fn multiply<T>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<PrimitiveArray<T>>
where
    T: datatypes::ArrowNumericType,
    T::Native: Add<Output = T::Native>
        + Sub<Output = T::Native>
        + Mul<Output = T::Native>
        + Div<Output = T::Native>
        + Zero,
{
    #[cfg(simd)]
    return simd_math_op(&left, &right, |a, b| a * b, |a, b| a * b);
    #[cfg(not(simd))]
    return math_op(left, right, |a, b| a * b);
}

/// Perform `left / right` operation on two arrays. If either left or right value is null
/// then the result is also null. If any right hand value is zero then the result of this
/// operation will be `Err(ArrowError::DivideByZero)`.
pub fn divide<T>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<PrimitiveArray<T>>
where
    T: datatypes::ArrowNumericType,
    T::Native: Add<Output = T::Native>
        + Sub<Output = T::Native>
        + Mul<Output = T::Native>
        + Div<Output = T::Native>
        + Zero
        + One,
{
    #[cfg(simd)]
    return simd_divide(&left, &right);
    #[cfg(not(simd))]
    return math_divide(&left, &right);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::Int32Array;

    #[test]
    fn test_primitive_array_add() {
        let a = Int32Array::from(vec![5, 6, 7, 8, 9]);
        let b = Int32Array::from(vec![6, 7, 8, 9, 8]);
        let c = add(&a, &b).unwrap();
        assert_eq!(11, c.value(0));
        assert_eq!(13, c.value(1));
        assert_eq!(15, c.value(2));
        assert_eq!(17, c.value(3));
        assert_eq!(17, c.value(4));
    }

    #[test]
    fn test_primitive_array_add_sliced() {
        let a = Int32Array::from(vec![0, 0, 0, 5, 6, 7, 8, 9, 0]);
        let b = Int32Array::from(vec![0, 0, 0, 6, 7, 8, 9, 8, 0]);
        let a = a.slice(3, 5);
        let b = b.slice(3, 5);
        let a = a.as_any().downcast_ref::<Int32Array>().unwrap();
        let b = b.as_any().downcast_ref::<Int32Array>().unwrap();

        assert_eq!(5, a.value(0));
        assert_eq!(6, b.value(0));

        let c = add(&a, &b).unwrap();
        assert_eq!(5, c.len());
        assert_eq!(11, c.value(0));
        assert_eq!(13, c.value(1));
        assert_eq!(15, c.value(2));
        assert_eq!(17, c.value(3));
        assert_eq!(17, c.value(4));
    }

    #[test]
    fn test_primitive_array_add_mismatched_length() {
        let a = Int32Array::from(vec![5, 6, 7, 8, 9]);
        let b = Int32Array::from(vec![6, 7, 8]);
        let e = add(&a, &b)
            .err()
            .expect("should have failed due to different lengths");
        assert_eq!(
            "ComputeError(\"Cannot perform math operation on arrays of different length\")",
            format!("{:?}", e)
        );
    }

    #[test]
    fn test_primitive_array_subtract() {
        let a = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let b = Int32Array::from(vec![5, 4, 3, 2, 1]);
        let c = subtract(&a, &b).unwrap();
        assert_eq!(-4, c.value(0));
        assert_eq!(-2, c.value(1));
        assert_eq!(0, c.value(2));
        assert_eq!(2, c.value(3));
        assert_eq!(4, c.value(4));
    }

    #[test]
    fn test_primitive_array_multiply() {
        let a = Int32Array::from(vec![5, 6, 7, 8, 9]);
        let b = Int32Array::from(vec![6, 7, 8, 9, 8]);
        let c = multiply(&a, &b).unwrap();
        assert_eq!(30, c.value(0));
        assert_eq!(42, c.value(1));
        assert_eq!(56, c.value(2));
        assert_eq!(72, c.value(3));
        assert_eq!(72, c.value(4));
    }

    #[test]
    fn test_primitive_array_divide() {
        let a = Int32Array::from(vec![15, 15, 8, 1, 9]);
        let b = Int32Array::from(vec![5, 6, 8, 9, 1]);
        let c = divide(&a, &b).unwrap();
        assert_eq!(3, c.value(0));
        assert_eq!(2, c.value(1));
        assert_eq!(1, c.value(2));
        assert_eq!(0, c.value(3));
        assert_eq!(9, c.value(4));
    }

    #[test]
    fn test_primitive_array_divide_sliced() {
        let a = Int32Array::from(vec![0, 0, 0, 15, 15, 8, 1, 9, 0]);
        let b = Int32Array::from(vec![0, 0, 0, 5, 6, 8, 9, 1, 0]);
        let a = a.slice(3, 5);
        let b = b.slice(3, 5);
        let a = a.as_any().downcast_ref::<Int32Array>().unwrap();
        let b = b.as_any().downcast_ref::<Int32Array>().unwrap();

        let c = divide(&a, &b).unwrap();
        assert_eq!(5, c.len());
        assert_eq!(3, c.value(0));
        assert_eq!(2, c.value(1));
        assert_eq!(1, c.value(2));
        assert_eq!(0, c.value(3));
        assert_eq!(9, c.value(4));
    }

    #[test]
    fn test_primitive_array_divide_with_nulls() {
        let a = Int32Array::from(vec![Some(15), None, Some(8), Some(1), Some(9), None]);
        let b = Int32Array::from(vec![Some(5), Some(6), Some(8), Some(9), None, None]);
        let c = divide(&a, &b).unwrap();
        assert_eq!(3, c.value(0));
        assert_eq!(true, c.is_null(1));
        assert_eq!(1, c.value(2));
        assert_eq!(0, c.value(3));
        assert_eq!(true, c.is_null(4));
        assert_eq!(true, c.is_null(5));
    }

    #[test]
    fn test_primitive_array_divide_with_nulls_sliced() {
        let a = Int32Array::from(vec![
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(15),
            None,
            Some(8),
            Some(1),
            Some(9),
            None,
            None,
        ]);
        let b = Int32Array::from(vec![
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(5),
            Some(6),
            Some(8),
            Some(9),
            None,
            None,
            None,
        ]);

        let a = a.slice(8, 6);
        let a = a.as_any().downcast_ref::<Int32Array>().unwrap();

        let b = b.slice(8, 6);
        let b = b.as_any().downcast_ref::<Int32Array>().unwrap();

        let c = divide(&a, &b).unwrap();
        assert_eq!(6, c.len());
        assert_eq!(3, c.value(0));
        assert_eq!(true, c.is_null(1));
        assert_eq!(1, c.value(2));
        assert_eq!(0, c.value(3));
        assert_eq!(true, c.is_null(4));
        assert_eq!(true, c.is_null(5));
    }

    #[test]
    #[should_panic(expected = "DivideByZero")]
    fn test_primitive_array_divide_by_zero() {
        let a = Int32Array::from(vec![15]);
        let b = Int32Array::from(vec![0]);
        divide(&a, &b).unwrap();
    }

    #[test]
    fn test_primitive_array_divide_f64() {
        let a = Float64Array::from(vec![15.0, 15.0, 8.0]);
        let b = Float64Array::from(vec![5.0, 6.0, 8.0]);
        let c = divide(&a, &b).unwrap();
        assert!(3.0 - c.value(0) < f64::EPSILON);
        assert!(2.5 - c.value(1) < f64::EPSILON);
        assert!(1.0 - c.value(2) < f64::EPSILON);
    }

    #[test]
    fn test_primitive_array_add_with_nulls() {
        let a = Int32Array::from(vec![Some(5), None, Some(7), None]);
        let b = Int32Array::from(vec![None, None, Some(6), Some(7)]);
        let c = add(&a, &b).unwrap();
        assert_eq!(true, c.is_null(0));
        assert_eq!(true, c.is_null(1));
        assert_eq!(false, c.is_null(2));
        assert_eq!(true, c.is_null(3));
        assert_eq!(13, c.value(2));
    }

    #[test]
    fn test_primitive_array_negate() {
        let a: Int64Array = (0..100).into_iter().map(Some).collect();
        let actual = negate(&a).unwrap();
        let expected: Int64Array = (0..100).into_iter().map(|i| Some(-i)).collect();
        assert_eq!(expected, actual);
    }

    #[test]
    fn test_arithmetic_kernel_should_not_rely_on_padding() {
        let a: UInt8Array = (0..128_u8).into_iter().map(Some).collect();
        let a = a.slice(63, 65);
        let a = a.as_any().downcast_ref::<UInt8Array>().unwrap();

        let b: UInt8Array = (0..128_u8).into_iter().map(Some).collect();
        let b = b.slice(63, 65);
        let b = b.as_any().downcast_ref::<UInt8Array>().unwrap();

        let actual = add(&a, &b).unwrap();
        let actual: Vec<Option<u8>> = actual.iter().collect();
        let expected: Vec<Option<u8>> = (63..63_u8 + 65_u8)
            .into_iter()
            .map(|i| Some(i + i))
            .collect();
        assert_eq!(expected, actual);
    }
}
