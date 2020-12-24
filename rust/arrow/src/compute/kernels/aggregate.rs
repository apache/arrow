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

//! Defines aggregations over Arrow arrays.

use std::ops::Add;

use crate::array::{
    Array, BooleanArray, GenericStringArray, PrimitiveArray, StringOffsetSizeTrait,
};
use crate::datatypes::{ArrowNativeType, ArrowNumericType};

/// Generic test for NaN, the optimizer should be able to remove this for integer types.
#[inline]
fn is_nan<T: ArrowNativeType + PartialOrd + Copy>(a: T) -> bool {
    #[allow(clippy::eq_op)]
    !(a == a)
}

/// Helper macro to perform min/max of strings
fn min_max_string<T: StringOffsetSizeTrait, F: Fn(&str, &str) -> bool>(
    array: &GenericStringArray<T>,
    cmp: F,
) -> Option<&str> {
    let null_count = array.null_count();

    if null_count == array.len() {
        return None;
    }
    let data = array.data();
    let mut n;
    if null_count == 0 {
        n = array.value(0);
        for i in 1..data.len() {
            let item = array.value(i);
            if cmp(&n, item) {
                n = item;
            }
        }
    } else {
        n = "";
        let mut has_value = false;

        for i in 0..data.len() {
            let item = array.value(i);
            if data.is_valid(i) && (!has_value || cmp(&n, item)) {
                has_value = true;
                n = item;
            }
        }
    }
    Some(n)
}

/// Returns the minimum value in the array, according to the natural order.
/// For floating point arrays any NaN values are considered to be greater than any other non-null value
#[cfg(not(simd_x86))]
pub fn min<T>(array: &PrimitiveArray<T>) -> Option<T::Native>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeType,
{
    min_max_helper(array, |a, b| (is_nan(*a) & !is_nan(*b)) || a > b)
}

/// Returns the maximum value in the array, according to the natural order.
/// For floating point arrays any NaN values are considered to be greater than any other non-null value
#[cfg(not(simd_x86))]
pub fn max<T>(array: &PrimitiveArray<T>) -> Option<T::Native>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeType,
{
    min_max_helper(array, |a, b| (!is_nan(*a) & is_nan(*b)) || a < b)
}

/// Returns the maximum value in the string array, according to the natural order.
pub fn max_string<T: StringOffsetSizeTrait>(
    array: &GenericStringArray<T>,
) -> Option<&str> {
    min_max_string(array, |a, b| a < b)
}

/// Returns the minimum value in the string array, according to the natural order.
pub fn min_string<T: StringOffsetSizeTrait>(
    array: &GenericStringArray<T>,
) -> Option<&str> {
    min_max_string(array, |a, b| a > b)
}

/// Helper function to perform min/max lambda function on values from a numeric array.
fn min_max_helper<T, F>(array: &PrimitiveArray<T>, cmp: F) -> Option<T::Native>
where
    T: ArrowNumericType,
    F: Fn(&T::Native, &T::Native) -> bool,
{
    let null_count = array.null_count();

    // Includes case array.len() == 0
    if null_count == array.len() {
        return None;
    }

    let data = array.data();
    let m = array.values();
    let mut n;

    if null_count == 0 {
        // optimized path for arrays without null values
        n = m[1..]
            .iter()
            .fold(m[0], |max, item| if cmp(&max, item) { *item } else { max });
    } else {
        n = T::default_value();
        let mut has_value = false;
        for (i, item) in m.iter().enumerate() {
            if data.is_valid(i) && (!has_value || cmp(&n, item)) {
                has_value = true;
                n = *item
            }
        }
    }
    Some(n)
}

/// Returns the minimum value in the boolean array.
///
/// ```
/// use arrow::{
///   array::BooleanArray,
///   compute::min_boolean,
/// };
///
/// let a = BooleanArray::from(vec![Some(true), None, Some(false)]);
/// assert_eq!(min_boolean(&a), Some(false))
/// ```
pub fn min_boolean(array: &BooleanArray) -> Option<bool> {
    // short circuit if all nulls / zero length array
    if array.null_count() == array.len() {
        return None;
    }

    // Note the min bool is false (0), so short circuit as soon as we see it
    array
        .iter()
        .find(|&b| b == Some(false))
        .flatten()
        .or(Some(true))
}

/// Returns the maximum value in the boolean array
///
/// ```
/// use arrow::{
///   array::BooleanArray,
///   compute::max_boolean,
/// };
///
/// let a = BooleanArray::from(vec![Some(true), None, Some(false)]);
/// assert_eq!(max_boolean(&a), Some(true))
/// ```
pub fn max_boolean(array: &BooleanArray) -> Option<bool> {
    // short circuit if all nulls / zero length array
    if array.null_count() == array.len() {
        return None;
    }

    // Note the max bool is true (1), so short circuit as soon as we see it
    array
        .iter()
        .find(|&b| b == Some(true))
        .flatten()
        .or(Some(false))
}

/// Returns the sum of values in the array.
///
/// Returns `None` if the array is empty or only contains null values.
#[cfg(not(simd_x86))]
pub fn sum<T>(array: &PrimitiveArray<T>) -> Option<T::Native>
where
    T: ArrowNumericType,
    T::Native: Add<Output = T::Native>,
{
    let null_count = array.null_count();

    if null_count == array.len() {
        return None;
    }

    let data: &[T::Native] = array.values();

    match array.data().null_buffer() {
        None => {
            let sum = data.iter().fold(T::default_value(), |accumulator, value| {
                accumulator + *value
            });

            Some(sum)
        }
        Some(buffer) => {
            let mut sum = T::default_value();
            let data_chunks = data.chunks_exact(64);
            let remainder = data_chunks.remainder();

            let bit_chunks = buffer.bit_chunks(array.offset(), array.len());
            data_chunks
                .zip(bit_chunks.iter())
                .for_each(|(chunk, mask)| {
                    // index_mask has value 1 << i in the loop
                    let mut index_mask = 1;
                    chunk.iter().for_each(|value| {
                        if (mask & index_mask) != 0 {
                            sum = sum + *value;
                        }
                        index_mask <<= 1;
                    });
                });

            let remainder_bits = bit_chunks.remainder_bits();

            remainder.iter().enumerate().for_each(|(i, value)| {
                if remainder_bits & (1 << i) != 0 {
                    sum = sum + *value;
                }
            });

            Some(sum)
        }
    }
}

#[cfg(simd_x86)]
mod simd {
    use super::is_nan;
    use crate::array::{Array, PrimitiveArray};
    use crate::datatypes::ArrowNumericType;
    use std::marker::PhantomData;
    use std::ops::Add;

    pub(super) trait SimdAggregate<T: ArrowNumericType> {
        type ScalarAccumulator;
        type SimdAccumulator;

        /// Returns the accumulator for aggregating scalar values
        fn init_accumulator_scalar() -> Self::ScalarAccumulator;

        /// Returns the accumulator for aggregating simd chunks of values
        fn init_accumulator_chunk() -> Self::SimdAccumulator;

        /// Updates the accumulator with the values of one chunk
        fn accumulate_chunk_non_null(
            accumulator: &mut Self::SimdAccumulator,
            chunk: T::Simd,
        );

        /// Updates the accumulator with the values of one chunk according to the given vector mask
        fn accumulate_chunk_nullable(
            accumulator: &mut Self::SimdAccumulator,
            chunk: T::Simd,
            mask: T::SimdMask,
        );

        /// Updates the accumulator with one value
        fn accumulate_scalar(accumulator: &mut Self::ScalarAccumulator, value: T::Native);

        /// Reduces the vector lanes of the simd accumulator and the scalar accumulator to a single value
        fn reduce(
            simd_accumulator: Self::SimdAccumulator,
            scalar_accumulator: Self::ScalarAccumulator,
        ) -> Option<T::Native>;
    }

    pub(super) struct SumAggregate<T: ArrowNumericType> {
        phantom: PhantomData<T>,
    }

    impl<T: ArrowNumericType> SimdAggregate<T> for SumAggregate<T>
    where
        T::Native: Add<Output = T::Native>,
    {
        type ScalarAccumulator = T::Native;
        type SimdAccumulator = T::Simd;

        fn init_accumulator_scalar() -> Self::ScalarAccumulator {
            T::default_value()
        }

        fn init_accumulator_chunk() -> Self::SimdAccumulator {
            T::init(Self::init_accumulator_scalar())
        }

        fn accumulate_chunk_non_null(accumulator: &mut T::Simd, chunk: T::Simd) {
            *accumulator = *accumulator + chunk;
        }

        fn accumulate_chunk_nullable(
            accumulator: &mut T::Simd,
            chunk: T::Simd,
            vecmask: T::SimdMask,
        ) {
            let zero = T::init(T::default_value());
            let blended = T::mask_select(vecmask, chunk, zero);

            *accumulator = *accumulator + blended;
        }

        fn accumulate_scalar(accumulator: &mut T::Native, value: T::Native) {
            *accumulator = *accumulator + value
        }

        fn reduce(
            simd_accumulator: Self::SimdAccumulator,
            scalar_accumulator: Self::ScalarAccumulator,
        ) -> Option<T::Native> {
            // we can't use T::lanes() as the slice len because it is not const,
            // instead always reserve the maximum number of lanes
            let mut tmp = [T::default_value(); 64];
            let slice = &mut tmp[0..T::lanes()];
            T::write(simd_accumulator, slice);

            let mut reduced = Self::init_accumulator_scalar();
            slice
                .iter()
                .for_each(|value| Self::accumulate_scalar(&mut reduced, *value));

            Self::accumulate_scalar(&mut reduced, scalar_accumulator);

            // result can not be None because we checked earlier for the null count
            Some(reduced)
        }
    }

    pub(super) struct MinAggregate<T: ArrowNumericType> {
        phantom: PhantomData<T>,
    }

    impl<T: ArrowNumericType> SimdAggregate<T> for MinAggregate<T>
    where
        T::Native: PartialOrd,
    {
        type ScalarAccumulator = (T::Native, bool);
        type SimdAccumulator = (T::Simd, T::SimdMask);

        fn init_accumulator_scalar() -> Self::ScalarAccumulator {
            (T::default_value(), false)
        }

        fn init_accumulator_chunk() -> Self::SimdAccumulator {
            (T::init(T::default_value()), T::mask_init(false))
        }

        fn accumulate_chunk_non_null(
            accumulator: &mut Self::SimdAccumulator,
            chunk: T::Simd,
        ) {
            let acc_is_nan = !T::eq(accumulator.0, accumulator.0);
            let is_lt = acc_is_nan | T::lt(chunk, accumulator.0);
            let first_or_lt = !accumulator.1 | is_lt;

            accumulator.0 = T::mask_select(first_or_lt, chunk, accumulator.0);
            accumulator.1 = T::mask_init(true);
        }

        fn accumulate_chunk_nullable(
            accumulator: &mut Self::SimdAccumulator,
            chunk: T::Simd,
            vecmask: T::SimdMask,
        ) {
            let acc_is_nan = !T::eq(accumulator.0, accumulator.0);
            let is_lt = vecmask & (acc_is_nan | T::lt(chunk, accumulator.0));
            let first_or_lt = !accumulator.1 | is_lt;

            accumulator.0 = T::mask_select(first_or_lt, chunk, accumulator.0);
            accumulator.1 |= vecmask;
        }

        fn accumulate_scalar(
            accumulator: &mut Self::ScalarAccumulator,
            value: T::Native,
        ) {
            if !accumulator.1 {
                accumulator.0 = value;
            } else {
                let acc_is_nan = is_nan(accumulator.0);
                if acc_is_nan || value < accumulator.0 {
                    accumulator.0 = value
                }
            }
            accumulator.1 = true
        }

        fn reduce(
            simd_accumulator: Self::SimdAccumulator,
            scalar_accumulator: Self::ScalarAccumulator,
        ) -> Option<T::Native> {
            // we can't use T::lanes() as the slice len because it is not const,
            // instead always reserve the maximum number of lanes
            let mut tmp = [T::default_value(); 64];
            let slice = &mut tmp[0..T::lanes()];
            T::write(simd_accumulator.0, slice);

            let mut reduced = Self::init_accumulator_scalar();
            slice
                .iter()
                .enumerate()
                .filter(|(i, _value)| T::mask_get(&simd_accumulator.1, *i))
                .for_each(|(_i, value)| Self::accumulate_scalar(&mut reduced, *value));

            if scalar_accumulator.1 {
                Self::accumulate_scalar(&mut reduced, scalar_accumulator.0);
            }

            if reduced.1 {
                Some(reduced.0)
            } else {
                None
            }
        }
    }

    pub(super) struct MaxAggregate<T: ArrowNumericType> {
        phantom: PhantomData<T>,
    }

    impl<T: ArrowNumericType> SimdAggregate<T> for MaxAggregate<T>
    where
        T::Native: PartialOrd,
    {
        type ScalarAccumulator = (T::Native, bool);
        type SimdAccumulator = (T::Simd, T::SimdMask);

        fn init_accumulator_scalar() -> Self::ScalarAccumulator {
            (T::default_value(), false)
        }

        fn init_accumulator_chunk() -> Self::SimdAccumulator {
            (T::init(T::default_value()), T::mask_init(false))
        }

        fn accumulate_chunk_non_null(
            accumulator: &mut Self::SimdAccumulator,
            chunk: T::Simd,
        ) {
            let chunk_is_nan = !T::eq(chunk, chunk);
            let is_gt = chunk_is_nan | T::gt(chunk, accumulator.0);
            let first_or_gt = !accumulator.1 | is_gt;

            accumulator.0 = T::mask_select(first_or_gt, chunk, accumulator.0);
            accumulator.1 = T::mask_init(true);
        }

        fn accumulate_chunk_nullable(
            accumulator: &mut Self::SimdAccumulator,
            chunk: T::Simd,
            vecmask: T::SimdMask,
        ) {
            let chunk_is_nan = !T::eq(chunk, chunk);
            let is_gt = vecmask & (chunk_is_nan | T::gt(chunk, accumulator.0));
            let first_or_gt = !accumulator.1 | is_gt;

            accumulator.0 = T::mask_select(first_or_gt, chunk, accumulator.0);
            accumulator.1 |= vecmask;
        }

        fn accumulate_scalar(
            accumulator: &mut Self::ScalarAccumulator,
            value: T::Native,
        ) {
            if !accumulator.1 {
                accumulator.0 = value;
            } else {
                let value_is_nan = is_nan(value);
                if value_is_nan || value > accumulator.0 {
                    accumulator.0 = value
                }
            }
            accumulator.1 = true;
        }

        fn reduce(
            simd_accumulator: Self::SimdAccumulator,
            scalar_accumulator: Self::ScalarAccumulator,
        ) -> Option<T::Native> {
            // we can't use T::lanes() as the slice len because it is not const,
            // instead always reserve the maximum number of lanes
            let mut tmp = [T::default_value(); 64];
            let slice = &mut tmp[0..T::lanes()];
            T::write(simd_accumulator.0, slice);

            let mut reduced = Self::init_accumulator_scalar();
            slice
                .iter()
                .enumerate()
                .filter(|(i, _value)| T::mask_get(&simd_accumulator.1, *i))
                .for_each(|(_i, value)| Self::accumulate_scalar(&mut reduced, *value));

            if scalar_accumulator.1 {
                Self::accumulate_scalar(&mut reduced, scalar_accumulator.0);
            }

            if reduced.1 {
                Some(reduced.0)
            } else {
                None
            }
        }
    }

    pub(super) fn simd_aggregation<T: ArrowNumericType, A: SimdAggregate<T>>(
        array: &PrimitiveArray<T>,
    ) -> Option<T::Native> {
        let null_count = array.null_count();

        if null_count == array.len() {
            return None;
        }

        let data: &[T::Native] = array.values();

        let mut chunk_acc = A::init_accumulator_chunk();
        let mut rem_acc = A::init_accumulator_scalar();

        match array.data().null_buffer() {
            None => {
                let data_chunks = data.chunks_exact(64);
                let remainder = data_chunks.remainder();

                data_chunks.for_each(|chunk| {
                    chunk.chunks_exact(T::lanes()).for_each(|chunk| {
                        let chunk = T::load(&chunk);
                        A::accumulate_chunk_non_null(&mut chunk_acc, chunk);
                    });
                });

                remainder.iter().for_each(|value| {
                    A::accumulate_scalar(&mut rem_acc, *value);
                });
            }
            Some(buffer) => {
                // process data in chunks of 64 elements since we also get 64 bits of validity information at a time
                let data_chunks = data.chunks_exact(64);
                let remainder = data_chunks.remainder();

                let bit_chunks = buffer.bit_chunks(array.offset(), array.len());
                let remainder_bits = bit_chunks.remainder_bits();

                data_chunks.zip(bit_chunks).for_each(|(chunk, mut mask)| {
                    // split chunks further into slices corresponding to the vector length
                    // the compiler is able to unroll this inner loop and remove bounds checks
                    // since the outer chunk size (64) is always a multiple of the number of lanes
                    chunk.chunks_exact(T::lanes()).for_each(|chunk| {
                        let vecmask = T::mask_from_u64(mask);
                        let chunk = T::load(&chunk);

                        A::accumulate_chunk_nullable(&mut chunk_acc, chunk, vecmask);

                        // skip the shift and avoid overflow for u8 type, which uses 64 lanes.
                        mask >>= T::lanes() % 64;
                    });
                });

                remainder.iter().enumerate().for_each(|(i, value)| {
                    if remainder_bits & (1 << i) != 0 {
                        A::accumulate_scalar(&mut rem_acc, *value)
                    }
                });
            }
        }

        A::reduce(chunk_acc, rem_acc)
    }
}

/// Returns the sum of values in the array.
///
/// Returns `None` if the array is empty or only contains null values.
#[cfg(simd_x86)]
pub fn sum<T: ArrowNumericType>(array: &PrimitiveArray<T>) -> Option<T::Native>
where
    T::Native: Add<Output = T::Native>,
{
    use simd::*;

    simd::simd_aggregation::<T, SumAggregate<T>>(&array)
}

#[cfg(simd_x86)]
/// Returns the minimum value in the array, according to the natural order.
/// For floating point arrays any NaN values are considered to be greater than any other non-null value
pub fn min<T: ArrowNumericType>(array: &PrimitiveArray<T>) -> Option<T::Native>
where
    T::Native: PartialOrd,
{
    use simd::*;

    simd::simd_aggregation::<T, MinAggregate<T>>(&array)
}

#[cfg(simd_x86)]
/// Returns the maximum value in the array, according to the natural order.
/// For floating point arrays any NaN values are considered to be greater than any other non-null value
pub fn max<T: ArrowNumericType>(array: &PrimitiveArray<T>) -> Option<T::Native>
where
    T::Native: PartialOrd,
{
    use simd::*;

    simd::simd_aggregation::<T, MaxAggregate<T>>(&array)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::*;
    use crate::compute::add;

    #[test]
    fn test_primitive_array_sum() {
        let a = Int32Array::from(vec![1, 2, 3, 4, 5]);
        assert_eq!(15, sum(&a).unwrap());
    }

    #[test]
    fn test_primitive_array_float_sum() {
        let a = Float64Array::from(vec![1.1, 2.2, 3.3, 4.4, 5.5]);
        assert!(16.5 - sum(&a).unwrap() < f64::EPSILON);
    }

    #[test]
    fn test_primitive_array_sum_with_nulls() {
        let a = Int32Array::from(vec![None, Some(2), Some(3), None, Some(5)]);
        assert_eq!(10, sum(&a).unwrap());
    }

    #[test]
    fn test_primitive_array_sum_all_nulls() {
        let a = Int32Array::from(vec![None, None, None]);
        assert_eq!(None, sum(&a));
    }

    #[test]
    fn test_primitive_array_sum_large_64() {
        let a: Int64Array = (1..=100)
            .map(|i| if i % 3 == 0 { Some(i) } else { None })
            .collect();
        let b: Int64Array = (1..=100)
            .map(|i| if i % 3 == 0 { Some(0) } else { Some(i) })
            .collect();
        // create an array that actually has non-zero values at the invalid indices
        let c = add(&a, &b).unwrap();
        assert_eq!(Some((1..=100).filter(|i| i % 3 == 0).sum()), sum(&c));
    }

    #[test]
    fn test_primitive_array_sum_large_32() {
        let a: Int32Array = (1..=100)
            .map(|i| if i % 3 == 0 { Some(i) } else { None })
            .collect();
        let b: Int32Array = (1..=100)
            .map(|i| if i % 3 == 0 { Some(0) } else { Some(i) })
            .collect();
        // create an array that actually has non-zero values at the invalid indices
        let c = add(&a, &b).unwrap();
        assert_eq!(Some((1..=100).filter(|i| i % 3 == 0).sum()), sum(&c));
    }

    #[test]
    fn test_primitive_array_sum_large_16() {
        let a: Int16Array = (1..=100)
            .map(|i| if i % 3 == 0 { Some(i) } else { None })
            .collect();
        let b: Int16Array = (1..=100)
            .map(|i| if i % 3 == 0 { Some(0) } else { Some(i) })
            .collect();
        // create an array that actually has non-zero values at the invalid indices
        let c = add(&a, &b).unwrap();
        assert_eq!(Some((1..=100).filter(|i| i % 3 == 0).sum()), sum(&c));
    }

    #[test]
    fn test_primitive_array_sum_large_8() {
        // include fewer values than other large tests so the result does not overflow the u8
        let a: UInt8Array = (1..=100)
            .map(|i| if i % 33 == 0 { Some(i) } else { None })
            .collect();
        let b: UInt8Array = (1..=100)
            .map(|i| if i % 33 == 0 { Some(0) } else { Some(i) })
            .collect();
        // create an array that actually has non-zero values at the invalid indices
        let c = add(&a, &b).unwrap();
        assert_eq!(Some((1..=100).filter(|i| i % 33 == 0).sum()), sum(&c));
    }

    #[test]
    fn test_primitive_array_min_max() {
        let a = Int32Array::from(vec![5, 6, 7, 8, 9]);
        assert_eq!(5, min(&a).unwrap());
        assert_eq!(9, max(&a).unwrap());
    }

    #[test]
    fn test_primitive_array_min_max_with_nulls() {
        let a = Int32Array::from(vec![Some(5), None, None, Some(8), Some(9)]);
        assert_eq!(5, min(&a).unwrap());
        assert_eq!(9, max(&a).unwrap());
    }

    #[test]
    fn test_primitive_min_max_1() {
        let a = Int32Array::from(vec![None, None, Some(5), Some(2)]);
        assert_eq!(Some(2), min(&a));
        assert_eq!(Some(5), max(&a));
    }

    #[test]
    fn test_primitive_min_max_float_large_nonnull_array() {
        let a: Float64Array = (0..256).map(|i| Some((i + 1) as f64)).collect();
        // min/max are on boundaries of chunked data
        assert_eq!(Some(1.0), min(&a));
        assert_eq!(Some(256.0), max(&a));

        // max is last value in remainder after chunking
        let a: Float64Array = (0..255).map(|i| Some((i + 1) as f64)).collect();
        assert_eq!(Some(255.0), max(&a));

        // max is first value in remainder after chunking
        let a: Float64Array = (0..257).map(|i| Some((i + 1) as f64)).collect();
        assert_eq!(Some(257.0), max(&a));
    }

    #[test]
    fn test_primitive_min_max_float_large_nullable_array() {
        let a: Float64Array = (0..256)
            .map(|i| {
                if (i + 1) % 3 == 0 {
                    None
                } else {
                    Some((i + 1) as f64)
                }
            })
            .collect();
        // min/max are on boundaries of chunked data
        assert_eq!(Some(1.0), min(&a));
        assert_eq!(Some(256.0), max(&a));

        let a: Float64Array = (0..256)
            .map(|i| {
                if i == 0 || i == 255 {
                    None
                } else {
                    Some((i + 1) as f64)
                }
            })
            .collect();
        // boundaries of chunked data are null
        assert_eq!(Some(2.0), min(&a));
        assert_eq!(Some(255.0), max(&a));

        let a: Float64Array = (0..256)
            .map(|i| if i != 100 { None } else { Some((i) as f64) })
            .collect();
        // a single non-null value somewhere in the middle
        assert_eq!(Some(100.0), min(&a));
        assert_eq!(Some(100.0), max(&a));

        // max is last value in remainder after chunking
        let a: Float64Array = (0..255).map(|i| Some((i + 1) as f64)).collect();
        assert_eq!(Some(255.0), max(&a));

        // max is first value in remainder after chunking
        let a: Float64Array = (0..257).map(|i| Some((i + 1) as f64)).collect();
        assert_eq!(Some(257.0), max(&a));
    }

    #[test]
    fn test_primitive_min_max_float_edge_cases() {
        let a: Float64Array = (0..100).map(|_| Some(f64::NEG_INFINITY)).collect();
        assert_eq!(Some(f64::NEG_INFINITY), min(&a));
        assert_eq!(Some(f64::NEG_INFINITY), max(&a));

        let a: Float64Array = (0..100).map(|_| Some(f64::MIN)).collect();
        assert_eq!(Some(f64::MIN), min(&a));
        assert_eq!(Some(f64::MIN), max(&a));

        let a: Float64Array = (0..100).map(|_| Some(f64::MAX)).collect();
        assert_eq!(Some(f64::MAX), min(&a));
        assert_eq!(Some(f64::MAX), max(&a));

        let a: Float64Array = (0..100).map(|_| Some(f64::INFINITY)).collect();
        assert_eq!(Some(f64::INFINITY), min(&a));
        assert_eq!(Some(f64::INFINITY), max(&a));
    }

    #[test]
    fn test_primitive_min_max_float_all_nans_non_null() {
        let a: Float64Array = (0..100).map(|_| Some(f64::NAN)).collect();
        assert!(max(&a).unwrap().is_nan());
        dbg!(min(&a));
        assert!(min(&a).unwrap().is_nan());
    }

    #[test]
    fn test_primitive_min_max_float_first_nan_nonnull() {
        let a: Float64Array = (0..100)
            .map(|i| {
                if i == 0 {
                    Some(f64::NAN)
                } else {
                    Some(i as f64)
                }
            })
            .collect();
        assert_eq!(Some(1.0), min(&a));
        assert!(max(&a).unwrap().is_nan());
    }

    #[test]
    fn test_primitive_min_max_float_last_nan_nonnull() {
        let a: Float64Array = (0..100)
            .map(|i| {
                if i == 99 {
                    Some(f64::NAN)
                } else {
                    Some((i + 1) as f64)
                }
            })
            .collect();
        assert_eq!(Some(1.0), min(&a));
        assert!(max(&a).unwrap().is_nan());
    }

    #[test]
    fn test_primitive_min_max_float_first_nan_nullable() {
        let a: Float64Array = (0..100)
            .map(|i| {
                if i == 0 {
                    Some(f64::NAN)
                } else if i % 2 == 0 {
                    None
                } else {
                    Some(i as f64)
                }
            })
            .collect();
        assert_eq!(Some(1.0), min(&a));
        assert!(max(&a).unwrap().is_nan());
    }

    #[test]
    fn test_primitive_min_max_float_last_nan_nullable() {
        let a: Float64Array = (0..100)
            .map(|i| {
                if i == 99 {
                    Some(f64::NAN)
                } else if i % 2 == 0 {
                    None
                } else {
                    Some(i as f64)
                }
            })
            .collect();
        assert_eq!(Some(1.0), min(&a));
        assert!(max(&a).unwrap().is_nan());
    }

    #[test]
    fn test_primitive_min_max_float_inf_and_nans() {
        let a: Float64Array = (0..100)
            .map(|i| {
                let x = match i % 10 {
                    0 => f64::NEG_INFINITY,
                    1 => f64::MIN,
                    2 => f64::MAX,
                    4 => f64::INFINITY,
                    5 => f64::NAN,
                    _ => i as f64,
                };
                Some(x)
            })
            .collect();
        assert_eq!(Some(f64::NEG_INFINITY), min(&a));
        assert!(max(&a).unwrap().is_nan());
    }

    #[test]
    fn test_string_min_max_with_nulls() {
        let a = StringArray::from(vec![Some("b"), None, None, Some("a"), Some("c")]);
        assert_eq!("a", min_string(&a).unwrap());
        assert_eq!("c", max_string(&a).unwrap());
    }

    #[test]
    fn test_string_min_max_all_nulls() {
        let a = StringArray::from(vec![None, None]);
        assert_eq!(None, min_string(&a));
        assert_eq!(None, max_string(&a));
    }

    #[test]
    fn test_string_min_max_1() {
        let a = StringArray::from(vec![None, None, Some("b"), Some("a")]);
        assert_eq!(Some("a"), min_string(&a));
        assert_eq!(Some("b"), max_string(&a));
    }

    #[test]
    fn test_boolean_min_max_empty() {
        let a = BooleanArray::from(vec![] as Vec<Option<bool>>);
        assert_eq!(None, min_boolean(&a));
        assert_eq!(None, max_boolean(&a));
    }

    #[test]
    fn test_boolean_min_max_all_null() {
        let a = BooleanArray::from(vec![None, None]);
        assert_eq!(None, min_boolean(&a));
        assert_eq!(None, max_boolean(&a));
    }

    #[test]
    fn test_boolean_min_max_no_null() {
        let a = BooleanArray::from(vec![Some(true), Some(false), Some(true)]);
        assert_eq!(Some(false), min_boolean(&a));
        assert_eq!(Some(true), max_boolean(&a));
    }

    #[test]
    fn test_boolean_min_max() {
        let a = BooleanArray::from(vec![Some(true), Some(true), None, Some(false), None]);
        assert_eq!(Some(false), min_boolean(&a));
        assert_eq!(Some(true), max_boolean(&a));

        let a = BooleanArray::from(vec![None, Some(true), None, Some(false), None]);
        assert_eq!(Some(false), min_boolean(&a));
        assert_eq!(Some(true), max_boolean(&a));

        let a =
            BooleanArray::from(vec![Some(false), Some(true), None, Some(false), None]);
        assert_eq!(Some(false), min_boolean(&a));
        assert_eq!(Some(true), max_boolean(&a));
    }

    #[test]
    fn test_boolean_min_max_smaller() {
        let a = BooleanArray::from(vec![Some(false)]);
        assert_eq!(Some(false), min_boolean(&a));
        assert_eq!(Some(false), max_boolean(&a));

        let a = BooleanArray::from(vec![None, Some(false)]);
        assert_eq!(Some(false), min_boolean(&a));
        assert_eq!(Some(false), max_boolean(&a));

        let a = BooleanArray::from(vec![None, Some(true)]);
        assert_eq!(Some(true), min_boolean(&a));
        assert_eq!(Some(true), max_boolean(&a));

        let a = BooleanArray::from(vec![Some(true)]);
        assert_eq!(Some(true), min_boolean(&a));
        assert_eq!(Some(true), max_boolean(&a));
    }
}
