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

use crate::array::{Array, GenericStringArray, PrimitiveArray, StringOffsetSizeTrait};
use crate::datatypes::ArrowNumericType;

/// Helper macro to perform min/max of strings
fn min_max_string<T: StringOffsetSizeTrait, F: Fn(&str, &str) -> bool>(
    array: &GenericStringArray<T>,
    cmp: F,
) -> Option<&str> {
    let null_count = array.null_count();

    if null_count == array.len() {
        return None;
    }
    let mut n = "";
    let mut has_value = false;
    let data = array.data();

    if null_count == 0 {
        for i in 0..data.len() {
            let item = array.value(i);
            if !has_value || cmp(&n, item) {
                has_value = true;
                n = item;
            }
        }
    } else {
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
#[cfg(not(all(any(target_arch = "x86", target_arch = "x86_64"), feature = "simd")))]
pub fn min<T>(array: &PrimitiveArray<T>) -> Option<T::Native>
where
    T: ArrowNumericType,
{
    min_max_helper(array, |a, b| a > b)
}

/// Returns the maximum value in the array, according to the natural order.
#[cfg(not(all(any(target_arch = "x86", target_arch = "x86_64"), feature = "simd")))]
pub fn max<T>(array: &PrimitiveArray<T>) -> Option<T::Native>
where
    T: ArrowNumericType,
{
    min_max_helper(array, |a, b| a < b)
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
    let m = array.value_slice(0, data.len());
    let mut n;

    if null_count == 0 {
        // optimized path for arrays without null values
        n = m[0];

        for item in &m[1..] {
            if cmp(&n, item) {
                n = *item
            }
        }
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

/// Returns the sum of values in the array.
///
/// Returns `None` if the array is empty or only contains null values.
#[cfg(not(all(any(target_arch = "x86", target_arch = "x86_64"), feature = "simd")))]
pub fn sum<T>(array: &PrimitiveArray<T>) -> Option<T::Native>
where
    T: ArrowNumericType,
    T::Native: Add<Output = T::Native>,
{
    let null_count = array.null_count();

    if null_count == array.len() {
        return None;
    }

    let data: &[T::Native] = array.value_slice(0, array.len());

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
                    chunk.iter().enumerate().for_each(|(i, value)| {
                        if (mask & (1 << i)) != 0 {
                            sum = sum + *value;
                        }
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

#[cfg(all(any(target_arch = "x86", target_arch = "x86_64"), feature = "simd"))]
mod simd {
    use crate::array::{Array, PrimitiveArray};
    use crate::datatypes::ArrowNumericType;
    use std::marker::PhantomData;
    use std::ops::Add;

    pub(super) trait SimdAggregate<T: ArrowNumericType> {
        fn init_accumulator_scalar() -> T::Native;

        #[inline]
        fn init_accumulator_chunk() -> T::Simd {
            T::init(Self::init_accumulator_scalar())
        }

        fn accumulate_chunk_non_null(accumulator: &mut T::Simd, chunk: T::Simd);
        fn accumulate_chunk_nullable(
            accumulator: &mut T::Simd,
            chunk: T::Simd,
            mask: T::SimdMask,
        );

        fn accumulate_scalar(accumulator: &mut T::Native, value: T::Native);

        #[inline]
        fn reduce(accumulator: T::Simd) -> T::Native {
            // reduce by first writing to a temporary and then use scalar operations
            // this should be about the same performance as extracting individual lanes
            // but allows us to reuse the scalar reduction logic
            let tmp = &mut [T::default_value(); 64];
            T::write(accumulator, &mut tmp[0..T::lanes()]);

            let mut reduced = Self::init_accumulator_scalar();
            tmp[0..T::lanes()]
                .iter()
                .for_each(|value| Self::accumulate_scalar(&mut reduced, *value));

            reduced
        }
    }

    pub(super) struct SumAggregate<T: ArrowNumericType> {
        phantom: PhantomData<T>,
    }

    impl<T: ArrowNumericType> SimdAggregate<T> for SumAggregate<T>
    where
        T::Native: Add<Output = T::Native>,
    {
        fn init_accumulator_scalar() -> T::Native {
            T::default_value()
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
    }

    pub(super) struct MinAggregate<T: ArrowNumericType> {
        phantom: PhantomData<T>,
    }

    impl<T: ArrowNumericType> SimdAggregate<T> for MinAggregate<T>
    where
        T::Native: PartialOrd,
    {
        fn init_accumulator_scalar() -> T::Native {
            T::identity_for_min_op()
        }

        fn accumulate_chunk_non_null(accumulator: &mut T::Simd, chunk: T::Simd) {
            let cmp_mask = T::lt(chunk, *accumulator);

            *accumulator = T::mask_select(cmp_mask, chunk, *accumulator);
        }

        fn accumulate_chunk_nullable(
            accumulator: &mut T::Simd,
            chunk: T::Simd,
            vecmask: T::SimdMask,
        ) {
            let cmp_mask = T::lt(chunk, *accumulator);
            let blend_mask = T::mask_and(vecmask, cmp_mask);

            *accumulator = T::mask_select(blend_mask, chunk, *accumulator);
        }

        fn accumulate_scalar(accumulator: &mut T::Native, value: T::Native) {
            if value < *accumulator {
                *accumulator = value
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
        fn init_accumulator_scalar() -> T::Native {
            T::identity_for_max_op()
        }

        fn accumulate_chunk_non_null(accumulator: &mut T::Simd, chunk: T::Simd) {
            let cmp_mask = T::gt(chunk, *accumulator);

            *accumulator = T::mask_select(cmp_mask, chunk, *accumulator);
        }

        fn accumulate_chunk_nullable(
            accumulator: &mut T::Simd,
            chunk: T::Simd,
            vecmask: T::SimdMask,
        ) {
            let cmp_mask = T::gt(chunk, *accumulator);
            let blend_mask = T::mask_and(vecmask, cmp_mask);

            *accumulator = T::mask_select(blend_mask, chunk, *accumulator);
        }

        fn accumulate_scalar(accumulator: &mut T::Native, value: T::Native) {
            if value > *accumulator {
                *accumulator = value
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

        let data: &[T::Native] = array.value_slice(0, array.len());

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

                        mask = mask >> T::lanes();
                    });
                });

                remainder.iter().enumerate().for_each(|(i, value)| {
                    if remainder_bits & (1 << i) != 0 {
                        A::accumulate_scalar(&mut rem_acc, *value)
                    }
                });
            }
        }

        let mut total = A::reduce(chunk_acc);
        A::accumulate_scalar(&mut total, rem_acc);

        Some(total)
    }
}

/// Returns the sum of values in the array.
///
/// Returns `None` if the array is empty or only contains null values.
#[cfg(all(any(target_arch = "x86", target_arch = "x86_64"), feature = "simd"))]
pub fn sum<T: ArrowNumericType>(array: &PrimitiveArray<T>) -> Option<T::Native>
where
    T::Native: Add<Output = T::Native>,
{
    use simd::*;

    simd::simd_aggregation::<T, SumAggregate<T>>(&array)
}

#[cfg(all(any(target_arch = "x86", target_arch = "x86_64"), feature = "simd"))]
pub fn min<T: ArrowNumericType>(array: &PrimitiveArray<T>) -> Option<T::Native>
where
    T::Native: PartialOrd,
{
    use simd::*;

    simd::simd_aggregation::<T, MinAggregate<T>>(&array)
}

#[cfg(all(any(target_arch = "x86", target_arch = "x86_64"), feature = "simd"))]
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
    fn test_buffer_array_min_max() {
        let a = Int32Array::from(vec![5, 6, 7, 8, 9]);
        assert_eq!(5, min(&a).unwrap());
        assert_eq!(9, max(&a).unwrap());
    }

    #[test]
    fn test_buffer_array_min_max_with_nulls() {
        let a = Int32Array::from(vec![Some(5), None, None, Some(8), Some(9)]);
        assert_eq!(5, min(&a).unwrap());
        assert_eq!(9, max(&a).unwrap());
    }

    #[test]
    fn test_buffer_min_max_1() {
        let a = Int32Array::from(vec![None, None, Some(5), Some(2)]);
        assert_eq!(Some(2), min(&a));
        assert_eq!(Some(5), max(&a));
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
}
