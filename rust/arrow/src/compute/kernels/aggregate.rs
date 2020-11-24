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

use std::ops::{Add, Sub};

use crate::array::{Array, GenericStringArray, PrimitiveArray, StringOffsetSizeTrait};
use crate::datatypes::ArrowNumericType;
use rayon::iter::IndexedParallelIterator;
use rayon::prelude::*;

use bitvec::prelude::BitField;
use std::iter::Sum;

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
pub fn min<T>(array: &PrimitiveArray<T>) -> Option<T::Native>
where
    T: ArrowNumericType,
{
    min_max_helper(array, |a, b| a > b)
}

/// Returns the maximum value in the array, according to the natural order.
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

/// Returns the sum of values in the array.
///
/// Returns `None` if the array is empty or only contains null values.
#[cfg(not(all(any(target_arch = "x86", target_arch = "x86_64"), feature = "simd")))]
pub fn sum<T>(array: &PrimitiveArray<T>) -> Option<T::Native>
where
    T: ArrowNumericType,
    T::Native: Add<Output = T::Native> + Sum,
{
    let null_count = array.null_count();

    if null_count == array.len() {
        return None;
    }

    let data: &[T::Native] = array.value_slice(0, array.len());

    match array.data().null_buffer() {
        None => {
            let total = data
                .par_iter()
                .map(|value| {
                    let mut sum = T::default_value();
                    sum = sum + *value;
                    sum
                })
                .sum();

            Some(total)
        }
        Some(buffer) => {
            let data_chunks = data.par_chunks_exact(64);
            let remainder = data_chunks.remainder();

            let buffer_slice = buffer.bit_slice().slicing(array.offset(), array.len());
            let buffer_chunks = buffer_slice.par_chunks::<u64>();

            let buffer_remainder_bits: u64 = buffer_chunks.remainder_bits();

            let mut sum = data_chunks
                .zip(buffer_chunks)
                .map(|(chunk, mask)| {
                    let mut sum = T::default_value();
                    chunk.iter().enumerate().for_each(|(i, value)| {
                        if (mask.load::<u64>() & (1 << i)) != 0 {
                            sum = sum + *value;
                        }
                    });
                    sum
                })
                .sum();

            remainder.iter().enumerate().for_each(|(i, value)| {
                if buffer_remainder_bits & (1 << i) != 0 {
                    sum = sum + *value;
                }
            });

            Some(sum)
        }
    }
}

/// Returns the sum of values in the array.
///
/// Returns `None` if the array is empty or only contains null values.
#[cfg(all(any(target_arch = "x86", target_arch = "x86_64"), feature = "simd"))]
pub fn sum<T>(array: &PrimitiveArray<T>) -> Option<T::Native>
where
    T: ArrowNumericType,
    // T::Native: Add<Output = T::Native> + Sum + Sum<T::Simd>,
    T::Native: Add<Output = T::Native> + Sum,
    T::Simd: Send + Sync + Sum,
{
    let null_count = array.null_count();

    if null_count == array.len() {
        return None;
    }

    let data: &[T::Native] = array.value_slice(0, array.len());

    match array.data().null_buffer() {
        None => {
            let data_chunks = data.par_chunks_exact(T::lanes());
            let remainder = data_chunks.remainder();

            let mut agg_sum = T::horizontal_sum(data_chunks
                .map(T::load)
                .sum::<T::Simd>());

            remainder.iter().for_each(|value| {
                agg_sum = agg_sum + *value;
            });

            Some(agg_sum)
        }
        Some(buffer) => {
            // process data in chunks of 64 elements since we also get 64 bits of validity information at a time
            let data_chunks = data.par_chunks_exact(64);
            let remainder = data_chunks.remainder();

            let bit_slice = buffer.bit_slice().slicing(array.offset(), array.len());
            let bit_chunks = bit_slice.par_chunks::<u64>();
            let remainder_bits = bit_chunks.remainder_bits();

            let agg_sum: T::Simd = data_chunks.zip(bit_chunks).map(
                |(chunk, mask)| {
                    chunk
                        .par_chunks_exact(T::lanes())
                        .fold(|| T::init(T::default_value()), |mut acc, chunk| {
                            let zero = T::init(T::default_value());
                            let vecmask = T::mask_from_u64(mask.load::<u64>());
                            let chunk = T::load(&chunk);
                            let blended = T::mask_select(vecmask, chunk, zero);
                            acc = acc + blended;

                            // mask = mask >> T::lanes();
                            acc
                        })
                        .sum::<T::Simd>()
                }).sum::<T::Simd>();

            let mut agg_sum: T::Native = T::horizontal_sum(agg_sum);

            remainder.iter().enumerate().for_each(|(i, value)| {
                if remainder_bits & (1 << i) != 0 {
                    agg_sum = agg_sum + *value;
                }
            });

            Some(agg_sum)
        }
    }
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
