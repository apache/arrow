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

//! Defines reduction kernels for `PrimitiveArrays`.
//!
//! These kernels can leverage SIMD if available on your system.  Currently no runtime
//! detection is provided, you should enable the specific SIMD intrinsics using
//! `RUSTFLAGS="-C target-feature=+avx2"` for example.  See the documentation
//! [here](https://doc.rust-lang.org/stable/core/arch/) for more information.


#[cfg(feature = "simd")]
use std::iter::Iterator;
use std::ops::{Add, AddAssign};

use num::Zero;

use crate::array::{Array, BooleanArray, PrimitiveArray};
#[cfg(feature = "simd")]
use crate::compute::util::simd_load_set_invalid;
use crate::datatypes::ArrowNumericType;
use crate::error::{ArrowError, Result};


//#[cfg(feature = "simd")]
//struct SIMDIter<'a, T> where
//    T: ArrowNumericType,
//    T::Native: Add<Output = T::Native>,
//{
//    current: usize,
//    array: &'a PrimitiveArray<T>,
//}
//
//impl<'a, T> SIMDIter<'a, T>
//    where
//    T: ArrowNumericType,
//    T::Native: Add<Output = T::Native>,
//{
//    fn new(array: &'a PrimitiveArray<T>) -> Self {
//        Self {
//            current: 0,
//            array,
//        }
//    }
//}

//impl<'a, T> Iterator for SIMDIter<'a, T>
//    where
//        T: ArrowNumericType,
//        T::Native: Add<Output = T::Native> + Zero,
//{
//    type Item = T::Simd;
//
//    fn next(&mut self) -> Option<Self::Item> {
//
//        if self.current > self.array.len() {
//            None
//        } else {
//            let lanes = T::lanes();
//            let nulls_zeroed = unsafe { simd_load_set_invalid(self.array, self.array.data().null_bitmap(), self.current, lanes, T::Native::zero()) };
//            self.current += lanes;
//            Some(nulls_zeroed)
//        }
//    }
//}


/// Returns the minimum value in the array, according to the natural order.
pub fn min<T>(array: &PrimitiveArray<T>) -> Option<T::Native>
    where
        T: ArrowNumericType,
{
    min_max_helper(array, |a, b| a < b)
}

/// Returns the maximum value in the array, according to the natural order.
pub fn max<T>(array: &PrimitiveArray<T>) -> Option<T::Native>
    where
        T: ArrowNumericType,
{
    min_max_helper(array, |a, b| a > b)
}

/// Helper function to perform min/max lambda function on values from a numeric array.
fn min_max_helper<T, F>(array: &PrimitiveArray<T>, cmp: F) -> Option<T::Native>
    where
        T: ArrowNumericType,
        F: Fn(T::Native, T::Native) -> bool,
{
    let mut n: Option<T::Native> = None;
    let data = array.data();
    for i in 0..data.len() {
        if data.is_null(i) {
            continue;
        }
        let m = array.value(i);
        match n {
            None => n = Some(m),
            Some(nn) => {
                if cmp(m, nn) {
                    n = Some(m)
                }
            }
        }
    }
    n
}

/// Returns the sum of values in the array.
///
/// Returns `None` if the array is empty or only contains null values.
pub fn simd_sum<T>(array: &PrimitiveArray<T>) -> Option<T::Native>
    where
        T: ArrowNumericType,
        T::Native: Add<Output = T::Native> + Zero,
{
    let null_count = array.null_count();
    if array.len() == null_count || array.len() == 0 {
        None
    } else {
        let lanes = T::lanes();
        let mut current_simd = unsafe {simd_load_set_invalid(array, array.data().null_bitmap(), 0, lanes, T::Native::zero())};
        for i in (lanes..array.len()).step_by(lanes) {
            let next_simd = unsafe {simd_load_set_invalid(array, array.data().null_bitmap(), i, lanes, T::Native::zero())};
            current_simd = T::bin_op(current_simd, next_simd, |a, b| a + b);
        }
        let mut partial_sum = T::Native::zero();
        for i in 0..lanes {
            partial_sum = partial_sum + T::get(&current_simd, i);
        }
        Some(partial_sum)
    }
}

/// Returns the sum of values in the array.
///
/// Returns `None` if the array is empty or only contains null values.
pub fn no_simd_sum<T>(array: &PrimitiveArray<T>) -> Option<T::Native>
    where
        T: ArrowNumericType,
        T::Native: Add<Output = T::Native>,
{
    let null_count = array.null_count();

    if null_count == array.len() {
        None
    } else if null_count == 0 {
        // optimized path for arrays without null values
        let mut n: T::Native = T::default_value();
        let data = array.data();
        let m = array.value_slice(0, data.len());
        for i in 0..data.len() {
            n = n + m[i];
        }
        Some(n)
    } else {
        let mut n: T::Native = T::default_value();
        let data = array.data();
        let m = array.value_slice(0, data.len());
        for i in 0..data.len() {
            if data.is_valid(i) {
                n = n + m[i];
            }
        }
        Some(n)
    }
}

pub fn sum<T>(array: &PrimitiveArray<T>) -> Option<T::Native>
    where
        T: ArrowNumericType,
        T::Native: Add<Output = T::Native> + Zero
{
    #[cfg(all(any(target_arch = "x86", target_arch = "x86_64"), feature = "simd"))]
    return simd_sum(&array);

    #[allow(unreachable_code)]
    no_simd_sum(&array)
}

/// Helper function to perform boolean lambda function on values from two arrays.
//fn bool_op<T, F>(
//    left: &PrimitiveArray<T>,
//    right: &PrimitiveArray<T>,
//    op: F,
//) -> Result<BooleanArray>
//    where
//        T: ArrowNumericType,
//        F: Fn(Option<T::Native>, Option<T::Native>) -> bool,
//{
//    if left.len() != right.len() {
//        return Err(ArrowError::ComputeError(
//            "Cannot perform math operation on arrays of different length".to_string(),
//        ));
//    }
//    let mut b = BooleanArray::builder(left.len());
//    for i in 0..left.len() {
//        let index = i;
//        let l = if left.is_null(i) {
//            None
//        } else {
//            Some(left.value(index))
//        };
//        let r = if right.is_null(i) {
//            None
//        } else {
//            Some(right.value(index))
//        };
//        b.append_value(op(l, r))?;
//    }
//    Ok(b.finish())
//}

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
        assert_eq!(16.5, sum(&a).unwrap());
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
}
