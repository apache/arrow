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

//! Defines primitive computations on arrays

use std::mem;
use std::ops::{Add, Div, Mul, Sub};
use std::slice::from_raw_parts_mut;

use num::Zero;

use crate::array::*;
use crate::buffer::MutableBuffer;
use crate::compute::array_ops::math_op;
use crate::datatypes;
use crate::error::{ArrowError, Result};

pub fn add_simd<T>(x: &PrimitiveArray<T>, y: &PrimitiveArray<T>) -> Result<PrimitiveArray<T>>
where
    T: datatypes::SimdType,
{
    if x.len() != y.len() {
        return Err(ArrowError::ComputeError(
            "Cannot perform math operation on arrays of different length".to_string(),
        ));
    }

    let lanes = T::lanes();
    let buffer_size = x.len() * mem::size_of::<T::Native>();
    let mut result = MutableBuffer::new(buffer_size).with_bitset(buffer_size, false);

    for i in (0..x.len()).step_by(lanes) {
        let simd_x = T::load(x.value_slice(i, lanes));
        let simd_y = T::load(y.value_slice(i, lanes));
        let simd_z = T::add(simd_x, simd_y);

        let result_slice: &mut [T::Native] = unsafe {
            from_raw_parts_mut(
                (result.data_mut().as_mut_ptr() as *mut T::Native).offset(i as isize),
                lanes,
            )
        };
        T::write(simd_z, result_slice);
    }

    Ok(PrimitiveArray::<T>::new(x.len(), result.freeze(), 0, 0))
}

/// Perform `left + right` operation on two arrays. If either left or right value is null then the result is also null.
pub fn add<T>(left: &PrimitiveArray<T>, right: &PrimitiveArray<T>) -> Result<PrimitiveArray<T>>
where
    T: datatypes::ArrowNumericType,
    T::Native: Add<Output = T::Native>
        + Sub<Output = T::Native>
        + Mul<Output = T::Native>
        + Div<Output = T::Native>
        + Zero,
{
    math_op(left, right, |a, b| Ok(a + b))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::Int32Array;

    #[test]
    fn test_supported() {
        println!("SSE: {}", is_x86_feature_detected!("sse"));
        println!("SSE2: {}", is_x86_feature_detected!("sse2"));
        println!("SSE3: {}", is_x86_feature_detected!("sse3"));
        println!("SSSE3: {}", is_x86_feature_detected!("ssse3"));
        println!("SSE4.1: {}", is_x86_feature_detected!("sse4.1"));
        println!("SSE4.2: {}", is_x86_feature_detected!("sse4.2"));
        println!("AVX: {}", is_x86_feature_detected!("avx"));
        println!("AVX2: {}", is_x86_feature_detected!("avx2"));
        println!("AVX512f: {}", is_x86_feature_detected!("avx512f"));
    }

    #[test]
    fn test_simd() {
        let a = Float32Array::from(vec![5.0, 6.0, 7.0, 8.0, 9.0, 10.0]);
        let b = Float32Array::from(vec![6.0, 7.0, 8.0, 9.0, 8.0, 20.0]);
        let c = add_simd(&a, &b).unwrap();

        assert_eq!(11.0, c.value(0));
        assert_eq!(13.0, c.value(1));
        assert_eq!(15.0, c.value(2));
        assert_eq!(17.0, c.value(3));
        assert_eq!(17.0, c.value(4));
        assert_eq!(30.0, c.value(5));
    }

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
}
