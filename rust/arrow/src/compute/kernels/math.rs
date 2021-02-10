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

//! Defines basic math kernels for `PrimitiveArrays`.

use num::{traits::Pow, Float};

use crate::datatypes::{ArrowFloatNumericType, ArrowNumericType};
use crate::{array::*, float_unary_simd};

#[cfg(simd)]
use super::arity::simd_unary;
use super::arity::unary;

float_unary_simd!(sqrt);

/// Raise a floating point array to the power of a float scalar.
pub fn powf_scalar<T>(array: &PrimitiveArray<T>, raise: T::Native) -> PrimitiveArray<T>
where
    T: ArrowFloatNumericType,
    T::Native: Pow<T::Native, Output = T::Native>,
{
    #[cfg(simd)]
    {
        let raise_vector = T::init(raise);
        return simd_unary(array, |x| T::pow(x, raise_vector), |x| x.pow(raise));
    }
    #[cfg(not(simd))]
    return unary(array, |x| x.pow(raise));
}

/// Raise array with floating point values to the power of an integer scalar.
///
/// This function currently has no SIMD equivalent, but is included because:
/// - `powi` is generally faster than `powf`
/// - If there is a SIMD implementation in future,
/// we might want to keep forwad compatibility.
///
/// If using SIMD, it will be quicker to use [`powf_scalar`] instead.
pub fn powi<T>(array: &PrimitiveArray<T>, raise: i32) -> PrimitiveArray<T>
where
    T: ArrowFloatNumericType,
    T::Native: Pow<i32, Output = T::Native>,
{
    // Note: packed_simd doesn't support `pow` or `powi`
    unary(array, |x| x.pow(raise))
}

/// Raise numeric array to the power of an integer scalar.
///
/// Due to the use of [num::traits::Pow], this function can take both integer
/// and floating point arrays as an input.
pub fn pow<T>(array: &PrimitiveArray<T>, power: isize) -> PrimitiveArray<T>
where
    T: ArrowNumericType,
    T::Native: Pow<isize, Output = T::Native>,
{
    unary::<_, _, T>(array, |x| x.pow(power))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_primitive_array_raise_power_scalar() {
        let a = Float64Array::from(vec![1.0, 2.0, 3.0]);
        let actual = powf_scalar(&a, 2.0);
        let expected = Float64Array::from(vec![1.0, 4.0, 9.0]);
        assert_eq!(expected, actual);
        let a = Float64Array::from(vec![Some(1.0), None, Some(3.0)]);
        let actual = powf_scalar(&a, 2.0);
        let expected = Float64Array::from(vec![Some(1.0), None, Some(9.0)]);
        assert_eq!(expected, actual);
    }
}
