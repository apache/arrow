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

//! Defines trigonometry kernels for `PrimitiveArrays` that are
//! floats, restricted by the [num::Float] trait.

use std::{iter::FromIterator, ops::Div};

use num::{Float, Zero};

use super::math::*;
use crate::buffer::Buffer;
use crate::datatypes::ArrowNumericType;
use crate::error::Result;
use crate::{
    array::*,
    compute::{add, math_op, multiply, subtract},
    datatypes::ArrowPrimitiveType,
    error::ArrowError,
    float_unary,
};

use super::arity::{into_primitive_array_data, unary};

float_unary!(to_degrees);
float_unary!(to_radians);
float_unary!(sin);
float_unary!(cos);
float_unary!(tan);
float_unary!(asin);
float_unary!(acos);
float_unary!(atan);
float_unary!(sinh);
float_unary!(cosh);
float_unary!(tanh);
float_unary!(asinh);
float_unary!(acosh);
float_unary!(atanh);

pub fn atan2<T>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<PrimitiveArray<T>>
where
    T: ArrowNumericType,
    T::Native: num::traits::Float,
{
    math_op(left, right, |x, y| x.atan2(y))
}

/// Perform `left * right` operation on two arrays. If either left or right value is null
/// then the result is also null.
pub fn sin_cos<T>(array: &PrimitiveArray<T>) -> (PrimitiveArray<T>, PrimitiveArray<T>)
where
    T: ArrowNumericType,
    T::Native: num::traits::Float,
{
    let (sin, cos): (Vec<T::Native>, Vec<T::Native>) =
        array.values().iter().map(|v| v.sin_cos()).unzip();
    // NOTE: due to `unzip` collecting and splitting the arrays,
    // this might be slower than Buffer::from_trusted_len_iter
    let sin_buffer = Buffer::from_iter(sin);
    let cos_buffer = Buffer::from_iter(cos);

    let sin_data = into_primitive_array_data::<_, T>(array, sin_buffer);
    let cos_data = into_primitive_array_data::<_, T>(array, cos_buffer);
    (
        PrimitiveArray::<T>::from(std::sync::Arc::new(sin_data)),
        PrimitiveArray::<T>::from(std::sync::Arc::new(cos_data)),
    )
}

/// Calculate the Haversine distance between two geographic coordinates.
/// Based on the haversine crate.
///
/// The distance returned is in meters, and the radius must be specified in meters.
pub fn haversine<T>(
    lat_a: &PrimitiveArray<T>,
    lng_a: &PrimitiveArray<T>,
    lat_b: &PrimitiveArray<T>,
    lng_b: &PrimitiveArray<T>,
    radius: impl num::traits::Float,
) -> Result<PrimitiveArray<T>>
where
    T: ArrowPrimitiveType + ArrowNumericType,
    T::Native: num::traits::Float + Div<Output = T::Native> + Zero + num::NumCast,
{
    // Check array lengths, must all equal
    let len = lat_a.len();
    if lat_b.len() != len || lng_a.len() != len || lng_b.len() != len {
        return Err(ArrowError::ComputeError(
            "Cannot perform math operation on arrays of different length".to_string(),
        ));
    }
    // These casts normally get optimized to f64 as f64
    let one = num::cast::<_, T::Native>(1.0).unwrap();
    let two = num::cast::<_, T::Native>(2.0).unwrap();
    let radius = num::cast::<_, T::Native>(radius).unwrap();
    let two_radius = two * radius;

    let lat_delta = to_radians(&subtract(lat_b, lat_a)?);
    let lng_delta = to_radians(&subtract(lng_b, lng_a)?);
    let lat_a_rad = to_radians(lat_a);
    let lat_b_rad = to_radians(lat_b);

    let v1: PrimitiveArray<T> = sin(&unary::<_, _, _>(&lat_delta, |x| x / two));
    let v2: PrimitiveArray<T> = sin(&unary::<_, _, _>(&lng_delta, |x| x / two));

    let a = add(
        // powf is slower than x * x
        &unary::<_, _, _>(&v1, |x: T::Native| x * x),
        // This could be simplified if we had a ternary kernel that takes 3 args
        // F(T::Native, T::Native, T::Native) -> T::Native
        &multiply(
            // powf is slower than x * x
            &unary::<_, _, _>(&v2, |x: T::Native| x * x),
            &math_op(&lat_a_rad, &lat_b_rad, |x, y| (x.cos() * y.cos()))?,
        )?,
    )?;
    Ok(unary::<_, _, _>(
        &atan2(&sqrt(&a), &sqrt(&unary::<_, _, _>(&a, |x| one - x)))?,
        |x| x * two_radius,
    ))
}

// only added here to test that it's accurate
pub fn haversine_no_unary<T>(
    lat_a: &PrimitiveArray<T>,
    lng_a: &PrimitiveArray<T>,
    lat_b: &PrimitiveArray<T>,
    lng_b: &PrimitiveArray<T>,
    radius: impl num::traits::Float,
) -> Result<PrimitiveArray<T>>
where
    T: ArrowPrimitiveType + ArrowNumericType,
    T::Native: num::traits::Float + Div<Output = T::Native> + Zero + num::NumCast,
{
    // isolate imports only used by this function
    use super::arithmetic::divide;
    use std::sync::Arc;

    // Check array lengths, must all equal
    let len = lat_a.len();
    if lat_b.len() != len || lng_a.len() != len || lng_b.len() != len {
        return Err(ArrowError::ComputeError(
            "Cannot perform math operation on arrays of different length".to_string(),
        ));
    }

    // there doesn't seem to be a way to satify the type system without branching
    let (one, two, radius) = match T::DATA_TYPE {
        crate::datatypes::DataType::Float32 => {
            let one = Float32Array::from(vec![1.0; len]);
            let two = Float32Array::from(vec![2.0; len]);
            let radius =
                Float32Array::from(vec![num::cast::<_, f32>(radius).unwrap(); len]);
            // cast to array then back to T
            (
                Arc::new(one) as ArrayRef,
                Arc::new(two) as ArrayRef,
                Arc::new(radius) as ArrayRef,
            )
        }
        crate::datatypes::DataType::Float64 => {
            let one = Float64Array::from(vec![1.0; len]);
            let two = Float64Array::from(vec![2.0; len]);
            let radius =
                Float64Array::from(vec![num::cast::<_, f64>(radius).unwrap(); len]);
            // cast to array then back to T
            (
                Arc::new(one) as ArrayRef,
                Arc::new(two) as ArrayRef,
                Arc::new(radius) as ArrayRef,
            )
        }
        _ => unreachable!("This function should only be callable from floats"),
    };
    let one = one.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
    let two = two.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
    let radius = radius.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();

    let lat_delta = to_radians(&subtract(lat_b, lat_a)?);
    let lng_delta = to_radians(&subtract(lng_b, lng_a)?);
    let lat_a_rad = to_radians(lat_a);
    let lat_b_rad = to_radians(lat_b);

    let v1 = &sin(&divide(&lat_delta, &two)?);
    let v2 = sin(&divide(&lng_delta, &two)?);

    let a = add(
        &multiply(&v1, &v1)?,
        &multiply(
            &multiply(&v2, &v2)?,
            &multiply(&cos(&lat_a_rad), &cos(&lat_b_rad))?,
        )?,
    )?;
    let c: PrimitiveArray<T> =
        multiply(&atan2(&sqrt(&a), &sqrt(&subtract(&one, &a)?))?, &two)?;

    multiply(&c, &radius)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_haversine_non_null() {
        let lat_a = Float64Array::from(vec![38.898556]);
        let lng_a = Float64Array::from(vec![-77.037852]);
        let lat_b = Float64Array::from(vec![38.897147]);
        let lng_b = Float64Array::from(vec![-77.043934]);

        let radius = 6371000.0;

        let result = haversine(&lat_a, &lng_a, &lat_b, &lng_b, radius).unwrap();
        assert_eq!(result.value(0), 549.1557912038084);
    }

    #[test]
    fn test_haversine_no_unary_non_null() {
        let lat_a = Float64Array::from(vec![38.898556]);
        let lng_a = Float64Array::from(vec![-77.037852]);
        let lat_b = Float64Array::from(vec![38.897147]);
        let lng_b = Float64Array::from(vec![-77.043934]);

        let radius = 6371000.0;

        let result = haversine_no_unary(&lat_a, &lng_a, &lat_b, &lng_b, radius).unwrap();
        assert_eq!(result.value(0), 549.1557912038084);
    }
}
