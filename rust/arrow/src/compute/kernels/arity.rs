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

//! Defines kernels suitable to perform operations to primitive arrays.

use crate::array::{Array, ArrayData, PrimitiveArray, TypedArray, TypedArrayRef};
use crate::buffer::Buffer;
use crate::compute::util::combine_option_bitmap;
use crate::datatypes::ArrowPrimitiveType;
use crate::error::{ArrowError, Result};

#[inline]
fn into_primitive_array_data<I: Array, O: ArrowPrimitiveType>(
    array: &I,
    buffer: Buffer,
) -> ArrayData {
    ArrayData::new(
        O::DATA_TYPE,
        array.len(),
        None,
        array.data_ref().null_buffer().cloned(),
        0,
        vec![buffer],
        vec![],
    )
}

#[inline]
fn into_primitive_array_data_with_nulls<I: Array, O: ArrowPrimitiveType>(
    array: &I,
    null_buffer: Option<Buffer>,
    buffer: Buffer,
) -> ArrayData {
    ArrayData::new(
        O::DATA_TYPE,
        array.len(),
        None,
        null_buffer,
        0,
        vec![buffer],
        vec![],
    )
}

/// Applies an unary and infalible function to a primitive array.
/// This is the fastest way to perform an operation on a primitive array when
/// the benefits of a vectorized operation outweights the cost of branching nulls and non-nulls.
/// # Implementation
/// This will apply the function for all values, including those on null slots.
/// This implies that the operation must be infalible for any value of the corresponding type
/// or this function may panic.
/// # Example
/// ```rust
/// # use arrow::array::Int32Array;
/// # use arrow::datatypes::Int32Type;
/// # use arrow::compute::kernels::arity::unary;
/// # fn main() {
/// let array = Int32Array::from(vec![Some(5), Some(7), None]);
/// let c = unary::<_, _, Int32Type>(&array, |x| x * 2 + 1);
/// assert_eq!(c, Int32Array::from(vec![Some(11), Some(15), None]));
/// # }
/// ```
pub fn unary<'a, I, F, O>(array: &'a I, op: F) -> PrimitiveArray<O>
where
    I: Array + TypedArray,
    &'a I: TypedArrayRef,
    O: ArrowPrimitiveType,
    F: Fn(<&'a I as TypedArrayRef>::ValueRef) -> O::Native,
{
    let values = array.iter_values().map(op);
    // JUSTIFICATION
    //  Benefit
    //      ~60% speedup
    //  Soundness
    //      `values` is an iterator with a known size because arrays are sized.
    let buffer = unsafe { Buffer::from_trusted_len_iter(values) };

    let data = into_primitive_array_data::<_, O>(array, buffer);
    PrimitiveArray::<O>::from(std::sync::Arc::new(data))
}

/// Applies an binary and infalible function to a primitive array.
/// This is the fastest way to perform an operation on a primitive array when
/// the benefits of a vectorized operation outweights the cost of branching nulls and non-nulls.
/// # Implementation
/// This will apply the function for all values, including those on null slots.
/// This implies that the operation must be infalible for any value of the corresponding type
/// or this function may panic.
/// # Example
/// ```rust
/// # use arrow::array::{Int32Array,Float32Array};
/// # use arrow::datatypes::Int32Type;
/// # use arrow::compute::kernels::arity::binary;
/// # fn main() {
/// let array_int = Int32Array::from(vec![Some(5), Some(7), None, Some(-1), None]);
/// let array_float = Float32Array::from(vec![Some(5.0f32), Some(7.0f32), Some(11.0f32), None, None]);
/// let c = binary::<_, _, _, Int32Type>(&array_int, &array_float, |x,y| (((x as f32) * y) as i32 + 1) ).unwrap();
/// assert_eq!(c, Int32Array::from(vec![Some(26), Some(50), None, None, None]));
/// # }
/// ```
pub fn binary<'a, I, J, F, O>(
    left: &'a I,
    right: &'a J,
    op: F,
) -> Result<PrimitiveArray<O>>
where
    I: Array + TypedArray,
    &'a I: TypedArrayRef,
    J: Array + TypedArray,
    &'a J: TypedArrayRef,
    O: ArrowPrimitiveType,
    F: Fn(
        <&'a I as TypedArrayRef>::ValueRef,
        <&'a J as TypedArrayRef>::ValueRef,
    ) -> O::Native,
{
    if left.len() != right.len() {
        return Err(ArrowError::ComputeError(
            "Cannot perform operation on arrays of different length".to_string(),
        ));
    };

    let null_buffer =
        combine_option_bitmap(left.data_ref(), right.data_ref(), left.len())?;

    let values = left
        .iter_values()
        .zip(right.iter_values())
        .map(|(l, r)| op(l, r));
    // JUSTIFICATION
    //  Benefit
    //      ~60% speedup
    //  Soundness
    //      `values` is an iterator with a known size because arrays are sized.
    let buffer = unsafe { Buffer::from_trusted_len_iter(values) };

    let data = into_primitive_array_data_with_nulls::<_, O>(left, null_buffer, buffer);
    Ok(PrimitiveArray::<O>::from(std::sync::Arc::new(data)))
}
