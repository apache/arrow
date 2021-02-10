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

#[cfg(simd)]
use std::sync::Arc;

use crate::array::{Array, ArrayData, PrimitiveArray};
use crate::buffer::Buffer;
#[cfg(simd)]
use crate::buffer::MutableBuffer;
use crate::datatypes::ArrowPrimitiveType;
#[cfg(simd)]
use crate::datatypes::{ArrowFloatNumericType, ArrowNumericType};
#[cfg(simd)]
use std::borrow::BorrowMut;

#[inline]
pub(super) fn into_primitive_array_data<I: ArrowPrimitiveType, O: ArrowPrimitiveType>(
    array: &PrimitiveArray<I>,
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
pub fn unary<I, F, O>(array: &PrimitiveArray<I>, op: F) -> PrimitiveArray<O>
where
    I: ArrowPrimitiveType,
    O: ArrowPrimitiveType,
    F: Fn(I::Native) -> O::Native,
{
    let values = array.values().iter().map(|v| op(*v));
    // JUSTIFICATION
    //  Benefit
    //      ~60% speedup
    //  Soundness
    //      `values` is an iterator with a known size because arrays are sized.
    let buffer = unsafe { Buffer::from_trusted_len_iter(values) };

    let data = into_primitive_array_data::<_, O>(array, buffer);
    PrimitiveArray::<O>::from(std::sync::Arc::new(data))
}

#[cfg(simd)]
pub(crate) fn simd_unary_float<T, SIMD_OP, SCALAR_OP>(
    array: &PrimitiveArray<T>,
    simd_op: SIMD_OP,
    scalar_op: SCALAR_OP,
) -> PrimitiveArray<T>
where
    T: ArrowFloatNumericType,
    SIMD_OP: Fn(T::Simd) -> T::Simd,
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
            let simd_input = T::load(input_slice);
            let simd_result = T::unary_op(simd_input, &simd_op);
            T::write(simd_result, result_slice);
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
    PrimitiveArray::<T>::from(Arc::new(data))
}

#[cfg(simd)]
pub(crate) fn simd_unary<T, SIMD_OP, SCALAR_OP>(
    array: &PrimitiveArray<T>,
    simd_op: SIMD_OP,
    scalar_op: SCALAR_OP,
) -> PrimitiveArray<T>
where
    T: ArrowNumericType,
    SIMD_OP: Fn(T::Simd) -> T::Simd,
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
            let simd_input = T::load(input_slice);
            let simd_result = T::unary_op(simd_input, &simd_op);
            T::write(simd_result, result_slice);
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
    PrimitiveArray::<T>::from(Arc::new(data))
}

#[macro_export]
macro_rules! float_unary {
    ($name:ident) => {
        pub fn $name<T>(array: &PrimitiveArray<T>) -> PrimitiveArray<T>
        where
            T: ArrowNumericType + ArrowFloatNumericType,
            T::Native: num::traits::Float,
        {
            return unary(array, |a| a.$name());
        }
    };
}

#[macro_export]
macro_rules! float_unary_simd {
    ($name:ident) => {
        /// Calculate the `$name` of a floating point array
        pub fn $name<T>(array: &PrimitiveArray<T>) -> PrimitiveArray<T>
        where
            T: ArrowNumericType + ArrowFloatNumericType,
            T::Native: num::traits::Float,
        {
            #[cfg(simd)]
            {
                return simd_unary(array, |x| T::$name(x), |x| x.$name());
            }
            #[cfg(not(simd))]
            return unary(array, |x| x.$name());
        }
    };
}

#[macro_export]
macro_rules! unary {
    ($name:ident) => {
        pub fn $name<T>(array: &PrimitiveArray<T>) -> PrimitiveArray<T>
        where
            T: ArrowNumericType,
            T::Native: num::traits::Float,
        {
            return unary(array, |a| a.$name());
        }
    };
}
