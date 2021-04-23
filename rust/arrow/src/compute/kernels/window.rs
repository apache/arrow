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

//! Defines windowing functions, like `shift`ing

use crate::compute::concat;
use num::{abs, clamp};

use crate::{
    array::{make_array, ArrayData, PrimitiveArray},
    datatypes::ArrowPrimitiveType,
    error::Result,
};
use crate::{
    array::{Array, ArrayRef},
    buffer::MutableBuffer,
};

/// Shifts array by defined number of items (to left or right)
/// A positive value for `offset` shifts the array to the right
/// a negative value shifts the array to the left.
/// # Examples
/// ```
/// use arrow::array::Int32Array;
/// use arrow::error::Result;
/// use arrow::compute::shift;
///
/// let a: Int32Array = vec![Some(1), None, Some(4)].into();
/// // shift array 1 element to the right
/// let res = shift(&a, 1).unwrap();
/// let expected: Int32Array = vec![None, Some(1), None].into();
/// assert_eq!(res.as_ref(), &expected)
/// ```
pub fn shift<T>(values: &PrimitiveArray<T>, offset: i64) -> Result<ArrayRef>
where
    T: ArrowPrimitiveType,
{
    // Compute slice
    let slice_offset = clamp(-offset, 0, values.len() as i64) as usize;
    let length = values.len() - abs(offset) as usize;
    let slice = values.slice(slice_offset, length);

    // Generate array with remaining `null` items
    let nulls = abs(offset as i64) as usize;

    let mut null_array = MutableBuffer::new(nulls);
    let mut null_data = MutableBuffer::new(nulls * T::get_byte_width());
    null_array.extend_zeros(nulls);
    null_data.extend_zeros(nulls * T::get_byte_width());

    let null_data = ArrayData::new(
        T::DATA_TYPE,
        nulls as usize,
        Some(nulls),
        Some(null_array.into()),
        0,
        vec![null_data.into()],
        vec![],
    );

    // Concatenate both arrays, add nulls after if shift > 0 else before
    let null_arr = make_array(null_data);
    if offset > 0 {
        concat(&[null_arr.as_ref(), slice.as_ref()])
    } else {
        concat(&[slice.as_ref(), null_arr.as_ref()])
    }
}

#[cfg(test)]
mod tests {
    use crate::array::Int32Array;

    use super::*;

    #[test]
    fn test_shift_neg() {
        let a: Int32Array = vec![Some(1), None, Some(4)].into();
        let res = shift(&a, -1).unwrap();

        let expected: Int32Array = vec![None, Some(4), None].into();

        assert_eq!(res.as_ref(), &expected);
    }

    #[test]
    fn test_shift_pos() {
        let a: Int32Array = vec![Some(1), None, Some(4)].into();
        let res = shift(&a, 1).unwrap();

        let expected: Int32Array = vec![None, Some(1), None].into();

        assert_eq!(res.as_ref(), &expected);
    }
}
