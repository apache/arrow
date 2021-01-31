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
use std::sync::Arc;

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
pub fn shift<T>(values: &PrimitiveArray<T>, shift: i64) -> Result<ArrayRef>
where
    T: ArrowPrimitiveType,
{
    // Compute slice
    let offset = clamp(shift, 0, values.len() as i64) as usize;
    println!("{}", offset);
    let length = values.len() - abs(shift) as usize;
    let slice = values.slice(offset, length);

    // Generate array with remaining `null` items
    let nulls = abs(shift as i64) as usize;

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
    let null_arr = make_array(Arc::new(null_data));
    if shift > 0 {
        concat(&[slice.as_ref(), null_arr.as_ref()])
    } else {
        concat(&[null_arr.as_ref(), slice.as_ref()])
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

        let b = res.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(a.len(), res.len());
        assert_eq!(false, b.is_valid(0));
        assert_eq!(1, b.value(1));
        assert_eq!(false, b.is_valid(2));
    }

    #[test]
    fn test_shift_pos() {
        let a: Int32Array = vec![Some(1), None, Some(4)].into();
        let res = shift(&a, 1).unwrap();

        let b = res.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(a.len(), res.len());
        assert_eq!(false, b.is_valid(0));
        assert_eq!(4, b.value(1));
        assert_eq!(false, b.is_valid(2));
    }
}
