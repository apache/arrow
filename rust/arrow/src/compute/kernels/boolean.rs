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

//! Defines boolean kernels on Arrow `BooleanArray`'s, e.g. `AND`, `OR` and `NOT`.
//!
//! These kernels can leverage SIMD if available on your system.  Currently no runtime
//! detection is provided, you should enable the specific SIMD intrinsics using
//! `RUSTFLAGS="-C target-feature=+avx2"` for example.  See the documentation
//! [here](https://doc.rust-lang.org/stable/core/arch/) for more information.

use std::sync::Arc;

use crate::array::{Array, ArrayData, ArrayRef, BooleanArray};
use crate::buffer::{
    buffer_bin_and, buffer_bin_or, buffer_unary_not, Buffer, MutableBuffer,
};
use crate::compute::util::combine_option_bitmap;
use crate::datatypes::DataType;
use crate::error::{ArrowError, Result};
use crate::util::bit_util::ceil;

/// Helper function to implement binary kernels
fn binary_boolean_kernel<F>(
    left: &BooleanArray,
    right: &BooleanArray,
    op: F,
) -> Result<BooleanArray>
where
    F: Fn(&Buffer, usize, &Buffer, usize, usize) -> Buffer,
{
    if left.len() != right.len() {
        return Err(ArrowError::ComputeError(
            "Cannot perform bitwise operation on arrays of different length".to_string(),
        ));
    }

    let len = left.len();

    let left_data = left.data_ref();
    let right_data = right.data_ref();
    let null_bit_buffer = combine_option_bitmap(&left_data, &right_data, len)?;

    let left_buffer = &left_data.buffers()[0];
    let right_buffer = &right_data.buffers()[0];
    let left_offset = left.offset();
    let right_offset = right.offset();

    let values = op(&left_buffer, left_offset, &right_buffer, right_offset, len);

    let data = ArrayData::new(
        DataType::Boolean,
        len,
        None,
        null_bit_buffer,
        0,
        vec![values],
        vec![],
    );
    Ok(BooleanArray::from(Arc::new(data)))
}

/// Performs `AND` operation on two arrays. If either left or right value is null then the
/// result is also null.
pub fn and(left: &BooleanArray, right: &BooleanArray) -> Result<BooleanArray> {
    binary_boolean_kernel(&left, &right, buffer_bin_and)
}

/// Performs `OR` operation on two arrays. If either left or right value is null then the
/// result is also null.
pub fn or(left: &BooleanArray, right: &BooleanArray) -> Result<BooleanArray> {
    binary_boolean_kernel(&left, &right, buffer_bin_or)
}

/// Performs unary `NOT` operation on an arrays. If value is null then the result is also
/// null.
pub fn not(left: &BooleanArray) -> Result<BooleanArray> {
    let left_offset = left.offset();
    let len = left.len();

    let data = left.data_ref();
    let null_bit_buffer = data
        .null_bitmap()
        .as_ref()
        .map(|b| b.bits.slice(left_offset));

    let values = buffer_unary_not(&data.buffers()[0], left_offset, len);

    let data = ArrayData::new(
        DataType::Boolean,
        len,
        None,
        null_bit_buffer,
        0,
        vec![values],
        vec![],
    );
    Ok(BooleanArray::from(Arc::new(data)))
}

pub fn is_null(input: &ArrayRef) -> Result<BooleanArray> {
    let len = input.len();

    let output = match input.data_ref().null_buffer() {
        None => {
            let len_bytes = ceil(len, 8);
            MutableBuffer::new(len_bytes)
                .with_bitset(len_bytes, false)
                .freeze()
        }
        Some(buffer) => buffer_unary_not(buffer, input.offset(), len),
    };

    let data =
        ArrayData::new(DataType::Boolean, len, None, None, 0, vec![output], vec![]);

    Ok(BooleanArray::from(Arc::new(data)))
}

pub fn is_not_null(input: &ArrayRef) -> Result<BooleanArray> {
    let len = input.len();

    let output = match input.data_ref().null_buffer() {
        None => {
            let len_bytes = ceil(len, 8);
            MutableBuffer::new(len_bytes)
                .with_bitset(len_bytes, true)
                .freeze()
        }
        Some(buffer) => buffer.bit_slice(input.offset(), len),
    };

    let data =
        ArrayData::new(DataType::Boolean, len, None, None, 0, vec![output], vec![]);

    Ok(BooleanArray::from(Arc::new(data)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::Int32Array;

    #[test]
    fn test_bool_array_and() {
        let a = BooleanArray::from(vec![false, false, true, true]);
        let b = BooleanArray::from(vec![false, true, false, true]);
        let c = and(&a, &b).unwrap();
        assert_eq!(false, c.value(0));
        assert_eq!(false, c.value(1));
        assert_eq!(false, c.value(2));
        assert_eq!(true, c.value(3));
    }

    #[test]
    fn test_bool_array_or() {
        let a = BooleanArray::from(vec![false, false, true, true]);
        let b = BooleanArray::from(vec![false, true, false, true]);
        let c = or(&a, &b).unwrap();
        assert_eq!(false, c.value(0));
        assert_eq!(true, c.value(1));
        assert_eq!(true, c.value(2));
        assert_eq!(true, c.value(3));
    }

    #[test]
    fn test_bool_array_or_nulls() {
        let a = BooleanArray::from(vec![None, Some(false), None, Some(false)]);
        let b = BooleanArray::from(vec![None, None, Some(false), Some(false)]);
        let c = or(&a, &b).unwrap();
        assert_eq!(true, c.is_null(0));
        assert_eq!(true, c.is_null(1));
        assert_eq!(true, c.is_null(2));
        assert_eq!(false, c.is_null(3));
    }

    #[test]
    fn test_bool_array_not() {
        let a = BooleanArray::from(vec![false, false, true, true]);
        let c = not(&a).unwrap();
        assert_eq!(true, c.value(0));
        assert_eq!(true, c.value(1));
        assert_eq!(false, c.value(2));
        assert_eq!(false, c.value(3));
    }

    #[test]
    fn test_bool_array_and_nulls() {
        let a = BooleanArray::from(vec![None, Some(false), None, Some(false)]);
        let b = BooleanArray::from(vec![None, None, Some(false), Some(false)]);
        let c = and(&a, &b).unwrap();
        assert_eq!(true, c.is_null(0));
        assert_eq!(true, c.is_null(1));
        assert_eq!(true, c.is_null(2));
        assert_eq!(false, c.is_null(3));
    }

    #[test]
    fn test_bool_array_and_sliced_same_offset() {
        let a = BooleanArray::from(vec![
            false, false, false, false, false, false, false, false, false, false, true,
            true,
        ]);
        let b = BooleanArray::from(vec![
            false, false, false, false, false, false, false, false, false, true, false,
            true,
        ]);

        let a = a.slice(8, 4);
        let a = a.as_any().downcast_ref::<BooleanArray>().unwrap();
        let b = b.slice(8, 4);
        let b = b.as_any().downcast_ref::<BooleanArray>().unwrap();

        let c = and(&a, &b).unwrap();
        assert_eq!(4, c.len());
        assert_eq!(false, c.value(0));
        assert_eq!(false, c.value(1));
        assert_eq!(false, c.value(2));
        assert_eq!(true, c.value(3));
    }

    #[test]
    fn test_bool_array_and_sliced_same_offset_mod8() {
        let a = BooleanArray::from(vec![
            false, false, true, true, false, false, false, false, false, false, false,
            false,
        ]);
        let b = BooleanArray::from(vec![
            false, false, false, false, false, false, false, false, false, true, false,
            true,
        ]);

        let a = a.slice(0, 4);
        let a = a.as_any().downcast_ref::<BooleanArray>().unwrap();
        let b = b.slice(8, 4);
        let b = b.as_any().downcast_ref::<BooleanArray>().unwrap();

        let c = and(&a, &b).unwrap();
        assert_eq!(4, c.len());
        assert_eq!(false, c.value(0));
        assert_eq!(false, c.value(1));
        assert_eq!(false, c.value(2));
        assert_eq!(true, c.value(3));
    }

    #[test]
    fn test_bool_array_and_sliced_offset1() {
        let a = BooleanArray::from(vec![
            false, false, false, false, false, false, false, false, false, false, true,
            true,
        ]);
        let b = BooleanArray::from(vec![false, true, false, true]);

        let a = a.slice(8, 4);
        let a = a.as_any().downcast_ref::<BooleanArray>().unwrap();

        let c = and(&a, &b).unwrap();
        assert_eq!(4, c.len());
        assert_eq!(false, c.value(0));
        assert_eq!(false, c.value(1));
        assert_eq!(false, c.value(2));
        assert_eq!(true, c.value(3));
    }

    #[test]
    fn test_bool_array_and_sliced_offset2() {
        let a = BooleanArray::from(vec![false, false, true, true]);
        let b = BooleanArray::from(vec![
            false, false, false, false, false, false, false, false, false, true, false,
            true,
        ]);

        let b = b.slice(8, 4);
        let b = b.as_any().downcast_ref::<BooleanArray>().unwrap();

        let c = and(&a, &b).unwrap();
        assert_eq!(4, c.len());
        assert_eq!(false, c.value(0));
        assert_eq!(false, c.value(1));
        assert_eq!(false, c.value(2));
        assert_eq!(true, c.value(3));
    }

    #[test]
    fn test_nonnull_array_is_null() {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4]));

        let res = is_null(&a).unwrap();

        assert_eq!(4, res.len());
        assert_eq!(0, res.null_count());
        assert_eq!(&None, res.data_ref().null_bitmap());
        assert_eq!(false, res.value(0));
        assert_eq!(false, res.value(1));
        assert_eq!(false, res.value(2));
        assert_eq!(false, res.value(3));
    }

    #[test]
    fn test_nonnull_array_with_offset_is_null() {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![
            1, 2, 3, 4, 5, 6, 7, 8, 7, 6, 5, 4, 3, 2, 1,
        ]));
        let a = a.slice(8, 4);

        let res = is_null(&a).unwrap();

        assert_eq!(4, res.len());
        assert_eq!(0, res.null_count());
        assert_eq!(&None, res.data_ref().null_bitmap());
        assert_eq!(false, res.value(0));
        assert_eq!(false, res.value(1));
        assert_eq!(false, res.value(2));
        assert_eq!(false, res.value(3));
    }

    #[test]
    fn test_nonnull_array_is_not_null() {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4]));

        let res = is_not_null(&a).unwrap();

        assert_eq!(4, res.len());
        assert_eq!(0, res.null_count());
        assert_eq!(&None, res.data_ref().null_bitmap());
        assert_eq!(true, res.value(0));
        assert_eq!(true, res.value(1));
        assert_eq!(true, res.value(2));
        assert_eq!(true, res.value(3));
    }

    #[test]
    fn test_nonnull_array_with_offset_is_not_null() {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![
            1, 2, 3, 4, 5, 6, 7, 8, 7, 6, 5, 4, 3, 2, 1,
        ]));
        let a = a.slice(8, 4);

        let res = is_not_null(&a).unwrap();

        assert_eq!(4, res.len());
        assert_eq!(0, res.null_count());
        assert_eq!(&None, res.data_ref().null_bitmap());
        assert_eq!(true, res.value(0));
        assert_eq!(true, res.value(1));
        assert_eq!(true, res.value(2));
        assert_eq!(true, res.value(3));
    }

    #[test]
    fn test_nullable_array_is_null() {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), None, Some(3), None]));

        let res = is_null(&a).unwrap();

        assert_eq!(4, res.len());
        assert_eq!(0, res.null_count());
        assert_eq!(&None, res.data_ref().null_bitmap());
        assert_eq!(false, res.value(0));
        assert_eq!(true, res.value(1));
        assert_eq!(false, res.value(2));
        assert_eq!(true, res.value(3));
    }

    #[test]
    fn test_nullable_array_with_offset_is_null() {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            // offset 8, previous None values are skipped by the slice
            Some(1),
            None,
            Some(2),
            None,
            Some(3),
            Some(4),
            None,
            None,
        ]));
        let a = a.slice(8, 4);

        let res = is_null(&a).unwrap();

        assert_eq!(4, res.len());
        assert_eq!(0, res.null_count());
        assert_eq!(&None, res.data_ref().null_bitmap());
        assert_eq!(false, res.value(0));
        assert_eq!(true, res.value(1));
        assert_eq!(false, res.value(2));
        assert_eq!(true, res.value(3));
    }

    #[test]
    fn test_nullable_array_is_not_null() {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), None, Some(3), None]));

        let res = is_not_null(&a).unwrap();

        assert_eq!(4, res.len());
        assert_eq!(0, res.null_count());
        assert_eq!(&None, res.data_ref().null_bitmap());
        assert_eq!(true, res.value(0));
        assert_eq!(false, res.value(1));
        assert_eq!(true, res.value(2));
        assert_eq!(false, res.value(3));
    }

    #[test]
    fn test_nullable_array_with_offset_is_not_null() {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            // offset 8, previous None values are skipped by the slice
            Some(1),
            None,
            Some(2),
            None,
            Some(3),
            Some(4),
            None,
            None,
        ]));
        let a = a.slice(8, 4);

        let res = is_not_null(&a).unwrap();

        assert_eq!(4, res.len());
        assert_eq!(0, res.null_count());
        assert_eq!(&None, res.data_ref().null_bitmap());
        assert_eq!(true, res.value(0));
        assert_eq!(false, res.value(1));
        assert_eq!(true, res.value(2));
        assert_eq!(false, res.value(3));
    }
}
