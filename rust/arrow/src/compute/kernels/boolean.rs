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

use std::ops::Not;
use std::sync::Arc;

use crate::array::{Array, ArrayData, BooleanArray, PrimitiveArray};
use crate::buffer::{
    buffer_bin_and, buffer_bin_or, buffer_unary_not, Buffer, MutableBuffer,
};
use crate::compute::util::combine_option_bitmap;
use crate::datatypes::{ArrowNumericType, DataType};
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
/// # Error
/// This function errors when the arrays have different lengths.
/// # Example
/// ```rust
/// use arrow::array::BooleanArray;
/// use arrow::error::Result;
/// use arrow::compute::kernels::boolean::and;
/// # fn main() -> Result<()> {
/// let a = BooleanArray::from(vec![Some(false), Some(true), None]);
/// let b = BooleanArray::from(vec![Some(true), Some(true), Some(false)]);
/// let and_ab = and(&a, &b)?;
/// assert_eq!(and_ab, BooleanArray::from(vec![Some(false), Some(true), None]));
/// # Ok(())
/// # }
/// ```
pub fn and(left: &BooleanArray, right: &BooleanArray) -> Result<BooleanArray> {
    binary_boolean_kernel(&left, &right, buffer_bin_and)
}

/// Performs `OR` operation on two arrays. If either left or right value is null then the
/// result is also null.
/// # Error
/// This function errors when the arrays have different lengths.
/// # Example
/// ```rust
/// use arrow::array::BooleanArray;
/// use arrow::error::Result;
/// use arrow::compute::kernels::boolean::or;
/// # fn main() -> Result<()> {
/// let a = BooleanArray::from(vec![Some(false), Some(true), None]);
/// let b = BooleanArray::from(vec![Some(true), Some(true), Some(false)]);
/// let or_ab = or(&a, &b)?;
/// assert_eq!(or_ab, BooleanArray::from(vec![Some(true), Some(true), None]));
/// # Ok(())
/// # }
/// ```
pub fn or(left: &BooleanArray, right: &BooleanArray) -> Result<BooleanArray> {
    binary_boolean_kernel(&left, &right, buffer_bin_or)
}

/// Performs unary `NOT` operation on an arrays. If value is null then the result is also
/// null.
/// # Error
/// This function never errors. It returns an error for consistency.
/// # Example
/// ```rust
/// use arrow::array::BooleanArray;
/// use arrow::error::Result;
/// use arrow::compute::kernels::boolean::not;
/// # fn main() -> Result<()> {
/// let a = BooleanArray::from(vec![Some(false), Some(true), None]);
/// let not_a = not(&a)?;
/// assert_eq!(not_a, BooleanArray::from(vec![Some(true), Some(false), None]));
/// # Ok(())
/// # }
/// ```
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

/// Returns a non-null [BooleanArray] with whether each value of the array is null.
/// # Error
/// This function never errors.
/// # Example
/// ```rust
/// # use arrow::error::Result;
/// use arrow::array::BooleanArray;
/// use arrow::compute::kernels::boolean::is_null;
/// # fn main() -> Result<()> {
/// let a = BooleanArray::from(vec![Some(false), Some(true), None]);
/// let a_is_null = is_null(&a)?;
/// assert_eq!(a_is_null, BooleanArray::from(vec![false, false, true]));
/// # Ok(())
/// # }
/// ```
pub fn is_null(input: &Array) -> Result<BooleanArray> {
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

/// Returns a non-null [BooleanArray] with whether each value of the array is not null.
/// # Error
/// This function never errors.
/// # Example
/// ```rust
/// # use arrow::error::Result;
/// use arrow::array::BooleanArray;
/// use arrow::compute::kernels::boolean::is_not_null;
/// # fn main() -> Result<()> {
/// let a = BooleanArray::from(vec![Some(false), Some(true), None]);
/// let a_is_not_null = is_not_null(&a)?;
/// assert_eq!(a_is_not_null, BooleanArray::from(vec![true, true, false]));
/// # Ok(())
/// # }
/// ```
pub fn is_not_null(input: &Array) -> Result<BooleanArray> {
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

/// Copies original array, setting null bit to true if a secondary comparison boolean array is set to true.
/// Typically used to implement NULLIF.
// NOTE: For now this only supports Primitive Arrays.  Although the code could be made generic, the issue
// is that currently the bitmap operations result in a final bitmap which is aligned to bit 0, and thus
// the left array's data needs to be sliced to a new offset, and for non-primitive arrays shifting the
// data might be too complicated.   In the future, to avoid shifting left array's data, we could instead
// shift the final bitbuffer to the right, prepending with 0's instead.
pub fn nullif<T>(
    left: &PrimitiveArray<T>,
    right: &BooleanArray,
) -> Result<PrimitiveArray<T>>
where
    T: ArrowNumericType,
{
    if left.len() != right.len() {
        return Err(ArrowError::ComputeError(
            "Cannot perform comparison operation on arrays of different length"
                .to_string(),
        ));
    }
    let left_data = left.data();
    let right_data = right.data();

    // If left has no bitmap, create a new one with all values set for nullity op later
    // left=0 (null)   right=null       output bitmap=null
    // left=0          right=1          output bitmap=null
    // left=1 (set)    right=null       output bitmap=set   (passthrough)
    // left=1          right=1 & comp=true    output bitmap=null
    // left=1          right=1 & comp=false   output bitmap=set
    //
    // Thus: result = left null bitmap & (!right_values | !right_bitmap)
    //              OR left null bitmap & !(right_values & right_bitmap)
    //
    // Do the right expression !(right_values & right_bitmap) first since there are two steps
    // TRICK: convert BooleanArray buffer as a bitmap for faster operation
    let right_combo_buffer = match right.data().null_bitmap() {
        Some(right_bitmap) => {
            // NOTE: right values and bitmaps are combined and stay at bit offset right.offset()
            (right.values() & &right_bitmap.bits).ok().map(|b| b.not())
        }
        None => Some(!right.values()),
    };

    // AND of original left null bitmap with right expression
    // Here we take care of the possible offsets of the left and right arrays all at once.
    let modified_null_buffer = match left_data.null_bitmap() {
        Some(left_null_bitmap) => match right_combo_buffer {
            Some(rcb) => Some(buffer_bin_and(
                &left_null_bitmap.bits,
                left_data.offset(),
                &rcb,
                right_data.offset(),
                left_data.len(),
            )),
            None => Some(
                left_null_bitmap
                    .bits
                    .bit_slice(left_data.offset(), left.len()),
            ),
        },
        None => right_combo_buffer
            .map(|rcb| rcb.bit_slice(right_data.offset(), right_data.len())),
    };

    // Align/shift left data on offset as needed, since new bitmaps are shifted and aligned to 0 already
    // NOTE: this probably only works for primitive arrays.
    let data_buffers = if left.offset() == 0 {
        left_data.buffers().to_vec()
    } else {
        // Shift each data buffer by type's bit_width * offset.
        left_data
            .buffers()
            .iter()
            .map(|buf| buf.slice(left.offset() * T::get_byte_width()))
            .collect::<Vec<_>>()
    };

    // Construct new array with same values but modified null bitmap
    // TODO: shift data buffer as needed
    let data = ArrayData::new(
        T::DATA_TYPE,
        left.len(),
        None, // force new to compute the number of null bits
        modified_null_buffer,
        0, // No need for offset since left data has been shifted
        data_buffers,
        left_data.child_data().to_vec(),
    );
    Ok(PrimitiveArray::<T>::from(Arc::new(data)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::{ArrayRef, Int32Array};

    #[test]
    fn test_bool_array_and() {
        let a = BooleanArray::from(vec![false, false, true, true]);
        let b = BooleanArray::from(vec![false, true, false, true]);
        let c = and(&a, &b).unwrap();

        let expected = BooleanArray::from(vec![false, false, false, true]);

        assert_eq!(c, expected);
    }

    #[test]
    fn test_bool_array_or() {
        let a = BooleanArray::from(vec![false, false, true, true]);
        let b = BooleanArray::from(vec![false, true, false, true]);
        let c = or(&a, &b).unwrap();

        let expected = BooleanArray::from(vec![false, true, true, true]);

        assert_eq!(c, expected);
    }

    #[test]
    fn test_bool_array_or_nulls() {
        let a = BooleanArray::from(vec![
            None,
            None,
            None,
            Some(false),
            Some(false),
            Some(false),
            Some(true),
            Some(true),
            Some(true),
        ]);
        let b = BooleanArray::from(vec![
            None,
            Some(false),
            Some(true),
            None,
            Some(false),
            Some(true),
            None,
            Some(false),
            Some(true),
        ]);
        let c = or(&a, &b).unwrap();

        let expected = BooleanArray::from(vec![
            None,
            None,
            None,
            None,
            Some(false),
            Some(true),
            None,
            Some(true),
            Some(true),
        ]);

        assert_eq!(c, expected);
    }

    #[test]
    fn test_bool_array_not() {
        let a = BooleanArray::from(vec![false, true]);
        let c = not(&a).unwrap();

        let expected = BooleanArray::from(vec![true, false]);

        assert_eq!(c, expected);
    }

    #[test]
    fn test_bool_array_and_nulls() {
        let a = BooleanArray::from(vec![
            None,
            None,
            None,
            Some(false),
            Some(false),
            Some(false),
            Some(true),
            Some(true),
            Some(true),
        ]);
        let b = BooleanArray::from(vec![
            None,
            Some(false),
            Some(true),
            None,
            Some(false),
            Some(true),
            None,
            Some(false),
            Some(true),
        ]);
        let c = and(&a, &b).unwrap();

        let expected = BooleanArray::from(vec![
            None,
            None,
            None,
            None,
            Some(false),
            Some(false),
            None,
            Some(false),
            Some(true),
        ]);

        assert_eq!(c, expected);
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

        let expected = BooleanArray::from(vec![false, false, false, true]);

        assert_eq!(expected, c);
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

        let expected = BooleanArray::from(vec![false, false, false, true]);

        assert_eq!(expected, c);
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

        let expected = BooleanArray::from(vec![false, false, false, true]);

        assert_eq!(expected, c);
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

        let expected = BooleanArray::from(vec![false, false, false, true]);

        assert_eq!(expected, c);
    }

    #[test]
    fn test_bool_array_and_nulls_offset() {
        let a = BooleanArray::from(vec![None, Some(false), Some(true), None, Some(true)]);
        let a = a.slice(1, 4);
        let a = a.as_any().downcast_ref::<BooleanArray>().unwrap();

        let b = BooleanArray::from(vec![
            None,
            None,
            Some(true),
            Some(false),
            Some(true),
            Some(true),
        ]);

        let b = b.slice(2, 4);
        let b = b.as_any().downcast_ref::<BooleanArray>().unwrap();

        let c = and(&a, &b).unwrap();

        let expected =
            BooleanArray::from(vec![Some(false), Some(false), None, Some(true)]);

        assert_eq!(expected, c);
    }

    #[test]
    fn test_nonnull_array_is_null() {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4]));

        let res = is_null(a.as_ref()).unwrap();

        let expected = BooleanArray::from(vec![false, false, false, false]);

        assert_eq!(expected, res);
        assert_eq!(&None, res.data_ref().null_bitmap());
    }

    #[test]
    fn test_nonnull_array_with_offset_is_null() {
        let a = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 7, 6, 5, 4, 3, 2, 1]);
        let a = a.slice(8, 4);

        let res = is_null(a.as_ref()).unwrap();

        let expected = BooleanArray::from(vec![false, false, false, false]);

        assert_eq!(expected, res);
        assert_eq!(&None, res.data_ref().null_bitmap());
    }

    #[test]
    fn test_nonnull_array_is_not_null() {
        let a = Int32Array::from(vec![1, 2, 3, 4]);

        let res = is_not_null(&a).unwrap();

        let expected = BooleanArray::from(vec![true, true, true, true]);

        assert_eq!(expected, res);
        assert_eq!(&None, res.data_ref().null_bitmap());
    }

    #[test]
    fn test_nonnull_array_with_offset_is_not_null() {
        let a = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 7, 6, 5, 4, 3, 2, 1]);
        let a = a.slice(8, 4);

        let res = is_not_null(a.as_ref()).unwrap();

        let expected = BooleanArray::from(vec![true, true, true, true]);

        assert_eq!(expected, res);
        assert_eq!(&None, res.data_ref().null_bitmap());
    }

    #[test]
    fn test_nullable_array_is_null() {
        let a = Int32Array::from(vec![Some(1), None, Some(3), None]);

        let res = is_null(&a).unwrap();

        let expected = BooleanArray::from(vec![false, true, false, true]);

        assert_eq!(expected, res);
        assert_eq!(&None, res.data_ref().null_bitmap());
    }

    #[test]
    fn test_nullable_array_with_offset_is_null() {
        let a = Int32Array::from(vec![
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
        ]);
        let a = a.slice(8, 4);

        let res = is_null(a.as_ref()).unwrap();

        let expected = BooleanArray::from(vec![false, true, false, true]);

        assert_eq!(expected, res);
        assert_eq!(&None, res.data_ref().null_bitmap());
    }

    #[test]
    fn test_nullable_array_is_not_null() {
        let a = Int32Array::from(vec![Some(1), None, Some(3), None]);

        let res = is_not_null(&a).unwrap();

        let expected = BooleanArray::from(vec![true, false, true, false]);

        assert_eq!(expected, res);
        assert_eq!(&None, res.data_ref().null_bitmap());
    }

    #[test]
    fn test_nullable_array_with_offset_is_not_null() {
        let a = Int32Array::from(vec![
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
        ]);
        let a = a.slice(8, 4);

        let res = is_not_null(a.as_ref()).unwrap();

        let expected = BooleanArray::from(vec![true, false, true, false]);

        assert_eq!(expected, res);
        assert_eq!(&None, res.data_ref().null_bitmap());
    }

    #[test]
    fn test_nullif_int_array() {
        let a = Int32Array::from(vec![Some(15), None, Some(8), Some(1), Some(9)]);
        let comp =
            BooleanArray::from(vec![Some(false), None, Some(true), Some(false), None]);
        let res = nullif(&a, &comp).unwrap();

        let expected = Int32Array::from(vec![
            Some(15),
            None,
            None, // comp true, slot 2 turned into null
            Some(1),
            // Even though comp array / right is null, should still pass through original value
            // comp true, slot 2 turned into null
            Some(9),
        ]);

        assert_eq!(expected, res);
    }

    #[test]
    fn test_nullif_int_array_offset() {
        let a = Int32Array::from(vec![None, Some(15), Some(8), Some(1), Some(9)]);
        let a = a.slice(1, 3); // Some(15), Some(8), Some(1)
        let a = a.as_any().downcast_ref::<Int32Array>().unwrap();
        let comp = BooleanArray::from(vec![
            Some(false),
            Some(false),
            Some(false),
            None,
            Some(true),
            Some(false),
            None,
        ]);
        let comp = comp.slice(2, 3); // Some(false), None, Some(true)
        let comp = comp.as_any().downcast_ref::<BooleanArray>().unwrap();
        let res = nullif(&a, &comp).unwrap();

        let expected = Int32Array::from(vec![
            Some(15), // False => keep it
            Some(8),  // None => keep it
            None,     // true => None
        ]);
        assert_eq!(&expected, &res)
    }
}
