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

//! Common utilities for computation kernels.

use crate::array::*;
#[cfg(feature = "simd")]
use crate::bitmap::Bitmap;
use crate::buffer::{buffer_bin_and, buffer_bin_or, Buffer};
use crate::error::Result;
#[cfg(feature = "simd")]
use std::cmp::min;

/// Combines the null bitmaps of two arrays using a bitwise `and` operation.
///
/// This function is useful when implementing operations on higher level arrays.
pub(super) fn combine_option_bitmap(
    left_data: &ArrayDataRef,
    right_data: &ArrayDataRef,
    len_in_bits: usize,
) -> Result<Option<Buffer>> {
    let left_offset_in_bits = left_data.offset();
    let right_offset_in_bits = right_data.offset();

    let left = left_data.null_buffer();
    let right = right_data.null_buffer();

    match left {
        None => match right {
            None => Ok(None),
            Some(r) => Ok(Some(r.bit_slice(right_offset_in_bits, len_in_bits))),
        },
        Some(l) => match right {
            None => Ok(Some(l.bit_slice(left_offset_in_bits, len_in_bits))),

            Some(r) => Ok(Some(buffer_bin_and(
                &l,
                left_offset_in_bits,
                &r,
                right_offset_in_bits,
                len_in_bits,
            ))),
        },
    }
}

/// Compares the null bitmaps of two arrays using a bitwise `or` operation.
///
/// This function is useful when implementing operations on higher level arrays.
pub(super) fn compare_option_bitmap(
    left_data: &ArrayDataRef,
    right_data: &ArrayDataRef,
    len_in_bits: usize,
) -> Result<Option<Buffer>> {
    let left_offset_in_bits = left_data.offset();
    let right_offset_in_bits = right_data.offset();

    let left = left_data.null_buffer();
    let right = right_data.null_buffer();

    match left {
        None => match right {
            None => Ok(None),
            Some(r) => Ok(Some(r.bit_slice(right_offset_in_bits, len_in_bits))),
        },
        Some(l) => match right {
            None => Ok(Some(l.bit_slice(left_offset_in_bits, len_in_bits))),

            Some(r) => Ok(Some(buffer_bin_or(
                &l,
                left_offset_in_bits,
                &r,
                right_offset_in_bits,
                len_in_bits,
            ))),
        },
    }
}

/// Creates a new SIMD mask, i.e. `packed_simd::m32x16` or similar. that indicates if the
/// corresponding array slots represented by the mask are 'valid'.  
///
/// Lanes of the SIMD mask can be set to 'valid' (`true`) if the corresponding array slot is not
/// `NULL`, as indicated by it's `Bitmap`, and is within the length of the array.  Lanes outside the
/// length represent padding and are set to 'invalid' (`false`).
#[cfg(all(any(target_arch = "x86", target_arch = "x86_64"), feature = "simd"))]
unsafe fn is_valid<T>(
    bitmap: &Option<Bitmap>,
    i: usize,
    simd_width: usize,
    array_len: usize,
) -> T::SimdMask
where
    T: ArrowNumericType,
{
    let simd_upper_bound = i + simd_width;
    let mut validity = T::mask_init(true);

    // Validity based on `Bitmap`
    if let Some(b) = bitmap {
        for j in i..min(array_len, simd_upper_bound) {
            if !b.is_set(j) {
                validity = T::mask_set(validity, j - i, false);
            }
        }
    }

    // Validity based on the length of the Array
    for j in array_len..simd_upper_bound {
        validity = T::mask_set(validity, j - i, false);
    }

    validity
}

/// Performs a SIMD load but sets all 'invalid' lanes to a constant value.
///
/// 'invalid' lanes are lanes where the corresponding array slots are either `NULL` or between the
/// length and capacity of the array, i.e. in the padded region.
///
/// Note that `array` below has it's own `Bitmap` separate from the `bitmap` argument.  This
/// function is used to prepare `array`'s for binary operations.  The `bitmap` argument is the
/// `Bitmap` after the binary operation.
#[cfg(all(any(target_arch = "x86", target_arch = "x86_64"), feature = "simd"))]
pub(super) unsafe fn simd_load_set_invalid<T>(
    array: &PrimitiveArray<T>,
    bitmap: &Option<Bitmap>,
    i: usize,
    simd_width: usize,
    fill_value: T::Native,
) -> T::Simd
where
    T: ArrowNumericType,
    T::Native: One,
{
    let simd_with_zeros = T::load(array.value_slice(i, simd_width));
    T::mask_select(
        is_valid::<T>(bitmap, i, simd_width, array.len()),
        simd_with_zeros,
        T::init(fill_value),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use crate::datatypes::{DataType, ToByteSlice};
    use crate::{array::ArrayData, datatypes::ArrowPrimitiveType};

    fn make_data_with_null_bit_buffer(
        len: usize,
        offset: usize,
        null_bit_buffer: Option<Buffer>,
    ) -> Arc<ArrayData> {
        // empty vec for buffers and children is not really correct, but for these tests we only care about the null bitmap
        Arc::new(ArrayData::new(
            DataType::UInt8,
            len,
            None,
            null_bit_buffer,
            offset,
            vec![],
            vec![],
        ))
    }

    #[test]
    fn test_combine_option_bitmap() {
        let none_bitmap = make_data_with_null_bit_buffer(8, 0, None);
        let some_bitmap =
            make_data_with_null_bit_buffer(8, 0, Some(Buffer::from([0b01001010])));
        let inverse_bitmap =
            make_data_with_null_bit_buffer(8, 0, Some(Buffer::from([0b10110101])));
        assert_eq!(
            None,
            combine_option_bitmap(&none_bitmap, &none_bitmap, 8).unwrap()
        );
        assert_eq!(
            Some(Buffer::from([0b01001010])),
            combine_option_bitmap(&some_bitmap, &none_bitmap, 8).unwrap()
        );
        assert_eq!(
            Some(Buffer::from([0b01001010])),
            combine_option_bitmap(&none_bitmap, &some_bitmap, 8,).unwrap()
        );
        assert_eq!(
            Some(Buffer::from([0b01001010])),
            combine_option_bitmap(&some_bitmap, &some_bitmap, 8,).unwrap()
        );
        assert_eq!(
            Some(Buffer::from([0b0])),
            combine_option_bitmap(&some_bitmap, &inverse_bitmap, 8,).unwrap()
        );
    }

    #[test]
    fn test_compare_option_bitmap() {
        let none_bitmap = make_data_with_null_bit_buffer(8, 0, None);
        let some_bitmap =
            make_data_with_null_bit_buffer(8, 0, Some(Buffer::from([0b01001010])));
        let inverse_bitmap =
            make_data_with_null_bit_buffer(8, 0, Some(Buffer::from([0b10110101])));
        assert_eq!(
            None,
            compare_option_bitmap(&none_bitmap, &none_bitmap, 8).unwrap()
        );
        assert_eq!(
            Some(Buffer::from([0b01001010])),
            compare_option_bitmap(&some_bitmap, &none_bitmap, 8).unwrap()
        );
        assert_eq!(
            Some(Buffer::from([0b01001010])),
            compare_option_bitmap(&none_bitmap, &some_bitmap, 8,).unwrap()
        );
        assert_eq!(
            Some(Buffer::from([0b01001010])),
            compare_option_bitmap(&some_bitmap, &some_bitmap, 8,).unwrap()
        );
        assert_eq!(
            Some(Buffer::from([0b11111111])),
            compare_option_bitmap(&some_bitmap, &inverse_bitmap, 8,).unwrap()
        );
    }

    fn build_list<P, S>(
        list_data_type: DataType,
        values: PrimitiveArray<P>,
        offsets: Vec<S>,
    ) -> ArrayRef
    where
        P: ArrowPrimitiveType,
        S: OffsetSizeTrait,
    {
        let value_data = values.data();
        let value_offsets = Buffer::from(&offsets[..].to_byte_slice());
        let list_data = ArrayData::builder(list_data_type)
            .len(offsets.len() - 1)
            .add_buffer(value_offsets)
            .add_child_data(value_data)
            .build();
        let array = Arc::new(GenericListArray::<S>::from(list_data)) as ArrayRef;
        array
    }

    #[test]
    #[cfg(all(any(target_arch = "x86", target_arch = "x86_64"), feature = "simd"))]
    fn test_is_valid() {
        let a = Int32Array::from(vec![
            Some(15),
            None,
            None,
            Some(1),
            None,
            None,
            Some(5),
            None,
            None,
            Some(4),
        ]);
        let simd_lanes = 16;
        let data = a.data();
        let bitmap = data.null_bitmap();
        let result = unsafe { is_valid::<Int32Type>(&bitmap, 0, simd_lanes, a.len()) };
        for i in 0..simd_lanes {
            if i % 3 != 0 || i > 9 {
                assert_eq!(false, result.extract(i));
            } else {
                assert_eq!(true, result.extract(i));
            }
        }
    }

    #[test]
    #[cfg(all(any(target_arch = "x86", target_arch = "x86_64"), feature = "simd"))]
    fn test_simd_load_set_invalid() {
        let a = Int64Array::from(vec![None, Some(15), Some(5), Some(0)]);
        let new_bitmap = &Some(Bitmap::from(Buffer::from([0b00001010])));
        let simd_lanes = 8;
        let result = unsafe {
            simd_load_set_invalid::<Int64Type>(&a, &new_bitmap, 0, simd_lanes, 1)
        };
        for i in 0..simd_lanes {
            if i == 1 {
                assert_eq!(15_i64, result.extract(i));
            } else if i == 3 {
                assert_eq!(0_i64, result.extract(i));
            } else {
                assert_eq!(1_i64, result.extract(i));
            }
        }
    }
}
