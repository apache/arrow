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
use crate::datatypes::*;
use crate::error::{ArrowError, Result};
use num::{One, ToPrimitive, Zero};
#[cfg(feature = "simd")]
use std::cmp::min;
use std::ops::Add;

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

/// Takes/filters a list array's inner data using the offsets of the list array.
///
/// Where a list array has indices `[0,2,5,10]`, taking indices of `[2,0]` returns
/// an array of the indices `[5..10, 0..2]` and offsets `[0,5,7]` (5 elements and 2
/// elements)
pub(super) fn take_value_indices_from_list<IndexType, OffsetType>(
    values: &ArrayRef,
    indices: &PrimitiveArray<IndexType>,
) -> Result<(PrimitiveArray<OffsetType>, Vec<OffsetType::Native>)>
where
    IndexType: ArrowNumericType,
    IndexType::Native: ToPrimitive,
    OffsetType: ArrowNumericType,
    OffsetType::Native: OffsetSizeTrait + Add + Zero + One,
    PrimitiveArray<OffsetType>: From<Vec<Option<OffsetType::Native>>>,
{
    // TODO: benchmark this function, there might be a faster unsafe alternative
    // get list array's offsets
    let list = values
        .as_any()
        .downcast_ref::<GenericListArray<OffsetType::Native>>()
        .unwrap();
    let offsets: Vec<OffsetType::Native> =
        (0..=list.len()).map(|i| list.value_offset(i)).collect();

    let mut new_offsets = Vec::with_capacity(indices.len());
    let mut values = Vec::new();
    let mut current_offset = OffsetType::Native::zero();
    // add first offset
    new_offsets.push(OffsetType::Native::zero());
    // compute the value indices, and set offsets accordingly
    for i in 0..indices.len() {
        if indices.is_valid(i) {
            let ix = ToPrimitive::to_usize(&indices.value(i)).ok_or_else(|| {
                ArrowError::ComputeError("Cast to usize failed".to_string())
            })?;
            let start = offsets[ix];
            let end = offsets[ix + 1];
            current_offset = current_offset + (end - start);
            new_offsets.push(current_offset);

            let mut curr = start;

            // if start == end, this slot is empty
            while curr < end {
                values.push(Some(curr));
                curr = curr + OffsetType::Native::one();
            }
        } else {
            new_offsets.push(current_offset);
        }
    }

    Ok((PrimitiveArray::<OffsetType>::from(values), new_offsets))
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

    use crate::array::ArrayData;
    use crate::datatypes::{DataType, ToByteSlice};

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
    fn test_take_value_index_from_list() {
        let list = build_list(
            DataType::List(Box::new(Field::new("item", DataType::Int32, true))),
            Int32Array::from(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
            vec![0i32, 2i32, 5i32, 10i32],
        );
        let indices = UInt32Array::from(vec![2, 0]);

        let (indexed, offsets) =
            take_value_indices_from_list::<_, Int32Type>(&list, &indices).unwrap();

        assert_eq!(indexed, Int32Array::from(vec![5, 6, 7, 8, 9, 0, 1]));
        assert_eq!(offsets, vec![0, 5, 7]);
    }

    #[test]
    fn test_take_value_index_from_large_list() {
        let list = build_list(
            DataType::LargeList(Box::new(Field::new("item", DataType::Int32, false))),
            Int32Array::from(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
            vec![0i64, 2i64, 5i64, 10i64],
        );
        let indices = UInt32Array::from(vec![2, 0]);

        let (indexed, offsets) =
            take_value_indices_from_list::<_, Int64Type>(&list, &indices).unwrap();

        assert_eq!(indexed, Int64Array::from(vec![5, 6, 7, 8, 9, 0, 1]));
        assert_eq!(offsets, vec![0, 5, 7]);
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
