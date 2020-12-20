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
use crate::buffer::{buffer_bin_and, buffer_bin_or, Buffer};
use crate::datatypes::*;
use crate::error::{ArrowError, Result};
use num::{One, ToPrimitive, Zero};
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
    list: &GenericListArray<OffsetType::Native>,
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

/// Takes/filters a fixed size list array's inner data using the offsets of the list array.
pub(super) fn take_value_indices_from_fixed_size_list<IndexType>(
    list: &FixedSizeListArray,
    indices: &PrimitiveArray<IndexType>,
    length: <UInt32Type as ArrowPrimitiveType>::Native,
) -> Result<PrimitiveArray<UInt32Type>>
where
    IndexType: ArrowNumericType,
    IndexType::Native: ToPrimitive,
{
    let mut values = vec![];

    for i in 0..indices.len() {
        if indices.is_valid(i) {
            let index = ToPrimitive::to_usize(&indices.value(i)).ok_or_else(|| {
                ArrowError::ComputeError("Cast to usize failed".to_string())
            })?;
            let start =
                list.value_offset(index) as <UInt32Type as ArrowPrimitiveType>::Native;

            values.extend(start..start + length);
        }
    }

    Ok(PrimitiveArray::<UInt32Type>::from(values))
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;

    use std::sync::Arc;

    use crate::datatypes::{DataType, ToByteSlice};
    use crate::util::bit_util;
    use crate::{array::ArrayData, buffer::MutableBuffer};

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

    pub(crate) fn build_generic_list<S, T>(
        data: Vec<Option<Vec<T::Native>>>,
    ) -> GenericListArray<S>
    where
        S: OffsetSizeTrait + 'static,
        T: ArrowPrimitiveType,
        PrimitiveArray<T>: From<Vec<Option<T::Native>>>,
    {
        let data = data
            .into_iter()
            .map(|subarray| {
                subarray.map(|item| {
                    item.into_iter()
                        .map(Some)
                        .collect::<Vec<Option<T::Native>>>()
                })
            })
            .collect();
        build_generic_list_nullable(data)
    }

    pub(crate) fn build_generic_list_nullable<S, T>(
        data: Vec<Option<Vec<Option<T::Native>>>>,
    ) -> GenericListArray<S>
    where
        S: OffsetSizeTrait + 'static,
        T: ArrowPrimitiveType,
        PrimitiveArray<T>: From<Vec<Option<T::Native>>>,
    {
        use std::any::TypeId;

        let mut offset = vec![0];
        let mut values = vec![];

        let list_len = data.len();
        let num_bytes = bit_util::ceil(list_len, 8);
        let mut list_null_count = 0;
        let mut list_bitmap = MutableBuffer::new(num_bytes).with_bitset(num_bytes, true);
        for (idx, array) in data.into_iter().enumerate() {
            if let Some(mut array) = array {
                values.append(&mut array);
            } else {
                list_null_count += 1;
                bit_util::unset_bit(&mut list_bitmap.data_mut(), idx);
            }
            offset.push(values.len() as i64);
        }

        let value_data = PrimitiveArray::<T>::from(values).data();
        let (list_data_type, value_offsets) = if TypeId::of::<S>() == TypeId::of::<i32>()
        {
            (
                DataType::List(Box::new(Field::new(
                    "item",
                    T::DATA_TYPE,
                    list_null_count == 0,
                ))),
                Buffer::from(
                    offset
                        .into_iter()
                        .map(|x| x as i32)
                        .collect::<Vec<i32>>()
                        .as_slice()
                        .to_byte_slice(),
                ),
            )
        } else if TypeId::of::<S>() == TypeId::of::<i64>() {
            (
                DataType::LargeList(Box::new(Field::new(
                    "item",
                    T::DATA_TYPE,
                    list_null_count == 0,
                ))),
                Buffer::from(offset.as_slice().to_byte_slice()),
            )
        } else {
            unreachable!()
        };

        let list_data = ArrayData::builder(list_data_type)
            .len(list_len)
            .null_count(list_null_count)
            .null_bit_buffer(list_bitmap.freeze())
            .add_buffer(value_offsets)
            .add_child_data(value_data)
            .build();

        GenericListArray::<S>::from(list_data)
    }

    pub(crate) fn build_fixed_size_list<T>(
        data: Vec<Option<Vec<T::Native>>>,
        length: <Int32Type as ArrowPrimitiveType>::Native,
    ) -> FixedSizeListArray
    where
        T: ArrowPrimitiveType,
        PrimitiveArray<T>: From<Vec<Option<T::Native>>>,
    {
        let data = data
            .into_iter()
            .map(|subarray| {
                subarray.map(|item| {
                    item.into_iter()
                        .map(Some)
                        .collect::<Vec<Option<T::Native>>>()
                })
            })
            .collect();
        build_fixed_size_list_nullable(data, length)
    }

    pub(crate) fn build_fixed_size_list_nullable<T>(
        list_values: Vec<Option<Vec<Option<T::Native>>>>,
        length: <Int32Type as ArrowPrimitiveType>::Native,
    ) -> FixedSizeListArray
    where
        T: ArrowPrimitiveType,
        PrimitiveArray<T>: From<Vec<Option<T::Native>>>,
    {
        let mut values = vec![];
        let mut list_null_count = 0;
        let list_len = list_values.len();

        let num_bytes = bit_util::ceil(list_len, 8);
        let mut list_bitmap = MutableBuffer::new(num_bytes).with_bitset(num_bytes, true);
        for (idx, list_element) in list_values.into_iter().enumerate() {
            if let Some(items) = list_element {
                // every sub-array should have the same length
                debug_assert_eq!(length as usize, items.len());

                values.extend(items.into_iter());
            } else {
                list_null_count += 1;
                bit_util::unset_bit(&mut list_bitmap.data_mut(), idx);
                values.extend(vec![None; length as usize].into_iter());
            }
        }

        let list_data_type = DataType::FixedSizeList(
            Box::new(Field::new("item", T::DATA_TYPE, list_null_count == 0)),
            length,
        );

        let child_data = PrimitiveArray::<T>::from(values).data();

        let list_data = ArrayData::builder(list_data_type)
            .len(list_len)
            .null_count(list_null_count)
            .null_bit_buffer(list_bitmap.freeze())
            .add_child_data(child_data)
            .build();

        FixedSizeListArray::from(list_data)
    }

    #[test]
    fn test_take_value_index_from_list() {
        let list = build_generic_list::<i32, Int32Type>(vec![
            Some(vec![0, 1]),
            Some(vec![2, 3, 4]),
            Some(vec![5, 6, 7, 8, 9]),
        ]);
        let indices = UInt32Array::from(vec![2, 0]);

        let (indexed, offsets) = take_value_indices_from_list(&list, &indices).unwrap();

        assert_eq!(indexed, Int32Array::from(vec![5, 6, 7, 8, 9, 0, 1]));
        assert_eq!(offsets, vec![0, 5, 7]);
    }

    #[test]
    fn test_take_value_index_from_large_list() {
        let list = build_generic_list::<i64, Int32Type>(vec![
            Some(vec![0, 1]),
            Some(vec![2, 3, 4]),
            Some(vec![5, 6, 7, 8, 9]),
        ]);
        let indices = UInt32Array::from(vec![2, 0]);

        let (indexed, offsets) =
            take_value_indices_from_list::<_, Int64Type>(&list, &indices).unwrap();

        assert_eq!(indexed, Int64Array::from(vec![5, 6, 7, 8, 9, 0, 1]));
        assert_eq!(offsets, vec![0, 5, 7]);
    }

    #[test]
    fn test_take_value_index_from_fixed_list() {
        let list = build_fixed_size_list_nullable::<Int32Type>(
            vec![
                Some(vec![Some(1), Some(2), None]),
                Some(vec![Some(4), None, Some(6)]),
                None,
                Some(vec![None, Some(8), Some(9)]),
            ],
            3,
        );

        let indices = UInt32Array::from(vec![2, 1, 0]);
        let indexed =
            take_value_indices_from_fixed_size_list(&list, &indices, 3).unwrap();

        assert_eq!(indexed, UInt32Array::from(vec![6, 7, 8, 3, 4, 5, 0, 1, 2]));

        let indices = UInt32Array::from(vec![3, 2, 1, 2, 0]);
        let indexed =
            take_value_indices_from_fixed_size_list(&list, &indices, 3).unwrap();

        assert_eq!(
            indexed,
            UInt32Array::from(vec![9, 10, 11, 6, 7, 8, 3, 4, 5, 6, 7, 8, 0, 1, 2])
        );
    }
}
