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
use crate::bitmap::Bitmap;
use crate::buffer::Buffer;
use crate::error::Result;

/// Applies a given binary operation, `op`, to two references to `Option<Bitmap>`'s.
///
/// This function is useful when implementing operations on higher level arrays.
pub(crate) fn apply_bin_op_to_option_bitmap<F>(
    left: &Option<Bitmap>,
    right: &Option<Bitmap>,
    op: F,
) -> Result<Option<Buffer>>
where
    F: Fn(&Buffer, &Buffer) -> Result<Buffer>,
{
    match *left {
        None => match *right {
            None => Ok(None),
            Some(ref r) => Ok(Some(r.bits.clone())),
        },
        Some(ref l) => match *right {
            None => Ok(Some(l.bits.clone())),
            Some(ref r) => Ok(Some(op(&l.bits, &r.bits)?)),
        },
    }
}

/// Takes/filters a list array's inner data using the offsets of the list array.
///
/// Where a list array has index `[0,2,5,10]`, taking an index of `[2,0]` returns
/// an array of the indices `[5..10, 0..2]` and offsets `[0,5,7]` (5 elements and 2
/// elements)
pub(crate) fn take_index_from_list(
    array: &ArrayRef,
    index: &UInt32Array,
) -> (UInt32Array, Vec<i32>) {
    // TODO benchmark this function, there might be a faster unsafe alternative
    // get list array's offsets
    let list: &ListArray = array.as_any().downcast_ref::<ListArray>().unwrap();
    let offsets: Vec<u32> = (0..=list.len())
        .map(|i| list.value_offset(i) as u32)
        .collect();
    let mut new_offsets = Vec::with_capacity(index.len());
    let mut current_offset = 0;
    // add first offset
    new_offsets.push(0);
    let values: Vec<Option<u32>> = (0..index.len())
        .flat_map(|i: usize| {
            if index.is_valid(i) {
                let ix = index.value(i) as usize;
                let start = offsets[ix];
                let end = offsets[ix + 1];
                current_offset += (end - start) as i32;
                new_offsets.push(current_offset);
                // type annotation needed to guide compiler a bit
                (start..end).map(|v| Some(v)).collect::<Vec<Option<u32>>>()
            } else {
                new_offsets.push(current_offset);
                vec![None]
            }
        })
        .collect();
    (UInt32Array::from(values), new_offsets)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use crate::array_data::ArrayData;
    use crate::datatypes::{DataType, ToByteSlice};

    #[test]
    fn test_apply_bin_op_to_option_bitmap() {
        assert_eq!(
            Ok(None),
            apply_bin_op_to_option_bitmap(&None, &None, |a, b| a & b)
        );
        assert_eq!(
            Ok(Some(Buffer::from([0b01101010]))),
            apply_bin_op_to_option_bitmap(
                &Some(Bitmap::from(Buffer::from([0b01101010]))),
                &None,
                |a, b| a & b
            )
        );
        assert_eq!(
            Ok(Some(Buffer::from([0b01001110]))),
            apply_bin_op_to_option_bitmap(
                &None,
                &Some(Bitmap::from(Buffer::from([0b01001110]))),
                |a, b| a & b
            )
        );
        assert_eq!(
            Ok(Some(Buffer::from([0b01001010]))),
            apply_bin_op_to_option_bitmap(
                &Some(Bitmap::from(Buffer::from([0b01101010]))),
                &Some(Bitmap::from(Buffer::from([0b01001110]))),
                |a, b| a & b
            )
        );
    }

    #[test]
    fn test_take_index_from_list() {
        let value_data = Int32Array::from((0..10).collect::<Vec<i32>>()).data();
        let value_offsets = Buffer::from(&[0, 2, 5, 10].to_byte_slice());
        let list_data_type = DataType::List(Box::new(DataType::Int32));
        let list_data = ArrayData::builder(list_data_type.clone())
            .len(3)
            .add_buffer(value_offsets.clone())
            .add_child_data(value_data.clone())
            .build();
        let array = Arc::new(ListArray::from(list_data)) as ArrayRef;
        let index = UInt32Array::from(vec![2, 0]);
        let (indexed, offsets) = take_index_from_list(&array, &index);
        assert_eq!(vec![0, 5, 7], offsets);
        let data = UInt32Array::from(vec![
            Some(5),
            Some(6),
            Some(7),
            Some(8),
            Some(9),
            Some(0),
            Some(1),
        ])
        .data();
        assert_eq!(data, indexed.data());
    }
}
