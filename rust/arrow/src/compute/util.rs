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

use crate::array::{Array, ArrayRef, ListArray, UInt32Array};
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

/// Takes/filters a list array's inner offsets
pub(crate) fn take_index_from_list(
    array: &ArrayRef,
    index: &UInt32Array,
) -> (UInt32Array, Vec<i32>) {
    // TODO complete documenting, and add an example
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
                vec![None]
            }
        })
        .collect();
    (UInt32Array::from(values), new_offsets)
}

#[cfg(test)]
mod tests {
    use super::*;

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

}
