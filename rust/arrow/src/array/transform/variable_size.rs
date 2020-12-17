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

use crate::{
    array::{ArrayData, OffsetSizeTrait},
    buffer::MutableBuffer,
    datatypes::ToByteSlice,
};

use super::{
    Extend, _MutableArrayData,
    utils::{extend_offsets, get_last_offset},
};

#[inline]
fn extend_offset_values<T: OffsetSizeTrait>(
    buffer: &mut MutableBuffer,
    offsets: &[T],
    values: &[u8],
    start: usize,
    len: usize,
) {
    let start_values = offsets[start].to_usize().unwrap();
    let end_values = offsets[start + len].to_usize().unwrap();
    let new_values = &values[start_values..end_values];
    buffer.extend_from_slice(new_values);
}

pub(super) fn build_extend<T: OffsetSizeTrait>(array: &ArrayData) -> Extend {
    let offsets = array.buffer::<T>(0);
    let values = &array.buffers()[1].data()[array.offset()..];
    if array.null_count() == 0 {
        // fast case where we can copy regions without null issues
        Box::new(
            move |mutable: &mut _MutableArrayData, _, start: usize, len: usize| {
                let offset_buffer = &mut mutable.buffer1;
                let values_buffer = &mut mutable.buffer2;

                // this is safe due to how offset is built. See details on `get_last_offset`
                let last_offset = unsafe { get_last_offset(offset_buffer) };

                extend_offsets::<T>(
                    offset_buffer,
                    last_offset,
                    &offsets[start..start + len + 1],
                );
                // values
                extend_offset_values::<T>(values_buffer, offsets, values, start, len);
            },
        )
    } else {
        Box::new(
            move |mutable: &mut _MutableArrayData, _, start: usize, len: usize| {
                let offset_buffer = &mut mutable.buffer1;
                let values_buffer = &mut mutable.buffer2;

                // this is safe due to how offset is built. See details on `get_last_offset`
                let mut last_offset: T = unsafe { get_last_offset(offset_buffer) };

                // nulls present: append item by item, ignoring null entries
                offset_buffer
                    .reserve(offset_buffer.len() + len * std::mem::size_of::<T>());

                (start..start + len).for_each(|i| {
                    if array.is_valid(i) {
                        // compute the new offset
                        let length = offsets[i + 1] - offsets[i];
                        last_offset = last_offset + length;
                        let length = length.to_usize().unwrap();

                        // append value
                        let start = offsets[i].to_usize().unwrap()
                            - offsets[0].to_usize().unwrap();
                        let bytes = &values[start..(start + length)];
                        values_buffer.extend_from_slice(bytes);
                    }
                    // offsets are always present
                    offset_buffer.extend_from_slice(last_offset.to_byte_slice());
                })
            },
        )
    }
}

pub(super) fn extend_nulls<T: OffsetSizeTrait>(
    mutable: &mut _MutableArrayData,
    len: usize,
) {
    let offset_buffer = &mut mutable.buffer1;

    // this is safe due to how offset is built. See details on `get_last_offset`
    let last_offset: T = unsafe { get_last_offset(offset_buffer) };

    let offsets = vec![last_offset; len];
    offset_buffer.extend_from_slice(offsets.to_byte_slice());
}
