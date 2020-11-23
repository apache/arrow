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
    datatypes::ToByteSlice,
};

use super::{Extend, _MutableArrayData, utils::extend_offsets};

pub(super) fn build_extend<T: OffsetSizeTrait>(array: &ArrayData) -> Extend {
    let offsets = array.buffer::<T>(0);
    if array.null_count() == 0 {
        // fast case where we can copy regions without nullability checks
        Box::new(
            move |mutable: &mut _MutableArrayData,
                  index: usize,
                  start: usize,
                  len: usize| {
                let mutable_offsets = mutable.buffer::<T>(0);
                let last_offset = mutable_offsets[mutable_offsets.len() - 1];
                // offsets
                extend_offsets::<T>(
                    &mut mutable.buffers[0],
                    last_offset,
                    &offsets[start..start + len + 1],
                );

                mutable.child_data[0].extend(
                    index,
                    offsets[start].to_usize().unwrap(),
                    offsets[start + len].to_usize().unwrap(),
                )
            },
        )
    } else {
        // nulls present: append item by item, ignoring null entries
        Box::new(
            move |mutable: &mut _MutableArrayData,
                  index: usize,
                  start: usize,
                  len: usize| {
                let mutable_offsets = mutable.buffer::<T>(0);
                let mut last_offset = mutable_offsets[mutable_offsets.len() - 1];

                let buffer = &mut mutable.buffers[0];
                let delta_len = array.len() - array.null_count();
                buffer.reserve(buffer.len() + delta_len * std::mem::size_of::<T>());

                let child = &mut mutable.child_data[0];
                (start..start + len).for_each(|i| {
                    if array.is_valid(i) {
                        // compute the new offset
                        last_offset = last_offset + offsets[i + 1] - offsets[i];

                        // append value
                        child.extend(
                            index,
                            offsets[i].to_usize().unwrap(),
                            offsets[i + 1].to_usize().unwrap(),
                        );
                    }
                    // append offset
                    buffer.extend_from_slice(last_offset.to_byte_slice());
                })
            },
        )
    }
}
