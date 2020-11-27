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

use crate::buffer::Buffer;
use crate::util::bit_util::get_bit;
use crate::{
    array::data::count_nulls,
    array::{ArrayData, OffsetSizeTrait},
};

use super::utils::equal_len;

fn offset_value_equal<T: OffsetSizeTrait>(
    lhs_values: &[u8],
    rhs_values: &[u8],
    lhs_offsets: &[T],
    rhs_offsets: &[T],
    lhs_pos: usize,
    rhs_pos: usize,
    len: usize,
) -> bool {
    let lhs_start = lhs_offsets[lhs_pos].to_usize().unwrap();
    let rhs_start = rhs_offsets[rhs_pos].to_usize().unwrap();
    let lhs_len = lhs_offsets[lhs_pos + len] - lhs_offsets[lhs_pos];
    let rhs_len = rhs_offsets[rhs_pos + len] - rhs_offsets[rhs_pos];

    lhs_len == rhs_len
        && equal_len(
            lhs_values,
            rhs_values,
            lhs_start,
            rhs_start,
            lhs_len.to_usize().unwrap(),
        )
}

pub(super) fn variable_sized_equal<T: OffsetSizeTrait>(
    lhs: &ArrayData,
    rhs: &ArrayData,
    lhs_nulls: Option<&Buffer>,
    rhs_nulls: Option<&Buffer>,
    lhs_start: usize,
    rhs_start: usize,
    len: usize,
) -> bool {
    let lhs_offsets = lhs.buffer::<T>(0);
    let rhs_offsets = rhs.buffer::<T>(0);

    // these are bytes, and thus the offset does not need to be multiplied
    let lhs_values = &lhs.buffers()[1].data()[lhs.offset()..];
    let rhs_values = &rhs.buffers()[1].data()[rhs.offset()..];

    let lhs_null_count = count_nulls(lhs_nulls, lhs_start, len);
    let rhs_null_count = count_nulls(rhs_nulls, rhs_start, len);

    if lhs_null_count == 0
        && rhs_null_count == 0
        && !lhs_values.is_empty()
        && !rhs_values.is_empty()
    {
        offset_value_equal(
            lhs_values,
            rhs_values,
            lhs_offsets,
            rhs_offsets,
            lhs_start,
            rhs_start,
            len,
        )
    } else {
        (0..len).all(|i| {
            let lhs_pos = lhs_start + i;
            let rhs_pos = rhs_start + i;

            // the null bits can still be `None`, so we don't unwrap
            let lhs_is_null = !lhs_nulls
                .map(|v| get_bit(v.data(), lhs_pos))
                .unwrap_or(false);
            let rhs_is_null = !rhs_nulls
                .map(|v| get_bit(v.data(), rhs_pos))
                .unwrap_or(false);

            lhs_is_null
                || (lhs_is_null == rhs_is_null)
                    && offset_value_equal(
                        lhs_values,
                        rhs_values,
                        lhs_offsets,
                        rhs_offsets,
                        lhs_pos,
                        rhs_pos,
                        1,
                    )
        })
    }
}
