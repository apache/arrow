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

use crate::{array::ArrayData, array::OffsetSizeTrait};

use super::equal_range;

fn lengths_equal<T: OffsetSizeTrait>(lhs: &[T], rhs: &[T]) -> bool {
    // invariant from `base_equal`
    debug_assert_eq!(lhs.len(), rhs.len());

    if lhs.is_empty() {
        return true;
    }

    if lhs[0] == T::zero() && rhs[0] == T::zero() {
        return lhs == rhs;
    };

    // The expensive case, e.g.
    // [0, 2, 4, 6, 9] == [4, 6, 8, 10, 13]
    lhs.windows(2)
        .zip(rhs.windows(2))
        .all(|(lhs_offsets, rhs_offsets)| {
            // length of left == length of right
            (lhs_offsets[1] - lhs_offsets[0]) == (rhs_offsets[1] - rhs_offsets[0])
        })
}

#[inline]
fn offset_value_equal<T: OffsetSizeTrait>(
    lhs_values: &ArrayData,
    rhs_values: &ArrayData,
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
        && equal_range(
            lhs_values,
            rhs_values,
            lhs_values.null_buffer(),
            rhs_values.null_buffer(),
            lhs_start,
            rhs_start,
            lhs_len.to_usize().unwrap(),
        )
}

pub(super) fn list_equal<T: OffsetSizeTrait>(
    lhs: &ArrayData,
    rhs: &ArrayData,
    lhs_start: usize,
    rhs_start: usize,
    len: usize,
) -> bool {
    let lhs_offsets = lhs.buffer::<T>(0);
    let rhs_offsets = rhs.buffer::<T>(0);

    let lhs_values = lhs.child_data()[0].as_ref();
    let rhs_values = rhs.child_data()[0].as_ref();

    if lhs.null_count() == 0 && rhs.null_count() == 0 {
        lengths_equal(
            &lhs_offsets[lhs_start..lhs_start + len],
            &rhs_offsets[rhs_start..rhs_start + len],
        ) && equal_range(
            lhs_values,
            rhs_values,
            lhs_values.null_buffer(),
            rhs_values.null_buffer(),
            lhs_offsets[lhs_start].to_usize().unwrap(),
            rhs_offsets[rhs_start].to_usize().unwrap(),
            (lhs_offsets[len] - lhs_offsets[lhs_start])
                .to_usize()
                .unwrap(),
        )
    } else {
        // with nulls, we need to compare item by item whenever it is not null
        (0..len).all(|i| {
            let lhs_pos = lhs_start + i;
            let rhs_pos = rhs_start + i;

            let lhs_is_null = lhs.is_null(lhs_pos);
            let rhs_is_null = rhs.is_null(rhs_pos);

            lhs_is_null
                || (lhs_is_null == rhs_is_null)
                    && offset_value_equal::<T>(
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
