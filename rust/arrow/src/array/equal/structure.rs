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
    array::data::count_nulls, array::ArrayData, buffer::Buffer, util::bit_util::get_bit,
};

use super::equal_range;

/// Compares the values of two [ArrayData] starting at `lhs_start` and `rhs_start` respectively
/// for `len` slots. The null buffers `lhs_nulls` and `rhs_nulls` inherit parent nullability.
///
/// If an array is a child of a struct or list, the array's nulls have to be merged with the parent.
/// This then affects the null count of the array, thus the merged nulls are passed separately
/// as `lhs_nulls` and `rhs_nulls` variables to functions.
/// The nulls are merged with a bitwise AND, and null counts are recomputed wheer necessary.
fn equal_values(
    lhs: &ArrayData,
    rhs: &ArrayData,
    lhs_nulls: Option<&Buffer>,
    rhs_nulls: Option<&Buffer>,
    lhs_start: usize,
    rhs_start: usize,
    len: usize,
) -> bool {
    let mut temp_lhs: Option<Buffer> = None;
    let mut temp_rhs: Option<Buffer> = None;

    lhs.child_data()
        .iter()
        .zip(rhs.child_data())
        .all(|(lhs_values, rhs_values)| {
            // merge the null data
            let lhs_merged_nulls = match (lhs_nulls, lhs_values.null_buffer()) {
                (None, None) => None,
                (None, Some(c)) => Some(c),
                (Some(p), None) => Some(p),
                (Some(p), Some(c)) => {
                    let merged = (p & c).unwrap();
                    temp_lhs = Some(merged);
                    temp_lhs.as_ref()
                }
            };
            let rhs_merged_nulls = match (rhs_nulls, rhs_values.null_buffer()) {
                (None, None) => None,
                (None, Some(c)) => Some(c),
                (Some(p), None) => Some(p),
                (Some(p), Some(c)) => {
                    let merged = (p & c).unwrap();
                    temp_rhs = Some(merged);
                    temp_rhs.as_ref()
                }
            };
            equal_range(
                lhs_values,
                rhs_values,
                lhs_merged_nulls,
                rhs_merged_nulls,
                lhs_start,
                rhs_start,
                len,
            )
        })
}

pub(super) fn struct_equal(
    lhs: &ArrayData,
    rhs: &ArrayData,
    lhs_nulls: Option<&Buffer>,
    rhs_nulls: Option<&Buffer>,
    lhs_start: usize,
    rhs_start: usize,
    len: usize,
) -> bool {
    // we have to recalculate null counts from the null buffers
    let lhs_null_count = count_nulls(lhs_nulls, lhs_start, len);
    let rhs_null_count = count_nulls(rhs_nulls, rhs_start, len);
    if lhs_null_count == 0 && rhs_null_count == 0 {
        equal_values(lhs, rhs, lhs_nulls, rhs_nulls, lhs_start, rhs_start, len)
    } else {
        // get a ref of the null buffer bytes, to use in testing for nullness
        let lhs_null_bytes = lhs_nulls.as_ref().unwrap().data();
        let rhs_null_bytes = rhs_nulls.as_ref().unwrap().data();
        // with nulls, we need to compare item by item whenever it is not null
        (0..len).all(|i| {
            let lhs_pos = lhs_start + i;
            let rhs_pos = rhs_start + i;
            // if both struct and child had no null buffers,
            let lhs_is_null = !get_bit(lhs_null_bytes, lhs_pos);
            let rhs_is_null = !get_bit(rhs_null_bytes, rhs_pos);

            lhs_is_null
                || (lhs_is_null == rhs_is_null)
                    && equal_values(lhs, rhs, lhs_nulls, rhs_nulls, lhs_pos, rhs_pos, 1)
        })
    }
}
