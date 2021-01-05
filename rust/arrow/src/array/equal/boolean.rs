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

use super::utils::{equal_bits, equal_len};
use crate::array::ArrayData;

pub(super) fn boolean_equal(
    lhs: &ArrayData,
    rhs: &ArrayData,
    lhs_start: usize,
    rhs_start: usize,
    len: usize,
) -> bool {
    let lhs_values = lhs.buffers()[0].as_slice();
    let rhs_values = rhs.buffers()[0].as_slice();

    // Try optimize for zero null counts and same align format.
    if lhs.null_count() == 0 && rhs.null_count() == 0 {
        let mut lhs_align_left = 0;
        let mut lhs_prefix_bits = 0;
        if lhs_start > 0 {
            lhs_align_left = lhs_start / 8_usize;
            if lhs_start % 8_usize > 0 {
                lhs_align_left += 1_usize;
            }
            lhs_prefix_bits = lhs_align_left * 8_usize - lhs_start;
        }

        let mut rhs_prefix_bits = 0;
        if rhs_start > 0 {
            let mut align = rhs_start / 8_usize;
            if rhs_start % 8_usize > 0 {
                align += 1;
            }
            rhs_prefix_bits = align * 8_usize - rhs_start;
        }

        // `lhs_prefix_len == lhs_prefix_len` means same align format:
        // prefix_bits | aligned_bytes | suffix_bits
        if lhs_prefix_bits == rhs_prefix_bits {
            // Compare prefix bit slices.
            if lhs_prefix_bits > 0
                && !equal_bits(
                    lhs_values,
                    rhs_values,
                    lhs.offset() + lhs_start,
                    rhs.offset() + rhs_start,
                    lhs_prefix_bits,
                )
            {
                return false;
            }

            let lhs_align_right = (lhs_start + len) / 8_usize;
            let align_bytes_len = lhs_align_right - lhs_align_left;
            let align_bits_len = align_bytes_len * 8_usize;
            let suffix_len = len - align_bits_len;

            // Compare suffix bit slices.
            if suffix_len > 0
                && !equal_bits(
                    lhs_values,
                    rhs_values,
                    lhs.offset() + lhs_start + align_bits_len,
                    rhs.offset() + rhs_start + align_bits_len,
                    suffix_len,
                )
            {
                return false;
            }

            // Compare byte slices.
            if align_bytes_len > 0
                && !equal_len(
                    lhs_values,
                    rhs_values,
                    lhs_align_left,
                    lhs_align_left,
                    align_bytes_len,
                )
            {
                return false;
            }

            true
        } else {
            equal_bits(
                lhs_values,
                rhs_values,
                lhs.offset() + lhs_start,
                rhs.offset() + rhs_start,
                len,
            )
        }
    } else {
        (0..len).all(|i| {
            let lhs_pos = lhs_start + i;
            let rhs_pos = rhs_start + i;
            let lhs_is_null = lhs.is_null(lhs_pos);
            let rhs_is_null = rhs.is_null(rhs_pos);
            if lhs_is_null != rhs_is_null {
                return false;
            }
            if lhs_is_null && rhs_is_null {
                return true;
            }
            equal_bits(
                lhs_values,
                rhs_values,
                lhs_pos + lhs.offset(),
                rhs_pos + rhs.offset(),
                1,
            )
        })
    }
}
