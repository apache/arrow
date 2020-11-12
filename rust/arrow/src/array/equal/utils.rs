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

use crate::{array::ArrayData, util::bit_util};

// whether bits along the positions are equal
// `lhs_start`, `rhs_start` and `len` are _measured in bits_.
#[inline]
pub(super) fn equal_bits(
    lhs_values: &[u8],
    rhs_values: &[u8],
    lhs_start: usize,
    rhs_start: usize,
    len: usize,
) -> bool {
    (0..len).all(|i| {
        bit_util::get_bit(lhs_values, lhs_start + i)
            == bit_util::get_bit(rhs_values, rhs_start + i)
    })
}

#[inline]
pub(super) fn equal_nulls(
    lhs: &ArrayData,
    rhs: &ArrayData,
    lhs_start: usize,
    rhs_start: usize,
    len: usize,
) -> bool {
    if lhs.null_count() > 0 || rhs.null_count() > 0 {
        let lhs_null_bitmap = lhs.null_bitmap().as_ref().unwrap();
        let rhs_null_bitmap = rhs.null_bitmap().as_ref().unwrap();
        let lhs_values = lhs_null_bitmap.bits.data();
        let rhs_values = rhs_null_bitmap.bits.data();
        equal_bits(
            lhs_values,
            rhs_values,
            lhs_start + lhs.offset(),
            rhs_start + rhs.offset(),
            len,
        )
    } else {
        true
    }
}

#[inline]
pub(super) fn base_equal(lhs: &ArrayData, rhs: &ArrayData) -> bool {
    lhs.data_type() == rhs.data_type() && lhs.len() == rhs.len()
}

// whether the two memory regions are equal
#[inline]
pub(super) fn equal_len(
    lhs_values: &[u8],
    rhs_values: &[u8],
    lhs_start: usize,
    rhs_start: usize,
    len: usize,
) -> bool {
    lhs_values[lhs_start..(lhs_start + len)] == rhs_values[rhs_start..(rhs_start + len)]
}
