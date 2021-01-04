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

use crate::array::{data::count_nulls, ArrayData};
use crate::buffer::Buffer;
use crate::datatypes::DataType;
use crate::util::bit_util::get_bit;

use super::utils::equal_len;

pub(super) fn fixed_binary_equal(
    lhs: &ArrayData,
    rhs: &ArrayData,
    lhs_nulls: Option<&Buffer>,
    rhs_nulls: Option<&Buffer>,
    lhs_start: usize,
    rhs_start: usize,
    len: usize,
) -> bool {
    let size = match lhs.data_type() {
        DataType::FixedSizeBinary(i) => *i as usize,
        _ => unreachable!(),
    };

    let lhs_values = &lhs.buffers()[0].as_slice()[lhs.offset() * size..];
    let rhs_values = &rhs.buffers()[0].as_slice()[rhs.offset() * size..];

    let lhs_null_count = count_nulls(lhs_nulls, lhs_start, len);
    let rhs_null_count = count_nulls(rhs_nulls, rhs_start, len);

    if lhs_null_count == 0 && rhs_null_count == 0 {
        equal_len(
            lhs_values,
            rhs_values,
            size * lhs_start,
            size * rhs_start,
            size * len,
        )
    } else {
        // get a ref of the null buffer bytes, to use in testing for nullness
        let lhs_null_bytes = lhs_nulls.as_ref().unwrap().as_slice();
        let rhs_null_bytes = rhs_nulls.as_ref().unwrap().as_slice();
        // with nulls, we need to compare item by item whenever it is not null
        (0..len).all(|i| {
            let lhs_pos = lhs_start + i;
            let rhs_pos = rhs_start + i;

            let lhs_is_null = !get_bit(lhs_null_bytes, lhs_pos);
            let rhs_is_null = !get_bit(rhs_null_bytes, rhs_pos);

            lhs_is_null
                || (lhs_is_null == rhs_is_null)
                    && equal_len(
                        lhs_values,
                        rhs_values,
                        lhs_pos * size,
                        rhs_pos * size,
                        size, // 1 * size since we are comparing a single entry
                    )
        })
    }
}
