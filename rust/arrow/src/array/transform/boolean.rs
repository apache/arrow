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

use crate::array::ArrayData;

use super::{
    Extend, _MutableArrayData,
    utils::{reserve_for_bits, set_bits},
};

pub(super) fn build_extend(array: &ArrayData) -> Extend {
    let values = array.buffers()[0].data();
    Box::new(
        move |mutable: &mut _MutableArrayData, _, start: usize, len: usize| {
            let buffer = &mut mutable.buffer1;
            reserve_for_bits(buffer, mutable.len + len);
            set_bits(
                &mut buffer.data_mut(),
                values,
                mutable.len,
                array.offset() + start,
                len,
            );
        },
    )
}

pub(super) fn extend_nulls(mutable: &mut _MutableArrayData, len: usize) {
    let buffer = &mut mutable.buffer1;
    reserve_for_bits(buffer, mutable.len + len);
}
