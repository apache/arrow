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

use crate::{array::ArrayData, datatypes::DataType};

use super::{Extend, _MutableArrayData};

pub(super) fn build_extend(array: &ArrayData) -> Extend {
    let size = match array.data_type() {
        DataType::FixedSizeBinary(i) => *i as usize,
        _ => unreachable!(),
    };

    let values = &array.buffers()[0].as_slice()[array.offset() * size..];
    if array.null_count() == 0 {
        // fast case where we can copy regions without null issues
        Box::new(
            move |mutable: &mut _MutableArrayData, _, start: usize, len: usize| {
                let buffer = &mut mutable.buffer1;
                buffer.extend_from_slice(&values[start * size..(start + len) * size]);
            },
        )
    } else {
        Box::new(
            move |mutable: &mut _MutableArrayData, _, start: usize, len: usize| {
                // nulls present: append item by item, ignoring null entries
                let values_buffer = &mut mutable.buffer1;

                (start..start + len).for_each(|i| {
                    if array.is_valid(i) {
                        // append value
                        let bytes = &values[i * size..(i + 1) * size];
                        values_buffer.extend_from_slice(bytes);
                    } else {
                        values_buffer.extend_zeros(size);
                    }
                })
            },
        )
    }
}

pub(super) fn extend_nulls(mutable: &mut _MutableArrayData, len: usize) {
    let size = match mutable.data_type {
        DataType::FixedSizeBinary(i) => i as usize,
        _ => unreachable!(),
    };

    let values_buffer = &mut mutable.buffer1;
    values_buffer.extend_zeros(len * size);
}
