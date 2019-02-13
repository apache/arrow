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
use std::sync::Arc;

use crate::array::{Array, BooleanArray};
use crate::array_data::ArrayData;
use crate::buffer::Buffer;
use crate::compute::util::apply_bin_op_to_option_bitmap;
use crate::datatypes::DataType;
use crate::error::Result;

/// Helper function to implement binary kernels
fn binary_boolean_kernel<F>(
    left: &BooleanArray,
    right: &BooleanArray,
    op: F,
) -> Result<BooleanArray>
where
    F: Fn(&Buffer, &Buffer) -> Result<Buffer>,
{
    let left_data = left.data();
    let right_data = right.data();
    let null_bit_buffer = apply_bin_op_to_option_bitmap(
        left_data.null_bitmap(),
        right_data.null_bitmap(),
        |a, b| a & b,
    )?;
    let values = op(&left_data.buffers()[0], &right_data.buffers()[0])?;
    let data = ArrayData::new(
        DataType::Boolean,
        left.len(),
        None,
        null_bit_buffer,
        0,
        vec![values],
        vec![],
    );
    Ok(BooleanArray::from(Arc::new(data)))
}

/// Perform `AND` operation on two arrays. If either left or right value is null then the
/// result is also null.
pub fn and(left: &BooleanArray, right: &BooleanArray) -> Result<BooleanArray> {
    binary_boolean_kernel(&left, &right, |a, b| a & b)
}

/// Perform `OR` operation on two arrays. If either left or right value is null then the
/// result is also null.
pub fn or(left: &BooleanArray, right: &BooleanArray) -> Result<BooleanArray> {
    binary_boolean_kernel(&left, &right, |a, b| a | b)
}

/// Perform unary `NOT` operation on an arrays. If value is null then the result is also
/// null.
pub fn not(left: &BooleanArray) -> Result<BooleanArray> {
    let mut b = BooleanArray::builder(left.len());
    for i in 0..left.len() {
        if left.is_null(i) {
            b.append_null()?;
        } else {
            b.append_value(!left.value(i))?;
        }
    }
    Ok(b.finish())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bool_array_and() {
        let a = BooleanArray::from(vec![false, false, true, true]);
        let b = BooleanArray::from(vec![false, true, false, true]);
        let c = and(&a, &b).unwrap();
        assert_eq!(false, c.value(0));
        assert_eq!(false, c.value(1));
        assert_eq!(false, c.value(2));
        assert_eq!(true, c.value(3));
    }

    #[test]
    fn test_bool_array_or() {
        let a = BooleanArray::from(vec![false, false, true, true]);
        let b = BooleanArray::from(vec![false, true, false, true]);
        let c = or(&a, &b).unwrap();
        assert_eq!(false, c.value(0));
        assert_eq!(true, c.value(1));
        assert_eq!(true, c.value(2));
        assert_eq!(true, c.value(3));
    }

    #[test]
    fn test_bool_array_or_nulls() {
        let a = BooleanArray::from(vec![None, Some(false), None, Some(false)]);
        let b = BooleanArray::from(vec![None, None, Some(false), Some(false)]);
        let c = or(&a, &b).unwrap();
        assert_eq!(true, c.is_null(0));
        assert_eq!(true, c.is_null(1));
        assert_eq!(true, c.is_null(2));
        assert_eq!(false, c.is_null(3));
    }

    #[test]
    fn test_bool_array_not() {
        let a = BooleanArray::from(vec![false, false, true, true]);
        let c = not(&a).unwrap();
        assert_eq!(true, c.value(0));
        assert_eq!(true, c.value(1));
        assert_eq!(false, c.value(2));
        assert_eq!(false, c.value(3));
    }

    #[test]
    fn test_bool_array_and_nulls() {
        let a = BooleanArray::from(vec![None, Some(false), None, Some(false)]);
        let b = BooleanArray::from(vec![None, None, Some(false), Some(false)]);
        let c = and(&a, &b).unwrap();
        assert_eq!(true, c.is_null(0));
        assert_eq!(true, c.is_null(1));
        assert_eq!(true, c.is_null(2));
        assert_eq!(false, c.is_null(3));
    }
}
