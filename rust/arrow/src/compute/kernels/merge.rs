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

//! Defines merge kernel for two `Array`s

use std::sync::Arc;

use crate::array::ArrayRef;
use crate::{
    array::{Array, PrimitiveArray},
    datatypes::ArrowPrimitiveType,
    error::{ArrowError, Result},
};

/// An iterator adapter that uses 3 iterators to build a new iterator whose elements come
/// from the first or second based on the third.
/// Given 3 iterators (`left`, `right`, `is_left`), on which the third one (`is_left`) is boolean,
/// returns an iterator on which the element `i` is from the left iterator if `is_left` or from the right otherwise.
/// The size of this iterator is given by the size of `is_left`.
struct TakeTogether<L, R, T, C>
where
    L: Iterator<Item = T>,
    R: Iterator<Item = T>,
    C: Iterator<Item = bool>,
{
    left: L,
    right: R,
    is_left: C,
}

impl<L, R, T, C> TakeTogether<L, R, T, C>
where
    L: Iterator<Item = T>,
    R: Iterator<Item = T>,
    C: Iterator<Item = bool>,
{
    fn new(left: L, right: R, is_left: C) -> Self {
        TakeTogether {
            left,
            right,
            is_left,
        }
    }
}

impl<L, R, T, C> Iterator for TakeTogether<L, R, T, C>
where
    L: Iterator<Item = T>,
    R: Iterator<Item = T>,
    C: Iterator<Item = bool>,
{
    type Item = T;

    fn next(&mut self) -> Option<L::Item> {
        let is_left = self.is_left.next();
        is_left.and_then(|is_left| {
            if is_left {
                self.left.next()
            } else {
                self.right.next()
            }
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.is_left.size_hint()
    }
}

/// Merges two primitive arrays, returning a new primitive array
pub fn merge_primitive<T: ArrowPrimitiveType>(
    lhs: &dyn Array,
    rhs: &dyn Array,
    is_left: &[bool],
) -> PrimitiveArray<T> {
    let lhs = lhs.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
    let rhs = rhs.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
    TakeTogether::new(lhs.iter(), rhs.iter(), is_left.to_owned().into_iter()).collect()
}

fn dyn_merge_primitive<T: ArrowPrimitiveType>(
    lhs: &dyn Array,
    rhs: &dyn Array,
    is_left: &[bool],
) -> Arc<Array> {
    Arc::new(merge_primitive::<T>(lhs, rhs, is_left))
}

/// Merges two arrays by taking the values from `lhs` and `rhs` depending on `is_left` slice.
/// `is_left` must have a length equal to the sum of the two lengths and the total number of `true`
/// must equal the length of `lhs`.
/// # Errors
/// This function errors when:
/// * the arrays are not of the same type
/// * the sum of the lengths of each array is not equal to the length of the `is_len` slice.
pub fn merge(lhs: &dyn Array, rhs: &dyn Array, is_left: &[bool]) -> Result<ArrayRef> {
    use crate::datatypes::*;
    if lhs.data_type() != rhs.data_type() {
        return Err(ArrowError::InvalidArgumentError(format!("Take merge requires both arrays to be of the same type. lhs: {:?} != rhs: {:?}", lhs.data_type(), rhs.data_type())));
    }
    if lhs.len() + rhs.len() != is_left.len() {
        return Err(ArrowError::InvalidArgumentError(format!("Take merge requires the sum of arrays' lengths to be equal to the length of the third argument. lhs: {} + rhs: {} != is_len: {}", lhs.len(), rhs.len(), is_left.len())));
    }

    Ok(match lhs.data_type() {
        DataType::Boolean => dyn_merge_primitive::<BooleanType>(lhs, rhs, is_left),
        DataType::Int8 => dyn_merge_primitive::<Int8Type>(lhs, rhs, is_left),
        DataType::Int16 => dyn_merge_primitive::<Int16Type>(lhs, rhs, is_left),
        DataType::Int32 => dyn_merge_primitive::<Int32Type>(lhs, rhs, is_left),
        DataType::Int64 => dyn_merge_primitive::<Int64Type>(lhs, rhs, is_left),
        DataType::UInt8 => dyn_merge_primitive::<UInt8Type>(lhs, rhs, is_left),
        DataType::UInt16 => dyn_merge_primitive::<UInt16Type>(lhs, rhs, is_left),
        DataType::UInt32 => dyn_merge_primitive::<UInt32Type>(lhs, rhs, is_left),
        DataType::UInt64 => dyn_merge_primitive::<UInt64Type>(lhs, rhs, is_left),
        DataType::Float32 => dyn_merge_primitive::<Float32Type>(lhs, rhs, is_left),
        DataType::Float64 => dyn_merge_primitive::<Float64Type>(lhs, rhs, is_left),
        DataType::Date32(_) => dyn_merge_primitive::<Date32Type>(lhs, rhs, is_left),
        DataType::Date64(_) => dyn_merge_primitive::<Date64Type>(lhs, rhs, is_left),
        DataType::Time32(_) => dyn_merge_primitive::<Time32SecondType>(lhs, rhs, is_left),
        DataType::Time64(_) => {
            dyn_merge_primitive::<Time64NanosecondType>(lhs, rhs, is_left)
        }
        DataType::Timestamp(_, _) => {
            dyn_merge_primitive::<TimestampSecondType>(lhs, rhs, is_left)
        }
        DataType::Interval(_) => {
            dyn_merge_primitive::<IntervalYearMonthType>(lhs, rhs, is_left)
        }
        DataType::Duration(_) => {
            dyn_merge_primitive::<DurationSecondType>(lhs, rhs, is_left)
        }
        other => {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Merge still not supported for type {:?}",
                other
            )))
        }
    })
}

#[cfg(test)]
mod tests {
    use crate::array::UInt32Array;

    use super::*;

    #[test]
    fn test_merge_array() -> Result<()> {
        let array1 = UInt32Array::from(vec![1, 2, 3]);
        let array2 = UInt32Array::from(vec![4, 5, 6]);
        let is_left = &[true, false, true, false, true, false];
        let expected = UInt32Array::from(vec![1, 4, 2, 5, 3, 6]);

        let result = merge(&array1, &array2, is_left)?;

        let result = result.as_any().downcast_ref::<UInt32Array>().unwrap();

        assert_eq!(&expected, result);
        Ok(())
    }
}
