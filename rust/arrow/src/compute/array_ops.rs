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

//! Defines primitive computations on arrays, e.g. addition, equality, boolean logic.

use std::cmp;
use std::ops::Add;
use std::sync::Arc;

use crate::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, Int8Array, PrimitiveArray, UInt16Array, UInt32Array,
    UInt64Array, UInt8Array,
};
use crate::datatypes::{ArrowNumericType, DataType};
use crate::error::{ArrowError, Result};

/// Returns the minimum value in the array, according to the natural order.
pub fn min<T>(array: &PrimitiveArray<T>) -> Option<T::Native>
where
    T: ArrowNumericType,
{
    min_max_helper(array, |a, b| a < b)
}

/// Returns the maximum value in the array, according to the natural order.
pub fn max<T>(array: &PrimitiveArray<T>) -> Option<T::Native>
where
    T: ArrowNumericType,
{
    min_max_helper(array, |a, b| a > b)
}

/// Helper function to perform min/max lambda function on values from a numeric array.
fn min_max_helper<T, F>(array: &PrimitiveArray<T>, cmp: F) -> Option<T::Native>
where
    T: ArrowNumericType,
    F: Fn(T::Native, T::Native) -> bool,
{
    let mut n: Option<T::Native> = None;
    let data = array.data();
    for i in 0..data.len() {
        if data.is_null(i) {
            continue;
        }
        let m = array.value(i);
        match n {
            None => n = Some(m),
            Some(nn) => {
                if cmp(m, nn) {
                    n = Some(m)
                }
            }
        }
    }
    n
}

/// Returns the sum of values in the array.
///
/// Returns `None` if the array is empty or only contains null values.
pub fn sum<T>(array: &PrimitiveArray<T>) -> Option<T::Native>
where
    T: ArrowNumericType,
    T::Native: Add<Output = T::Native>,
{
    let null_count = array.null_count();

    if null_count == array.len() {
        None
    } else if null_count == 0 {
        // optimized path for arrays without null values
        let mut n: T::Native = T::default_value();
        let data = array.data();
        let m = array.value_slice(0, data.len());
        for i in 0..data.len() {
            n = n + m[i];
        }
        Some(n)
    } else {
        let mut n: T::Native = T::default_value();
        let data = array.data();
        let m = array.value_slice(0, data.len());
        for i in 0..data.len() {
            if data.is_valid(i) {
                n = n + m[i];
            }
        }
        Some(n)
    }
}

/// Helper function to perform boolean lambda function on values from two arrays.
fn bool_op<T, F>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
    op: F,
) -> Result<BooleanArray>
where
    T: ArrowNumericType,
    F: Fn(Option<T::Native>, Option<T::Native>) -> bool,
{
    if left.len() != right.len() {
        return Err(ArrowError::ComputeError(
            "Cannot perform math operation on arrays of different length".to_string(),
        ));
    }
    let mut b = BooleanArray::builder(left.len());
    for i in 0..left.len() {
        let index = i;
        let l = if left.is_null(i) {
            None
        } else {
            Some(left.value(index))
        };
        let r = if right.is_null(i) {
            None
        } else {
            Some(right.value(index))
        };
        b.append_value(op(l, r))?;
    }
    Ok(b.finish())
}

macro_rules! filter_array {
    ($array:expr, $filter:expr, $array_type:ident) => {{
        let b = $array.as_any().downcast_ref::<$array_type>().unwrap();
        let mut builder = $array_type::builder(b.len());
        for i in 0..b.len() {
            if $filter.value(i) {
                if b.is_null(i) {
                    builder.append_null()?;
                } else {
                    builder.append_value(b.value(i))?;
                }
            }
        }
        Ok(Arc::new(builder.finish()))
    }};
}

pub fn filter(array: &Array, filter: &BooleanArray) -> Result<ArrayRef> {
    match array.data_type() {
        DataType::UInt8 => filter_array!(array, filter, UInt8Array),
        DataType::UInt16 => filter_array!(array, filter, UInt16Array),
        DataType::UInt32 => filter_array!(array, filter, UInt32Array),
        DataType::UInt64 => filter_array!(array, filter, UInt64Array),
        DataType::Int8 => filter_array!(array, filter, Int8Array),
        DataType::Int16 => filter_array!(array, filter, Int16Array),
        DataType::Int32 => filter_array!(array, filter, Int32Array),
        DataType::Int64 => filter_array!(array, filter, Int64Array),
        DataType::Float32 => filter_array!(array, filter, Float32Array),
        DataType::Float64 => filter_array!(array, filter, Float64Array),
        DataType::Boolean => filter_array!(array, filter, BooleanArray),
        DataType::Utf8 => {
            let b = array.as_any().downcast_ref::<BinaryArray>().unwrap();
            let mut values: Vec<&[u8]> = Vec::with_capacity(b.len());
            for i in 0..b.len() {
                if filter.value(i) {
                    values.push(b.value(i));
                }
            }
            Ok(Arc::new(BinaryArray::from(values)))
        }
        other => Err(ArrowError::ComputeError(format!(
            "filter not supported for {:?}",
            other
        ))),
    }
}

macro_rules! limit_array {
    ($array:expr, $num_elements:expr, $array_type:ident) => {{
        let b = $array.as_any().downcast_ref::<$array_type>().unwrap();
        let mut builder = $array_type::builder($num_elements);
        for i in 0..$num_elements {
            if b.is_null(i) {
                builder.append_null()?;
            } else {
                builder.append_value(b.value(i))?;
            }
        }
        Ok(Arc::new(builder.finish()))
    }};
}

/// Returns the array, taking only the number of elements specified
///
/// Returns the whole array if the number of elements specified is larger than the length of the array
pub fn limit(array: &Array, num_elements: usize) -> Result<ArrayRef> {
    let num_elements_safe: usize = cmp::min(array.len(), num_elements);

    match array.data_type() {
        DataType::UInt8 => limit_array!(array, num_elements_safe, UInt8Array),
        DataType::UInt16 => limit_array!(array, num_elements_safe, UInt16Array),
        DataType::UInt32 => limit_array!(array, num_elements_safe, UInt32Array),
        DataType::UInt64 => limit_array!(array, num_elements_safe, UInt64Array),
        DataType::Int8 => limit_array!(array, num_elements_safe, Int8Array),
        DataType::Int16 => limit_array!(array, num_elements_safe, Int16Array),
        DataType::Int32 => limit_array!(array, num_elements_safe, Int32Array),
        DataType::Int64 => limit_array!(array, num_elements_safe, Int64Array),
        DataType::Float32 => limit_array!(array, num_elements_safe, Float32Array),
        DataType::Float64 => limit_array!(array, num_elements_safe, Float64Array),
        DataType::Boolean => limit_array!(array, num_elements_safe, BooleanArray),
        DataType::Utf8 => {
            let b = array.as_any().downcast_ref::<BinaryArray>().unwrap();
            let mut values: Vec<&[u8]> = Vec::with_capacity(num_elements_safe);
            for i in 0..num_elements_safe {
                values.push(b.value(i));
            }
            Ok(Arc::new(BinaryArray::from(values)))
        }
        other => Err(ArrowError::ComputeError(format!(
            "limit not supported for {:?}",
            other
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::{Float64Array, Int32Array};

    #[test]
    fn test_primitive_array_sum() {
        let a = Int32Array::from(vec![1, 2, 3, 4, 5]);
        assert_eq!(15, sum(&a).unwrap());
    }

    #[test]
    fn test_primitive_array_float_sum() {
        let a = Float64Array::from(vec![1.1, 2.2, 3.3, 4.4, 5.5]);
        assert_eq!(16.5, sum(&a).unwrap());
    }

    #[test]
    fn test_primitive_array_sum_with_nulls() {
        let a = Int32Array::from(vec![None, Some(2), Some(3), None, Some(5)]);
        assert_eq!(10, sum(&a).unwrap());
    }

    #[test]
    fn test_primitive_array_sum_all_nulls() {
        let a = Int32Array::from(vec![None, None, None]);
        assert_eq!(None, sum(&a));
    }

    #[test]
    fn test_buffer_array_min_max() {
        let a = Int32Array::from(vec![5, 6, 7, 8, 9]);
        assert_eq!(5, min(&a).unwrap());
        assert_eq!(9, max(&a).unwrap());
    }

    #[test]
    fn test_buffer_array_min_max_with_nulls() {
        let a = Int32Array::from(vec![Some(5), None, None, Some(8), Some(9)]);
        assert_eq!(5, min(&a).unwrap());
        assert_eq!(9, max(&a).unwrap());
    }

    #[test]
    fn test_filter_array() {
        let a = Int32Array::from(vec![5, 6, 7, 8, 9]);
        let b = BooleanArray::from(vec![true, false, false, true, false]);
        let c = filter(&a, &b).unwrap();
        let d = c.as_ref().as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(2, d.len());
        assert_eq!(5, d.value(0));
        assert_eq!(8, d.value(1));
    }

    #[test]
    fn test_filter_binary_array() {
        let a = BinaryArray::from(vec!["hello", " ", "world", "!"]);
        let b = BooleanArray::from(vec![true, false, true, false]);
        let c = filter(&a, &b).unwrap();
        let d = c.as_ref().as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(2, d.len());
        assert_eq!("hello", d.get_string(0));
        assert_eq!("world", d.get_string(1));
    }

    #[test]
    fn test_filter_array_with_null() {
        let a = Int32Array::from(vec![Some(5), None]);
        let b = BooleanArray::from(vec![false, true]);
        let c = filter(&a, &b).unwrap();
        let d = c.as_ref().as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(1, d.len());
        assert_eq!(true, d.is_null(0));
    }

    #[test]
    fn test_limit_array() {
        let a = Int32Array::from(vec![5, 6, 7, 8, 9]);
        let b = limit(&a, 3).unwrap();
        let c = b.as_ref().as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(3, c.len());
        assert_eq!(5, c.value(0));
        assert_eq!(6, c.value(1));
        assert_eq!(7, c.value(2));
    }

    #[test]
    fn test_limit_binary_array() {
        let a = BinaryArray::from(vec!["hello", " ", "world", "!"]);
        let b = limit(&a, 2).unwrap();
        let c = b.as_ref().as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(2, c.len());
        assert_eq!("hello", c.get_string(0));
        assert_eq!(" ", c.get_string(1));
    }

    #[test]
    fn test_limit_array_with_null() {
        let a = Int32Array::from(vec![None, Some(5)]);
        let b = limit(&a, 1).unwrap();
        let c = b.as_ref().as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(1, c.len());
        assert_eq!(true, c.is_null(0));
    }

    #[test]
    fn test_limit_array_with_limit_too_large() {
        let a = Int32Array::from(vec![5, 6, 7, 8, 9]);
        let b = limit(&a, 6).unwrap();
        let c = b.as_ref().as_any().downcast_ref::<Int32Array>().unwrap();

        assert_eq!(5, c.len());
        assert_eq!(a.value(0), c.value(0));
        assert_eq!(a.value(1), c.value(1));
        assert_eq!(a.value(2), c.value(2));
        assert_eq!(a.value(3), c.value(3));
        assert_eq!(a.value(4), c.value(4));
    }
}
