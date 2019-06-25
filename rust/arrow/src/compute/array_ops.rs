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

/// Returns the array, taking only the elements matching the filter
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

/// Returns the array, taking only the number of elements specified
///
/// Limit performs a zero-copy slice of the array, and is a convenience method on slice
/// where:
/// * it performs a bounds-check on the array
/// * it slices from offset 0
pub fn limit(array: &ArrayRef, num_elements: usize) -> Result<ArrayRef> {
    let lim = num_elements.min(array.len());
    Ok(array.slice(0, lim))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::*;
    use crate::buffer::Buffer;
    use crate::datatypes::{Field, ToByteSlice};
    use crate::util::bit_util;

    use std::sync::Arc;

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
        let a: ArrayRef = Arc::new(Int32Array::from(vec![5, 6, 7, 8, 9]));
        let b = limit(&a, 3).unwrap();
        let c = b.as_ref().as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(3, c.len());
        assert_eq!(5, c.value(0));
        assert_eq!(6, c.value(1));
        assert_eq!(7, c.value(2));
    }

    #[test]
    fn test_limit_binary_array() {
        let a: ArrayRef = Arc::new(BinaryArray::from(vec!["hello", " ", "world", "!"]));
        let b = limit(&a, 2).unwrap();
        let c = b.as_ref().as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(2, c.len());
        assert_eq!("hello", c.get_string(0));
        assert_eq!(" ", c.get_string(1));
    }

    #[test]
    fn test_limit_array_with_null() {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![None, Some(5)]));
        let b = limit(&a, 1).unwrap();
        let c = b.as_ref().as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(1, c.len());
        assert_eq!(true, c.is_null(0));
    }

    #[test]
    fn test_limit_array_with_limit_too_large() {
        let a = Int32Array::from(vec![5, 6, 7, 8, 9]);
        let a_ref: ArrayRef = Arc::new(a);
        let b = limit(&a_ref, 6).unwrap();
        let c = b.as_ref().as_any().downcast_ref::<Int32Array>().unwrap();

        assert_eq!(5, c.len());
        assert_eq!(5, c.value(0));
        assert_eq!(6, c.value(1));
        assert_eq!(7, c.value(2));
        assert_eq!(8, c.value(3));
        assert_eq!(9, c.value(4));
    }

    #[test]
    fn test_list_array_limit() {
        // adapted from crate::array::test::test_list_array_slice
        // Construct a value array
        let value_data = ArrayData::builder(DataType::Int32)
            .len(10)
            .add_buffer(Buffer::from(
                &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9].to_byte_slice(),
            ))
            .build();

        // Construct a buffer for value offsets, for the nested array:
        //  [[0, 1], null, [2, 3], null, [4, 5], null, [6, 7, 8], null, [9]]
        let value_offsets =
            Buffer::from(&[0, 2, 2, 4, 4, 6, 6, 9, 9, 10].to_byte_slice());
        // 01010101 00000001
        let mut null_bits: [u8; 2] = [0; 2];
        bit_util::set_bit(&mut null_bits, 0);
        bit_util::set_bit(&mut null_bits, 2);
        bit_util::set_bit(&mut null_bits, 4);
        bit_util::set_bit(&mut null_bits, 6);
        bit_util::set_bit(&mut null_bits, 8);

        // Construct a list array from the above two
        let list_data_type = DataType::List(Box::new(DataType::Int32));
        let list_data = ArrayData::builder(list_data_type.clone())
            .len(9)
            .add_buffer(value_offsets.clone())
            .add_child_data(value_data.clone())
            .null_bit_buffer(Buffer::from(null_bits))
            .build();
        let list_array: ArrayRef = Arc::new(ListArray::from(list_data));

        let limit_array = limit(&list_array, 6).unwrap();
        assert_eq!(6, limit_array.len());
        assert_eq!(0, limit_array.offset());
        assert_eq!(3, limit_array.null_count());

        // Check offset and length for each non-null value.
        let limit_array: &ListArray =
            limit_array.as_any().downcast_ref::<ListArray>().unwrap();
        for i in 0..limit_array.len() {
            let offset = limit_array.value_offset(i);
            let length = limit_array.value_length(i);
            if i % 2 == 0 {
                assert_eq!(2, length);
                assert_eq!(i as i32, offset);
            } else {
                assert_eq!(0, length);
            }
        }
    }

    #[test]
    fn test_struct_array_limit() {
        // adapted from crate::array::test::test_struct_array_slice
        let boolean_data = ArrayData::builder(DataType::Boolean)
            .len(5)
            .add_buffer(Buffer::from([0b00010000]))
            .null_bit_buffer(Buffer::from([0b00010001]))
            .build();
        let int_data = ArrayData::builder(DataType::Int32)
            .len(5)
            .add_buffer(Buffer::from([0, 28, 42, 0, 0].to_byte_slice()))
            .null_bit_buffer(Buffer::from([0b00000110]))
            .build();

        let mut field_types = vec![];
        field_types.push(Field::new("a", DataType::Boolean, false));
        field_types.push(Field::new("b", DataType::Int32, false));
        let struct_array_data = ArrayData::builder(DataType::Struct(field_types))
            .len(5)
            .add_child_data(boolean_data.clone())
            .add_child_data(int_data.clone())
            .null_bit_buffer(Buffer::from([0b00010111]))
            .build();
        let struct_array = StructArray::from(struct_array_data);

        assert_eq!(5, struct_array.len());
        assert_eq!(1, struct_array.null_count());
        assert_eq!(boolean_data, struct_array.column(0).data());
        assert_eq!(int_data, struct_array.column(1).data());

        let array: ArrayRef = Arc::new(struct_array);

        let sliced_array = limit(&array, 3).unwrap();
        let sliced_array = sliced_array.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(3, sliced_array.len());
        assert_eq!(0, sliced_array.offset());
        assert_eq!(0, sliced_array.null_count());
        assert!(sliced_array.is_valid(0));
        assert!(sliced_array.is_valid(1));
        assert!(sliced_array.is_valid(2));

        let sliced_c0 = sliced_array.column(0);
        let sliced_c0 = sliced_c0.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(3, sliced_c0.len());
        assert_eq!(0, sliced_c0.offset());
        assert_eq!(2, sliced_c0.null_count());
        assert!(sliced_c0.is_valid(0));
        assert!(sliced_c0.is_null(1));
        assert!(sliced_c0.is_null(2));
        assert_eq!(false, sliced_c0.value(0));

        let sliced_c1 = sliced_array.column(1);
        let sliced_c1 = sliced_c1.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(3, sliced_c1.len());
        assert_eq!(0, sliced_c1.offset());
        assert_eq!(1, sliced_c1.null_count());
        assert!(sliced_c1.is_null(0));
        assert_eq!(28, sliced_c1.value(1));
        assert_eq!(42, sliced_c1.value(2));
    }
}
