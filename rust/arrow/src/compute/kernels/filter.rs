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

//! Defines miscellaneous array kernels.

use std::sync::Arc;

use crate::array::*;
use crate::datatypes::*;
use crate::error::{ArrowError, Result};

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

macro_rules! filter_primitive_item_list_array {
    ($array:expr, $filter:expr, $item_type:ident) => {{
        let list_of_lists = $array.as_any().downcast_ref::<ListArray>().unwrap();
        let values_builder = PrimitiveBuilder::<$item_type>::new(list_of_lists.len());
        let mut builder = ListBuilder::new(values_builder);
        for i in 0..list_of_lists.len() {
            if $filter.value(i) {
                if list_of_lists.is_null(i) {
                    builder.append(false)?;
                } else {
                    let this_inner_list = list_of_lists.value(i);
                    let inner_list = this_inner_list
                        .as_any()
                        .downcast_ref::<PrimitiveArray<$item_type>>()
                        .unwrap();
                    for j in 0..inner_list.len() {
                        if inner_list.is_null(j) {
                            builder.values().append_null()?;
                        } else {
                            builder.values().append_value(inner_list.value(j))?;
                        }
                    }
                    builder.append(true)?;
                }
            }
        }
        Ok(Arc::new(builder.finish()))
    }};
}

macro_rules! filter_non_primitive_item_list_array {
    ($array:expr, $filter:expr, $item_array_type:ident, $item_builder:ident) => {{
        let list_of_lists = $array.as_any().downcast_ref::<ListArray>().unwrap();
        let values_builder = $item_builder::new(list_of_lists.len());
        let mut builder = ListBuilder::new(values_builder);
        for i in 0..list_of_lists.len() {
            if $filter.value(i) {
                if list_of_lists.is_null(i) {
                    builder.append(false)?;
                } else {
                    let this_inner_list = list_of_lists.value(i);
                    let inner_list = this_inner_list
                        .as_any()
                        .downcast_ref::<$item_array_type>()
                        .unwrap();
                    for j in 0..inner_list.len() {
                        if inner_list.is_null(j) {
                            builder.values().append_null()?;
                        } else {
                            builder.values().append_value(inner_list.value(j))?;
                        }
                    }
                    builder.append(true)?;
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
        DataType::Date32(_) => filter_array!(array, filter, Date32Array),
        DataType::Date64(_) => filter_array!(array, filter, Date64Array),
        DataType::Time32(TimeUnit::Second) => {
            filter_array!(array, filter, Time32SecondArray)
        }
        DataType::Time32(TimeUnit::Millisecond) => {
            filter_array!(array, filter, Time32MillisecondArray)
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            filter_array!(array, filter, Time64MicrosecondArray)
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            filter_array!(array, filter, Time64NanosecondArray)
        }
        DataType::Duration(TimeUnit::Second) => {
            filter_array!(array, filter, DurationSecondArray)
        }
        DataType::Duration(TimeUnit::Millisecond) => {
            filter_array!(array, filter, DurationMillisecondArray)
        }
        DataType::Duration(TimeUnit::Microsecond) => {
            filter_array!(array, filter, DurationMicrosecondArray)
        }
        DataType::Duration(TimeUnit::Nanosecond) => {
            filter_array!(array, filter, DurationNanosecondArray)
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            filter_array!(array, filter, TimestampSecondArray)
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            filter_array!(array, filter, TimestampMillisecondArray)
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            filter_array!(array, filter, TimestampMicrosecondArray)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            filter_array!(array, filter, TimestampNanosecondArray)
        }
        DataType::List(dt) => match &**dt {
            DataType::UInt8 => {
                filter_primitive_item_list_array!(array, filter, UInt8Type)
            }
            DataType::UInt16 => {
                filter_primitive_item_list_array!(array, filter, UInt16Type)
            }
            DataType::UInt32 => {
                filter_primitive_item_list_array!(array, filter, UInt32Type)
            }
            DataType::UInt64 => {
                filter_primitive_item_list_array!(array, filter, UInt64Type)
            }
            DataType::Int8 => filter_primitive_item_list_array!(array, filter, Int8Type),
            DataType::Int16 => {
                filter_primitive_item_list_array!(array, filter, Int16Type)
            }
            DataType::Int32 => {
                filter_primitive_item_list_array!(array, filter, Int32Type)
            }
            DataType::Int64 => {
                filter_primitive_item_list_array!(array, filter, Int64Type)
            }
            DataType::Float32 => {
                filter_primitive_item_list_array!(array, filter, Float32Type)
            }
            DataType::Float64 => {
                filter_primitive_item_list_array!(array, filter, Float64Type)
            }
            DataType::Boolean => {
                filter_primitive_item_list_array!(array, filter, BooleanType)
            }
            DataType::Date32(_) => {
                filter_primitive_item_list_array!(array, filter, Date32Type)
            }
            DataType::Date64(_) => {
                filter_primitive_item_list_array!(array, filter, Date64Type)
            }
            DataType::Time32(TimeUnit::Second) => {
                filter_primitive_item_list_array!(array, filter, Time32SecondType)
            }
            DataType::Time32(TimeUnit::Millisecond) => {
                filter_primitive_item_list_array!(array, filter, Time32MillisecondType)
            }
            DataType::Time64(TimeUnit::Microsecond) => {
                filter_primitive_item_list_array!(array, filter, Time64MicrosecondType)
            }
            DataType::Time64(TimeUnit::Nanosecond) => {
                filter_primitive_item_list_array!(array, filter, Time64NanosecondType)
            }
            DataType::Duration(TimeUnit::Second) => {
                filter_primitive_item_list_array!(array, filter, DurationSecondType)
            }
            DataType::Duration(TimeUnit::Millisecond) => {
                filter_primitive_item_list_array!(array, filter, DurationMillisecondType)
            }
            DataType::Duration(TimeUnit::Microsecond) => {
                filter_primitive_item_list_array!(array, filter, DurationMicrosecondType)
            }
            DataType::Duration(TimeUnit::Nanosecond) => {
                filter_primitive_item_list_array!(array, filter, DurationNanosecondType)
            }
            DataType::Timestamp(TimeUnit::Second, _) => {
                filter_primitive_item_list_array!(array, filter, TimestampSecondType)
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                filter_primitive_item_list_array!(array, filter, TimestampMillisecondType)
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                filter_primitive_item_list_array!(array, filter, TimestampMicrosecondType)
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                filter_primitive_item_list_array!(array, filter, TimestampNanosecondType)
            }
            DataType::Binary => filter_non_primitive_item_list_array!(
                array,
                filter,
                BinaryArray,
                BinaryBuilder
            ),
            DataType::Utf8 => filter_non_primitive_item_list_array!(
                array,
                filter,
                StringArray,
                StringBuilder
            ),
            other => {
                return Err(ArrowError::ComputeError(format!(
                    "filter not supported for List({:?})",
                    other
                )));
            }
        },
        DataType::Binary => {
            let b = array.as_any().downcast_ref::<BinaryArray>().unwrap();
            let mut values: Vec<&[u8]> = Vec::with_capacity(b.len());
            for i in 0..b.len() {
                if filter.value(i) {
                    values.push(b.value(i));
                }
            }
            Ok(Arc::new(BinaryArray::from(values)))
        }
        DataType::Utf8 => {
            let b = array.as_any().downcast_ref::<StringArray>().unwrap();
            let mut values: Vec<&str> = Vec::with_capacity(b.len());
            for i in 0..b.len() {
                if filter.value(i) {
                    values.push(b.value(i));
                }
            }
            Ok(Arc::new(StringArray::from(values)))
        }
        other => Err(ArrowError::ComputeError(format!(
            "filter not supported for {:?}",
            other
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::Buffer;
    use crate::datatypes::ToByteSlice;

    macro_rules! def_temporal_test {
        ($test:ident, $array_type: ident, $data: expr) => {
            #[test]
            fn $test() {
                let a = $data;
                let b = BooleanArray::from(vec![true, false, true, false]);
                let c = filter(&a, &b).unwrap();
                let d = c.as_ref().as_any().downcast_ref::<$array_type>().unwrap();
                assert_eq!(2, d.len());
                assert_eq!(1, d.value(0));
                assert_eq!(3, d.value(1));
            }
        };
    }

    def_temporal_test!(
        test_filter_date32,
        Date32Array,
        Date32Array::from(vec![1, 2, 3, 4])
    );
    def_temporal_test!(
        test_filter_date64,
        Date64Array,
        Date64Array::from(vec![1, 2, 3, 4])
    );
    def_temporal_test!(
        test_filter_time32_second,
        Time32SecondArray,
        Time32SecondArray::from(vec![1, 2, 3, 4])
    );
    def_temporal_test!(
        test_filter_time32_millisecond,
        Time32MillisecondArray,
        Time32MillisecondArray::from(vec![1, 2, 3, 4])
    );
    def_temporal_test!(
        test_filter_time64_microsecond,
        Time64MicrosecondArray,
        Time64MicrosecondArray::from(vec![1, 2, 3, 4])
    );
    def_temporal_test!(
        test_filter_time64_nanosecond,
        Time64NanosecondArray,
        Time64NanosecondArray::from(vec![1, 2, 3, 4])
    );
    def_temporal_test!(
        test_filter_duration_second,
        DurationSecondArray,
        DurationSecondArray::from(vec![1, 2, 3, 4])
    );
    def_temporal_test!(
        test_filter_duration_millisecond,
        DurationMillisecondArray,
        DurationMillisecondArray::from(vec![1, 2, 3, 4])
    );
    def_temporal_test!(
        test_filter_duration_microsecond,
        DurationMicrosecondArray,
        DurationMicrosecondArray::from(vec![1, 2, 3, 4])
    );
    def_temporal_test!(
        test_filter_duration_nanosecond,
        DurationNanosecondArray,
        DurationNanosecondArray::from(vec![1, 2, 3, 4])
    );
    def_temporal_test!(
        test_filter_timestamp_second,
        TimestampSecondArray,
        TimestampSecondArray::from_vec(vec![1, 2, 3, 4], None)
    );
    def_temporal_test!(
        test_filter_timestamp_millisecond,
        TimestampMillisecondArray,
        TimestampMillisecondArray::from_vec(vec![1, 2, 3, 4], None)
    );
    def_temporal_test!(
        test_filter_timestamp_microsecond,
        TimestampMicrosecondArray,
        TimestampMicrosecondArray::from_vec(vec![1, 2, 3, 4], None)
    );
    def_temporal_test!(
        test_filter_timestamp_nanosecond,
        TimestampNanosecondArray,
        TimestampNanosecondArray::from_vec(vec![1, 2, 3, 4], None)
    );

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
    fn test_filter_string_array() {
        let a = StringArray::from(vec!["hello", " ", "world", "!"]);
        let b = BooleanArray::from(vec![true, false, true, false]);
        let c = filter(&a, &b).unwrap();
        let d = c.as_ref().as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(2, d.len());
        assert_eq!("hello", d.value(0));
        assert_eq!("world", d.value(1));
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
    fn test_filter_list_array() {
        let value_data = ArrayData::builder(DataType::Int32)
            .len(8)
            .add_buffer(Buffer::from(&[0, 1, 2, 3, 4, 5, 6, 7].to_byte_slice()))
            .build();

        let value_offsets = Buffer::from(&[0, 3, 6, 8, 8].to_byte_slice());

        let list_data_type = DataType::List(Box::new(DataType::Int32));
        let list_data = ArrayData::builder(list_data_type.clone())
            .len(4)
            .add_buffer(value_offsets.clone())
            .add_child_data(value_data.clone())
            .null_bit_buffer(Buffer::from([0b00000111]))
            .build();

        //  a = [[0, 1, 2], [3, 4, 5], [6, 7], null]
        let a = ListArray::from(list_data);
        let b = BooleanArray::from(vec![false, true, false, true]);
        let c = filter(&a, &b).unwrap();
        let d = c.as_ref().as_any().downcast_ref::<ListArray>().unwrap();

        assert_eq!(DataType::Int32, d.value_type());

        // result should be [[3, 4, 5], null]
        assert_eq!(2, d.len());
        assert_eq!(1, d.null_count());
        assert_eq!(true, d.is_null(1));

        assert_eq!(0, d.value_offset(0));
        assert_eq!(3, d.value_length(0));
        assert_eq!(3, d.value_offset(1));
        assert_eq!(0, d.value_length(1));
        assert_eq!(
            Buffer::from(&[3, 4, 5].to_byte_slice()),
            d.values().data().buffers()[0].clone()
        );
        assert_eq!(
            Buffer::from(&[0, 3, 3].to_byte_slice()),
            d.data().buffers()[0].clone()
        );
        let inner_list = d.value(0);
        let inner_list = inner_list.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(3, inner_list.len());
        assert_eq!(0, inner_list.null_count());
        assert_eq!(inner_list, &Int32Array::from(vec![3, 4, 5]));
    }
}
