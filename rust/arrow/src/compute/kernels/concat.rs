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

//! Defines concat kernel for `ArrayRef`
//!
//! Example:
//!
//! ```
//! use std::sync::Arc;
//! use arrow::array::{ArrayRef, StringArray};
//! use arrow::compute::concat;
//!
//! let arr = concat(&vec![
//!     Arc::new(StringArray::from(vec!["hello", "world"])) as ArrayRef,
//!     Arc::new(StringArray::from(vec!["!"])) as ArrayRef,
//! ]).unwrap();
//! assert_eq!(arr.len(), 3);
//! ```

use std::sync::Arc;

use crate::array::*;
use crate::datatypes::*;
use crate::error::{ArrowError, Result};

use TimeUnit::*;

/// Concatenate multiple `ArrayRef` with the same type.
///
/// Returns a new ArrayRef.
pub fn concat(array_list: &Vec<ArrayRef>) -> Result<ArrayRef> {
    let mut data_type: Option<DataType> = None;
    let array_data_list = &array_list
        .iter()
        .map(|a| {
            let array_data = a.data_ref().clone();
            let curr_data_type = array_data.data_type().clone();
            match &data_type {
                Some(t) => {
                    if t != &curr_data_type {
                        return Err(ArrowError::ComputeError(
                            "Cannot concat arrays with different data types".to_string(),
                        ));
                    }
                }
                None => {
                    data_type = Some(curr_data_type);
                }
            }
            Ok(array_data)
        })
        .collect::<Result<Vec<ArrayDataRef>>>()?;

    let data_type = match data_type {
        None => {
            return Err(ArrowError::ComputeError(
                "Cannot concat 0 array".to_string(),
            ));
        }
        Some(t) => t,
    };
    match data_type {
        DataType::Utf8 => concat_raw_string(array_data_list),
        DataType::Boolean => concat_primitive::<BooleanType>(array_data_list),
        DataType::Int8 => concat_raw_primitive::<Int8Type>(array_data_list),
        DataType::Int16 => concat_raw_primitive::<Int16Type>(array_data_list),
        DataType::Int32 => concat_raw_primitive::<Int32Type>(array_data_list),
        DataType::Int64 => concat_raw_primitive::<Int64Type>(array_data_list),
        DataType::UInt8 => concat_raw_primitive::<UInt8Type>(array_data_list),
        DataType::UInt16 => concat_raw_primitive::<UInt16Type>(array_data_list),
        DataType::UInt32 => concat_raw_primitive::<UInt32Type>(array_data_list),
        DataType::UInt64 => concat_raw_primitive::<UInt64Type>(array_data_list),
        DataType::Float32 => concat_raw_primitive::<Float32Type>(array_data_list),
        DataType::Float64 => concat_raw_primitive::<Float64Type>(array_data_list),
        DataType::Date32(_) => concat_raw_primitive::<Date32Type>(array_data_list),
        DataType::Date64(_) => concat_raw_primitive::<Date64Type>(array_data_list),
        DataType::Time32(Second) => {
            concat_raw_primitive::<Time32SecondType>(array_data_list)
        }
        DataType::Time32(Millisecond) => {
            concat_raw_primitive::<Time32MillisecondType>(array_data_list)
        }
        DataType::Time64(Microsecond) => {
            concat_raw_primitive::<Time64MicrosecondType>(array_data_list)
        }
        DataType::Time64(Nanosecond) => {
            concat_raw_primitive::<Time64NanosecondType>(array_data_list)
        }
        DataType::Timestamp(Second, _) => {
            concat_raw_primitive::<TimestampSecondType>(array_data_list)
        }
        DataType::Timestamp(Millisecond, _) => {
            concat_raw_primitive::<TimestampMillisecondType>(array_data_list)
        }
        DataType::Timestamp(Microsecond, _) => {
            concat_raw_primitive::<TimestampMicrosecondType>(array_data_list)
        }
        DataType::Timestamp(Nanosecond, _) => {
            concat_raw_primitive::<TimestampNanosecondType>(array_data_list)
        }
        DataType::Interval(IntervalUnit::YearMonth) => {
            concat_raw_primitive::<IntervalYearMonthType>(array_data_list)
        }
        DataType::Interval(IntervalUnit::DayTime) => {
            concat_raw_primitive::<IntervalDayTimeType>(array_data_list)
        }
        DataType::Duration(TimeUnit::Second) => {
            concat_raw_primitive::<DurationSecondType>(array_data_list)
        }
        DataType::Duration(TimeUnit::Millisecond) => {
            concat_raw_primitive::<DurationMillisecondType>(array_data_list)
        }
        DataType::Duration(TimeUnit::Microsecond) => {
            concat_raw_primitive::<DurationMicrosecondType>(array_data_list)
        }
        DataType::Duration(TimeUnit::Nanosecond) => {
            concat_raw_primitive::<DurationNanosecondType>(array_data_list)
        }
        t => unimplemented!("Concat not supported for data type {:?}", t),
    }
}

fn concat_primitive<T>(array_data_list: &[ArrayDataRef]) -> Result<ArrayRef>
where
    T: ArrowPrimitiveType,
{
    let rows_count = array_data_list.iter().map(|a| a.len()).sum::<usize>();
    let mut builder = PrimitiveBuilder::<T>::new(rows_count);

    for array_data in array_data_list {
        let a = PrimitiveArray::<T>::from(array_data.clone());
        for i in 0..a.len() {
            if a.is_valid(i) {
                builder.append_value(a.value(i))?;
            } else {
                builder.append_null()?;
            }
        }
    }

    Ok(Arc::new(builder.finish()) as ArrayRef)
}

// for better performance, we manually concat primitive value buffers instead of using
// PrimitiveBuilder
fn concat_raw_primitive<T>(array_data_list: &[ArrayDataRef]) -> Result<ArrayRef>
where
    T: ArrowPrimitiveType,
{
    let value_count = array_data_list.iter().map(|a| a.len()).sum::<usize>();
    let mut null_count = 0;
    let mut values_builder = BufferBuilder::<T>::new(value_count);
    let mut null_bit_builder = BooleanBufferBuilder::new(value_count);

    for array_data in array_data_list {
        null_count += array_data.null_count();
        let value_buffer = &array_data.buffers()[0];
        values_builder.write_bytes(value_buffer.data(), value_buffer.len())?;
        for i in 0..array_data.len() {
            null_bit_builder.append(array_data.is_valid(i))?;
        }
    }

    Ok(Arc::new(PrimitiveArray::<T>::from(
        ArrayData::builder(T::get_data_type())
            .len(value_count)
            .add_buffer(values_builder.finish())
            .null_count(null_count)
            .null_bit_buffer(null_bit_builder.finish())
            .build(),
    )))
}

// for better performance, we manually concat string value buffers instead of using StringBuilder
fn concat_raw_string(array_data_list: &[ArrayDataRef]) -> Result<ArrayRef> {
    let (str_count, str_size) = array_data_list
        .iter()
        .map(|d| (d.len(), d.buffers()[1].len()))
        .fold((0, 0), |acc, x| (acc.0 + x.0, acc.1 + x.1));
    let mut null_count = 0;
    let mut offsets_builder = Int32BufferBuilder::new(str_count);
    let mut null_bit_builder = BooleanBufferBuilder::new(str_count);
    let mut values_builder = UInt8BufferBuilder::new(str_size);
    let mut base_offset: i32 = 0;

    offsets_builder.append(0)?;
    for array_data in array_data_list {
        null_count += array_data.null_count();

        let buffers = array_data.buffers();
        assert_eq!(
            buffers.len(),
            2,
            "StringArray data should contain 2 buffers only (offsets and values)"
        );

        let mut last_offset = base_offset;
        let value_offsets = RawPtrBox::new(buffers[0].raw_data() as *const i32);
        for i in 0..array_data.len() {
            unsafe {
                last_offset = *(value_offsets.get().add(i + 1)) + base_offset;
            }
            offsets_builder.append(last_offset)?;
            null_bit_builder.append(array_data.is_valid(i))?;
        }
        base_offset = last_offset;

        values_builder.write_bytes(buffers[1].data(), buffers[1].len())?;
    }

    Ok(Arc::new(StringArray::from(
        ArrayData::builder(DataType::Utf8)
            .len(str_count)
            .add_buffer(offsets_builder.finish())
            .add_buffer(values_builder.finish())
            .null_count(null_count)
            .null_bit_buffer(null_bit_builder.finish())
            .build(),
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::TryFrom;

    #[test]
    fn test_concat_empty_vec() -> Result<()> {
        let re = concat(&vec![]);
        assert!(re.is_err());
        Ok(())
    }

    #[test]
    fn test_concat_incompatible_datatypes() -> Result<()> {
        let re = concat(&vec![
            Arc::new(PrimitiveArray::<Int64Type>::from(vec![
                Some(-1),
                Some(2),
                None,
            ])) as ArrayRef,
            Arc::new(
                StringArray::try_from(vec![Some("hello"), Some("bar"), Some("world")])
                    .expect("Unable to create string array"),
            ) as ArrayRef,
        ]);
        assert!(re.is_err());
        Ok(())
    }

    #[test]
    fn test_concat_string_arrays() -> Result<()> {
        let arr = concat(&vec![
            Arc::new(
                StringArray::try_from(vec![Some("hello"), Some("world")])
                    .expect("Unable to create string array"),
            ) as ArrayRef,
            Arc::new(StringArray::from(vec!["1", "2", "3", "4", "6"])) as ArrayRef,
            Arc::new(
                StringArray::try_from(vec![Some("foo"), Some("bar"), None, Some("baz")])
                    .expect("Unable to create string array"),
            ) as ArrayRef,
        ])?;

        let expected_output = Arc::new(
            StringArray::try_from(vec![
                Some("hello"),
                Some("world"),
                Some("1"),
                Some("2"),
                Some("3"),
                Some("4"),
                Some("6"),
                Some("foo"),
                Some("bar"),
                None,
                Some("baz"),
            ])
            .expect("Unable to create string array"),
        ) as ArrayRef;

        assert!(
            arr.equals(&(*expected_output)),
            "expect {:#?} to be: {:#?}",
            arr,
            &expected_output
        );

        Ok(())
    }

    #[test]
    fn test_concat_primitive_arrays() -> Result<()> {
        let arr = concat(&vec![
            Arc::new(PrimitiveArray::<Int64Type>::from(vec![
                Some(-1),
                Some(-1),
                Some(2),
                None,
                None,
            ])) as ArrayRef,
            Arc::new(PrimitiveArray::<Int64Type>::from(vec![
                Some(101),
                Some(102),
                Some(103),
                None,
            ])) as ArrayRef,
            Arc::new(PrimitiveArray::<Int64Type>::from(vec![
                Some(256),
                Some(512),
                Some(1024),
            ])) as ArrayRef,
        ])?;

        let expected_output = Arc::new(PrimitiveArray::<Int64Type>::from(vec![
            Some(-1),
            Some(-1),
            Some(2),
            None,
            None,
            Some(101),
            Some(102),
            Some(103),
            None,
            Some(256),
            Some(512),
            Some(1024),
        ])) as ArrayRef;

        assert!(
            arr.equals(&(*expected_output)),
            "expect {:#?} to be: {:#?}",
            arr,
            &expected_output
        );

        Ok(())
    }

    #[test]
    fn test_concat_boolean_primitive_arrays() -> Result<()> {
        let arr = concat(&vec![
            Arc::new(PrimitiveArray::<BooleanType>::from(vec![
                Some(true),
                Some(true),
                Some(false),
                None,
                None,
                Some(false),
            ])) as ArrayRef,
            Arc::new(PrimitiveArray::<BooleanType>::from(vec![
                None,
                Some(false),
                Some(true),
                Some(false),
            ])) as ArrayRef,
        ])?;

        let expected_output = Arc::new(PrimitiveArray::<BooleanType>::from(vec![
            Some(true),
            Some(true),
            Some(false),
            None,
            None,
            Some(false),
            None,
            Some(false),
            Some(true),
            Some(false),
        ])) as ArrayRef;

        assert!(
            arr.equals(&(*expected_output)),
            "expect {:#?} to be: {:#?}",
            arr,
            &expected_output
        );

        Ok(())
    }
}
