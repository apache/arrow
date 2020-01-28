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

//! Defines sort kernel for `ArrayRef`

use crate::array::*;
use crate::compute::take;
use crate::datatypes::*;
use crate::error::{ArrowError, Result};

use TimeUnit::*;

/// Sort the `ArrayRef` using `SortOptions`.
///
/// Performs a stable sort on values and indices, returning nulls after sorted valid values,
/// while preserving the order of the nulls.
///
/// Returns and `ArrowError::ComputeError(String)` if the array type is either unsupported by `sort_to_indices` or `take`.
pub fn sort(values: &ArrayRef, _options: Option<SortOptions>) -> Result<ArrayRef> {
    let indices = sort_to_indices(values, _options)?;
    take(values, &indices, None)
}

/// Sort elements from `ArrayRef` into an unsigned integer (`UInt32Array`) of indices
pub fn sort_to_indices(
    values: &ArrayRef,
    _options: Option<SortOptions>,
) -> Result<UInt32Array> {
    let range = values.offset()..values.len();
    let (v, n): (Vec<usize>, Vec<usize>) =
        range.partition(|index| values.is_valid(*index));
    match values.data_type() {
        DataType::Boolean => sort_primitive::<BooleanType>(values, v, n),
        DataType::Int8 => sort_primitive::<Int8Type>(values, v, n),
        DataType::Int16 => sort_primitive::<Int16Type>(values, v, n),
        DataType::Int32 => sort_primitive::<Int32Type>(values, v, n),
        DataType::Int64 => sort_primitive::<Int64Type>(values, v, n),
        DataType::UInt8 => sort_primitive::<UInt8Type>(values, v, n),
        DataType::UInt16 => sort_primitive::<UInt16Type>(values, v, n),
        DataType::UInt32 => sort_primitive::<UInt32Type>(values, v, n),
        DataType::UInt64 => sort_primitive::<UInt64Type>(values, v, n),
        // DataType::Float32 => sort_primitive::<Float32Type>(values, v, n),
        // DataType::Float64 => sort_primitive::<Float64Type>(values, v, n),
        DataType::Date32(_) => sort_primitive::<Date32Type>(values, v, n),
        DataType::Date64(_) => sort_primitive::<Date64Type>(values, v, n),
        DataType::Time32(Second) => sort_primitive::<Time32SecondType>(values, v, n),
        DataType::Time32(Millisecond) => {
            sort_primitive::<Time32MillisecondType>(values, v, n)
        }
        DataType::Time64(Microsecond) => {
            sort_primitive::<Time64MicrosecondType>(values, v, n)
        }
        DataType::Time64(Nanosecond) => {
            sort_primitive::<Time64NanosecondType>(values, v, n)
        }
        DataType::Timestamp(Second, _) => {
            sort_primitive::<TimestampSecondType>(values, v, n)
        }
        DataType::Timestamp(Millisecond, _) => {
            sort_primitive::<TimestampMillisecondType>(values, v, n)
        }
        DataType::Timestamp(Microsecond, _) => {
            sort_primitive::<TimestampMicrosecondType>(values, v, n)
        }
        DataType::Timestamp(Nanosecond, _) => {
            sort_primitive::<TimestampNanosecondType>(values, v, n)
        }
        DataType::Interval(IntervalUnit::YearMonth) => {
            sort_primitive::<IntervalYearMonthType>(values, v, n)
        }
        DataType::Interval(IntervalUnit::DayTime) => {
            sort_primitive::<IntervalDayTimeType>(values, v, n)
        }
        DataType::Duration(TimeUnit::Second) => {
            sort_primitive::<DurationSecondType>(values, v, n)
        }
        DataType::Duration(TimeUnit::Millisecond) => {
            sort_primitive::<DurationMillisecondType>(values, v, n)
        }
        DataType::Duration(TimeUnit::Microsecond) => {
            sort_primitive::<DurationMicrosecondType>(values, v, n)
        }
        DataType::Duration(TimeUnit::Nanosecond) => {
            sort_primitive::<DurationNanosecondType>(values, v, n)
        }
        DataType::Utf8 => sort_string(values, v, n),
        t @ _ => Err(ArrowError::ComputeError(format!(
            "Sort not supported for data type {:?}",
            t
        ))),
    }
}

/// Options that define how sort kernels should behave
#[derive(Clone)]
pub struct SortOptions {}

/// Sort primitive values
fn sort_primitive<T>(
    values: &ArrayRef,
    value_indices: Vec<usize>,
    null_indices: Vec<usize>,
) -> Result<UInt32Array>
where
    T: ArrowPrimitiveType,
    T::Native: std::cmp::Ord,
{
    let values = values.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
    // create tuples that are used for sorting
    let mut valids = value_indices
        .into_iter()
        .map(|index| (index as u32, values.value(index)))
        .collect::<Vec<(u32, T::Native)>>();
    valids.sort_by_key(|a| a.1);
    // collect the order of valid tuples
    let mut valid_indices: Vec<u32> = valids.iter().map(|tuple| tuple.0).collect();

    // no need to sort nulls as they are in the correct order already
    valid_indices.append(&mut null_indices.into_iter().map(|i| i as u32).collect());

    Ok(UInt32Array::from(valid_indices))
}

/// Sort strings
fn sort_string(
    values: &ArrayRef,
    value_indices: Vec<usize>,
    null_indices: Vec<usize>,
) -> Result<UInt32Array> {
    let values = values.as_any().downcast_ref::<StringArray>().unwrap();
    let mut valids = value_indices
        .into_iter()
        .map(|index| (index as u32, values.value(index)))
        .collect::<Vec<(u32, &str)>>();
    valids.sort_by_key(|a| a.1);
    // collect the order of valid tuplies
    let mut valid_indices: Vec<u32> = valids.iter().map(|tuple| tuple.0).collect();

    // no need to sort nulls as they are in the correct order already
    valid_indices.append(&mut null_indices.into_iter().map(|i| i as u32).collect());

    Ok(UInt32Array::from(valid_indices))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn test_sort_to_indices_primitive_arrays<T>(
        data: Vec<Option<T::Native>>,
        options: Option<SortOptions>,
        expected_data: Vec<u32>,
    ) where
        T: ArrowPrimitiveType,
        PrimitiveArray<T>: From<Vec<Option<T::Native>>> + ArrayEqual,
    {
        let output = PrimitiveArray::<T>::from(data);
        let expected = UInt32Array::from(expected_data);
        let output = sort_to_indices(&(Arc::new(output) as ArrayRef), options).unwrap();
        assert!(output.equals(&expected))
    }

    fn test_sort_primitive_arrays<T>(
        data: Vec<Option<T::Native>>,
        options: Option<SortOptions>,
        expected_data: Vec<Option<T::Native>>,
    ) where
        T: ArrowPrimitiveType,
        PrimitiveArray<T>: From<Vec<Option<T::Native>>> + ArrayEqual,
    {
        let output = PrimitiveArray::<T>::from(data);
        let expected = PrimitiveArray::<T>::from(expected_data);
        let output = sort(&(Arc::new(output) as ArrayRef), options).unwrap();
        let output = output.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
        assert!(output.equals(&expected))
    }

    #[test]
    fn test_sort_to_indices_primitives() {
        // int8
        test_sort_to_indices_primitive_arrays::<Int8Type>(
            vec![None, Some(0), Some(2), Some(-1), Some(0), None],
            None,
            vec![3, 1, 4, 2, 0, 5],
        );

        // boolean
        test_sort_to_indices_primitive_arrays::<BooleanType>(
            vec![None, Some(false), Some(true), Some(true), Some(false), None],
            None,
            vec![1, 4, 2, 3, 0, 5],
        );
    }

    #[test]
    fn test_sort_primitives() {
        // int8
        test_sort_primitive_arrays::<Int8Type>(
            vec![None, Some(0), Some(2), Some(-1), Some(0), None],
            None,
            vec![Some(-1), Some(0), Some(0), Some(2), None, None],
        )
    }
}
