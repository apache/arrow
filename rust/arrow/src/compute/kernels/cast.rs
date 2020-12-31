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

//! Defines cast kernels for `ArrayRef`, to convert `Array`s between
//! supported datatypes.
//!
//! Example:
//!
//! ```
//! use arrow::array::*;
//! use arrow::compute::cast;
//! use arrow::datatypes::DataType;
//! use std::sync::Arc;
//!
//! let a = Int32Array::from(vec![5, 6, 7]);
//! let array = Arc::new(a) as ArrayRef;
//! let b = cast(&array, &DataType::Float64).unwrap();
//! let c = b.as_any().downcast_ref::<Float64Array>().unwrap();
//! assert_eq!(5.0, c.value(0));
//! assert_eq!(6.0, c.value(1));
//! assert_eq!(7.0, c.value(2));
//! ```

use std::str;
use std::sync::Arc;

use crate::buffer::Buffer;
use crate::compute::kernels::arithmetic::{divide, multiply};
use crate::datatypes::*;
use crate::error::{ArrowError, Result};
use crate::{array::*, compute::take};

/// Return true if a value of type `from_type` can be cast into a
/// value of `to_type`. Note that such as cast may be lossy.
///
/// If this function returns true to stay consistent with the `cast` kernel below.
pub fn can_cast_types(from_type: &DataType, to_type: &DataType) -> bool {
    use self::DataType::*;
    if from_type == to_type {
        return true;
    }

    match (from_type, to_type) {
        (Struct(_), _) => false,
        (_, Struct(_)) => false,
        (List(list_from), List(list_to)) => {
            can_cast_types(list_from.data_type(), list_to.data_type())
        }
        (List(_), _) => false,
        (_, List(list_to)) => can_cast_types(from_type, list_to.data_type()),
        (Dictionary(_, from_value_type), Dictionary(_, to_value_type)) => {
            can_cast_types(from_value_type, to_value_type)
        }
        (Dictionary(_, value_type), _) => can_cast_types(value_type, to_type),
        (_, Dictionary(_, value_type)) => can_cast_types(from_type, value_type),

        (_, Boolean) => DataType::is_numeric(from_type),
        (Boolean, _) => DataType::is_numeric(to_type) || to_type == &Utf8,

        (Utf8, Date32(DateUnit::Day)) => true,
        (Utf8, Date64(DateUnit::Millisecond)) => true,
        (Utf8, _) => DataType::is_numeric(to_type),
        (_, Utf8) => DataType::is_numeric(from_type) || from_type == &Binary,

        // start numeric casts
        (UInt8, UInt16) => true,
        (UInt8, UInt32) => true,
        (UInt8, UInt64) => true,
        (UInt8, Int8) => true,
        (UInt8, Int16) => true,
        (UInt8, Int32) => true,
        (UInt8, Int64) => true,
        (UInt8, Float32) => true,
        (UInt8, Float64) => true,

        (UInt16, UInt8) => true,
        (UInt16, UInt32) => true,
        (UInt16, UInt64) => true,
        (UInt16, Int8) => true,
        (UInt16, Int16) => true,
        (UInt16, Int32) => true,
        (UInt16, Int64) => true,
        (UInt16, Float32) => true,
        (UInt16, Float64) => true,

        (UInt32, UInt8) => true,
        (UInt32, UInt16) => true,
        (UInt32, UInt64) => true,
        (UInt32, Int8) => true,
        (UInt32, Int16) => true,
        (UInt32, Int32) => true,
        (UInt32, Int64) => true,
        (UInt32, Float32) => true,
        (UInt32, Float64) => true,

        (UInt64, UInt8) => true,
        (UInt64, UInt16) => true,
        (UInt64, UInt32) => true,
        (UInt64, Int8) => true,
        (UInt64, Int16) => true,
        (UInt64, Int32) => true,
        (UInt64, Int64) => true,
        (UInt64, Float32) => true,
        (UInt64, Float64) => true,

        (Int8, UInt8) => true,
        (Int8, UInt16) => true,
        (Int8, UInt32) => true,
        (Int8, UInt64) => true,
        (Int8, Int16) => true,
        (Int8, Int32) => true,
        (Int8, Int64) => true,
        (Int8, Float32) => true,
        (Int8, Float64) => true,

        (Int16, UInt8) => true,
        (Int16, UInt16) => true,
        (Int16, UInt32) => true,
        (Int16, UInt64) => true,
        (Int16, Int8) => true,
        (Int16, Int32) => true,
        (Int16, Int64) => true,
        (Int16, Float32) => true,
        (Int16, Float64) => true,

        (Int32, UInt8) => true,
        (Int32, UInt16) => true,
        (Int32, UInt32) => true,
        (Int32, UInt64) => true,
        (Int32, Int8) => true,
        (Int32, Int16) => true,
        (Int32, Int64) => true,
        (Int32, Float32) => true,
        (Int32, Float64) => true,

        (Int64, UInt8) => true,
        (Int64, UInt16) => true,
        (Int64, UInt32) => true,
        (Int64, UInt64) => true,
        (Int64, Int8) => true,
        (Int64, Int16) => true,
        (Int64, Int32) => true,
        (Int64, Float32) => true,
        (Int64, Float64) => true,

        (Float32, UInt8) => true,
        (Float32, UInt16) => true,
        (Float32, UInt32) => true,
        (Float32, UInt64) => true,
        (Float32, Int8) => true,
        (Float32, Int16) => true,
        (Float32, Int32) => true,
        (Float32, Int64) => true,
        (Float32, Float64) => true,

        (Float64, UInt8) => true,
        (Float64, UInt16) => true,
        (Float64, UInt32) => true,
        (Float64, UInt64) => true,
        (Float64, Int8) => true,
        (Float64, Int16) => true,
        (Float64, Int32) => true,
        (Float64, Int64) => true,
        (Float64, Float32) => true,
        // end numeric casts

        // temporal casts
        (Int32, Date32(_)) => true,
        (Int32, Time32(_)) => true,
        (Date32(_), Int32) => true,
        (Time32(_), Int32) => true,
        (Int64, Date64(_)) => true,
        (Int64, Time64(_)) => true,
        (Date64(_), Int64) => true,
        (Time64(_), Int64) => true,
        (Date32(DateUnit::Day), Date64(DateUnit::Millisecond)) => true,
        (Date64(DateUnit::Millisecond), Date32(DateUnit::Day)) => true,
        (Time32(TimeUnit::Second), Time32(TimeUnit::Millisecond)) => true,
        (Time32(TimeUnit::Millisecond), Time32(TimeUnit::Second)) => true,
        (Time32(_), Time64(_)) => true,
        (Time64(TimeUnit::Microsecond), Time64(TimeUnit::Nanosecond)) => true,
        (Time64(TimeUnit::Nanosecond), Time64(TimeUnit::Microsecond)) => true,
        (Time64(_), Time32(to_unit)) => {
            matches!(to_unit, TimeUnit::Second | TimeUnit::Millisecond)
        }
        (Timestamp(_, _), Int64) => true,
        (Int64, Timestamp(_, _)) => true,
        (Timestamp(_, _), Timestamp(_, _)) => true,
        (Timestamp(_, _), Date32(_)) => true,
        (Timestamp(_, _), Date64(_)) => true,
        // date64 to timestamp might not make sense,
        (Int64, Duration(_)) => true,
        (Null, Int32) => true,
        (_, _) => false,
    }
}

/// Cast `array` to the provided data type and return a new Array with
/// type `to_type`, if possible.
///
/// Behavior:
/// * Boolean to Utf8: `true` => '1', `false` => `0`
/// * Utf8 to numeric: strings that can't be parsed to numbers return null, float strings
///   in integer casts return null
/// * Numeric to boolean: 0 returns `false`, any other value returns `true`
/// * List to List: the underlying data type is cast
/// * Primitive to List: a list array with 1 value per slot is created
/// * Date32 and Date64: precision lost when going to higher interval
/// * Time32 and Time64: precision lost when going to higher interval
/// * Timestamp and Date{32|64}: precision lost when going to higher interval
/// * Temporal to/from backing primitive: zero-copy with data type change
///
/// Unsupported Casts
/// * To or from `StructArray`
/// * List to primitive
/// * Utf8 to boolean
/// * Interval and duration
pub fn cast(array: &ArrayRef, to_type: &DataType) -> Result<ArrayRef> {
    use DataType::*;
    let from_type = array.data_type();

    // clone array if types are the same
    if from_type == to_type {
        return Ok(array.clone());
    }
    match (from_type, to_type) {
        (Struct(_), _) => Err(ArrowError::ComputeError(
            "Cannot cast from struct to other types".to_string(),
        )),
        (_, Struct(_)) => Err(ArrowError::ComputeError(
            "Cannot cast to struct from other types".to_string(),
        )),
        (List(_), List(ref to)) => {
            let data = array.data_ref();
            let underlying_array = make_array(data.child_data()[0].clone());
            let cast_array = cast(&underlying_array, to.data_type())?;
            let array_data = ArrayData::new(
                to.data_type().clone(),
                array.len(),
                Some(cast_array.null_count()),
                cast_array
                    .data()
                    .null_bitmap()
                    .clone()
                    .map(|bitmap| bitmap.bits),
                array.offset(),
                // reuse offset buffer
                data.buffers().to_vec(),
                vec![cast_array.data()],
            );
            let list = ListArray::from(Arc::new(array_data));
            Ok(Arc::new(list) as ArrayRef)
        }
        (List(_), _) => Err(ArrowError::ComputeError(
            "Cannot cast list to non-list data types".to_string(),
        )),
        (_, List(ref to)) => {
            // cast primitive to list's primitive
            let cast_array = cast(array, to.data_type())?;
            // create offsets, where if array.len() = 2, we have [0,1,2]
            let offsets: Vec<i32> = (0..=array.len() as i32).collect();
            let value_offsets = Buffer::from(offsets[..].to_byte_slice());
            let list_data = ArrayData::new(
                to.data_type().clone(),
                array.len(),
                Some(cast_array.null_count()),
                cast_array
                    .data()
                    .null_bitmap()
                    .clone()
                    .map(|bitmap| bitmap.bits),
                0,
                vec![value_offsets],
                vec![cast_array.data()],
            );
            let list_array = Arc::new(ListArray::from(Arc::new(list_data))) as ArrayRef;

            Ok(list_array)
        }
        (Dictionary(index_type, _), _) => match **index_type {
            DataType::Int8 => dictionary_cast::<Int8Type>(array, to_type),
            DataType::Int16 => dictionary_cast::<Int16Type>(array, to_type),
            DataType::Int32 => dictionary_cast::<Int32Type>(array, to_type),
            DataType::Int64 => dictionary_cast::<Int64Type>(array, to_type),
            DataType::UInt8 => dictionary_cast::<UInt8Type>(array, to_type),
            DataType::UInt16 => dictionary_cast::<UInt16Type>(array, to_type),
            DataType::UInt32 => dictionary_cast::<UInt32Type>(array, to_type),
            DataType::UInt64 => dictionary_cast::<UInt64Type>(array, to_type),
            _ => Err(ArrowError::ComputeError(format!(
                "Casting from dictionary type {:?} to {:?} not supported",
                from_type, to_type,
            ))),
        },
        (_, Dictionary(index_type, value_type)) => match **index_type {
            DataType::Int8 => cast_to_dictionary::<Int8Type>(array, value_type),
            DataType::Int16 => cast_to_dictionary::<Int16Type>(array, value_type),
            DataType::Int32 => cast_to_dictionary::<Int32Type>(array, value_type),
            DataType::Int64 => cast_to_dictionary::<Int64Type>(array, value_type),
            DataType::UInt8 => cast_to_dictionary::<UInt8Type>(array, value_type),
            DataType::UInt16 => cast_to_dictionary::<UInt16Type>(array, value_type),
            DataType::UInt32 => cast_to_dictionary::<UInt32Type>(array, value_type),
            DataType::UInt64 => cast_to_dictionary::<UInt64Type>(array, value_type),
            _ => Err(ArrowError::ComputeError(format!(
                "Casting from type {:?} to dictionary type {:?} not supported",
                from_type, to_type,
            ))),
        },
        (_, Boolean) => match from_type {
            UInt8 => cast_numeric_to_bool::<UInt8Type>(array),
            UInt16 => cast_numeric_to_bool::<UInt16Type>(array),
            UInt32 => cast_numeric_to_bool::<UInt32Type>(array),
            UInt64 => cast_numeric_to_bool::<UInt64Type>(array),
            Int8 => cast_numeric_to_bool::<Int8Type>(array),
            Int16 => cast_numeric_to_bool::<Int16Type>(array),
            Int32 => cast_numeric_to_bool::<Int32Type>(array),
            Int64 => cast_numeric_to_bool::<Int64Type>(array),
            Float32 => cast_numeric_to_bool::<Float32Type>(array),
            Float64 => cast_numeric_to_bool::<Float64Type>(array),
            Utf8 => Err(ArrowError::ComputeError(format!(
                "Casting from {:?} to {:?} not supported",
                from_type, to_type,
            ))),
            _ => Err(ArrowError::ComputeError(format!(
                "Casting from {:?} to {:?} not supported",
                from_type, to_type,
            ))),
        },
        (Boolean, _) => match to_type {
            UInt8 => cast_bool_to_numeric::<UInt8Type>(array),
            UInt16 => cast_bool_to_numeric::<UInt16Type>(array),
            UInt32 => cast_bool_to_numeric::<UInt32Type>(array),
            UInt64 => cast_bool_to_numeric::<UInt64Type>(array),
            Int8 => cast_bool_to_numeric::<Int8Type>(array),
            Int16 => cast_bool_to_numeric::<Int16Type>(array),
            Int32 => cast_bool_to_numeric::<Int32Type>(array),
            Int64 => cast_bool_to_numeric::<Int64Type>(array),
            Float32 => cast_bool_to_numeric::<Float32Type>(array),
            Float64 => cast_bool_to_numeric::<Float64Type>(array),
            Utf8 => {
                let from = array.as_any().downcast_ref::<BooleanArray>().unwrap();
                let mut b = StringBuilder::new(array.len());
                for i in 0..array.len() {
                    if array.is_null(i) {
                        b.append(false)?;
                    } else {
                        b.append_value(if from.value(i) { "1" } else { "0" })?;
                    }
                }

                Ok(Arc::new(b.finish()) as ArrayRef)
            }
            _ => Err(ArrowError::ComputeError(format!(
                "Casting from {:?} to {:?} not supported",
                from_type, to_type,
            ))),
        },
        (Utf8, _) => match to_type {
            UInt8 => cast_string_to_numeric::<UInt8Type>(array),
            UInt16 => cast_string_to_numeric::<UInt16Type>(array),
            UInt32 => cast_string_to_numeric::<UInt32Type>(array),
            UInt64 => cast_string_to_numeric::<UInt64Type>(array),
            Int8 => cast_string_to_numeric::<Int8Type>(array),
            Int16 => cast_string_to_numeric::<Int16Type>(array),
            Int32 => cast_string_to_numeric::<Int32Type>(array),
            Int64 => cast_string_to_numeric::<Int64Type>(array),
            Float32 => cast_string_to_numeric::<Float32Type>(array),
            Float64 => cast_string_to_numeric::<Float64Type>(array),
            Date32(DateUnit::Day) => {
                use chrono::Datelike;
                let string_array = array.as_any().downcast_ref::<StringArray>().unwrap();
                let mut builder = PrimitiveBuilder::<Date32Type>::new(string_array.len());
                for i in 0..string_array.len() {
                    if string_array.is_null(i) {
                        builder.append_null()?;
                    } else {
                        match string_array.value(i).parse::<chrono::NaiveDate>() {
                            Ok(date) => builder.append_value(
                                date.num_days_from_ce() - EPOCH_DAYS_FROM_CE,
                            )?,
                            Err(_) => builder.append_null()?, // not a valid date
                        };
                    }
                }
                Ok(Arc::new(builder.finish()) as ArrayRef)
            }
            Date64(DateUnit::Millisecond) => {
                let string_array = array.as_any().downcast_ref::<StringArray>().unwrap();
                let mut builder = PrimitiveBuilder::<Date64Type>::new(string_array.len());
                for i in 0..string_array.len() {
                    if string_array.is_null(i) {
                        builder.append_null()?;
                    } else {
                        match string_array.value(i).parse::<chrono::NaiveDateTime>() {
                            Ok(date_time) => {
                                builder.append_value(date_time.timestamp_millis())?
                            }
                            Err(_) => builder.append_null()?, // not a valid date
                        };
                    }
                }
                Ok(Arc::new(builder.finish()) as ArrayRef)
            }
            _ => Err(ArrowError::ComputeError(format!(
                "Casting from {:?} to {:?} not supported",
                from_type, to_type,
            ))),
        },
        (_, Utf8) => match from_type {
            UInt8 => cast_numeric_to_string::<UInt8Type>(array),
            UInt16 => cast_numeric_to_string::<UInt16Type>(array),
            UInt32 => cast_numeric_to_string::<UInt32Type>(array),
            UInt64 => cast_numeric_to_string::<UInt64Type>(array),
            Int8 => cast_numeric_to_string::<Int8Type>(array),
            Int16 => cast_numeric_to_string::<Int16Type>(array),
            Int32 => cast_numeric_to_string::<Int32Type>(array),
            Int64 => cast_numeric_to_string::<Int64Type>(array),
            Float32 => cast_numeric_to_string::<Float32Type>(array),
            Float64 => cast_numeric_to_string::<Float64Type>(array),
            Binary => {
                let from = array.as_any().downcast_ref::<BinaryArray>().unwrap();
                let mut b = StringBuilder::new(array.len());
                for i in 0..array.len() {
                    if array.is_null(i) {
                        b.append_null()?;
                    } else {
                        match str::from_utf8(from.value(i)) {
                            Ok(s) => b.append_value(s)?,
                            Err(_) => b.append_null()?, // not valid UTF8
                        }
                    }
                }

                Ok(Arc::new(b.finish()) as ArrayRef)
            }
            _ => Err(ArrowError::ComputeError(format!(
                "Casting from {:?} to {:?} not supported",
                from_type, to_type,
            ))),
        },

        // start numeric casts
        (UInt8, UInt16) => cast_numeric_arrays::<UInt8Type, UInt16Type>(array),
        (UInt8, UInt32) => cast_numeric_arrays::<UInt8Type, UInt32Type>(array),
        (UInt8, UInt64) => cast_numeric_arrays::<UInt8Type, UInt64Type>(array),
        (UInt8, Int8) => cast_numeric_arrays::<UInt8Type, Int8Type>(array),
        (UInt8, Int16) => cast_numeric_arrays::<UInt8Type, Int16Type>(array),
        (UInt8, Int32) => cast_numeric_arrays::<UInt8Type, Int32Type>(array),
        (UInt8, Int64) => cast_numeric_arrays::<UInt8Type, Int64Type>(array),
        (UInt8, Float32) => cast_numeric_arrays::<UInt8Type, Float32Type>(array),
        (UInt8, Float64) => cast_numeric_arrays::<UInt8Type, Float64Type>(array),

        (UInt16, UInt8) => cast_numeric_arrays::<UInt16Type, UInt8Type>(array),
        (UInt16, UInt32) => cast_numeric_arrays::<UInt16Type, UInt32Type>(array),
        (UInt16, UInt64) => cast_numeric_arrays::<UInt16Type, UInt64Type>(array),
        (UInt16, Int8) => cast_numeric_arrays::<UInt16Type, Int8Type>(array),
        (UInt16, Int16) => cast_numeric_arrays::<UInt16Type, Int16Type>(array),
        (UInt16, Int32) => cast_numeric_arrays::<UInt16Type, Int32Type>(array),
        (UInt16, Int64) => cast_numeric_arrays::<UInt16Type, Int64Type>(array),
        (UInt16, Float32) => cast_numeric_arrays::<UInt16Type, Float32Type>(array),
        (UInt16, Float64) => cast_numeric_arrays::<UInt16Type, Float64Type>(array),

        (UInt32, UInt8) => cast_numeric_arrays::<UInt32Type, UInt8Type>(array),
        (UInt32, UInt16) => cast_numeric_arrays::<UInt32Type, UInt16Type>(array),
        (UInt32, UInt64) => cast_numeric_arrays::<UInt32Type, UInt64Type>(array),
        (UInt32, Int8) => cast_numeric_arrays::<UInt32Type, Int8Type>(array),
        (UInt32, Int16) => cast_numeric_arrays::<UInt32Type, Int16Type>(array),
        (UInt32, Int32) => cast_numeric_arrays::<UInt32Type, Int32Type>(array),
        (UInt32, Int64) => cast_numeric_arrays::<UInt32Type, Int64Type>(array),
        (UInt32, Float32) => cast_numeric_arrays::<UInt32Type, Float32Type>(array),
        (UInt32, Float64) => cast_numeric_arrays::<UInt32Type, Float64Type>(array),

        (UInt64, UInt8) => cast_numeric_arrays::<UInt64Type, UInt8Type>(array),
        (UInt64, UInt16) => cast_numeric_arrays::<UInt64Type, UInt16Type>(array),
        (UInt64, UInt32) => cast_numeric_arrays::<UInt64Type, UInt32Type>(array),
        (UInt64, Int8) => cast_numeric_arrays::<UInt64Type, Int8Type>(array),
        (UInt64, Int16) => cast_numeric_arrays::<UInt64Type, Int16Type>(array),
        (UInt64, Int32) => cast_numeric_arrays::<UInt64Type, Int32Type>(array),
        (UInt64, Int64) => cast_numeric_arrays::<UInt64Type, Int64Type>(array),
        (UInt64, Float32) => cast_numeric_arrays::<UInt64Type, Float32Type>(array),
        (UInt64, Float64) => cast_numeric_arrays::<UInt64Type, Float64Type>(array),

        (Int8, UInt8) => cast_numeric_arrays::<Int8Type, UInt8Type>(array),
        (Int8, UInt16) => cast_numeric_arrays::<Int8Type, UInt16Type>(array),
        (Int8, UInt32) => cast_numeric_arrays::<Int8Type, UInt32Type>(array),
        (Int8, UInt64) => cast_numeric_arrays::<Int8Type, UInt64Type>(array),
        (Int8, Int16) => cast_numeric_arrays::<Int8Type, Int16Type>(array),
        (Int8, Int32) => cast_numeric_arrays::<Int8Type, Int32Type>(array),
        (Int8, Int64) => cast_numeric_arrays::<Int8Type, Int64Type>(array),
        (Int8, Float32) => cast_numeric_arrays::<Int8Type, Float32Type>(array),
        (Int8, Float64) => cast_numeric_arrays::<Int8Type, Float64Type>(array),

        (Int16, UInt8) => cast_numeric_arrays::<Int16Type, UInt8Type>(array),
        (Int16, UInt16) => cast_numeric_arrays::<Int16Type, UInt16Type>(array),
        (Int16, UInt32) => cast_numeric_arrays::<Int16Type, UInt32Type>(array),
        (Int16, UInt64) => cast_numeric_arrays::<Int16Type, UInt64Type>(array),
        (Int16, Int8) => cast_numeric_arrays::<Int16Type, Int8Type>(array),
        (Int16, Int32) => cast_numeric_arrays::<Int16Type, Int32Type>(array),
        (Int16, Int64) => cast_numeric_arrays::<Int16Type, Int64Type>(array),
        (Int16, Float32) => cast_numeric_arrays::<Int16Type, Float32Type>(array),
        (Int16, Float64) => cast_numeric_arrays::<Int16Type, Float64Type>(array),

        (Int32, UInt8) => cast_numeric_arrays::<Int32Type, UInt8Type>(array),
        (Int32, UInt16) => cast_numeric_arrays::<Int32Type, UInt16Type>(array),
        (Int32, UInt32) => cast_numeric_arrays::<Int32Type, UInt32Type>(array),
        (Int32, UInt64) => cast_numeric_arrays::<Int32Type, UInt64Type>(array),
        (Int32, Int8) => cast_numeric_arrays::<Int32Type, Int8Type>(array),
        (Int32, Int16) => cast_numeric_arrays::<Int32Type, Int16Type>(array),
        (Int32, Int64) => cast_numeric_arrays::<Int32Type, Int64Type>(array),
        (Int32, Float32) => cast_numeric_arrays::<Int32Type, Float32Type>(array),
        (Int32, Float64) => cast_numeric_arrays::<Int32Type, Float64Type>(array),

        (Int64, UInt8) => cast_numeric_arrays::<Int64Type, UInt8Type>(array),
        (Int64, UInt16) => cast_numeric_arrays::<Int64Type, UInt16Type>(array),
        (Int64, UInt32) => cast_numeric_arrays::<Int64Type, UInt32Type>(array),
        (Int64, UInt64) => cast_numeric_arrays::<Int64Type, UInt64Type>(array),
        (Int64, Int8) => cast_numeric_arrays::<Int64Type, Int8Type>(array),
        (Int64, Int16) => cast_numeric_arrays::<Int64Type, Int16Type>(array),
        (Int64, Int32) => cast_numeric_arrays::<Int64Type, Int32Type>(array),
        (Int64, Float32) => cast_numeric_arrays::<Int64Type, Float32Type>(array),
        (Int64, Float64) => cast_numeric_arrays::<Int64Type, Float64Type>(array),

        (Float32, UInt8) => cast_numeric_arrays::<Float32Type, UInt8Type>(array),
        (Float32, UInt16) => cast_numeric_arrays::<Float32Type, UInt16Type>(array),
        (Float32, UInt32) => cast_numeric_arrays::<Float32Type, UInt32Type>(array),
        (Float32, UInt64) => cast_numeric_arrays::<Float32Type, UInt64Type>(array),
        (Float32, Int8) => cast_numeric_arrays::<Float32Type, Int8Type>(array),
        (Float32, Int16) => cast_numeric_arrays::<Float32Type, Int16Type>(array),
        (Float32, Int32) => cast_numeric_arrays::<Float32Type, Int32Type>(array),
        (Float32, Int64) => cast_numeric_arrays::<Float32Type, Int64Type>(array),
        (Float32, Float64) => cast_numeric_arrays::<Float32Type, Float64Type>(array),

        (Float64, UInt8) => cast_numeric_arrays::<Float64Type, UInt8Type>(array),
        (Float64, UInt16) => cast_numeric_arrays::<Float64Type, UInt16Type>(array),
        (Float64, UInt32) => cast_numeric_arrays::<Float64Type, UInt32Type>(array),
        (Float64, UInt64) => cast_numeric_arrays::<Float64Type, UInt64Type>(array),
        (Float64, Int8) => cast_numeric_arrays::<Float64Type, Int8Type>(array),
        (Float64, Int16) => cast_numeric_arrays::<Float64Type, Int16Type>(array),
        (Float64, Int32) => cast_numeric_arrays::<Float64Type, Int32Type>(array),
        (Float64, Int64) => cast_numeric_arrays::<Float64Type, Int64Type>(array),
        (Float64, Float32) => cast_numeric_arrays::<Float64Type, Float32Type>(array),
        // end numeric casts

        // temporal casts
        (Int32, Date32(_)) => cast_array_data::<Date32Type>(array, to_type.clone()),
        (Int32, Time32(TimeUnit::Second)) => {
            cast_array_data::<Time32SecondType>(array, to_type.clone())
        }
        (Int32, Time32(TimeUnit::Millisecond)) => {
            cast_array_data::<Time32MillisecondType>(array, to_type.clone())
        }
        // No support for microsecond/nanosecond with i32
        (Date32(_), Int32) => cast_array_data::<Int32Type>(array, to_type.clone()),
        (Time32(_), Int32) => cast_array_data::<Int32Type>(array, to_type.clone()),
        (Int64, Date64(_)) => cast_array_data::<Date64Type>(array, to_type.clone()),
        // No support for second/milliseconds with i64
        (Int64, Time64(TimeUnit::Microsecond)) => {
            cast_array_data::<Time64MicrosecondType>(array, to_type.clone())
        }
        (Int64, Time64(TimeUnit::Nanosecond)) => {
            cast_array_data::<Time64NanosecondType>(array, to_type.clone())
        }

        (Date64(_), Int64) => cast_array_data::<Int64Type>(array, to_type.clone()),
        (Time64(_), Int64) => cast_array_data::<Int64Type>(array, to_type.clone()),
        (Date32(DateUnit::Day), Date64(DateUnit::Millisecond)) => {
            let date_array = array.as_any().downcast_ref::<Date32Array>().unwrap();
            let mut b = Date64Builder::new(array.len());
            for i in 0..array.len() {
                if array.is_null(i) {
                    b.append_null()?;
                } else {
                    b.append_value(date_array.value(i) as i64 * MILLISECONDS_IN_DAY)?;
                }
            }

            Ok(Arc::new(b.finish()) as ArrayRef)
        }
        (Date64(DateUnit::Millisecond), Date32(DateUnit::Day)) => {
            let date_array = array.as_any().downcast_ref::<Date64Array>().unwrap();
            let mut b = Date32Builder::new(array.len());
            for i in 0..array.len() {
                if array.is_null(i) {
                    b.append_null()?;
                } else {
                    b.append_value((date_array.value(i) / MILLISECONDS_IN_DAY) as i32)?;
                }
            }

            Ok(Arc::new(b.finish()) as ArrayRef)
        }
        (Time32(TimeUnit::Second), Time32(TimeUnit::Millisecond)) => {
            let time_array = Time32MillisecondArray::from(array.data());
            let mult =
                Time32MillisecondArray::from(vec![MILLISECONDS as i32; array.len()]);
            let time32_ms = multiply(&time_array, &mult)?;

            Ok(Arc::new(time32_ms) as ArrayRef)
        }
        (Time32(TimeUnit::Millisecond), Time32(TimeUnit::Second)) => {
            let time_array = Time32SecondArray::from(array.data());
            let divisor = Time32SecondArray::from(vec![MILLISECONDS as i32; array.len()]);
            let time32_s = divide(&time_array, &divisor)?;

            Ok(Arc::new(time32_s) as ArrayRef)
        }
        (Time32(from_unit), Time64(to_unit)) => {
            let time_array = Int32Array::from(array.data());
            // note: (numeric_cast + SIMD multiply) is faster than (cast & multiply)
            let c: Int64Array = numeric_cast(&time_array);
            let from_size = time_unit_multiple(&from_unit);
            let to_size = time_unit_multiple(&to_unit);
            // from is only smaller than to if 64milli/64second don't exist
            let mult = Int64Array::from(vec![to_size / from_size; array.len()]);
            let converted = multiply(&c, &mult)?;
            let array_ref = Arc::new(converted) as ArrayRef;
            use TimeUnit::*;
            match to_unit {
                Microsecond => cast_array_data::<TimestampMicrosecondType>(
                    &array_ref,
                    to_type.clone(),
                ),
                Nanosecond => cast_array_data::<TimestampNanosecondType>(
                    &array_ref,
                    to_type.clone(),
                ),
                _ => unreachable!("array type not supported"),
            }
        }
        (Time64(TimeUnit::Microsecond), Time64(TimeUnit::Nanosecond)) => {
            let time_array = Time64NanosecondArray::from(array.data());
            let mult = Time64NanosecondArray::from(vec![MILLISECONDS; array.len()]);
            let time64_ns = multiply(&time_array, &mult)?;

            Ok(Arc::new(time64_ns) as ArrayRef)
        }
        (Time64(TimeUnit::Nanosecond), Time64(TimeUnit::Microsecond)) => {
            let time_array = Time64MicrosecondArray::from(array.data());
            let divisor = Time64MicrosecondArray::from(vec![MILLISECONDS; array.len()]);
            let time64_us = divide(&time_array, &divisor)?;

            Ok(Arc::new(time64_us) as ArrayRef)
        }
        (Time64(from_unit), Time32(to_unit)) => {
            let time_array = Int64Array::from(array.data());
            let from_size = time_unit_multiple(&from_unit);
            let to_size = time_unit_multiple(&to_unit);
            let divisor = from_size / to_size;
            match to_unit {
                TimeUnit::Second => {
                    let mut b = Time32SecondBuilder::new(array.len());
                    for i in 0..array.len() {
                        if array.is_null(i) {
                            b.append_null()?;
                        } else {
                            b.append_value(
                                (time_array.value(i) as i64 / divisor) as i32,
                            )?;
                        }
                    }

                    Ok(Arc::new(b.finish()) as ArrayRef)
                }
                TimeUnit::Millisecond => {
                    // currently can't dedup this builder [ARROW-4164]
                    let mut b = Time32MillisecondBuilder::new(array.len());
                    for i in 0..array.len() {
                        if array.is_null(i) {
                            b.append_null()?;
                        } else {
                            b.append_value(
                                (time_array.value(i) as i64 / divisor) as i32,
                            )?;
                        }
                    }

                    Ok(Arc::new(b.finish()) as ArrayRef)
                }
                _ => unreachable!("array type not supported"),
            }
        }
        (Timestamp(_, _), Int64) => cast_array_data::<Int64Type>(array, to_type.clone()),
        (Int64, Timestamp(to_unit, _)) => {
            use TimeUnit::*;
            match to_unit {
                Second => cast_array_data::<TimestampSecondType>(array, to_type.clone()),
                Millisecond => {
                    cast_array_data::<TimestampMillisecondType>(array, to_type.clone())
                }
                Microsecond => {
                    cast_array_data::<TimestampMicrosecondType>(array, to_type.clone())
                }
                Nanosecond => {
                    cast_array_data::<TimestampNanosecondType>(array, to_type.clone())
                }
            }
        }
        (Timestamp(from_unit, _), Timestamp(to_unit, _)) => {
            let time_array = Int64Array::from(array.data());
            let from_size = time_unit_multiple(&from_unit);
            let to_size = time_unit_multiple(&to_unit);
            // we either divide or multiply, depending on size of each unit
            // units are never the same when the types are the same
            let converted = if from_size >= to_size {
                divide(
                    &time_array,
                    &Int64Array::from(vec![from_size / to_size; array.len()]),
                )?
            } else {
                multiply(
                    &time_array,
                    &Int64Array::from(vec![to_size / from_size; array.len()]),
                )?
            };
            let array_ref = Arc::new(converted) as ArrayRef;
            use TimeUnit::*;
            match to_unit {
                Second => {
                    cast_array_data::<TimestampSecondType>(&array_ref, to_type.clone())
                }
                Millisecond => cast_array_data::<TimestampMillisecondType>(
                    &array_ref,
                    to_type.clone(),
                ),
                Microsecond => cast_array_data::<TimestampMicrosecondType>(
                    &array_ref,
                    to_type.clone(),
                ),
                Nanosecond => cast_array_data::<TimestampNanosecondType>(
                    &array_ref,
                    to_type.clone(),
                ),
            }
        }
        (Timestamp(from_unit, _), Date32(_)) => {
            let time_array = Int64Array::from(array.data());
            let from_size = time_unit_multiple(&from_unit) * SECONDS_IN_DAY;
            let mut b = Date32Builder::new(array.len());
            for i in 0..array.len() {
                if array.is_null(i) {
                    b.append_null()?;
                } else {
                    b.append_value((time_array.value(i) / from_size) as i32)?;
                }
            }

            Ok(Arc::new(b.finish()) as ArrayRef)
        }
        (Timestamp(from_unit, _), Date64(_)) => {
            let from_size = time_unit_multiple(&from_unit);
            let to_size = MILLISECONDS;

            // Scale time_array by (to_size / from_size) using a
            // single integer operation, but need to avoid integer
            // math rounding down to zero

            match to_size.cmp(&from_size) {
                std::cmp::Ordering::Less => {
                    let time_array = Date64Array::from(array.data());
                    Ok(Arc::new(divide(
                        &time_array,
                        &Date64Array::from(vec![from_size / to_size; array.len()]),
                    )?) as ArrayRef)
                }
                std::cmp::Ordering::Equal => {
                    cast_array_data::<Date64Type>(array, to_type.clone())
                }
                std::cmp::Ordering::Greater => {
                    let time_array = Date64Array::from(array.data());
                    Ok(Arc::new(multiply(
                        &time_array,
                        &Date64Array::from(vec![to_size / from_size; array.len()]),
                    )?) as ArrayRef)
                }
            }
        }
        // date64 to timestamp might not make sense,
        (Int64, Duration(to_unit)) => {
            use TimeUnit::*;
            match to_unit {
                Second => cast_array_data::<DurationSecondType>(array, to_type.clone()),
                Millisecond => {
                    cast_array_data::<DurationMillisecondType>(array, to_type.clone())
                }
                Microsecond => {
                    cast_array_data::<DurationMicrosecondType>(array, to_type.clone())
                }
                Nanosecond => {
                    cast_array_data::<DurationNanosecondType>(array, to_type.clone())
                }
            }
        }

        // null to primitive/flat types
        (Null, Int32) => Ok(Arc::new(Int32Array::from(vec![None; array.len()]))),

        (_, _) => Err(ArrowError::ComputeError(format!(
            "Casting from {:?} to {:?} not supported",
            from_type, to_type,
        ))),
    }
}

/// Get the time unit as a multiple of a second
fn time_unit_multiple(unit: &TimeUnit) -> i64 {
    match unit {
        TimeUnit::Second => 1,
        TimeUnit::Millisecond => MILLISECONDS,
        TimeUnit::Microsecond => MICROSECONDS,
        TimeUnit::Nanosecond => NANOSECONDS,
    }
}

/// Number of seconds in a day
const SECONDS_IN_DAY: i64 = 86_400;
/// Number of milliseconds in a second
const MILLISECONDS: i64 = 1_000;
/// Number of microseconds in a second
const MICROSECONDS: i64 = 1_000_000;
/// Number of nanoseconds in a second
const NANOSECONDS: i64 = 1_000_000_000;
/// Number of milliseconds in a day
const MILLISECONDS_IN_DAY: i64 = SECONDS_IN_DAY * MILLISECONDS;
/// Number of days between 0001-01-01 and 1970-01-01
const EPOCH_DAYS_FROM_CE: i32 = 719_163;

/// Cast an array by changing its array_data type to the desired type
///
/// Arrays should have the same primitive data type, otherwise this should fail.
/// We do not perform this check on primitive data types as we only use this
/// function internally, where it is guaranteed to be infallible.
fn cast_array_data<TO>(array: &ArrayRef, to_type: DataType) -> Result<ArrayRef>
where
    TO: ArrowNumericType,
{
    let data = Arc::new(ArrayData::new(
        to_type,
        array.len(),
        Some(array.null_count()),
        array.data().null_bitmap().clone().map(|bitmap| bitmap.bits),
        array.data().offset(),
        array.data().buffers().to_vec(),
        vec![],
    ));
    Ok(Arc::new(PrimitiveArray::<TO>::from(data)) as ArrayRef)
}

/// Convert Array into a PrimitiveArray of type, and apply numeric cast
fn cast_numeric_arrays<FROM, TO>(from: &ArrayRef) -> Result<ArrayRef>
where
    FROM: ArrowNumericType,
    TO: ArrowNumericType,
    FROM::Native: num::NumCast,
    TO::Native: num::NumCast,
{
    Ok(Arc::new(numeric_cast::<FROM, TO>(
        from.as_any()
            .downcast_ref::<PrimitiveArray<FROM>>()
            .unwrap(),
    )))
}

/// Natural cast between numeric types
fn numeric_cast<T, R>(from: &PrimitiveArray<T>) -> PrimitiveArray<R>
where
    T: ArrowNumericType,
    R: ArrowNumericType,
    T::Native: num::NumCast,
    R::Native: num::NumCast,
{
    from.iter()
        .map(|v| v.and_then(num::cast::cast::<T::Native, R::Native>))
        .collect()
}

/// Cast numeric types to Utf8
fn cast_numeric_to_string<FROM>(array: &ArrayRef) -> Result<ArrayRef>
where
    FROM: ArrowNumericType,
    FROM::Native: std::string::ToString,
{
    numeric_to_string_cast::<FROM>(
        array
            .as_any()
            .downcast_ref::<PrimitiveArray<FROM>>()
            .unwrap(),
    )
    .map(|to| Arc::new(to) as ArrayRef)
}

fn numeric_to_string_cast<T>(from: &PrimitiveArray<T>) -> Result<StringArray>
where
    T: ArrowPrimitiveType + ArrowNumericType,
    T::Native: std::string::ToString,
{
    let mut b = StringBuilder::new(from.len());

    for i in 0..from.len() {
        if from.is_null(i) {
            b.append(false)?;
        } else {
            b.append_value(&from.value(i).to_string())?;
        }
    }

    Ok(b.finish())
}

/// Cast numeric types to Utf8
fn cast_string_to_numeric<T>(from: &ArrayRef) -> Result<ArrayRef>
where
    T: ArrowNumericType,
    <T as ArrowPrimitiveType>::Native: lexical_core::FromLexical,
{
    Ok(Arc::new(string_to_numeric_cast::<T>(
        from.as_any().downcast_ref::<StringArray>().unwrap(),
    )))
}

fn string_to_numeric_cast<T>(from: &StringArray) -> PrimitiveArray<T>
where
    T: ArrowNumericType,
    <T as ArrowPrimitiveType>::Native: lexical_core::FromLexical,
{
    (0..from.len())
        .map(|i| {
            if from.is_null(i) {
                None
            } else {
                lexical_core::parse(from.value(i).as_bytes()).ok()
            }
        })
        .collect()
}

/// Cast numeric types to Boolean
///
/// Any zero value returns `false` while non-zero returns `true`
fn cast_numeric_to_bool<FROM>(from: &ArrayRef) -> Result<ArrayRef>
where
    FROM: ArrowNumericType,
{
    numeric_to_bool_cast::<FROM>(
        from.as_any()
            .downcast_ref::<PrimitiveArray<FROM>>()
            .unwrap(),
    )
    .map(|to| Arc::new(to) as ArrayRef)
}

fn numeric_to_bool_cast<T>(from: &PrimitiveArray<T>) -> Result<BooleanArray>
where
    T: ArrowPrimitiveType + ArrowNumericType,
{
    let mut b = BooleanBuilder::new(from.len());

    for i in 0..from.len() {
        if from.is_null(i) {
            b.append_null()?;
        } else if from.value(i) != T::default_value() {
            b.append_value(true)?;
        } else {
            b.append_value(false)?;
        }
    }

    Ok(b.finish())
}

/// Cast Boolean types to numeric
///
/// `false` returns 0 while `true` returns 1
fn cast_bool_to_numeric<TO>(from: &ArrayRef) -> Result<ArrayRef>
where
    TO: ArrowNumericType,
    TO::Native: num::cast::NumCast,
{
    Ok(Arc::new(bool_to_numeric_cast::<TO>(
        from.as_any().downcast_ref::<BooleanArray>().unwrap(),
    )))
}

fn bool_to_numeric_cast<T>(from: &BooleanArray) -> PrimitiveArray<T>
where
    T: ArrowNumericType,
    T::Native: num::NumCast,
{
    (0..from.len())
        .map(|i| {
            if from.is_null(i) {
                None
            } else if from.value(i) {
                // a workaround to cast a primitive to T::Native, infallible
                num::cast::cast(1)
            } else {
                Some(T::default_value())
            }
        })
        .collect()
}

/// Attempts to cast an `ArrayDictionary` with index type K into
/// `to_type` for supported types.
///
/// K is the key type
fn dictionary_cast<K: ArrowDictionaryKeyType>(
    array: &ArrayRef,
    to_type: &DataType,
) -> Result<ArrayRef> {
    use DataType::*;

    match to_type {
        Dictionary(to_index_type, to_value_type) => {
            let dict_array = array
                .as_any()
                .downcast_ref::<DictionaryArray<K>>()
                .ok_or_else(|| {
                    ArrowError::ComputeError(
                        "Internal Error: Cannot cast dictionary to DictionaryArray of expected type".to_string(),
                    )
                })?;

            let keys_array: ArrayRef = Arc::new(dict_array.keys_array());
            let values_array: ArrayRef = dict_array.values();
            let cast_keys = cast(&keys_array, to_index_type)?;
            let cast_values = cast(&values_array, to_value_type)?;

            // Failure to cast keys (because they don't fit in the
            // target type) results in NULL values;
            if cast_keys.null_count() > keys_array.null_count() {
                return Err(ArrowError::ComputeError(format!(
                    "Could not convert {} dictionary indexes from {:?} to {:?}",
                    cast_keys.null_count() - keys_array.null_count(),
                    keys_array.data_type(),
                    to_index_type
                )));
            }

            // keys are data, child_data is values (dictionary)
            let data = Arc::new(ArrayData::new(
                to_type.clone(),
                cast_keys.len(),
                Some(cast_keys.null_count()),
                cast_keys
                    .data()
                    .null_bitmap()
                    .clone()
                    .map(|bitmap| bitmap.bits),
                cast_keys.data().offset(),
                cast_keys.data().buffers().to_vec(),
                vec![cast_values.data()],
            ));

            // create the appropriate array type
            let new_array: ArrayRef = match **to_index_type {
                Int8 => Arc::new(DictionaryArray::<Int8Type>::from(data)),
                Int16 => Arc::new(DictionaryArray::<Int16Type>::from(data)),
                Int32 => Arc::new(DictionaryArray::<Int32Type>::from(data)),
                Int64 => Arc::new(DictionaryArray::<Int64Type>::from(data)),
                UInt8 => Arc::new(DictionaryArray::<UInt8Type>::from(data)),
                UInt16 => Arc::new(DictionaryArray::<UInt16Type>::from(data)),
                UInt32 => Arc::new(DictionaryArray::<UInt32Type>::from(data)),
                UInt64 => Arc::new(DictionaryArray::<UInt64Type>::from(data)),
                _ => {
                    return Err(ArrowError::ComputeError(format!(
                        "Unsupported type {:?} for dictionary index",
                        to_index_type
                    )))
                }
            };

            Ok(new_array)
        }
        _ => unpack_dictionary::<K>(array, to_type),
    }
}

// Unpack a dictionary where the keys are of type <K> into a flattened array of type to_type
fn unpack_dictionary<K>(array: &ArrayRef, to_type: &DataType) -> Result<ArrayRef>
where
    K: ArrowDictionaryKeyType,
{
    let dict_array = array
        .as_any()
        .downcast_ref::<DictionaryArray<K>>()
        .ok_or_else(|| {
            ArrowError::ComputeError(
                "Internal Error: Cannot cast dictionary to DictionaryArray of expected type".to_string(),
            )
        })?;

    // attempt to cast the dict values to the target type
    // use the take kernel to expand out the dictionary
    let cast_dict_values = cast(&dict_array.values(), to_type)?;

    // Note take requires first casting the indices to u32
    let keys_array: ArrayRef = Arc::new(dict_array.keys_array());
    let indicies = cast(&keys_array, &DataType::UInt32)?;
    let u32_indicies =
        indicies
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| {
                ArrowError::ComputeError(
                    "Internal Error: Cannot cast dict indices to UInt32".to_string(),
                )
            })?;

    take(cast_dict_values.as_ref(), u32_indicies, None)
}

/// Attempts to encode an array into an `ArrayDictionary` with index
/// type K and value (dictionary) type value_type
///
/// K is the key type
fn cast_to_dictionary<K: ArrowDictionaryKeyType>(
    array: &ArrayRef,
    dict_value_type: &DataType,
) -> Result<ArrayRef> {
    use DataType::*;

    match *dict_value_type {
        Int8 => pack_numeric_to_dictionary::<K, Int8Type>(array, dict_value_type),
        Int16 => pack_numeric_to_dictionary::<K, Int16Type>(array, dict_value_type),
        Int32 => pack_numeric_to_dictionary::<K, Int32Type>(array, dict_value_type),
        Int64 => pack_numeric_to_dictionary::<K, Int64Type>(array, dict_value_type),
        UInt8 => pack_numeric_to_dictionary::<K, UInt8Type>(array, dict_value_type),
        UInt16 => pack_numeric_to_dictionary::<K, UInt16Type>(array, dict_value_type),
        UInt32 => pack_numeric_to_dictionary::<K, UInt32Type>(array, dict_value_type),
        UInt64 => pack_numeric_to_dictionary::<K, UInt64Type>(array, dict_value_type),
        Utf8 => pack_string_to_dictionary::<K>(array),
        _ => Err(ArrowError::ComputeError(format!(
            "Internal Error: Unsupported output type for dictionary packing: {:?}",
            dict_value_type
        ))),
    }
}

// Packs the data from the primitive array of type <V> to a
// DictionaryArray with keys of type K and values of value_type V
fn pack_numeric_to_dictionary<K, V>(
    array: &ArrayRef,
    dict_value_type: &DataType,
) -> Result<ArrayRef>
where
    K: ArrowDictionaryKeyType,
    V: ArrowNumericType,
{
    // attempt to cast the source array values to the target value type (the dictionary values type)
    let cast_values = cast(array, &dict_value_type)?;
    let values = cast_values
        .as_any()
        .downcast_ref::<PrimitiveArray<V>>()
        .unwrap();

    let keys_builder = PrimitiveBuilder::<K>::new(values.len());
    let values_builder = PrimitiveBuilder::<V>::new(values.len());
    let mut b = PrimitiveDictionaryBuilder::new(keys_builder, values_builder);

    // copy each element one at a time
    for i in 0..values.len() {
        if values.is_null(i) {
            b.append_null()?;
        } else {
            b.append(values.value(i))?;
        }
    }
    Ok(Arc::new(b.finish()))
}

// Packs the data as a StringDictionaryArray, if possible, with the
// key types of K
fn pack_string_to_dictionary<K>(array: &ArrayRef) -> Result<ArrayRef>
where
    K: ArrowDictionaryKeyType,
{
    let cast_values = cast(array, &DataType::Utf8)?;
    let values = cast_values.as_any().downcast_ref::<StringArray>().unwrap();

    let keys_builder = PrimitiveBuilder::<K>::new(values.len());
    let values_builder = StringBuilder::new(values.len());
    let mut b = StringDictionaryBuilder::new(keys_builder, values_builder);

    // copy each element one at a time
    for i in 0..values.len() {
        if values.is_null(i) {
            b.append_null()?;
        } else {
            b.append(values.value(i))?;
        }
    }
    Ok(Arc::new(b.finish()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{buffer::Buffer, util::display::array_value_to_string};

    #[test]
    fn test_cast_i32_to_f64() {
        let a = Int32Array::from(vec![5, 6, 7, 8, 9]);
        let array = Arc::new(a) as ArrayRef;
        let b = cast(&array, &DataType::Float64).unwrap();
        let c = b.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!(5.0 - c.value(0) < f64::EPSILON);
        assert!(6.0 - c.value(1) < f64::EPSILON);
        assert!(7.0 - c.value(2) < f64::EPSILON);
        assert!(8.0 - c.value(3) < f64::EPSILON);
        assert!(9.0 - c.value(4) < f64::EPSILON);
    }

    #[test]
    fn test_cast_i32_to_u8() {
        let a = Int32Array::from(vec![-5, 6, -7, 8, 100000000]);
        let array = Arc::new(a) as ArrayRef;
        let b = cast(&array, &DataType::UInt8).unwrap();
        let c = b.as_any().downcast_ref::<UInt8Array>().unwrap();
        assert_eq!(false, c.is_valid(0));
        assert_eq!(6, c.value(1));
        assert_eq!(false, c.is_valid(2));
        assert_eq!(8, c.value(3));
        // overflows return None
        assert_eq!(false, c.is_valid(4));
    }

    #[test]
    fn test_cast_i32_to_u8_sliced() {
        let a = Int32Array::from(vec![-5, 6, -7, 8, 100000000]);
        let array = Arc::new(a) as ArrayRef;
        assert_eq!(0, array.offset());
        let array = array.slice(2, 3);
        assert_eq!(2, array.offset());
        let b = cast(&array, &DataType::UInt8).unwrap();
        assert_eq!(3, b.len());
        assert_eq!(0, b.offset());
        let c = b.as_any().downcast_ref::<UInt8Array>().unwrap();
        assert_eq!(false, c.is_valid(0));
        assert_eq!(8, c.value(1));
        // overflows return None
        assert_eq!(false, c.is_valid(2));
    }

    #[test]
    fn test_cast_i32_to_i32() {
        let a = Int32Array::from(vec![5, 6, 7, 8, 9]);
        let array = Arc::new(a) as ArrayRef;
        let b = cast(&array, &DataType::Int32).unwrap();
        let c = b.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(5, c.value(0));
        assert_eq!(6, c.value(1));
        assert_eq!(7, c.value(2));
        assert_eq!(8, c.value(3));
        assert_eq!(9, c.value(4));
    }

    #[test]
    fn test_cast_i32_to_list_i32() {
        let a = Int32Array::from(vec![5, 6, 7, 8, 9]);
        let array = Arc::new(a) as ArrayRef;
        let b = cast(
            &array,
            &DataType::List(Box::new(Field::new("item", DataType::Int32, true))),
        )
        .unwrap();
        assert_eq!(5, b.len());
        let arr = b.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(0, arr.value_offset(0));
        assert_eq!(1, arr.value_offset(1));
        assert_eq!(2, arr.value_offset(2));
        assert_eq!(3, arr.value_offset(3));
        assert_eq!(4, arr.value_offset(4));
        assert_eq!(1, arr.value_length(0));
        assert_eq!(1, arr.value_length(1));
        assert_eq!(1, arr.value_length(2));
        assert_eq!(1, arr.value_length(3));
        assert_eq!(1, arr.value_length(4));
        let values = arr.values();
        let c = values.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(5, c.value(0));
        assert_eq!(6, c.value(1));
        assert_eq!(7, c.value(2));
        assert_eq!(8, c.value(3));
        assert_eq!(9, c.value(4));
    }

    #[test]
    fn test_cast_i32_to_list_i32_nullable() {
        let a = Int32Array::from(vec![Some(5), None, Some(7), Some(8), Some(9)]);
        let array = Arc::new(a) as ArrayRef;
        let b = cast(
            &array,
            &DataType::List(Box::new(Field::new("item", DataType::Int32, true))),
        )
        .unwrap();
        assert_eq!(5, b.len());
        assert_eq!(1, b.null_count());
        let arr = b.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(0, arr.value_offset(0));
        assert_eq!(1, arr.value_offset(1));
        assert_eq!(2, arr.value_offset(2));
        assert_eq!(3, arr.value_offset(3));
        assert_eq!(4, arr.value_offset(4));
        assert_eq!(1, arr.value_length(0));
        assert_eq!(1, arr.value_length(1));
        assert_eq!(1, arr.value_length(2));
        assert_eq!(1, arr.value_length(3));
        assert_eq!(1, arr.value_length(4));
        let values = arr.values();
        let c = values.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(1, c.null_count());
        assert_eq!(5, c.value(0));
        assert_eq!(false, c.is_valid(1));
        assert_eq!(7, c.value(2));
        assert_eq!(8, c.value(3));
        assert_eq!(9, c.value(4));
    }

    #[test]
    fn test_cast_i32_to_list_f64_nullable_sliced() {
        let a = Int32Array::from(vec![Some(5), None, Some(7), Some(8), None, Some(10)]);
        let array = Arc::new(a) as ArrayRef;
        let array = array.slice(2, 4);
        let b = cast(
            &array,
            &DataType::List(Box::new(Field::new("item", DataType::Float64, true))),
        )
        .unwrap();
        assert_eq!(4, b.len());
        assert_eq!(1, b.null_count());
        let arr = b.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(0, arr.value_offset(0));
        assert_eq!(1, arr.value_offset(1));
        assert_eq!(2, arr.value_offset(2));
        assert_eq!(3, arr.value_offset(3));
        assert_eq!(1, arr.value_length(0));
        assert_eq!(1, arr.value_length(1));
        assert_eq!(1, arr.value_length(2));
        assert_eq!(1, arr.value_length(3));
        let values = arr.values();
        let c = values.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(1, c.null_count());
        assert!(7.0 - c.value(0) < f64::EPSILON);
        assert!(8.0 - c.value(1) < f64::EPSILON);
        assert_eq!(false, c.is_valid(2));
        assert!(10.0 - c.value(3) < f64::EPSILON);
    }

    #[test]
    fn test_cast_utf8_to_i32() {
        let a = StringArray::from(vec!["5", "6", "seven", "8", "9.1"]);
        let array = Arc::new(a) as ArrayRef;
        let b = cast(&array, &DataType::Int32).unwrap();
        let c = b.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(5, c.value(0));
        assert_eq!(6, c.value(1));
        assert_eq!(false, c.is_valid(2));
        assert_eq!(8, c.value(3));
        assert_eq!(false, c.is_valid(2));
    }

    #[test]
    fn test_cast_bool_to_i32() {
        let a = BooleanArray::from(vec![Some(true), Some(false), None]);
        let array = Arc::new(a) as ArrayRef;
        let b = cast(&array, &DataType::Int32).unwrap();
        let c = b.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(1, c.value(0));
        assert_eq!(0, c.value(1));
        assert_eq!(false, c.is_valid(2));
    }

    #[test]
    fn test_cast_bool_to_f64() {
        let a = BooleanArray::from(vec![Some(true), Some(false), None]);
        let array = Arc::new(a) as ArrayRef;
        let b = cast(&array, &DataType::Float64).unwrap();
        let c = b.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!(1.0 - c.value(0) < f64::EPSILON);
        assert!(0.0 - c.value(1) < f64::EPSILON);
        assert_eq!(false, c.is_valid(2));
    }

    #[test]
    #[should_panic(
        expected = "Casting from Int32 to Timestamp(Microsecond, None) not supported"
    )]
    fn test_cast_int32_to_timestamp() {
        let a = Int32Array::from(vec![Some(2), Some(10), None]);
        let array = Arc::new(a) as ArrayRef;
        cast(&array, &DataType::Timestamp(TimeUnit::Microsecond, None)).unwrap();
    }

    #[test]
    fn test_cast_list_i32_to_list_u16() {
        // Construct a value array
        let value_data = Int32Array::from(vec![0, 0, 0, -1, -2, -1, 2, 100000000]).data();

        let value_offsets = Buffer::from(&[0, 3, 6, 8].to_byte_slice());

        // Construct a list array from the above two
        let list_data_type =
            DataType::List(Box::new(Field::new("item", DataType::Int32, true)));
        let list_data = ArrayData::builder(list_data_type)
            .len(3)
            .add_buffer(value_offsets)
            .add_child_data(value_data)
            .build();
        let list_array = Arc::new(ListArray::from(list_data)) as ArrayRef;

        let cast_array = cast(
            &list_array,
            &DataType::List(Box::new(Field::new("item", DataType::UInt16, true))),
        )
        .unwrap();
        // 3 negative values should get lost when casting to unsigned,
        // 1 value should overflow
        assert_eq!(4, cast_array.null_count());
        // offsets should be the same
        assert_eq!(
            list_array.data().buffers().to_vec(),
            cast_array.data().buffers().to_vec()
        );
        let array = cast_array
            .as_ref()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(DataType::UInt16, array.value_type());
        assert_eq!(4, array.values().null_count());
        assert_eq!(3, array.value_length(0));
        assert_eq!(3, array.value_length(1));
        assert_eq!(2, array.value_length(2));
        let values = array.values();
        let u16arr = values.as_any().downcast_ref::<UInt16Array>().unwrap();
        assert_eq!(8, u16arr.len());
        assert_eq!(4, u16arr.null_count());

        assert_eq!(0, u16arr.value(0));
        assert_eq!(0, u16arr.value(1));
        assert_eq!(0, u16arr.value(2));
        assert_eq!(false, u16arr.is_valid(3));
        assert_eq!(false, u16arr.is_valid(4));
        assert_eq!(false, u16arr.is_valid(5));
        assert_eq!(2, u16arr.value(6));
        assert_eq!(false, u16arr.is_valid(7));
    }

    #[test]
    #[should_panic(
        expected = "Casting from Int32 to Timestamp(Microsecond, None) not supported"
    )]
    fn test_cast_list_i32_to_list_timestamp() {
        // Construct a value array
        let value_data =
            Int32Array::from(vec![0, 0, 0, -1, -2, -1, 2, 8, 100000000]).data();

        let value_offsets = Buffer::from(&[0, 3, 6, 9].to_byte_slice());

        // Construct a list array from the above two
        let list_data_type =
            DataType::List(Box::new(Field::new("item", DataType::Int32, true)));
        let list_data = ArrayData::builder(list_data_type)
            .len(3)
            .add_buffer(value_offsets)
            .add_child_data(value_data)
            .build();
        let list_array = Arc::new(ListArray::from(list_data)) as ArrayRef;

        cast(
            &list_array,
            &DataType::List(Box::new(Field::new(
                "item",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ))),
        )
        .unwrap();
    }

    #[test]
    fn test_cast_date32_to_date64() {
        let a = Date32Array::from(vec![10000, 17890]);
        let array = Arc::new(a) as ArrayRef;
        let b = cast(&array, &DataType::Date64(DateUnit::Millisecond)).unwrap();
        let c = b.as_any().downcast_ref::<Date64Array>().unwrap();
        assert_eq!(864000000000, c.value(0));
        assert_eq!(1545696000000, c.value(1));
    }

    #[test]
    fn test_cast_date64_to_date32() {
        let a = Date64Array::from(vec![Some(864000000005), Some(1545696000001), None]);
        let array = Arc::new(a) as ArrayRef;
        let b = cast(&array, &DataType::Date32(DateUnit::Day)).unwrap();
        let c = b.as_any().downcast_ref::<Date32Array>().unwrap();
        assert_eq!(10000, c.value(0));
        assert_eq!(17890, c.value(1));
        assert!(c.is_null(2));
    }

    #[test]
    fn test_cast_date32_to_int32() {
        let a = Date32Array::from(vec![10000, 17890]);
        let array = Arc::new(a) as ArrayRef;
        let b = cast(&array, &DataType::Int32).unwrap();
        let c = b.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(10000, c.value(0));
        assert_eq!(17890, c.value(1));
    }

    #[test]
    fn test_cast_int32_to_date32() {
        let a = Int32Array::from(vec![10000, 17890]);
        let array = Arc::new(a) as ArrayRef;
        let b = cast(&array, &DataType::Date32(DateUnit::Day)).unwrap();
        let c = b.as_any().downcast_ref::<Date32Array>().unwrap();
        assert_eq!(10000, c.value(0));
        assert_eq!(17890, c.value(1));
    }

    #[test]
    fn test_cast_timestamp_to_date32() {
        let a = TimestampMillisecondArray::from_opt_vec(
            vec![Some(864000000005), Some(1545696000001), None],
            Some(String::from("UTC")),
        );
        let array = Arc::new(a) as ArrayRef;
        let b = cast(&array, &DataType::Date32(DateUnit::Day)).unwrap();
        let c = b.as_any().downcast_ref::<Date32Array>().unwrap();
        assert_eq!(10000, c.value(0));
        assert_eq!(17890, c.value(1));
        assert!(c.is_null(2));
    }

    #[test]
    fn test_cast_timestamp_to_date64() {
        let a = TimestampMillisecondArray::from_opt_vec(
            vec![Some(864000000005), Some(1545696000001), None],
            None,
        );
        let array = Arc::new(a) as ArrayRef;
        let b = cast(&array, &DataType::Date64(DateUnit::Millisecond)).unwrap();
        let c = b.as_any().downcast_ref::<Date64Array>().unwrap();
        assert_eq!(864000000005, c.value(0));
        assert_eq!(1545696000001, c.value(1));
        assert!(c.is_null(2));
    }

    #[test]
    fn test_cast_timestamp_to_i64() {
        let a = TimestampMillisecondArray::from_opt_vec(
            vec![Some(864000000005), Some(1545696000001), None],
            Some("UTC".to_string()),
        );
        let array = Arc::new(a) as ArrayRef;
        let b = cast(&array, &DataType::Int64).unwrap();
        let c = b.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(&DataType::Int64, c.data_type());
        assert_eq!(864000000005, c.value(0));
        assert_eq!(1545696000001, c.value(1));
        assert!(c.is_null(2));
    }

    #[test]
    fn test_cast_between_timestamps() {
        let a = TimestampMillisecondArray::from_opt_vec(
            vec![Some(864000003005), Some(1545696002001), None],
            None,
        );
        let array = Arc::new(a) as ArrayRef;
        let b = cast(&array, &DataType::Timestamp(TimeUnit::Second, None)).unwrap();
        let c = b.as_any().downcast_ref::<TimestampSecondArray>().unwrap();
        assert_eq!(864000003, c.value(0));
        assert_eq!(1545696002, c.value(1));
        assert!(c.is_null(2));
    }

    #[test]
    fn test_cast_from_f64() {
        let f64_values: Vec<f64> = vec![
            std::i64::MIN as f64,
            std::i32::MIN as f64,
            std::i16::MIN as f64,
            std::i8::MIN as f64,
            0_f64,
            std::u8::MAX as f64,
            std::u16::MAX as f64,
            std::u32::MAX as f64,
            std::u64::MAX as f64,
        ];
        let f64_array: ArrayRef = Arc::new(Float64Array::from(f64_values));

        let f64_expected = vec![
            "-9223372036854776000.0",
            "-2147483648.0",
            "-32768.0",
            "-128.0",
            "0.0",
            "255.0",
            "65535.0",
            "4294967295.0",
            "18446744073709552000.0",
        ];
        assert_eq!(
            f64_expected,
            get_cast_values::<Float64Type>(&f64_array, &DataType::Float64)
        );

        let f32_expected = vec![
            "-9223372000000000000.0",
            "-2147483600.0",
            "-32768.0",
            "-128.0",
            "0.0",
            "255.0",
            "65535.0",
            "4294967300.0",
            "18446744000000000000.0",
        ];
        assert_eq!(
            f32_expected,
            get_cast_values::<Float32Type>(&f64_array, &DataType::Float32)
        );

        let i64_expected = vec![
            "-9223372036854775808",
            "-2147483648",
            "-32768",
            "-128",
            "0",
            "255",
            "65535",
            "4294967295",
            "null",
        ];
        assert_eq!(
            i64_expected,
            get_cast_values::<Int64Type>(&f64_array, &DataType::Int64)
        );

        let i32_expected = vec![
            "null",
            "-2147483648",
            "-32768",
            "-128",
            "0",
            "255",
            "65535",
            "null",
            "null",
        ];
        assert_eq!(
            i32_expected,
            get_cast_values::<Int32Type>(&f64_array, &DataType::Int32)
        );

        let i16_expected = vec![
            "null", "null", "-32768", "-128", "0", "255", "null", "null", "null",
        ];
        assert_eq!(
            i16_expected,
            get_cast_values::<Int16Type>(&f64_array, &DataType::Int16)
        );

        let i8_expected = vec![
            "null", "null", "null", "-128", "0", "null", "null", "null", "null",
        ];
        assert_eq!(
            i8_expected,
            get_cast_values::<Int8Type>(&f64_array, &DataType::Int8)
        );

        let u64_expected = vec![
            "null",
            "null",
            "null",
            "null",
            "0",
            "255",
            "65535",
            "4294967295",
            "null",
        ];
        assert_eq!(
            u64_expected,
            get_cast_values::<UInt64Type>(&f64_array, &DataType::UInt64)
        );

        let u32_expected = vec![
            "null",
            "null",
            "null",
            "null",
            "0",
            "255",
            "65535",
            "4294967295",
            "null",
        ];
        assert_eq!(
            u32_expected,
            get_cast_values::<UInt32Type>(&f64_array, &DataType::UInt32)
        );

        let u16_expected = vec![
            "null", "null", "null", "null", "0", "255", "65535", "null", "null",
        ];
        assert_eq!(
            u16_expected,
            get_cast_values::<UInt16Type>(&f64_array, &DataType::UInt16)
        );

        let u8_expected = vec![
            "null", "null", "null", "null", "0", "255", "null", "null", "null",
        ];
        assert_eq!(
            u8_expected,
            get_cast_values::<UInt8Type>(&f64_array, &DataType::UInt8)
        );
    }

    #[test]
    fn test_cast_from_f32() {
        let f32_values: Vec<f32> = vec![
            std::i32::MIN as f32,
            std::i32::MIN as f32,
            std::i16::MIN as f32,
            std::i8::MIN as f32,
            0_f32,
            std::u8::MAX as f32,
            std::u16::MAX as f32,
            std::u32::MAX as f32,
            std::u32::MAX as f32,
        ];
        let f32_array: ArrayRef = Arc::new(Float32Array::from(f32_values));

        let f64_expected = vec![
            "-2147483648.0",
            "-2147483648.0",
            "-32768.0",
            "-128.0",
            "0.0",
            "255.0",
            "65535.0",
            "4294967296.0",
            "4294967296.0",
        ];
        assert_eq!(
            f64_expected,
            get_cast_values::<Float64Type>(&f32_array, &DataType::Float64)
        );

        let f32_expected = vec![
            "-2147483600.0",
            "-2147483600.0",
            "-32768.0",
            "-128.0",
            "0.0",
            "255.0",
            "65535.0",
            "4294967300.0",
            "4294967300.0",
        ];
        assert_eq!(
            f32_expected,
            get_cast_values::<Float32Type>(&f32_array, &DataType::Float32)
        );

        let i64_expected = vec![
            "-2147483648",
            "-2147483648",
            "-32768",
            "-128",
            "0",
            "255",
            "65535",
            "4294967296",
            "4294967296",
        ];
        assert_eq!(
            i64_expected,
            get_cast_values::<Int64Type>(&f32_array, &DataType::Int64)
        );

        let i32_expected = vec![
            "-2147483648",
            "-2147483648",
            "-32768",
            "-128",
            "0",
            "255",
            "65535",
            "null",
            "null",
        ];
        assert_eq!(
            i32_expected,
            get_cast_values::<Int32Type>(&f32_array, &DataType::Int32)
        );

        let i16_expected = vec![
            "null", "null", "-32768", "-128", "0", "255", "null", "null", "null",
        ];
        assert_eq!(
            i16_expected,
            get_cast_values::<Int16Type>(&f32_array, &DataType::Int16)
        );

        let i8_expected = vec![
            "null", "null", "null", "-128", "0", "null", "null", "null", "null",
        ];
        assert_eq!(
            i8_expected,
            get_cast_values::<Int8Type>(&f32_array, &DataType::Int8)
        );

        let u64_expected = vec![
            "null",
            "null",
            "null",
            "null",
            "0",
            "255",
            "65535",
            "4294967296",
            "4294967296",
        ];
        assert_eq!(
            u64_expected,
            get_cast_values::<UInt64Type>(&f32_array, &DataType::UInt64)
        );

        let u32_expected = vec![
            "null", "null", "null", "null", "0", "255", "65535", "null", "null",
        ];
        assert_eq!(
            u32_expected,
            get_cast_values::<UInt32Type>(&f32_array, &DataType::UInt32)
        );

        let u16_expected = vec![
            "null", "null", "null", "null", "0", "255", "65535", "null", "null",
        ];
        assert_eq!(
            u16_expected,
            get_cast_values::<UInt16Type>(&f32_array, &DataType::UInt16)
        );

        let u8_expected = vec![
            "null", "null", "null", "null", "0", "255", "null", "null", "null",
        ];
        assert_eq!(
            u8_expected,
            get_cast_values::<UInt8Type>(&f32_array, &DataType::UInt8)
        );
    }

    #[test]
    fn test_cast_from_uint64() {
        let u64_values: Vec<u64> = vec![
            0,
            std::u8::MAX as u64,
            std::u16::MAX as u64,
            std::u32::MAX as u64,
            std::u64::MAX,
        ];
        let u64_array: ArrayRef = Arc::new(UInt64Array::from(u64_values));

        let f64_expected = vec![
            "0.0",
            "255.0",
            "65535.0",
            "4294967295.0",
            "18446744073709552000.0",
        ];
        assert_eq!(
            f64_expected,
            get_cast_values::<Float64Type>(&u64_array, &DataType::Float64)
        );

        let f32_expected = vec![
            "0.0",
            "255.0",
            "65535.0",
            "4294967300.0",
            "18446744000000000000.0",
        ];
        assert_eq!(
            f32_expected,
            get_cast_values::<Float32Type>(&u64_array, &DataType::Float32)
        );

        let i64_expected = vec!["0", "255", "65535", "4294967295", "null"];
        assert_eq!(
            i64_expected,
            get_cast_values::<Int64Type>(&u64_array, &DataType::Int64)
        );

        let i32_expected = vec!["0", "255", "65535", "null", "null"];
        assert_eq!(
            i32_expected,
            get_cast_values::<Int32Type>(&u64_array, &DataType::Int32)
        );

        let i16_expected = vec!["0", "255", "null", "null", "null"];
        assert_eq!(
            i16_expected,
            get_cast_values::<Int16Type>(&u64_array, &DataType::Int16)
        );

        let i8_expected = vec!["0", "null", "null", "null", "null"];
        assert_eq!(
            i8_expected,
            get_cast_values::<Int8Type>(&u64_array, &DataType::Int8)
        );

        let u64_expected =
            vec!["0", "255", "65535", "4294967295", "18446744073709551615"];
        assert_eq!(
            u64_expected,
            get_cast_values::<UInt64Type>(&u64_array, &DataType::UInt64)
        );

        let u32_expected = vec!["0", "255", "65535", "4294967295", "null"];
        assert_eq!(
            u32_expected,
            get_cast_values::<UInt32Type>(&u64_array, &DataType::UInt32)
        );

        let u16_expected = vec!["0", "255", "65535", "null", "null"];
        assert_eq!(
            u16_expected,
            get_cast_values::<UInt16Type>(&u64_array, &DataType::UInt16)
        );

        let u8_expected = vec!["0", "255", "null", "null", "null"];
        assert_eq!(
            u8_expected,
            get_cast_values::<UInt8Type>(&u64_array, &DataType::UInt8)
        );
    }

    #[test]
    fn test_cast_from_uint32() {
        let u32_values: Vec<u32> = vec![
            0,
            std::u8::MAX as u32,
            std::u16::MAX as u32,
            std::u32::MAX as u32,
        ];
        let u32_array: ArrayRef = Arc::new(UInt32Array::from(u32_values));

        let f64_expected = vec!["0.0", "255.0", "65535.0", "4294967295.0"];
        assert_eq!(
            f64_expected,
            get_cast_values::<Float64Type>(&u32_array, &DataType::Float64)
        );

        let f32_expected = vec!["0.0", "255.0", "65535.0", "4294967300.0"];
        assert_eq!(
            f32_expected,
            get_cast_values::<Float32Type>(&u32_array, &DataType::Float32)
        );

        let i64_expected = vec!["0", "255", "65535", "4294967295"];
        assert_eq!(
            i64_expected,
            get_cast_values::<Int64Type>(&u32_array, &DataType::Int64)
        );

        let i32_expected = vec!["0", "255", "65535", "null"];
        assert_eq!(
            i32_expected,
            get_cast_values::<Int32Type>(&u32_array, &DataType::Int32)
        );

        let i16_expected = vec!["0", "255", "null", "null"];
        assert_eq!(
            i16_expected,
            get_cast_values::<Int16Type>(&u32_array, &DataType::Int16)
        );

        let i8_expected = vec!["0", "null", "null", "null"];
        assert_eq!(
            i8_expected,
            get_cast_values::<Int8Type>(&u32_array, &DataType::Int8)
        );

        let u64_expected = vec!["0", "255", "65535", "4294967295"];
        assert_eq!(
            u64_expected,
            get_cast_values::<UInt64Type>(&u32_array, &DataType::UInt64)
        );

        let u32_expected = vec!["0", "255", "65535", "4294967295"];
        assert_eq!(
            u32_expected,
            get_cast_values::<UInt32Type>(&u32_array, &DataType::UInt32)
        );

        let u16_expected = vec!["0", "255", "65535", "null"];
        assert_eq!(
            u16_expected,
            get_cast_values::<UInt16Type>(&u32_array, &DataType::UInt16)
        );

        let u8_expected = vec!["0", "255", "null", "null"];
        assert_eq!(
            u8_expected,
            get_cast_values::<UInt8Type>(&u32_array, &DataType::UInt8)
        );
    }

    #[test]
    fn test_cast_from_uint16() {
        let u16_values: Vec<u16> = vec![0, std::u8::MAX as u16, std::u16::MAX as u16];
        let u16_array: ArrayRef = Arc::new(UInt16Array::from(u16_values));

        let f64_expected = vec!["0.0", "255.0", "65535.0"];
        assert_eq!(
            f64_expected,
            get_cast_values::<Float64Type>(&u16_array, &DataType::Float64)
        );

        let f32_expected = vec!["0.0", "255.0", "65535.0"];
        assert_eq!(
            f32_expected,
            get_cast_values::<Float32Type>(&u16_array, &DataType::Float32)
        );

        let i64_expected = vec!["0", "255", "65535"];
        assert_eq!(
            i64_expected,
            get_cast_values::<Int64Type>(&u16_array, &DataType::Int64)
        );

        let i32_expected = vec!["0", "255", "65535"];
        assert_eq!(
            i32_expected,
            get_cast_values::<Int32Type>(&u16_array, &DataType::Int32)
        );

        let i16_expected = vec!["0", "255", "null"];
        assert_eq!(
            i16_expected,
            get_cast_values::<Int16Type>(&u16_array, &DataType::Int16)
        );

        let i8_expected = vec!["0", "null", "null"];
        assert_eq!(
            i8_expected,
            get_cast_values::<Int8Type>(&u16_array, &DataType::Int8)
        );

        let u64_expected = vec!["0", "255", "65535"];
        assert_eq!(
            u64_expected,
            get_cast_values::<UInt64Type>(&u16_array, &DataType::UInt64)
        );

        let u32_expected = vec!["0", "255", "65535"];
        assert_eq!(
            u32_expected,
            get_cast_values::<UInt32Type>(&u16_array, &DataType::UInt32)
        );

        let u16_expected = vec!["0", "255", "65535"];
        assert_eq!(
            u16_expected,
            get_cast_values::<UInt16Type>(&u16_array, &DataType::UInt16)
        );

        let u8_expected = vec!["0", "255", "null"];
        assert_eq!(
            u8_expected,
            get_cast_values::<UInt8Type>(&u16_array, &DataType::UInt8)
        );
    }

    #[test]
    fn test_cast_from_uint8() {
        let u8_values: Vec<u8> = vec![0, std::u8::MAX];
        let u8_array: ArrayRef = Arc::new(UInt8Array::from(u8_values));

        let f64_expected = vec!["0.0", "255.0"];
        assert_eq!(
            f64_expected,
            get_cast_values::<Float64Type>(&u8_array, &DataType::Float64)
        );

        let f32_expected = vec!["0.0", "255.0"];
        assert_eq!(
            f32_expected,
            get_cast_values::<Float32Type>(&u8_array, &DataType::Float32)
        );

        let i64_expected = vec!["0", "255"];
        assert_eq!(
            i64_expected,
            get_cast_values::<Int64Type>(&u8_array, &DataType::Int64)
        );

        let i32_expected = vec!["0", "255"];
        assert_eq!(
            i32_expected,
            get_cast_values::<Int32Type>(&u8_array, &DataType::Int32)
        );

        let i16_expected = vec!["0", "255"];
        assert_eq!(
            i16_expected,
            get_cast_values::<Int16Type>(&u8_array, &DataType::Int16)
        );

        let i8_expected = vec!["0", "null"];
        assert_eq!(
            i8_expected,
            get_cast_values::<Int8Type>(&u8_array, &DataType::Int8)
        );

        let u64_expected = vec!["0", "255"];
        assert_eq!(
            u64_expected,
            get_cast_values::<UInt64Type>(&u8_array, &DataType::UInt64)
        );

        let u32_expected = vec!["0", "255"];
        assert_eq!(
            u32_expected,
            get_cast_values::<UInt32Type>(&u8_array, &DataType::UInt32)
        );

        let u16_expected = vec!["0", "255"];
        assert_eq!(
            u16_expected,
            get_cast_values::<UInt16Type>(&u8_array, &DataType::UInt16)
        );

        let u8_expected = vec!["0", "255"];
        assert_eq!(
            u8_expected,
            get_cast_values::<UInt8Type>(&u8_array, &DataType::UInt8)
        );
    }

    #[test]
    fn test_cast_from_int64() {
        let i64_values: Vec<i64> = vec![
            std::i64::MIN,
            std::i32::MIN as i64,
            std::i16::MIN as i64,
            std::i8::MIN as i64,
            0,
            std::i8::MAX as i64,
            std::i16::MAX as i64,
            std::i32::MAX as i64,
            std::i64::MAX,
        ];
        let i64_array: ArrayRef = Arc::new(Int64Array::from(i64_values));

        let f64_expected = vec![
            "-9223372036854776000.0",
            "-2147483648.0",
            "-32768.0",
            "-128.0",
            "0.0",
            "127.0",
            "32767.0",
            "2147483647.0",
            "9223372036854776000.0",
        ];
        assert_eq!(
            f64_expected,
            get_cast_values::<Float64Type>(&i64_array, &DataType::Float64)
        );

        let f32_expected = vec![
            "-9223372000000000000.0",
            "-2147483600.0",
            "-32768.0",
            "-128.0",
            "0.0",
            "127.0",
            "32767.0",
            "2147483600.0",
            "9223372000000000000.0",
        ];
        assert_eq!(
            f32_expected,
            get_cast_values::<Float32Type>(&i64_array, &DataType::Float32)
        );

        let i64_expected = vec![
            "-9223372036854775808",
            "-2147483648",
            "-32768",
            "-128",
            "0",
            "127",
            "32767",
            "2147483647",
            "9223372036854775807",
        ];
        assert_eq!(
            i64_expected,
            get_cast_values::<Int64Type>(&i64_array, &DataType::Int64)
        );

        let i32_expected = vec![
            "null",
            "-2147483648",
            "-32768",
            "-128",
            "0",
            "127",
            "32767",
            "2147483647",
            "null",
        ];
        assert_eq!(
            i32_expected,
            get_cast_values::<Int32Type>(&i64_array, &DataType::Int32)
        );

        let i16_expected = vec![
            "null", "null", "-32768", "-128", "0", "127", "32767", "null", "null",
        ];
        assert_eq!(
            i16_expected,
            get_cast_values::<Int16Type>(&i64_array, &DataType::Int16)
        );

        let i8_expected = vec![
            "null", "null", "null", "-128", "0", "127", "null", "null", "null",
        ];
        assert_eq!(
            i8_expected,
            get_cast_values::<Int8Type>(&i64_array, &DataType::Int8)
        );

        let u64_expected = vec![
            "null",
            "null",
            "null",
            "null",
            "0",
            "127",
            "32767",
            "2147483647",
            "9223372036854775807",
        ];
        assert_eq!(
            u64_expected,
            get_cast_values::<UInt64Type>(&i64_array, &DataType::UInt64)
        );

        let u32_expected = vec![
            "null",
            "null",
            "null",
            "null",
            "0",
            "127",
            "32767",
            "2147483647",
            "null",
        ];
        assert_eq!(
            u32_expected,
            get_cast_values::<UInt32Type>(&i64_array, &DataType::UInt32)
        );

        let u16_expected = vec![
            "null", "null", "null", "null", "0", "127", "32767", "null", "null",
        ];
        assert_eq!(
            u16_expected,
            get_cast_values::<UInt16Type>(&i64_array, &DataType::UInt16)
        );

        let u8_expected = vec![
            "null", "null", "null", "null", "0", "127", "null", "null", "null",
        ];
        assert_eq!(
            u8_expected,
            get_cast_values::<UInt8Type>(&i64_array, &DataType::UInt8)
        );
    }

    #[test]
    fn test_cast_from_int32() {
        let i32_values: Vec<i32> = vec![
            std::i32::MIN as i32,
            std::i16::MIN as i32,
            std::i8::MIN as i32,
            0,
            std::i8::MAX as i32,
            std::i16::MAX as i32,
            std::i32::MAX as i32,
        ];
        let i32_array: ArrayRef = Arc::new(Int32Array::from(i32_values));

        let f64_expected = vec![
            "-2147483648.0",
            "-32768.0",
            "-128.0",
            "0.0",
            "127.0",
            "32767.0",
            "2147483647.0",
        ];
        assert_eq!(
            f64_expected,
            get_cast_values::<Float64Type>(&i32_array, &DataType::Float64)
        );

        let f32_expected = vec![
            "-2147483600.0",
            "-32768.0",
            "-128.0",
            "0.0",
            "127.0",
            "32767.0",
            "2147483600.0",
        ];
        assert_eq!(
            f32_expected,
            get_cast_values::<Float32Type>(&i32_array, &DataType::Float32)
        );

        let i16_expected = vec!["null", "-32768", "-128", "0", "127", "32767", "null"];
        assert_eq!(
            i16_expected,
            get_cast_values::<Int16Type>(&i32_array, &DataType::Int16)
        );

        let i8_expected = vec!["null", "null", "-128", "0", "127", "null", "null"];
        assert_eq!(
            i8_expected,
            get_cast_values::<Int8Type>(&i32_array, &DataType::Int8)
        );

        let u64_expected =
            vec!["null", "null", "null", "0", "127", "32767", "2147483647"];
        assert_eq!(
            u64_expected,
            get_cast_values::<UInt64Type>(&i32_array, &DataType::UInt64)
        );

        let u32_expected =
            vec!["null", "null", "null", "0", "127", "32767", "2147483647"];
        assert_eq!(
            u32_expected,
            get_cast_values::<UInt32Type>(&i32_array, &DataType::UInt32)
        );

        let u16_expected = vec!["null", "null", "null", "0", "127", "32767", "null"];
        assert_eq!(
            u16_expected,
            get_cast_values::<UInt16Type>(&i32_array, &DataType::UInt16)
        );

        let u8_expected = vec!["null", "null", "null", "0", "127", "null", "null"];
        assert_eq!(
            u8_expected,
            get_cast_values::<UInt8Type>(&i32_array, &DataType::UInt8)
        );
    }

    #[test]
    fn test_cast_from_int16() {
        let i16_values: Vec<i16> = vec![
            std::i16::MIN,
            std::i8::MIN as i16,
            0,
            std::i8::MAX as i16,
            std::i16::MAX,
        ];
        let i16_array: ArrayRef = Arc::new(Int16Array::from(i16_values));

        let f64_expected = vec!["-32768.0", "-128.0", "0.0", "127.0", "32767.0"];
        assert_eq!(
            f64_expected,
            get_cast_values::<Float64Type>(&i16_array, &DataType::Float64)
        );

        let f32_expected = vec!["-32768.0", "-128.0", "0.0", "127.0", "32767.0"];
        assert_eq!(
            f32_expected,
            get_cast_values::<Float32Type>(&i16_array, &DataType::Float32)
        );

        let i64_expected = vec!["-32768", "-128", "0", "127", "32767"];
        assert_eq!(
            i64_expected,
            get_cast_values::<Int64Type>(&i16_array, &DataType::Int64)
        );

        let i32_expected = vec!["-32768", "-128", "0", "127", "32767"];
        assert_eq!(
            i32_expected,
            get_cast_values::<Int32Type>(&i16_array, &DataType::Int32)
        );

        let i16_expected = vec!["-32768", "-128", "0", "127", "32767"];
        assert_eq!(
            i16_expected,
            get_cast_values::<Int16Type>(&i16_array, &DataType::Int16)
        );

        let i8_expected = vec!["null", "-128", "0", "127", "null"];
        assert_eq!(
            i8_expected,
            get_cast_values::<Int8Type>(&i16_array, &DataType::Int8)
        );

        let u64_expected = vec!["null", "null", "0", "127", "32767"];
        assert_eq!(
            u64_expected,
            get_cast_values::<UInt64Type>(&i16_array, &DataType::UInt64)
        );

        let u32_expected = vec!["null", "null", "0", "127", "32767"];
        assert_eq!(
            u32_expected,
            get_cast_values::<UInt32Type>(&i16_array, &DataType::UInt32)
        );

        let u16_expected = vec!["null", "null", "0", "127", "32767"];
        assert_eq!(
            u16_expected,
            get_cast_values::<UInt16Type>(&i16_array, &DataType::UInt16)
        );

        let u8_expected = vec!["null", "null", "0", "127", "null"];
        assert_eq!(
            u8_expected,
            get_cast_values::<UInt8Type>(&i16_array, &DataType::UInt8)
        );
    }

    #[test]
    fn test_cast_from_int8() {
        let i8_values: Vec<i8> = vec![std::i8::MIN, 0, std::i8::MAX];
        let i8_array: ArrayRef = Arc::new(Int8Array::from(i8_values));

        let f64_expected = vec!["-128.0", "0.0", "127.0"];
        assert_eq!(
            f64_expected,
            get_cast_values::<Float64Type>(&i8_array, &DataType::Float64)
        );

        let f32_expected = vec!["-128.0", "0.0", "127.0"];
        assert_eq!(
            f32_expected,
            get_cast_values::<Float32Type>(&i8_array, &DataType::Float32)
        );

        let i64_expected = vec!["-128", "0", "127"];
        assert_eq!(
            i64_expected,
            get_cast_values::<Int64Type>(&i8_array, &DataType::Int64)
        );

        let i32_expected = vec!["-128", "0", "127"];
        assert_eq!(
            i32_expected,
            get_cast_values::<Int32Type>(&i8_array, &DataType::Int32)
        );

        let i16_expected = vec!["-128", "0", "127"];
        assert_eq!(
            i16_expected,
            get_cast_values::<Int16Type>(&i8_array, &DataType::Int16)
        );

        let i8_expected = vec!["-128", "0", "127"];
        assert_eq!(
            i8_expected,
            get_cast_values::<Int8Type>(&i8_array, &DataType::Int8)
        );

        let u64_expected = vec!["null", "0", "127"];
        assert_eq!(
            u64_expected,
            get_cast_values::<UInt64Type>(&i8_array, &DataType::UInt64)
        );

        let u32_expected = vec!["null", "0", "127"];
        assert_eq!(
            u32_expected,
            get_cast_values::<UInt32Type>(&i8_array, &DataType::UInt32)
        );

        let u16_expected = vec!["null", "0", "127"];
        assert_eq!(
            u16_expected,
            get_cast_values::<UInt16Type>(&i8_array, &DataType::UInt16)
        );

        let u8_expected = vec!["null", "0", "127"];
        assert_eq!(
            u8_expected,
            get_cast_values::<UInt8Type>(&i8_array, &DataType::UInt8)
        );
    }

    /// Convert `array` into a vector of strings by casting to data type dt
    fn get_cast_values<T>(array: &ArrayRef, dt: &DataType) -> Vec<String>
    where
        T: ArrowNumericType,
    {
        let c = cast(&array, dt).unwrap();
        let a = c.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
        let mut v: Vec<String> = vec![];
        for i in 0..array.len() {
            if a.is_null(i) {
                v.push("null".to_string())
            } else {
                v.push(format!("{:?}", a.value(i)));
            }
        }
        v
    }

    #[test]
    fn test_cast_utf8_dict() {
        // FROM a dictionary with of Utf8 values
        use DataType::*;

        let keys_builder = PrimitiveBuilder::<Int8Type>::new(10);
        let values_builder = StringBuilder::new(10);
        let mut builder = StringDictionaryBuilder::new(keys_builder, values_builder);
        builder.append("one").unwrap();
        builder.append_null().unwrap();
        builder.append("three").unwrap();
        let array: ArrayRef = Arc::new(builder.finish());

        let expected = vec!["one", "null", "three"];

        // Test casting TO StringArray
        let cast_type = Utf8;
        let cast_array = cast(&array, &cast_type).expect("cast to UTF-8 failed");
        assert_eq!(cast_array.data_type(), &cast_type);
        assert_eq!(array_to_strings(&cast_array), expected);

        // Test casting TO Dictionary (with different index sizes)

        let cast_type = Dictionary(Box::new(Int16), Box::new(Utf8));
        let cast_array = cast(&array, &cast_type).expect("cast failed");
        assert_eq!(cast_array.data_type(), &cast_type);
        assert_eq!(array_to_strings(&cast_array), expected);

        let cast_type = Dictionary(Box::new(Int32), Box::new(Utf8));
        let cast_array = cast(&array, &cast_type).expect("cast failed");
        assert_eq!(cast_array.data_type(), &cast_type);
        assert_eq!(array_to_strings(&cast_array), expected);

        let cast_type = Dictionary(Box::new(Int64), Box::new(Utf8));
        let cast_array = cast(&array, &cast_type).expect("cast failed");
        assert_eq!(cast_array.data_type(), &cast_type);
        assert_eq!(array_to_strings(&cast_array), expected);

        let cast_type = Dictionary(Box::new(UInt8), Box::new(Utf8));
        let cast_array = cast(&array, &cast_type).expect("cast failed");
        assert_eq!(cast_array.data_type(), &cast_type);
        assert_eq!(array_to_strings(&cast_array), expected);

        let cast_type = Dictionary(Box::new(UInt16), Box::new(Utf8));
        let cast_array = cast(&array, &cast_type).expect("cast failed");
        assert_eq!(cast_array.data_type(), &cast_type);
        assert_eq!(array_to_strings(&cast_array), expected);

        let cast_type = Dictionary(Box::new(UInt32), Box::new(Utf8));
        let cast_array = cast(&array, &cast_type).expect("cast failed");
        assert_eq!(cast_array.data_type(), &cast_type);
        assert_eq!(array_to_strings(&cast_array), expected);

        let cast_type = Dictionary(Box::new(UInt64), Box::new(Utf8));
        let cast_array = cast(&array, &cast_type).expect("cast failed");
        assert_eq!(cast_array.data_type(), &cast_type);
        assert_eq!(array_to_strings(&cast_array), expected);
    }

    #[test]
    fn test_cast_dict_to_dict_bad_index_value_primitive() {
        use DataType::*;
        // test converting from an array that has indexes of a type
        // that are out of bounds for a particular other kind of
        // index.

        let keys_builder = PrimitiveBuilder::<Int32Type>::new(10);
        let values_builder = PrimitiveBuilder::<Int64Type>::new(10);
        let mut builder = PrimitiveDictionaryBuilder::new(keys_builder, values_builder);

        // add 200 distinct values (which can be stored by a
        // dictionary indexed by int32, but not a dictionary indexed
        // with int8)
        for i in 0..200 {
            builder.append(i).unwrap();
        }
        let array: ArrayRef = Arc::new(builder.finish());

        let cast_type = Dictionary(Box::new(Int8), Box::new(Utf8));
        let res = cast(&array, &cast_type);
        assert!(res.is_err());
        let actual_error = format!("{:?}", res);
        let expected_error = "Could not convert 72 dictionary indexes from Int32 to Int8";
        assert!(
            actual_error.contains(expected_error),
            "did not find expected error '{}' in actual error '{}'",
            actual_error,
            expected_error
        );
    }

    #[test]
    fn test_cast_dict_to_dict_bad_index_value_utf8() {
        use DataType::*;
        // Same test as test_cast_dict_to_dict_bad_index_value but use
        // string values (and encode the expected behavior here);

        let keys_builder = PrimitiveBuilder::<Int32Type>::new(10);
        let values_builder = StringBuilder::new(10);
        let mut builder = StringDictionaryBuilder::new(keys_builder, values_builder);

        // add 200 distinct values (which can be stored by a
        // dictionary indexed by int32, but not a dictionary indexed
        // with int8)
        for i in 0..200 {
            let val = format!("val{}", i);
            builder.append(&val).unwrap();
        }
        let array: ArrayRef = Arc::new(builder.finish());

        let cast_type = Dictionary(Box::new(Int8), Box::new(Utf8));
        let res = cast(&array, &cast_type);
        assert!(res.is_err());
        let actual_error = format!("{:?}", res);
        let expected_error = "Could not convert 72 dictionary indexes from Int32 to Int8";
        assert!(
            actual_error.contains(expected_error),
            "did not find expected error '{}' in actual error '{}'",
            actual_error,
            expected_error
        );
    }

    #[test]
    fn test_cast_primitive_dict() {
        // FROM a dictionary with of INT32 values
        use DataType::*;

        let keys_builder = PrimitiveBuilder::<Int8Type>::new(10);
        let values_builder = PrimitiveBuilder::<Int32Type>::new(10);
        let mut builder = PrimitiveDictionaryBuilder::new(keys_builder, values_builder);
        builder.append(1).unwrap();
        builder.append_null().unwrap();
        builder.append(3).unwrap();
        let array: ArrayRef = Arc::new(builder.finish());

        let expected = vec!["1", "null", "3"];

        // Test casting TO PrimitiveArray, different dictionary type
        let cast_array = cast(&array, &Utf8).expect("cast to UTF-8 failed");
        assert_eq!(array_to_strings(&cast_array), expected);
        assert_eq!(cast_array.data_type(), &Utf8);

        let cast_array = cast(&array, &Int64).expect("cast to int64 failed");
        assert_eq!(array_to_strings(&cast_array), expected);
        assert_eq!(cast_array.data_type(), &Int64);
    }

    #[test]
    fn test_cast_primitive_array_to_dict() {
        use DataType::*;

        let mut builder = PrimitiveBuilder::<Int32Type>::new(10);
        builder.append_value(1).unwrap();
        builder.append_null().unwrap();
        builder.append_value(3).unwrap();
        let array: ArrayRef = Arc::new(builder.finish());

        let expected = vec!["1", "null", "3"];

        // Cast to a dictionary (same value type, Int32)
        let cast_type = Dictionary(Box::new(UInt8), Box::new(Int32));
        let cast_array = cast(&array, &cast_type).expect("cast failed");
        assert_eq!(cast_array.data_type(), &cast_type);
        assert_eq!(array_to_strings(&cast_array), expected);

        // Cast to a dictionary (different value type, Int8)
        let cast_type = Dictionary(Box::new(UInt8), Box::new(Int8));
        let cast_array = cast(&array, &cast_type).expect("cast failed");
        assert_eq!(cast_array.data_type(), &cast_type);
        assert_eq!(array_to_strings(&cast_array), expected);
    }

    #[test]
    fn test_cast_string_array_to_dict() {
        use DataType::*;

        let mut builder = StringBuilder::new(10);
        builder.append_value("one").unwrap();
        builder.append_null().unwrap();
        builder.append_value("three").unwrap();
        let array: ArrayRef = Arc::new(builder.finish());

        let expected = vec!["one", "null", "three"];

        // Cast to a dictionary (same value type, Utf8)
        let cast_type = Dictionary(Box::new(UInt8), Box::new(Utf8));
        let cast_array = cast(&array, &cast_type).expect("cast failed");
        assert_eq!(cast_array.data_type(), &cast_type);
        assert_eq!(array_to_strings(&cast_array), expected);
    }

    #[test]
    fn test_cast_null_array_to_int32() {
        let array = Arc::new(NullArray::new(6)) as ArrayRef;

        let expected = Int32Array::from(vec![None; 6]);

        // Cast to a dictionary (same value type, Utf8)
        let cast_type = DataType::Int32;
        let cast_array = cast(&array, &cast_type).expect("cast failed");
        let cast_array = as_primitive_array::<Int32Type>(&cast_array);
        assert_eq!(cast_array.data_type(), &cast_type);
        assert_eq!(cast_array, &expected);
    }

    /// Print the `DictionaryArray` `array` as a vector of strings
    fn array_to_strings(array: &ArrayRef) -> Vec<String> {
        (0..array.len())
            .map(|i| {
                if array.is_null(i) {
                    "null".to_string()
                } else {
                    array_value_to_string(array, i).expect("Convert array to String")
                }
            })
            .collect()
    }

    #[test]
    fn test_cast_utf8_to_date32() {
        use chrono::NaiveDate;
        let from_ymd = chrono::NaiveDate::from_ymd;
        let since = chrono::NaiveDate::signed_duration_since;

        let a = StringArray::from(vec![
            "2000-01-01",          // valid date with leading 0s
            "2000-2-2",            // valid date without leading 0s
            "2000-00-00",          // invalid month and day
            "2000-01-01T12:00:00", // date + time is invalid
            "2000",                // just a year is invalid
        ]);
        let array = Arc::new(a) as ArrayRef;
        let b = cast(&array, &DataType::Date32(DateUnit::Day)).unwrap();
        let c = b.as_any().downcast_ref::<Date32Array>().unwrap();

        // test valid inputs
        let date_value = since(NaiveDate::from_ymd(2000, 1, 1), from_ymd(1970, 1, 1))
            .num_days() as i32;
        assert_eq!(true, c.is_valid(0)); // "2000-01-01"
        assert_eq!(date_value, c.value(0));

        let date_value = since(NaiveDate::from_ymd(2000, 2, 2), from_ymd(1970, 1, 1))
            .num_days() as i32;
        assert_eq!(true, c.is_valid(1)); // "2000-2-2"
        assert_eq!(date_value, c.value(1));

        // test invalid inputs
        assert_eq!(false, c.is_valid(2)); // "2000-00-00"
        assert_eq!(false, c.is_valid(3)); // "2000-01-01T12:00:00"
        assert_eq!(false, c.is_valid(4)); // "2000"
    }

    #[test]
    fn test_cast_utf8_to_date64() {
        let a = StringArray::from(vec![
            "2000-01-01T12:00:00", // date + time valid
            "2020-12-15T12:34:56", // date + time valid
            "2020-2-2T12:34:56",   // valid date time without leading 0s
            "2000-00-00T12:00:00", // invalid month and day
            "2000-01-01 12:00:00", // missing the 'T'
            "2000-01-01",          // just a date is invalid
        ]);
        let array = Arc::new(a) as ArrayRef;
        let b = cast(&array, &DataType::Date64(DateUnit::Millisecond)).unwrap();
        let c = b.as_any().downcast_ref::<Date64Array>().unwrap();

        // test valid inputs
        assert_eq!(true, c.is_valid(0)); // "2000-01-01T12:00:00"
        assert_eq!(946728000000, c.value(0));
        assert_eq!(true, c.is_valid(1)); // "2020-12-15T12:34:56"
        assert_eq!(1608035696000, c.value(1));
        assert_eq!(true, c.is_valid(2)); // "2020-2-2T12:34:56"
        assert_eq!(1580646896000, c.value(2));

        // test invalid inputs
        assert_eq!(false, c.is_valid(3)); // "2000-00-00T12:00:00"
        assert_eq!(false, c.is_valid(4)); // "2000-01-01 12:00:00"
        assert_eq!(false, c.is_valid(5)); // "2000-01-01"
    }

    #[test]
    fn test_can_cast_types() {
        // this function attempts to ensure that can_cast_types stays
        // in sync with cast.  It simply tries all combinations of
        // types and makes sure that if `can_cast_types` returns
        // true, so does `cast`

        let all_types = get_all_types();

        for array in get_arrays_of_all_types() {
            for to_type in &all_types {
                println!("Test casting {:?} --> {:?}", array.data_type(), to_type);
                let cast_result = cast(&array, &to_type);
                let reported_cast_ability = can_cast_types(array.data_type(), to_type);

                // check for mismatch
                match (cast_result, reported_cast_ability) {
                    (Ok(_), false) => {
                        panic!("Was able to cast array from {:?} to {:?} but can_cast_types reported false",
                               array.data_type(), to_type)
                    }
                    (Err(e), true) => {
                        panic!("Was not able to cast array from {:?} to {:?} but can_cast_types reported true. \
                                Error was {:?}",
                               array.data_type(), to_type, e)
                    }
                    // otherwise it was a match
                    _ => {}
                };
            }
        }
    }

    /// Create instances of arrays with varying types for cast tests
    fn get_arrays_of_all_types() -> Vec<ArrayRef> {
        let tz_name = String::from("America/New_York");
        let binary_data: Vec<&[u8]> = vec![b"foo", b"bar"];
        vec![
            Arc::new(BinaryArray::from(binary_data.clone())),
            Arc::new(LargeBinaryArray::from(binary_data.clone())),
            make_dictionary_primitive::<Int8Type>(),
            make_dictionary_primitive::<Int16Type>(),
            make_dictionary_primitive::<Int32Type>(),
            make_dictionary_primitive::<Int64Type>(),
            make_dictionary_primitive::<UInt8Type>(),
            make_dictionary_primitive::<UInt16Type>(),
            make_dictionary_primitive::<UInt32Type>(),
            make_dictionary_primitive::<UInt64Type>(),
            make_dictionary_utf8::<Int8Type>(),
            make_dictionary_utf8::<Int16Type>(),
            make_dictionary_utf8::<Int32Type>(),
            make_dictionary_utf8::<Int64Type>(),
            make_dictionary_utf8::<UInt8Type>(),
            make_dictionary_utf8::<UInt16Type>(),
            make_dictionary_utf8::<UInt32Type>(),
            make_dictionary_utf8::<UInt64Type>(),
            Arc::new(make_list_array()),
            Arc::new(make_large_list_array()),
            Arc::new(make_fixed_size_list_array()),
            Arc::new(make_fixed_size_binary_array()),
            Arc::new(StructArray::from(vec![
                (
                    Field::new("a", DataType::Boolean, false),
                    Arc::new(BooleanArray::from(vec![false, false, true, true]))
                        as Arc<Array>,
                ),
                (
                    Field::new("b", DataType::Int32, false),
                    Arc::new(Int32Array::from(vec![42, 28, 19, 31])),
                ),
            ])),
            //Arc::new(make_union_array()),
            Arc::new(NullArray::new(10)),
            Arc::new(StringArray::from(vec!["foo", "bar"])),
            Arc::new(LargeStringArray::from(vec!["foo", "bar"])),
            Arc::new(BooleanArray::from(vec![true, false])),
            Arc::new(Int8Array::from(vec![1, 2])),
            Arc::new(Int16Array::from(vec![1, 2])),
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(UInt8Array::from(vec![1, 2])),
            Arc::new(UInt16Array::from(vec![1, 2])),
            Arc::new(UInt32Array::from(vec![1, 2])),
            Arc::new(UInt64Array::from(vec![1, 2])),
            Arc::new(Float32Array::from(vec![1.0, 2.0])),
            Arc::new(Float64Array::from(vec![1.0, 2.0])),
            Arc::new(TimestampSecondArray::from_vec(vec![1000, 2000], None)),
            Arc::new(TimestampMillisecondArray::from_vec(vec![1000, 2000], None)),
            Arc::new(TimestampMicrosecondArray::from_vec(vec![1000, 2000], None)),
            Arc::new(TimestampNanosecondArray::from_vec(vec![1000, 2000], None)),
            Arc::new(TimestampSecondArray::from_vec(
                vec![1000, 2000],
                Some(tz_name.clone()),
            )),
            Arc::new(TimestampMillisecondArray::from_vec(
                vec![1000, 2000],
                Some(tz_name.clone()),
            )),
            Arc::new(TimestampMicrosecondArray::from_vec(
                vec![1000, 2000],
                Some(tz_name.clone()),
            )),
            Arc::new(TimestampNanosecondArray::from_vec(
                vec![1000, 2000],
                Some(tz_name),
            )),
            Arc::new(Date32Array::from(vec![1000, 2000])),
            Arc::new(Date64Array::from(vec![1000, 2000])),
            Arc::new(Time32SecondArray::from(vec![1000, 2000])),
            Arc::new(Time32MillisecondArray::from(vec![1000, 2000])),
            Arc::new(Time64MicrosecondArray::from(vec![1000, 2000])),
            Arc::new(Time64NanosecondArray::from(vec![1000, 2000])),
            Arc::new(IntervalYearMonthArray::from(vec![1000, 2000])),
            Arc::new(IntervalDayTimeArray::from(vec![1000, 2000])),
            Arc::new(DurationSecondArray::from(vec![1000, 2000])),
            Arc::new(DurationMillisecondArray::from(vec![1000, 2000])),
            Arc::new(DurationMicrosecondArray::from(vec![1000, 2000])),
            Arc::new(DurationNanosecondArray::from(vec![1000, 2000])),
        ]
    }

    fn make_list_array() -> ListArray {
        // Construct a value array
        let value_data = ArrayData::builder(DataType::Int32)
            .len(8)
            .add_buffer(Buffer::from(&[0, 1, 2, 3, 4, 5, 6, 7].to_byte_slice()))
            .build();

        // Construct a buffer for value offsets, for the nested array:
        //  [[0, 1, 2], [3, 4, 5], [6, 7]]
        let value_offsets = Buffer::from(&[0, 3, 6, 8].to_byte_slice());

        // Construct a list array from the above two
        let list_data_type =
            DataType::List(Box::new(Field::new("item", DataType::Int32, true)));
        let list_data = ArrayData::builder(list_data_type)
            .len(3)
            .add_buffer(value_offsets)
            .add_child_data(value_data)
            .build();
        ListArray::from(list_data)
    }

    fn make_large_list_array() -> LargeListArray {
        // Construct a value array
        let value_data = ArrayData::builder(DataType::Int32)
            .len(8)
            .add_buffer(Buffer::from(&[0, 1, 2, 3, 4, 5, 6, 7].to_byte_slice()))
            .build();

        // Construct a buffer for value offsets, for the nested array:
        //  [[0, 1, 2], [3, 4, 5], [6, 7]]
        let value_offsets = Buffer::from(&[0i64, 3, 6, 8].to_byte_slice());

        // Construct a list array from the above two
        let list_data_type =
            DataType::LargeList(Box::new(Field::new("item", DataType::Int32, true)));
        let list_data = ArrayData::builder(list_data_type)
            .len(3)
            .add_buffer(value_offsets)
            .add_child_data(value_data)
            .build();
        LargeListArray::from(list_data)
    }

    fn make_fixed_size_list_array() -> FixedSizeListArray {
        // Construct a value array
        let value_data = ArrayData::builder(DataType::Int32)
            .len(10)
            .add_buffer(Buffer::from(
                &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9].to_byte_slice(),
            ))
            .build();

        // Construct a fixed size list array from the above two
        let list_data_type = DataType::FixedSizeList(
            Box::new(Field::new("item", DataType::Int32, true)),
            2,
        );
        let list_data = ArrayData::builder(list_data_type)
            .len(5)
            .add_child_data(value_data)
            .build();
        FixedSizeListArray::from(list_data)
    }

    fn make_fixed_size_binary_array() -> FixedSizeBinaryArray {
        let values: [u8; 15] = *b"hellotherearrow";

        let array_data = ArrayData::builder(DataType::FixedSizeBinary(5))
            .len(3)
            .add_buffer(Buffer::from(&values[..]))
            .build();
        FixedSizeBinaryArray::from(array_data)
    }

    fn make_union_array() -> UnionArray {
        let mut builder = UnionBuilder::new_dense(7);
        builder.append::<Int32Type>("a", 1).unwrap();
        builder.append::<Int64Type>("b", 2).unwrap();
        builder.build().unwrap()
    }

    /// Creates a dictionary with primitive dictionary values, and keys of type K
    fn make_dictionary_primitive<K: ArrowDictionaryKeyType>() -> ArrayRef {
        let keys_builder = PrimitiveBuilder::<K>::new(2);
        // Pick Int32 arbitrarily for dictionary values
        let values_builder = PrimitiveBuilder::<Int32Type>::new(2);
        let mut b = PrimitiveDictionaryBuilder::new(keys_builder, values_builder);
        b.append(1).unwrap();
        b.append(2).unwrap();
        Arc::new(b.finish())
    }

    /// Creates a dictionary with utf8 values, and keys of type K
    fn make_dictionary_utf8<K: ArrowDictionaryKeyType>() -> ArrayRef {
        let keys_builder = PrimitiveBuilder::<K>::new(2);
        // Pick Int32 arbitrarily for dictionary values
        let values_builder = StringBuilder::new(2);
        let mut b = StringDictionaryBuilder::new(keys_builder, values_builder);
        b.append("foo").unwrap();
        b.append("bar").unwrap();
        Arc::new(b.finish())
    }

    // Get a selection of datatypes to try and cast to
    fn get_all_types() -> Vec<DataType> {
        use DataType::*;
        let tz_name = String::from("America/New_York");

        vec![
            Null,
            Boolean,
            Int8,
            Int16,
            Int32,
            UInt64,
            UInt8,
            UInt16,
            UInt32,
            UInt64,
            Float16,
            Float32,
            Float64,
            Timestamp(TimeUnit::Second, None),
            Timestamp(TimeUnit::Millisecond, None),
            Timestamp(TimeUnit::Microsecond, None),
            Timestamp(TimeUnit::Nanosecond, None),
            Timestamp(TimeUnit::Second, Some(tz_name.clone())),
            Timestamp(TimeUnit::Millisecond, Some(tz_name.clone())),
            Timestamp(TimeUnit::Microsecond, Some(tz_name.clone())),
            Timestamp(TimeUnit::Nanosecond, Some(tz_name)),
            Date32(DateUnit::Day),
            Date64(DateUnit::Millisecond),
            Time32(TimeUnit::Second),
            Time32(TimeUnit::Millisecond),
            Time64(TimeUnit::Microsecond),
            Time64(TimeUnit::Nanosecond),
            Duration(TimeUnit::Second),
            Duration(TimeUnit::Millisecond),
            Duration(TimeUnit::Microsecond),
            Duration(TimeUnit::Nanosecond),
            Interval(IntervalUnit::YearMonth),
            Interval(IntervalUnit::DayTime),
            Binary,
            FixedSizeBinary(10),
            LargeBinary,
            Utf8,
            LargeUtf8,
            List(Box::new(Field::new("item", DataType::Int8, true))),
            List(Box::new(Field::new("item", DataType::Utf8, true))),
            FixedSizeList(Box::new(Field::new("item", DataType::Int8, true)), 10),
            FixedSizeList(Box::new(Field::new("item", DataType::Utf8, false)), 10),
            LargeList(Box::new(Field::new("item", DataType::Int8, true))),
            LargeList(Box::new(Field::new("item", DataType::Utf8, false))),
            Struct(vec![
                Field::new("f1", DataType::Int32, false),
                Field::new("f2", DataType::Utf8, true),
            ]),
            Union(vec![
                Field::new("f1", DataType::Int32, false),
                Field::new("f2", DataType::Utf8, true),
            ]),
            Dictionary(Box::new(DataType::Int8), Box::new(DataType::Int32)),
            Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8)),
            Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
        ]
    }
}
