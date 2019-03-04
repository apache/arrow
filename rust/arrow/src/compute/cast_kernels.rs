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

//! Defines cast kernels for `ArrayRef`.
//!
//! Allows casting arrays between supported datatypes.
//!
//! ## Behavior
//!
//! * Boolean to Utf8: `true` => '1', `false` => `0`
//! * Utf8 to numeric: strings that can't be parsed to numbers return null, float strings
//!   in integer casts return null
//! * Numeric to boolean: 0 returns `false`, any other value returns `true`
//!
//! ## Unsupported Casts
//!
//! * To or from `StructArray`
//! * To or from `ListArray`
//! * Boolean to float
//! * Utf8 to boolean
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

use std::sync::Arc;

use crate::array::*;
use crate::builder::*;
use crate::datatypes::*;
use crate::error::{ArrowError, Result};

/// Macro rule to cast between numeric types
macro_rules! cast_numeric_arrays {
    ($array:expr, $from_ty:ident, $to_ty:ident) => {{
        match numeric_cast::<$from_ty, $to_ty>(
            $array
                .as_any()
                .downcast_ref::<PrimitiveArray<$from_ty>>()
                .unwrap(),
        ) {
            Ok(to) => Ok(Arc::new(to) as ArrayRef),
            Err(e) => Err(e),
        }
    }};
}

macro_rules! cast_numeric_to_string {
    ($array:expr, $from_ty:ident) => {{
        match cast_numeric_to_string::<$from_ty>(
            $array
                .as_any()
                .downcast_ref::<PrimitiveArray<$from_ty>>()
                .unwrap(),
        ) {
            Ok(to) => Ok(Arc::new(to) as ArrayRef),
            Err(e) => Err(e),
        }
    }};
}

macro_rules! cast_string_to_numeric {
    ($array:expr, $to_ty:ident) => {{
        match cast_string_to_numeric::<$to_ty>(
            $array.as_any().downcast_ref::<BinaryArray>().unwrap(),
        ) {
            Ok(to) => Ok(Arc::new(to) as ArrayRef),
            Err(e) => Err(e),
        }
    }};
}

macro_rules! cast_numeric_to_bool {
    ($array:expr, $from_ty:ident) => {{
        match cast_numeric_to_bool::<$from_ty>(
            $array
                .as_any()
                .downcast_ref::<PrimitiveArray<$from_ty>>()
                .unwrap(),
        ) {
            Ok(to) => Ok(Arc::new(to) as ArrayRef),
            Err(e) => Err(e),
        }
    }};
}

macro_rules! cast_bool_to_numeric {
    ($array:expr, $to_ty:ident) => {{
        match cast_bool_to_numeric::<$to_ty>(
            $array.as_any().downcast_ref::<BooleanArray>().unwrap(),
        ) {
            Ok(to) => Ok(Arc::new(to) as ArrayRef),
            Err(e) => Err(e),
        }
    }};
}

/// Cast array to provided data type
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
        (List(_), List(_)) => Err(ArrowError::ComputeError(
            "Casting between lists not yet supported".to_string(),
        )),
        (List(_), _) => Err(ArrowError::ComputeError(
            "Cannot cast list to non-list data types".to_string(),
        )),
        (_, List(_)) => Err(ArrowError::ComputeError(
            "Cannot cast primitive types to lists".to_string(),
        )),
        (_, Boolean) => match from_type {
            UInt8 => cast_numeric_to_bool!(array, UInt8Type),
            UInt16 => cast_numeric_to_bool!(array, UInt16Type),
            UInt32 => cast_numeric_to_bool!(array, UInt32Type),
            UInt64 => cast_numeric_to_bool!(array, UInt64Type),
            Int8 => cast_numeric_to_bool!(array, Int8Type),
            Int16 => cast_numeric_to_bool!(array, Int16Type),
            Int32 => cast_numeric_to_bool!(array, Int32Type),
            Int64 => cast_numeric_to_bool!(array, Int64Type),
            Float32 => cast_numeric_to_bool!(array, Float32Type),
            Float64 => cast_numeric_to_bool!(array, Float64Type),
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
            UInt8 => cast_bool_to_numeric!(array, UInt8Type),
            UInt16 => cast_bool_to_numeric!(array, UInt16Type),
            UInt32 => cast_bool_to_numeric!(array, UInt32Type),
            UInt64 => cast_bool_to_numeric!(array, UInt64Type),
            Int8 => cast_bool_to_numeric!(array, Int8Type),
            Int16 => cast_bool_to_numeric!(array, Int16Type),
            Int32 => cast_bool_to_numeric!(array, Int32Type),
            Int64 => cast_bool_to_numeric!(array, Int64Type),
            Float32 | Float64 => Err(ArrowError::ComputeError(format!(
                "Casting from {:?} to {:?} not supported",
                from_type, to_type,
            ))),
            Utf8 => {
                let from = array.as_any().downcast_ref::<BooleanArray>().unwrap();
                let mut b = BinaryBuilder::new(array.len());
                for i in 0..array.len() {
                    if array.is_null(i) {
                        b.append(false)?;
                    } else {
                        b.append_string(match from.value(i) {
                            true => "1",
                            false => "0",
                        })?;
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
            UInt8 => cast_string_to_numeric!(array, UInt8Type),
            UInt16 => cast_string_to_numeric!(array, UInt16Type),
            UInt32 => cast_string_to_numeric!(array, UInt32Type),
            UInt64 => cast_string_to_numeric!(array, UInt64Type),
            Int8 => cast_string_to_numeric!(array, Int8Type),
            Int16 => cast_string_to_numeric!(array, Int16Type),
            Int32 => cast_string_to_numeric!(array, Int32Type),
            Int64 => cast_string_to_numeric!(array, Int64Type),
            Float32 => cast_string_to_numeric!(array, Float32Type),
            Float64 => cast_string_to_numeric!(array, Float64Type),
            _ => Err(ArrowError::ComputeError(format!(
                "Casting from {:?} to {:?} not supported",
                from_type, to_type,
            ))),
        },
        (_, Utf8) => match from_type {
            UInt8 => cast_numeric_to_string!(array, UInt8Type),
            UInt16 => cast_numeric_to_string!(array, UInt16Type),
            UInt32 => cast_numeric_to_string!(array, UInt32Type),
            UInt64 => cast_numeric_to_string!(array, UInt64Type),
            Int8 => cast_numeric_to_string!(array, Int8Type),
            Int16 => cast_numeric_to_string!(array, Int16Type),
            Int32 => cast_numeric_to_string!(array, Int32Type),
            Int64 => cast_numeric_to_string!(array, Int64Type),
            Float32 => cast_numeric_to_string!(array, Float32Type),
            Float64 => cast_numeric_to_string!(array, Float64Type),
            _ => Err(ArrowError::ComputeError(format!(
                "Casting from {:?} to {:?} not supported",
                from_type, to_type,
            ))),
        },

        // start numeric casts
        (UInt8, UInt16) => cast_numeric_arrays!(array, UInt8Type, UInt16Type),
        (UInt8, UInt32) => cast_numeric_arrays!(array, UInt8Type, UInt32Type),
        (UInt8, UInt64) => cast_numeric_arrays!(array, UInt8Type, UInt64Type),
        (UInt8, Int8) => cast_numeric_arrays!(array, UInt8Type, Int8Type),
        (UInt8, Int16) => cast_numeric_arrays!(array, UInt8Type, Int16Type),
        (UInt8, Int32) => cast_numeric_arrays!(array, UInt8Type, Int32Type),
        (UInt8, Int64) => cast_numeric_arrays!(array, UInt8Type, Int64Type),
        (UInt8, Float32) => cast_numeric_arrays!(array, UInt8Type, Float32Type),
        (UInt8, Float64) => cast_numeric_arrays!(array, UInt8Type, Float64Type),

        (UInt16, UInt8) => cast_numeric_arrays!(array, UInt16Type, UInt8Type),
        (UInt16, UInt32) => cast_numeric_arrays!(array, UInt16Type, UInt32Type),
        (UInt16, UInt64) => cast_numeric_arrays!(array, UInt16Type, UInt64Type),
        (UInt16, Int8) => cast_numeric_arrays!(array, UInt16Type, Int8Type),
        (UInt16, Int16) => cast_numeric_arrays!(array, UInt16Type, Int16Type),
        (UInt16, Int32) => cast_numeric_arrays!(array, UInt16Type, Int32Type),
        (UInt16, Int64) => cast_numeric_arrays!(array, UInt16Type, Int64Type),
        (UInt16, Float32) => cast_numeric_arrays!(array, UInt16Type, Float32Type),
        (UInt16, Float64) => cast_numeric_arrays!(array, UInt16Type, Float64Type),

        (UInt32, UInt8) => cast_numeric_arrays!(array, UInt32Type, UInt8Type),
        (UInt32, UInt16) => cast_numeric_arrays!(array, UInt32Type, UInt16Type),
        (UInt32, UInt64) => cast_numeric_arrays!(array, UInt32Type, UInt64Type),
        (UInt32, Int8) => cast_numeric_arrays!(array, UInt32Type, Int8Type),
        (UInt32, Int16) => cast_numeric_arrays!(array, UInt32Type, Int16Type),
        (UInt32, Int32) => cast_numeric_arrays!(array, UInt32Type, Int32Type),
        (UInt32, Int64) => cast_numeric_arrays!(array, UInt32Type, Int64Type),
        (UInt32, Float32) => cast_numeric_arrays!(array, UInt32Type, Float32Type),
        (UInt32, Float64) => cast_numeric_arrays!(array, UInt32Type, Float64Type),

        (UInt64, UInt8) => cast_numeric_arrays!(array, UInt64Type, UInt8Type),
        (UInt64, UInt16) => cast_numeric_arrays!(array, UInt64Type, UInt16Type),
        (UInt64, UInt32) => cast_numeric_arrays!(array, UInt64Type, UInt32Type),
        (UInt64, Int8) => cast_numeric_arrays!(array, UInt64Type, Int8Type),
        (UInt64, Int16) => cast_numeric_arrays!(array, UInt64Type, Int16Type),
        (UInt64, Int32) => cast_numeric_arrays!(array, UInt64Type, Int32Type),
        (UInt64, Int64) => cast_numeric_arrays!(array, UInt64Type, Int64Type),
        (UInt64, Float32) => cast_numeric_arrays!(array, UInt64Type, Float32Type),
        (UInt64, Float64) => cast_numeric_arrays!(array, UInt64Type, Float64Type),

        (Int8, UInt8) => cast_numeric_arrays!(array, Int8Type, UInt8Type),
        (Int8, UInt16) => cast_numeric_arrays!(array, Int8Type, UInt16Type),
        (Int8, UInt32) => cast_numeric_arrays!(array, Int8Type, UInt32Type),
        (Int8, UInt64) => cast_numeric_arrays!(array, Int8Type, UInt64Type),
        (Int8, Int16) => cast_numeric_arrays!(array, Int8Type, Int16Type),
        (Int8, Int32) => cast_numeric_arrays!(array, Int8Type, Int32Type),
        (Int8, Int64) => cast_numeric_arrays!(array, Int8Type, Int64Type),
        (Int8, Float32) => cast_numeric_arrays!(array, Int8Type, Float32Type),
        (Int8, Float64) => cast_numeric_arrays!(array, Int8Type, Float64Type),

        (Int16, UInt8) => cast_numeric_arrays!(array, Int16Type, UInt8Type),
        (Int16, UInt16) => cast_numeric_arrays!(array, Int16Type, UInt16Type),
        (Int16, UInt32) => cast_numeric_arrays!(array, Int16Type, UInt32Type),
        (Int16, UInt64) => cast_numeric_arrays!(array, Int16Type, UInt64Type),
        (Int16, Int8) => cast_numeric_arrays!(array, Int16Type, Int8Type),
        (Int16, Int32) => cast_numeric_arrays!(array, Int16Type, Int32Type),
        (Int16, Int64) => cast_numeric_arrays!(array, Int16Type, Int64Type),
        (Int16, Float32) => cast_numeric_arrays!(array, Int16Type, Float32Type),
        (Int16, Float64) => cast_numeric_arrays!(array, Int16Type, Float64Type),

        (Int32, UInt8) => cast_numeric_arrays!(array, Int32Type, UInt8Type),
        (Int32, UInt16) => cast_numeric_arrays!(array, Int32Type, UInt16Type),
        (Int32, UInt32) => cast_numeric_arrays!(array, Int32Type, UInt32Type),
        (Int32, UInt64) => cast_numeric_arrays!(array, Int32Type, UInt64Type),
        (Int32, Int8) => cast_numeric_arrays!(array, Int32Type, Int8Type),
        (Int32, Int16) => cast_numeric_arrays!(array, Int32Type, Int16Type),
        (Int32, Int64) => cast_numeric_arrays!(array, Int32Type, Int64Type),
        (Int32, Float32) => cast_numeric_arrays!(array, Int32Type, Float32Type),
        (Int32, Float64) => cast_numeric_arrays!(array, Int32Type, Float64Type),

        (Float32, UInt8) => cast_numeric_arrays!(array, Float32Type, UInt8Type),
        (Float32, UInt16) => cast_numeric_arrays!(array, Float32Type, UInt16Type),
        (Float32, UInt32) => cast_numeric_arrays!(array, Float32Type, UInt32Type),
        (Float32, UInt64) => cast_numeric_arrays!(array, Float32Type, UInt64Type),
        (Float32, Int8) => cast_numeric_arrays!(array, Float32Type, Int8Type),
        (Float32, Int16) => cast_numeric_arrays!(array, Float32Type, Int16Type),
        (Float32, Int32) => cast_numeric_arrays!(array, Float32Type, Int32Type),
        (Float32, Int64) => cast_numeric_arrays!(array, Float32Type, Int64Type),
        (Float32, Float64) => cast_numeric_arrays!(array, Float32Type, Float64Type),

        (Float64, UInt8) => cast_numeric_arrays!(array, Float64Type, UInt8Type),
        (Float64, UInt16) => cast_numeric_arrays!(array, UInt16Type, Float32Type),
        (Float64, UInt32) => cast_numeric_arrays!(array, Float64Type, UInt32Type),
        (Float64, UInt64) => cast_numeric_arrays!(array, Float64Type, UInt64Type),
        (Float64, Int8) => cast_numeric_arrays!(array, Float64Type, Int8Type),
        (Float64, Int16) => cast_numeric_arrays!(array, Float64Type, Int16Type),
        (Float64, Int32) => cast_numeric_arrays!(array, Float64Type, Int32Type),
        (Float64, Int64) => cast_numeric_arrays!(array, Float64Type, Int64Type),
        (Float64, Float32) => cast_numeric_arrays!(array, Float64Type, Float32Type),
        // end numeric casts
        (_, _) => Err(ArrowError::ComputeError(format!(
            "Casting from {:?} to {:?} not supported",
            from_type, to_type,
        ))),
    }
}

/// Natural cast between numeric types
fn numeric_cast<T, R>(from: &PrimitiveArray<T>) -> Result<PrimitiveArray<R>>
where
    T: ArrowNumericType,
    R: ArrowNumericType,
    T::Native: num::NumCast,
    R::Native: num::NumCast,
{
    let mut b = PrimitiveBuilder::<R>::new(from.len());

    for i in 0..from.len() {
        if from.is_null(i) {
            b.append_null()?;
        } else {
            // some casts return None, such as a negative value to u{8|16|32|64}
            match num::cast::cast(from.value(i)) {
                Some(v) => b.append_value(v)?,
                None => b.append_null()?,
            };
        }
    }

    Ok(b.finish())
}

/// Cast numeric types to Utf8
fn cast_numeric_to_string<T>(from: &PrimitiveArray<T>) -> Result<BinaryArray>
where
    T: ArrowPrimitiveType + ArrowNumericType,
    T::Native: ::std::string::ToString,
{
    let mut b = BinaryBuilder::new(from.len());

    for i in 0..from.len() {
        if from.is_null(i) {
            b.append(false)?;
        } else {
            b.append_string(from.value(i).to_string().as_str())?;
        }
    }

    Ok(b.finish())
}

/// Cast numeric types to Utf8
fn cast_string_to_numeric<T>(from: &BinaryArray) -> Result<PrimitiveArray<T>>
where
    T: ArrowPrimitiveType + ArrowNumericType,
    T::Native: ::std::string::ToString,
{
    let mut b = PrimitiveBuilder::<T>::new(from.len());

    for i in 0..from.len() {
        if from.is_null(i) {
            b.append_null()?;
        } else {
            match std::str::from_utf8(from.value(i))
                .unwrap_or("")
                .parse::<T::Native>()
            {
                Ok(v) => b.append_value(v)?,
                _ => b.append_null()?,
            };
        }
    }

    Ok(b.finish())
}

/// Cast numeric types to Boolean
///
/// Any zero value returns `false` while non-zero returns `true`
fn cast_numeric_to_bool<T>(from: &PrimitiveArray<T>) -> Result<BooleanArray>
where
    T: ArrowPrimitiveType + ArrowNumericType,
{
    let mut b = BooleanBuilder::new(from.len());

    for i in 0..from.len() {
        if from.is_null(i) {
            b.append_null()?;
        } else {
            if from.value(i) != T::default_value() {
                b.append_value(true)?;
            } else {
                b.append_value(false)?;
            }
        }
    }

    Ok(b.finish())
}

/// Cast Boolean types to numeric
///
/// `false` returns 0 while `true` returns 1. Although this cast supports floats, they are
/// unsupported in the upstream cast
fn cast_bool_to_numeric<T>(from: &BooleanArray) -> Result<PrimitiveArray<T>>
where
    T: ArrowPrimitiveType + ArrowNumericType,
    T::Native: num::NumCast,
{
    let mut b = PrimitiveBuilder::<T>::new(from.len());

    for i in 0..from.len() {
        if from.is_null(i) {
            b.append_null()?;
        } else {
            if from.value(i) {
                // a workaround to cast a primitive to T::Native, infallible
                match num::cast::cast(1) {
                    Some(v) => b.append_value(v)?,
                    None => b.append_null()?,
                };
            } else {
                b.append_value(T::default_value())?;
            }
        }
    }

    Ok(b.finish())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cast_i32_to_f64() {
        let a = Int32Array::from(vec![5, 6, 7, 8, 9]);
        let array = Arc::new(a) as ArrayRef;
        let b = cast(&array, &DataType::Float64).unwrap();
        let c = b.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(5.0, c.value(0));
        assert_eq!(6.0, c.value(1));
        assert_eq!(7.0, c.value(2));
        assert_eq!(8.0, c.value(3));
        assert_eq!(9.0, c.value(4));
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
    fn test_cast_utf_to_i32() {
        let a = BinaryArray::from(vec!["5", "6", "seven", "8", "9.1"]);
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
    #[should_panic(expected = "Casting from Boolean to Float64 not supported")]
    fn test_cast_bool_to_f64() {
        let a = BooleanArray::from(vec![Some(true), Some(false), None]);
        let array = Arc::new(a) as ArrayRef;
        cast(&array, &DataType::Float64).unwrap();
    }

    #[test]
    #[should_panic(
        expected = "Casting from Int32 to Timestamp(Microsecond) not supported"
    )]
    fn test_cast_int32_to_timestamp() {
        let a = Int32Array::from(vec![Some(2), Some(10), None]);
        let array = Arc::new(a) as ArrayRef;
        cast(&array, &DataType::Timestamp(TimeUnit::Microsecond)).unwrap();
    }
}
