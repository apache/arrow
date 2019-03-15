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

//! Defines cast kernels for `ArrayRef`, allowing casting arrays between supported
//! datatypes.
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
use crate::array_data::ArrayData;
use crate::buffer::Buffer;
use crate::builder::*;
use crate::datatypes::*;
use crate::error::{ArrowError, Result};

/// Cast array to provided data type
///
/// Behavior:
/// * Boolean to Utf8: `true` => '1', `false` => `0`
/// * Utf8 to numeric: strings that can't be parsed to numbers return null, float strings
///   in integer casts return null
/// * Numeric to boolean: 0 returns `false`, any other value returns `true`
/// * List to List: the underlying data type is cast
/// * Primitive to List: a list array with 1 value per slot is created
///
/// Unsupported Casts
/// * To or from `StructArray`
/// * List to primitive
/// * Utf8 to boolean
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
            let cast_array = cast(&underlying_array, &to)?;
            let array_data = ArrayData::new(
                *to.clone(),
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
            // see ARROW-4886 for this limitation
            if array.offset() != 0 {
                return Err(ArrowError::ComputeError(
                    "Cast kernel does not yet support sliced (non-zero offset) arrays"
                        .to_string(),
                ));
            }
            // cast primitive to list's primitive
            let cast_array = cast(array, &to)?;
            // create offsets, where if array.len() = 2, we have [0,1,2]
            let offsets: Vec<i32> = (0..array.len() as i32 + 1).collect();
            let value_offsets = Buffer::from(offsets[..].to_byte_slice());
            let list_data = ArrayData::new(
                *to.clone(),
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
        (Float64, UInt16) => cast_numeric_arrays::<UInt16Type, Float32Type>(array),
        (Float64, UInt32) => cast_numeric_arrays::<Float64Type, UInt32Type>(array),
        (Float64, UInt64) => cast_numeric_arrays::<Float64Type, UInt64Type>(array),
        (Float64, Int8) => cast_numeric_arrays::<Float64Type, Int8Type>(array),
        (Float64, Int16) => cast_numeric_arrays::<Float64Type, Int16Type>(array),
        (Float64, Int32) => cast_numeric_arrays::<Float64Type, Int32Type>(array),
        (Float64, Int64) => cast_numeric_arrays::<Float64Type, Int64Type>(array),
        (Float64, Float32) => cast_numeric_arrays::<Float64Type, Float32Type>(array),
        // end numeric casts
        (_, _) => Err(ArrowError::ComputeError(format!(
            "Casting from {:?} to {:?} not supported",
            from_type, to_type,
        ))),
    }
}

/// Convert Array into a PrimitiveArray of type, and apply numeric cast
fn cast_numeric_arrays<FROM, TO>(from: &ArrayRef) -> Result<ArrayRef>
where
    FROM: ArrowNumericType,
    TO: ArrowNumericType,
    FROM::Native: num::NumCast,
    TO::Native: num::NumCast,
{
    match numeric_cast::<FROM, TO>(
        from.as_any()
            .downcast_ref::<PrimitiveArray<FROM>>()
            .unwrap(),
    ) {
        Ok(to) => Ok(Arc::new(to) as ArrayRef),
        Err(e) => Err(e),
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
fn cast_numeric_to_string<FROM>(array: &ArrayRef) -> Result<ArrayRef>
where
    FROM: ArrowNumericType,
    FROM::Native: ::std::string::ToString,
{
    match numeric_to_string_cast::<FROM>(
        array
            .as_any()
            .downcast_ref::<PrimitiveArray<FROM>>()
            .unwrap(),
    ) {
        Ok(to) => Ok(Arc::new(to) as ArrayRef),
        Err(e) => Err(e),
    }
}

fn numeric_to_string_cast<T>(from: &PrimitiveArray<T>) -> Result<BinaryArray>
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
fn cast_string_to_numeric<TO>(from: &ArrayRef) -> Result<ArrayRef>
where
    TO: ArrowNumericType,
{
    match string_to_numeric_cast::<TO>(
        from.as_any().downcast_ref::<BinaryArray>().unwrap(),
    ) {
        Ok(to) => Ok(Arc::new(to) as ArrayRef),
        Err(e) => Err(e),
    }
}

fn string_to_numeric_cast<T>(from: &BinaryArray) -> Result<PrimitiveArray<T>>
where
    T: ArrowNumericType,
    // T::Native: ::std::string::ToString,
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
fn cast_numeric_to_bool<FROM>(from: &ArrayRef) -> Result<ArrayRef>
where
    FROM: ArrowNumericType,
{
    match numeric_to_bool_cast::<FROM>(
        from.as_any()
            .downcast_ref::<PrimitiveArray<FROM>>()
            .unwrap(),
    ) {
        Ok(to) => Ok(Arc::new(to) as ArrayRef),
        Err(e) => Err(e),
    }
}

fn numeric_to_bool_cast<T>(from: &PrimitiveArray<T>) -> Result<BooleanArray>
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
/// `false` returns 0 while `true` returns 1
fn cast_bool_to_numeric<TO>(from: &ArrayRef) -> Result<ArrayRef>
where
    TO: ArrowNumericType,
    TO::Native: num::cast::NumCast,
{
    match bool_to_numeric_cast::<TO>(
        from.as_any().downcast_ref::<BooleanArray>().unwrap(),
    ) {
        Ok(to) => Ok(Arc::new(to) as ArrayRef),
        Err(e) => Err(e),
    }
}

fn bool_to_numeric_cast<T>(from: &BooleanArray) -> Result<PrimitiveArray<T>>
where
    T: ArrowNumericType,
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
    use crate::buffer::Buffer;

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
        let b = cast(&array, &DataType::List(Box::new(DataType::Int32))).unwrap();
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
        let b = cast(&array, &DataType::List(Box::new(DataType::Int32))).unwrap();
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
    #[should_panic(
        expected = "Cast kernel does not yet support sliced (non-zero offset) arrays"
    )]
    fn test_cast_i32_to_list_i32_nullable_sliced() {
        let a = Int32Array::from(vec![Some(5), None, Some(7), Some(8), None]);
        let array = Arc::new(a) as ArrayRef;
        let array = array.slice(2, 3);
        let b = cast(&array, &DataType::List(Box::new(DataType::Int32))).unwrap();
        assert_eq!(3, b.len());
        assert_eq!(1, b.null_count());
        let arr = b.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(0, arr.value_offset(0));
        assert_eq!(1, arr.value_offset(1));
        assert_eq!(2, arr.value_offset(2));
        assert_eq!(1, arr.value_length(0));
        assert_eq!(1, arr.value_length(1));
        assert_eq!(1, arr.value_length(2));
        let values = arr.values();
        let c = values.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(1, c.null_count());
        assert_eq!(7, c.value(0));
        assert_eq!(8, c.value(1));
        // if one removes the non-zero-offset limitation, this assertion passes when it
        // shouldn't
        assert_eq!(0, c.value(2));
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
    fn test_cast_bool_to_f64() {
        let a = BooleanArray::from(vec![Some(true), Some(false), None]);
        let array = Arc::new(a) as ArrayRef;
        let b = cast(&array, &DataType::Float64).unwrap();
        let c = b.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(1.0, c.value(0));
        assert_eq!(0.0, c.value(1));
        assert_eq!(false, c.is_valid(2));
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

    #[test]
    fn test_cast_list_i32_to_list_u16() {
        // Construct a value array
        let value_data = Int32Array::from(vec![0, 0, 0, -1, -2, -1, 2, 100000000]).data();

        let value_offsets = Buffer::from(&[0, 3, 6, 8].to_byte_slice());

        // Construct a list array from the above two
        let list_data_type = DataType::List(Box::new(DataType::Int32));
        let list_data = ArrayData::builder(list_data_type.clone())
            .len(3)
            .add_buffer(value_offsets.clone())
            .add_child_data(value_data.clone())
            .build();
        let list_array = Arc::new(ListArray::from(list_data)) as ArrayRef;

        let cast_array =
            cast(&list_array, &DataType::List(Box::new(DataType::UInt16))).unwrap();
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
        expected = "Casting from Int32 to Timestamp(Microsecond) not supported"
    )]
    fn test_cast_list_i32_to_list_timestamp() {
        // Construct a value array
        let value_data =
            Int32Array::from(vec![0, 0, 0, -1, -2, -1, 2, 8, 100000000]).data();

        let value_offsets = Buffer::from(&[0, 3, 6, 9].to_byte_slice());

        // Construct a list array from the above two
        let list_data_type = DataType::List(Box::new(DataType::Int32));
        let list_data = ArrayData::builder(list_data_type.clone())
            .len(3)
            .add_buffer(value_offsets.clone())
            .add_child_data(value_data.clone())
            .build();
        let list_array = Arc::new(ListArray::from(list_data)) as ArrayRef;

        cast(
            &list_array,
            &DataType::List(Box::new(DataType::Timestamp(TimeUnit::Microsecond))),
        )
        .unwrap();
    }
}
