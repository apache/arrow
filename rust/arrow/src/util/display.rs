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

//! Functions for printing array values, as strings, for debugging
//! purposes. See the `pretty` crate for additional functions for
//! record batch pretty printing.

use crate::array;
use crate::array::Array;
use crate::datatypes::{
    ArrowNativeType, ArrowPrimitiveType, DataType, Int16Type, Int32Type, Int64Type,
    Int8Type, TimeUnit, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};

use array::DictionaryArray;

use crate::error::{ArrowError, Result};

macro_rules! make_string {
    ($array_type:ty, $column: ident, $row: ident) => {{
        let array = $column.as_any().downcast_ref::<$array_type>().unwrap();

        let s = if array.is_null($row) {
            "".to_string()
        } else {
            array.value($row).to_string()
        };

        Ok(s)
    }};
}

macro_rules! make_string_date {
    ($array_type:ty, $column: ident, $row: ident) => {{
        let array = $column.as_any().downcast_ref::<$array_type>().unwrap();

        let s = if array.is_null($row) {
            "".to_string()
        } else {
            array
                .value_as_date($row)
                .map(|d| d.to_string())
                .unwrap_or_else(|| "ERROR CONVERTING DATE".to_string())
        };

        Ok(s)
    }};
}

macro_rules! make_string_time {
    ($array_type:ty, $column: ident, $row: ident) => {{
        let array = $column.as_any().downcast_ref::<$array_type>().unwrap();

        let s = if array.is_null($row) {
            "".to_string()
        } else {
            array
                .value_as_time($row)
                .map(|d| d.to_string())
                .unwrap_or_else(|| "ERROR CONVERTING DATE".to_string())
        };

        Ok(s)
    }};
}

macro_rules! make_string_datetime {
    ($array_type:ty, $column: ident, $row: ident) => {{
        let array = $column.as_any().downcast_ref::<$array_type>().unwrap();

        let s = if array.is_null($row) {
            "".to_string()
        } else {
            array
                .value_as_datetime($row)
                .map(|d| d.to_string())
                .unwrap_or_else(|| "ERROR CONVERTING DATE".to_string())
        };

        Ok(s)
    }};
}

// It's not possible to do array.value($row).to_string() for &[u8], let's format it as hex
macro_rules! make_string_hex {
    ($array_type:ty, $column: ident, $row: ident) => {{
        let array = $column.as_any().downcast_ref::<$array_type>().unwrap();

        let s = if array.is_null($row) {
            "".to_string()
        } else {
            let mut tmp = "".to_string();

            for character in array.value($row) {
                tmp += &format!("{:02x}", character);
            }

            tmp
        };

        Ok(s)
    }};
}

macro_rules! make_string_from_list {
    ($column: ident, $row: ident) => {{
        let list = $column
            .as_any()
            .downcast_ref::<array::ListArray>()
            .ok_or(ArrowError::InvalidArgumentError(format!(
                "Repl error: could not convert list column to list array."
            )))?
            .value($row);
        let string_values = (0..list.len())
            .map(|i| array_value_to_string(&list.clone(), i))
            .collect::<Result<Vec<String>>>()?;
        Ok(format!("[{}]", string_values.join(", ")))
    }};
}

/// Get the value at the given row in an array as a String.
///
/// Note this function is quite inefficient and is unlikely to be
/// suitable for converting large arrays or record batches.
pub fn array_value_to_string(column: &array::ArrayRef, row: usize) -> Result<String> {
    match column.data_type() {
        DataType::Utf8 => make_string!(array::StringArray, column, row),
        DataType::LargeUtf8 => make_string!(array::LargeStringArray, column, row),
        DataType::Binary => make_string_hex!(array::BinaryArray, column, row),
        DataType::LargeBinary => make_string_hex!(array::LargeBinaryArray, column, row),
        DataType::Boolean => make_string!(array::BooleanArray, column, row),
        DataType::Int8 => make_string!(array::Int8Array, column, row),
        DataType::Int16 => make_string!(array::Int16Array, column, row),
        DataType::Int32 => make_string!(array::Int32Array, column, row),
        DataType::Int64 => make_string!(array::Int64Array, column, row),
        DataType::UInt8 => make_string!(array::UInt8Array, column, row),
        DataType::UInt16 => make_string!(array::UInt16Array, column, row),
        DataType::UInt32 => make_string!(array::UInt32Array, column, row),
        DataType::UInt64 => make_string!(array::UInt64Array, column, row),
        DataType::Float16 => make_string!(array::Float32Array, column, row),
        DataType::Float32 => make_string!(array::Float32Array, column, row),
        DataType::Float64 => make_string!(array::Float64Array, column, row),
        DataType::Timestamp(unit, _) if *unit == TimeUnit::Second => {
            make_string_datetime!(array::TimestampSecondArray, column, row)
        }
        DataType::Timestamp(unit, _) if *unit == TimeUnit::Millisecond => {
            make_string_datetime!(array::TimestampMillisecondArray, column, row)
        }
        DataType::Timestamp(unit, _) if *unit == TimeUnit::Microsecond => {
            make_string_datetime!(array::TimestampMicrosecondArray, column, row)
        }
        DataType::Timestamp(unit, _) if *unit == TimeUnit::Nanosecond => {
            make_string_datetime!(array::TimestampNanosecondArray, column, row)
        }
        DataType::Date32(_) => make_string_date!(array::Date32Array, column, row),
        DataType::Date64(_) => make_string_date!(array::Date64Array, column, row),
        DataType::Time32(unit) if *unit == TimeUnit::Second => {
            make_string_time!(array::Time32SecondArray, column, row)
        }
        DataType::Time32(unit) if *unit == TimeUnit::Millisecond => {
            make_string_time!(array::Time32MillisecondArray, column, row)
        }
        DataType::Time64(unit) if *unit == TimeUnit::Microsecond => {
            make_string_time!(array::Time64MicrosecondArray, column, row)
        }
        DataType::Time64(unit) if *unit == TimeUnit::Nanosecond => {
            make_string_time!(array::Time64NanosecondArray, column, row)
        }
        DataType::List(_) => make_string_from_list!(column, row),
        DataType::Dictionary(index_type, _value_type) => match **index_type {
            DataType::Int8 => dict_array_value_to_string::<Int8Type>(column, row),
            DataType::Int16 => dict_array_value_to_string::<Int16Type>(column, row),
            DataType::Int32 => dict_array_value_to_string::<Int32Type>(column, row),
            DataType::Int64 => dict_array_value_to_string::<Int64Type>(column, row),
            DataType::UInt8 => dict_array_value_to_string::<UInt8Type>(column, row),
            DataType::UInt16 => dict_array_value_to_string::<UInt16Type>(column, row),
            DataType::UInt32 => dict_array_value_to_string::<UInt32Type>(column, row),
            DataType::UInt64 => dict_array_value_to_string::<UInt64Type>(column, row),
            _ => Err(ArrowError::InvalidArgumentError(format!(
                "Pretty printing not supported for {:?} due to index type",
                column.data_type()
            ))),
        },
        _ => Err(ArrowError::InvalidArgumentError(format!(
            "Pretty printing not implemented for {:?} type",
            column.data_type()
        ))),
    }
}

/// Converts the value of the dictionary array at `row` to a String
fn dict_array_value_to_string<K: ArrowPrimitiveType>(
    colum: &array::ArrayRef,
    row: usize,
) -> Result<String> {
    let dict_array = colum.as_any().downcast_ref::<DictionaryArray<K>>().unwrap();

    let keys_array = dict_array.keys_array();

    if keys_array.is_null(row) {
        return Ok(String::from(""));
    }

    let dict_index = keys_array.value(row).to_usize().ok_or_else(|| {
        ArrowError::InvalidArgumentError(format!(
            "Can not convert value {:?} at index {:?} to usize for string conversion.",
            keys_array.value(row),
            row
        ))
    })?;

    array_value_to_string(&dict_array.values(), dict_index)
}
