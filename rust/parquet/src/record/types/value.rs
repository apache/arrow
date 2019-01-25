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

use std::{
    collections::HashMap,
    convert::TryInto,
    hash::{Hash, Hasher},
};

use crate::{
    basic::{LogicalType, Repetition, Type as PhysicalType},
    column::reader::ColumnReader,
    data_type::Decimal,
    errors::ParquetError,
    record::{
        reader::ValueReader,
        schemas::{
            BoolSchema, BsonSchema, DateSchema, DecimalSchema, EnumSchema, F32Schema, F64Schema,
            GroupSchema, I16Schema, I32Schema, I64Schema, I8Schema, JsonSchema, ListSchema,
            ListSchemaType, OptionSchema, StringSchema, TimeSchema, TimestampSchema, U16Schema,
            U32Schema, U64Schema, U8Schema, ValueSchema, VecSchema,
        },
        types::{
            list::parse_list, map::parse_map, Bson, Date, Downcast, Enum, Group, Json, List, Map,
            Time, Timestamp,
        },
        Deserialize,
    },
    schema::types::{ColumnDescPtr, ColumnPath, Type},
};

/// Represents any valid Parquet value.
#[derive(Clone, PartialEq, Debug)]
pub enum Value {
    // Primitive types
    /// Boolean value (`true`, `false`).
    Bool(bool),
    /// Signed integer INT_8.
    U8(u8),
    /// Signed integer INT_16.
    I8(i8),
    /// Signed integer INT_32.
    U16(u16),
    /// Signed integer INT_64.
    I16(i16),
    /// Unsigned integer UINT_8.
    U32(u32),
    /// Unsigned integer UINT_16.
    I32(i32),
    /// Unsigned integer UINT_32.
    U64(u64),
    /// Unsigned integer UINT_64.
    I64(i64),
    /// IEEE 32-bit floating point value.
    F32(f32),
    /// IEEE 64-bit floating point value.
    F64(f64),
    /// Date without a time of day, stores the number of days from the Unix epoch, 1
    /// January 1970.
    Date(Date),
    /// Time of day, stores the number of microseconds from midnight.
    Time(Time),
    /// Milliseconds from the Unix epoch, 1 January 1970.
    Timestamp(Timestamp),
    /// Decimal value.
    Decimal(Decimal),
    /// General binary value.
    Array(Vec<u8>),
    /// BSON binary value.
    Bson(Bson),
    /// UTF-8 encoded character string.
    String(String),
    /// JSON string.
    Json(Json),
    /// Enum string.
    Enum(Enum),

    // Complex types
    /// List of elements.
    List(List<Value>),
    /// Map of key-value pairs.
    Map(Map<Value, Value>),
    /// Struct, child elements are tuples of field-value pairs.
    Group(Group),
    /// Optional element.
    Option(Box<Option<Value>>),
}
#[allow(clippy::derive_hash_xor_eq)]
impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Value::Bool(value) => {
                0u8.hash(state);
                value.hash(state);
            }
            Value::U8(value) => {
                1u8.hash(state);
                value.hash(state);
            }
            Value::I8(value) => {
                2u8.hash(state);
                value.hash(state);
            }
            Value::U16(value) => {
                3u8.hash(state);
                value.hash(state);
            }
            Value::I16(value) => {
                4u8.hash(state);
                value.hash(state);
            }
            Value::U32(value) => {
                5u8.hash(state);
                value.hash(state);
            }
            Value::I32(value) => {
                6u8.hash(state);
                value.hash(state);
            }
            Value::U64(value) => {
                7u8.hash(state);
                value.hash(state);
            }
            Value::I64(value) => {
                8u8.hash(state);
                value.hash(state);
            }
            Value::F32(_value) => {
                9u8.hash(state);
            }
            Value::F64(_value) => {
                10u8.hash(state);
            }
            Value::Date(value) => {
                11u8.hash(state);
                value.hash(state);
            }
            Value::Time(value) => {
                12u8.hash(state);
                value.hash(state);
            }
            Value::Timestamp(value) => {
                13u8.hash(state);
                value.hash(state);
            }
            Value::Decimal(_value) => {
                14u8.hash(state);
            }
            Value::Array(value) => {
                15u8.hash(state);
                value.hash(state);
            }
            Value::Bson(value) => {
                16u8.hash(state);
                value.hash(state);
            }
            Value::String(value) => {
                17u8.hash(state);
                value.hash(state);
            }
            Value::Json(value) => {
                18u8.hash(state);
                value.hash(state);
            }
            Value::Enum(value) => {
                19u8.hash(state);
                value.hash(state);
            }
            Value::List(value) => {
                20u8.hash(state);
                value.hash(state);
            }
            Value::Map(_value) => {
                21u8.hash(state);
            }
            Value::Group(_value) => {
                22u8.hash(state);
            }
            Value::Option(value) => {
                23u8.hash(state);
                value.hash(state);
            }
        }
    }
}
impl Eq for Value {}

impl Value {
    /// Returns true if the `Value` is an Bool. Returns false otherwise.
    pub fn is_bool(&self) -> bool {
        if let Value::Bool(_) = self {
            true
        } else {
            false
        }
    }

    /// If the `Value` is an Bool, return a reference to it. Returns Err otherwise.
    pub fn as_bool(&self) -> Result<bool, ParquetError> {
        if let Value::Bool(ret) = self {
            Ok(*ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as bool",
                self
            )))
        }
    }

    /// If the `Value` is an Bool, return it. Returns Err otherwise.
    pub fn into_bool(self) -> Result<bool, ParquetError> {
        if let Value::Bool(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as bool",
                self
            )))
        }
    }

    /// Returns true if the `Value` is an U8. Returns false otherwise.
    pub fn is_u8(&self) -> bool {
        if let Value::U8(_) = self {
            true
        } else {
            false
        }
    }

    /// If the `Value` is an U8, return a reference to it. Returns Err otherwise.
    pub fn as_u8(&self) -> Result<u8, ParquetError> {
        if let Value::U8(ret) = self {
            Ok(*ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as u8",
                self
            )))
        }
    }

    /// If the `Value` is an U8, return it. Returns Err otherwise.
    pub fn into_u8(self) -> Result<u8, ParquetError> {
        if let Value::U8(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as u8",
                self
            )))
        }
    }

    /// Returns true if the `Value` is an I8. Returns false otherwise.
    pub fn is_i8(&self) -> bool {
        if let Value::I8(_) = self {
            true
        } else {
            false
        }
    }

    /// If the `Value` is an I8, return a reference to it. Returns Err otherwise.
    pub fn as_i8(&self) -> Result<i8, ParquetError> {
        if let Value::I8(ret) = self {
            Ok(*ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as i8",
                self
            )))
        }
    }

    /// If the `Value` is an I8, return it. Returns Err otherwise.
    pub fn into_i8(self) -> Result<i8, ParquetError> {
        if let Value::I8(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as i8",
                self
            )))
        }
    }

    /// Returns true if the `Value` is an U16. Returns false otherwise.
    pub fn is_u16(&self) -> bool {
        if let Value::U16(_) = self {
            true
        } else {
            false
        }
    }

    /// If the `Value` is an U16, return a reference to it. Returns Err otherwise.
    pub fn as_u16(&self) -> Result<u16, ParquetError> {
        if let Value::U16(ret) = self {
            Ok(*ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as u16",
                self
            )))
        }
    }

    /// If the `Value` is an U16, return it. Returns Err otherwise.
    pub fn into_u16(self) -> Result<u16, ParquetError> {
        if let Value::U16(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as u16",
                self
            )))
        }
    }

    /// Returns true if the `Value` is an I16. Returns false otherwise.
    pub fn is_i16(&self) -> bool {
        if let Value::I16(_) = self {
            true
        } else {
            false
        }
    }

    /// If the `Value` is an I16, return a reference to it. Returns Err otherwise.
    pub fn as_i16(&self) -> Result<i16, ParquetError> {
        if let Value::I16(ret) = self {
            Ok(*ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as i16",
                self
            )))
        }
    }

    /// If the `Value` is an I16, return it. Returns Err otherwise.
    pub fn into_i16(self) -> Result<i16, ParquetError> {
        if let Value::I16(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as i16",
                self
            )))
        }
    }

    /// Returns true if the `Value` is an U32. Returns false otherwise.
    pub fn is_u32(&self) -> bool {
        if let Value::U32(_) = self {
            true
        } else {
            false
        }
    }

    /// If the `Value` is an U32, return a reference to it. Returns Err otherwise.
    pub fn as_u32(&self) -> Result<u32, ParquetError> {
        if let Value::U32(ret) = self {
            Ok(*ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as u32",
                self
            )))
        }
    }

    /// If the `Value` is an U32, return it. Returns Err otherwise.
    pub fn into_u32(self) -> Result<u32, ParquetError> {
        if let Value::U32(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as u32",
                self
            )))
        }
    }

    /// Returns true if the `Value` is an I32. Returns false otherwise.
    pub fn is_i32(&self) -> bool {
        if let Value::I32(_) = self {
            true
        } else {
            false
        }
    }

    /// If the `Value` is an I32, return a reference to it. Returns Err otherwise.
    pub fn as_i32(&self) -> Result<i32, ParquetError> {
        if let Value::I32(ret) = self {
            Ok(*ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as i32",
                self
            )))
        }
    }

    /// If the `Value` is an I32, return it. Returns Err otherwise.
    pub fn into_i32(self) -> Result<i32, ParquetError> {
        if let Value::I32(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as i32",
                self
            )))
        }
    }

    /// Returns true if the `Value` is an U64. Returns false otherwise.
    pub fn is_u64(&self) -> bool {
        if let Value::U64(_) = self {
            true
        } else {
            false
        }
    }

    /// If the `Value` is an U64, return a reference to it. Returns Err otherwise.
    pub fn as_u64(&self) -> Result<u64, ParquetError> {
        if let Value::U64(ret) = self {
            Ok(*ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as u64",
                self
            )))
        }
    }

    /// If the `Value` is an U64, return it. Returns Err otherwise.
    pub fn into_u64(self) -> Result<u64, ParquetError> {
        if let Value::U64(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as u64",
                self
            )))
        }
    }

    /// Returns true if the `Value` is an I64. Returns false otherwise.
    pub fn is_i64(&self) -> bool {
        if let Value::I64(_) = self {
            true
        } else {
            false
        }
    }

    /// If the `Value` is an I64, return a reference to it. Returns Err otherwise.
    pub fn as_i64(&self) -> Result<i64, ParquetError> {
        if let Value::I64(ret) = self {
            Ok(*ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as i64",
                self
            )))
        }
    }

    /// If the `Value` is an I64, return it. Returns Err otherwise.
    pub fn into_i64(self) -> Result<i64, ParquetError> {
        if let Value::I64(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as i64",
                self
            )))
        }
    }

    /// Returns true if the `Value` is an F32. Returns false otherwise.
    pub fn is_f32(&self) -> bool {
        if let Value::F32(_) = self {
            true
        } else {
            false
        }
    }

    /// If the `Value` is an F32, return a reference to it. Returns Err otherwise.
    pub fn as_f32(&self) -> Result<f32, ParquetError> {
        if let Value::F32(ret) = self {
            Ok(*ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as f32",
                self
            )))
        }
    }

    /// If the `Value` is an F32, return it. Returns Err otherwise.
    pub fn into_f32(self) -> Result<f32, ParquetError> {
        if let Value::F32(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as f32",
                self
            )))
        }
    }

    /// Returns true if the `Value` is an F64. Returns false otherwise.
    pub fn is_f64(&self) -> bool {
        if let Value::F64(_) = self {
            true
        } else {
            false
        }
    }

    /// If the `Value` is an F64, return a reference to it. Returns Err otherwise.
    pub fn as_f64(&self) -> Result<f64, ParquetError> {
        if let Value::F64(ret) = self {
            Ok(*ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as f64",
                self
            )))
        }
    }

    /// If the `Value` is an F64, return it. Returns Err otherwise.
    pub fn into_f64(self) -> Result<f64, ParquetError> {
        if let Value::F64(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as f64",
                self
            )))
        }
    }

    /// Returns true if the `Value` is an Date. Returns false otherwise.
    pub fn is_date(&self) -> bool {
        if let Value::Date(_) = self {
            true
        } else {
            false
        }
    }

    /// If the `Value` is an Date, return a reference to it. Returns Err otherwise.
    pub fn as_date(&self) -> Result<&Date, ParquetError> {
        if let Value::Date(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as date",
                self
            )))
        }
    }

    /// If the `Value` is an Date, return it. Returns Err otherwise.
    pub fn into_date(self) -> Result<Date, ParquetError> {
        if let Value::Date(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as date",
                self
            )))
        }
    }

    /// Returns true if the `Value` is an Time. Returns false otherwise.
    pub fn is_time(&self) -> bool {
        if let Value::Time(_) = self {
            true
        } else {
            false
        }
    }

    /// If the `Value` is an Time, return a reference to it. Returns Err otherwise.
    pub fn as_time(&self) -> Result<&Time, ParquetError> {
        if let Value::Time(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as time",
                self
            )))
        }
    }

    /// If the `Value` is an Time, return it. Returns Err otherwise.
    pub fn into_time(self) -> Result<Time, ParquetError> {
        if let Value::Time(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as time",
                self
            )))
        }
    }

    /// Returns true if the `Value` is an Timestamp. Returns false otherwise.
    pub fn is_timestamp(&self) -> bool {
        if let Value::Timestamp(_) = self {
            true
        } else {
            false
        }
    }

    /// If the `Value` is an Timestamp, return a reference to it. Returns Err otherwise.
    pub fn as_timestamp(&self) -> Result<&Timestamp, ParquetError> {
        if let Value::Timestamp(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as timestamp",
                self
            )))
        }
    }

    /// If the `Value` is an Timestamp, return it. Returns Err otherwise.
    pub fn into_timestamp(self) -> Result<Timestamp, ParquetError> {
        if let Value::Timestamp(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as timestamp",
                self
            )))
        }
    }

    /// Returns true if the `Value` is an Decimal. Returns false otherwise.
    pub fn is_decimal(&self) -> bool {
        if let Value::Decimal(_) = self {
            true
        } else {
            false
        }
    }

    /// If the `Value` is an Decimal, return a reference to it. Returns Err otherwise.
    pub fn as_decimal(&self) -> Result<&Decimal, ParquetError> {
        if let Value::Decimal(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as decimal",
                self
            )))
        }
    }

    /// If the `Value` is an Decimal, return it. Returns Err otherwise.
    pub fn into_decimal(self) -> Result<Decimal, ParquetError> {
        if let Value::Decimal(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as decimal",
                self
            )))
        }
    }

    /// Returns true if the `Value` is an Array. Returns false otherwise.
    pub fn is_array(&self) -> bool {
        if let Value::Array(_) = self {
            true
        } else {
            false
        }
    }

    /// If the `Value` is an Array, return a reference to it. Returns Err otherwise.
    pub fn as_array(&self) -> Result<&Vec<u8>, ParquetError> {
        if let Value::Array(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as array",
                self
            )))
        }
    }

    /// If the `Value` is an Array, return it. Returns Err otherwise.
    pub fn into_array(self) -> Result<Vec<u8>, ParquetError> {
        if let Value::Array(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as array",
                self
            )))
        }
    }

    /// Returns true if the `Value` is an Bson. Returns false otherwise.
    pub fn is_bson(&self) -> bool {
        if let Value::Bson(_) = self {
            true
        } else {
            false
        }
    }

    /// If the `Value` is an Bson, return a reference to it. Returns Err otherwise.
    pub fn as_bson(&self) -> Result<&Bson, ParquetError> {
        if let Value::Bson(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as bson",
                self
            )))
        }
    }

    /// If the `Value` is an Bson, return it. Returns Err otherwise.
    pub fn into_bson(self) -> Result<Bson, ParquetError> {
        if let Value::Bson(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as bson",
                self
            )))
        }
    }

    /// Returns true if the `Value` is an String. Returns false otherwise.
    pub fn is_string(&self) -> bool {
        if let Value::String(_) = self {
            true
        } else {
            false
        }
    }

    /// If the `Value` is an String, return a reference to it. Returns Err otherwise.
    pub fn as_string(&self) -> Result<&String, ParquetError> {
        if let Value::String(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as string",
                self
            )))
        }
    }

    /// If the `Value` is an String, return it. Returns Err otherwise.
    pub fn into_string(self) -> Result<String, ParquetError> {
        if let Value::String(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as string",
                self
            )))
        }
    }

    /// Returns true if the `Value` is an Json. Returns false otherwise.
    pub fn is_json(&self) -> bool {
        if let Value::Json(_) = self {
            true
        } else {
            false
        }
    }

    /// If the `Value` is an Json, return a reference to it. Returns Err otherwise.
    pub fn as_json(&self) -> Result<&Json, ParquetError> {
        if let Value::Json(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as json",
                self
            )))
        }
    }

    /// If the `Value` is an Json, return it. Returns Err otherwise.
    pub fn into_json(self) -> Result<Json, ParquetError> {
        if let Value::Json(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as json",
                self
            )))
        }
    }

    /// Returns true if the `Value` is an Enum. Returns false otherwise.
    pub fn is_enum(&self) -> bool {
        if let Value::Enum(_) = self {
            true
        } else {
            false
        }
    }

    /// If the `Value` is an Enum, return a reference to it. Returns Err otherwise.
    pub fn as_enum(&self) -> Result<&Enum, ParquetError> {
        if let Value::Enum(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as enum",
                self
            )))
        }
    }

    /// If the `Value` is an Enum, return it. Returns Err otherwise.
    pub fn into_enum(self) -> Result<Enum, ParquetError> {
        if let Value::Enum(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as enum",
                self
            )))
        }
    }

    /// Returns true if the `Value` is an List. Returns false otherwise.
    pub fn is_list(&self) -> bool {
        if let Value::List(_) = self {
            true
        } else {
            false
        }
    }

    /// If the `Value` is an List, return a reference to it. Returns Err otherwise.
    pub fn as_list(&self) -> Result<&List<Value>, ParquetError> {
        if let Value::List(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as list",
                self
            )))
        }
    }

    /// If the `Value` is an List, return it. Returns Err otherwise.
    pub fn into_list(self) -> Result<List<Value>, ParquetError> {
        if let Value::List(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as list",
                self
            )))
        }
    }

    /// Returns true if the `Value` is an Map. Returns false otherwise.
    pub fn is_map(&self) -> bool {
        if let Value::Map(_) = self {
            true
        } else {
            false
        }
    }

    /// If the `Value` is an Map, return a reference to it. Returns Err otherwise.
    pub fn as_map(&self) -> Result<&Map<Value, Value>, ParquetError> {
        if let Value::Map(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as map",
                self
            )))
        }
    }

    /// If the `Value` is an Map, return it. Returns Err otherwise.
    pub fn into_map(self) -> Result<Map<Value, Value>, ParquetError> {
        if let Value::Map(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as map",
                self
            )))
        }
    }

    /// Returns true if the `Value` is an Group. Returns false otherwise.
    pub fn is_group(&self) -> bool {
        if let Value::Group(_) = self {
            true
        } else {
            false
        }
    }

    /// If the `Value` is an Group, return a reference to it. Returns Err otherwise.
    pub fn as_group(&self) -> Result<&Group, ParquetError> {
        if let Value::Group(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as group",
                self
            )))
        }
    }

    /// If the `Value` is an Group, return it. Returns Err otherwise.
    pub fn into_group(self) -> Result<Group, ParquetError> {
        if let Value::Group(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as group",
                self
            )))
        }
    }

    /// Returns true if the `Value` is an Option. Returns false otherwise.
    pub fn is_option(&self) -> bool {
        if let Value::Option(_) = self {
            true
        } else {
            false
        }
    }

    /// If the `Value` is an Option, return a reference to it. Returns Err otherwise.
    pub fn as_option(&self) -> Result<&Option<Value>, ParquetError> {
        if let Value::Option(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as option",
                self
            )))
        }
    }

    /// If the `Value` is an Option, return it. Returns Err otherwise.
    pub fn into_option(self) -> Result<Option<Value>, ParquetError> {
        if let Value::Option(ret) = self {
            Ok(*ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as option",
                self
            )))
        }
    }
}

impl Downcast<Value> for Value {
    fn downcast(self) -> Result<Value, ParquetError> {
        Ok(self)
    }
}
impl Downcast<bool> for Value {
    fn downcast(self) -> Result<bool, ParquetError> {
        self.into_bool()
    }
}
impl Downcast<u8> for Value {
    fn downcast(self) -> Result<u8, ParquetError> {
        self.into_u8()
    }
}
impl Downcast<i8> for Value {
    fn downcast(self) -> Result<i8, ParquetError> {
        self.into_i8()
    }
}
impl Downcast<u16> for Value {
    fn downcast(self) -> Result<u16, ParquetError> {
        self.into_u16()
    }
}
impl Downcast<i16> for Value {
    fn downcast(self) -> Result<i16, ParquetError> {
        self.into_i16()
    }
}
impl Downcast<u32> for Value {
    fn downcast(self) -> Result<u32, ParquetError> {
        self.into_u32()
    }
}
impl Downcast<i32> for Value {
    fn downcast(self) -> Result<i32, ParquetError> {
        self.into_i32()
    }
}
impl Downcast<u64> for Value {
    fn downcast(self) -> Result<u64, ParquetError> {
        self.into_u64()
    }
}
impl Downcast<i64> for Value {
    fn downcast(self) -> Result<i64, ParquetError> {
        self.into_i64()
    }
}
impl Downcast<f32> for Value {
    fn downcast(self) -> Result<f32, ParquetError> {
        self.into_f32()
    }
}
impl Downcast<f64> for Value {
    fn downcast(self) -> Result<f64, ParquetError> {
        self.into_f64()
    }
}
impl Downcast<Date> for Value {
    fn downcast(self) -> Result<Date, ParquetError> {
        self.into_date()
    }
}
impl Downcast<Time> for Value {
    fn downcast(self) -> Result<Time, ParquetError> {
        self.into_time()
    }
}
impl Downcast<Timestamp> for Value {
    fn downcast(self) -> Result<Timestamp, ParquetError> {
        self.into_timestamp()
    }
}
impl Downcast<Decimal> for Value {
    fn downcast(self) -> Result<Decimal, ParquetError> {
        self.into_decimal()
    }
}
impl Downcast<Vec<u8>> for Value {
    fn downcast(self) -> Result<Vec<u8>, ParquetError> {
        self.into_array()
    }
}
impl Downcast<Bson> for Value {
    fn downcast(self) -> Result<Bson, ParquetError> {
        self.into_bson()
    }
}
impl Downcast<String> for Value {
    fn downcast(self) -> Result<String, ParquetError> {
        self.into_string()
    }
}
impl Downcast<Json> for Value {
    fn downcast(self) -> Result<Json, ParquetError> {
        self.into_json()
    }
}
impl Downcast<Enum> for Value {
    fn downcast(self) -> Result<Enum, ParquetError> {
        self.into_enum()
    }
}
impl<T> Downcast<List<T>> for Value
where
    Value: Downcast<T>,
{
    default fn downcast(self) -> Result<List<T>, ParquetError> {
        let ret = self.into_list()?;
        ret.0
            .into_iter()
            .map(Downcast::downcast)
            .collect::<Result<Vec<_>, _>>()
            .map(List)
    }
}
impl Downcast<List<Value>> for Value {
    fn downcast(self) -> Result<List<Value>, ParquetError> {
        self.into_list()
    }
}
impl<K, V> Downcast<Map<K, V>> for Value
where
    Value: Downcast<K> + Downcast<V>,
    K: Hash + Eq,
{
    default fn downcast(self) -> Result<Map<K, V>, ParquetError> {
        let ret = self.into_map()?;
        ret.0
            .into_iter()
            .map(|(k, v)| Ok((k.downcast()?, v.downcast()?)))
            .collect::<Result<HashMap<_, _>, _>>()
            .map(Map)
    }
}
impl Downcast<Map<Value, Value>> for Value {
    fn downcast(self) -> Result<Map<Value, Value>, ParquetError> {
        self.into_map()
    }
}
impl Downcast<Group> for Value {
    fn downcast(self) -> Result<Group, ParquetError> {
        self.into_group()
    }
}
impl<T> Downcast<Option<T>> for Value
where
    Value: Downcast<T>,
{
    default fn downcast(self) -> Result<Option<T>, ParquetError> {
        let ret = self.into_option()?;
        match ret {
            Some(t) => Downcast::<T>::downcast(t).map(Some),
            None => Ok(None),
        }
    }
}
impl Downcast<Option<Value>> for Value {
    fn downcast(self) -> Result<Option<Value>, ParquetError> {
        self.into_option()
    }
}

impl Deserialize for Value {
    type Reader = ValueReader;
    type Schema = ValueSchema;

    fn parse(
        schema: &Type,
        repetition: Option<Repetition>,
    ) -> Result<(String, Self::Schema), ParquetError> {
        let mut value = None;
        if repetition.is_some() && schema.is_primitive() {
            value = Some(
                match (
                    schema.get_physical_type(),
                    schema.get_basic_info().logical_type(),
                ) {
                    // https://github.com/apache/parquet-format/blob/master/LogicalTypes.md
                    (PhysicalType::BOOLEAN, LogicalType::NONE) => ValueSchema::Bool(BoolSchema),
                    (PhysicalType::INT32, LogicalType::UINT_8) => ValueSchema::U8(U8Schema),
                    (PhysicalType::INT32, LogicalType::INT_8) => ValueSchema::I8(I8Schema),
                    (PhysicalType::INT32, LogicalType::UINT_16) => ValueSchema::U16(U16Schema),
                    (PhysicalType::INT32, LogicalType::INT_16) => ValueSchema::I16(I16Schema),
                    (PhysicalType::INT32, LogicalType::UINT_32) => ValueSchema::U32(U32Schema),
                    (PhysicalType::INT32, LogicalType::INT_32)
                    | (PhysicalType::INT32, LogicalType::NONE) => ValueSchema::I32(I32Schema),
                    (PhysicalType::INT32, LogicalType::DATE) => ValueSchema::Date(DateSchema),
                    (PhysicalType::INT32, LogicalType::TIME_MILLIS) => {
                        ValueSchema::Time(TimeSchema::Millis)
                    }
                    (PhysicalType::INT32, LogicalType::DECIMAL) => {
                        let (precision, scale) = (schema.get_precision(), schema.get_scale());
                        let (precision, scale) =
                            (precision.try_into().unwrap(), scale.try_into().unwrap());
                        ValueSchema::Decimal(DecimalSchema::Int32 { precision, scale })
                    }
                    (PhysicalType::INT64, LogicalType::UINT_64) => ValueSchema::U64(U64Schema),
                    (PhysicalType::INT64, LogicalType::INT_64)
                    | (PhysicalType::INT64, LogicalType::NONE) => ValueSchema::I64(I64Schema),
                    (PhysicalType::INT64, LogicalType::TIME_MICROS) => {
                        ValueSchema::Time(TimeSchema::Micros)
                    }
                    (PhysicalType::INT64, LogicalType::TIMESTAMP_MILLIS) => {
                        ValueSchema::Timestamp(TimestampSchema::Millis)
                    }
                    (PhysicalType::INT64, LogicalType::TIMESTAMP_MICROS) => {
                        ValueSchema::Timestamp(TimestampSchema::Micros)
                    }
                    (PhysicalType::INT64, LogicalType::DECIMAL) => {
                        let (precision, scale) = (schema.get_precision(), schema.get_scale());
                        let (precision, scale) =
                            (precision.try_into().unwrap(), scale.try_into().unwrap());
                        ValueSchema::Decimal(DecimalSchema::Int64 { precision, scale })
                    }
                    (PhysicalType::INT96, LogicalType::NONE) => {
                        ValueSchema::Timestamp(TimestampSchema::Int96)
                    }
                    (PhysicalType::FLOAT, LogicalType::NONE) => ValueSchema::F32(F32Schema),
                    (PhysicalType::DOUBLE, LogicalType::NONE) => ValueSchema::F64(F64Schema),
                    (PhysicalType::BYTE_ARRAY, LogicalType::UTF8)
                    | (PhysicalType::FIXED_LEN_BYTE_ARRAY, LogicalType::UTF8) => {
                        ValueSchema::String(StringSchema)
                    }
                    (PhysicalType::BYTE_ARRAY, LogicalType::JSON)
                    | (PhysicalType::FIXED_LEN_BYTE_ARRAY, LogicalType::JSON) => {
                        ValueSchema::Json(JsonSchema)
                    }
                    (PhysicalType::BYTE_ARRAY, LogicalType::ENUM)
                    | (PhysicalType::FIXED_LEN_BYTE_ARRAY, LogicalType::ENUM) => {
                        ValueSchema::Enum(EnumSchema)
                    }
                    (PhysicalType::BYTE_ARRAY, LogicalType::NONE)
                    | (PhysicalType::FIXED_LEN_BYTE_ARRAY, LogicalType::NONE) => {
                        ValueSchema::Array(VecSchema(
                            if schema.get_physical_type() == PhysicalType::FIXED_LEN_BYTE_ARRAY {
                                Some(schema.get_type_length().try_into().unwrap())
                            } else {
                                None
                            },
                        ))
                    }
                    (PhysicalType::BYTE_ARRAY, LogicalType::BSON)
                    | (PhysicalType::FIXED_LEN_BYTE_ARRAY, LogicalType::BSON) => {
                        ValueSchema::Bson(BsonSchema(
                            if schema.get_physical_type() == PhysicalType::FIXED_LEN_BYTE_ARRAY {
                                Some(schema.get_type_length().try_into().unwrap())
                            } else {
                                None
                            },
                        ))
                    }
                    (PhysicalType::BYTE_ARRAY, LogicalType::DECIMAL)
                    | (PhysicalType::FIXED_LEN_BYTE_ARRAY, LogicalType::DECIMAL) => {
                        let (precision, scale) = (schema.get_precision(), schema.get_scale());
                        let (precision, scale) =
                            (precision.try_into().unwrap(), scale.try_into().unwrap());
                        ValueSchema::Decimal(DecimalSchema::Array { precision, scale })
                    }
                    (PhysicalType::BYTE_ARRAY, LogicalType::INTERVAL)
                    | (PhysicalType::FIXED_LEN_BYTE_ARRAY, LogicalType::INTERVAL) => {
                        unimplemented!()
                    }
                    (physical_type, logical_type) => {
                        return Err(ParquetError::General(format!(
                            "Can't parse primitive ({:?}, {:?})",
                            physical_type, logical_type
                        )));
                    }
                },
            );
        }
        // https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#backward-compatibility-rules
        if repetition.is_some() && value.is_none() {
            value = parse_list::<Value>(schema)
                .ok()
                .map(|value| ValueSchema::List(Box::new(value)));
        }
        if repetition.is_some() && value.is_none() {
            value = parse_map::<Value, Value>(schema)
                .ok()
                .map(|value| ValueSchema::Map(Box::new(value)));
        }

        if repetition.is_some() && value.is_none() && schema.is_group() {
            let mut lookup = HashMap::new();
            value = Some(ValueSchema::Group(GroupSchema(
                schema
                    .get_fields()
                    .iter()
                    .map(|schema| {
                        Value::parse(&*schema, Some(schema.get_basic_info().repetition())).map(
                            |(name, schema)| {
                                let x = lookup.insert(name, lookup.len());
                                assert!(x.is_none());
                                schema
                            },
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                lookup,
            )));
        }

        let mut value = value
            .ok_or_else(|| ParquetError::General(format!("Can't parse value {:?}", schema)))?;

        match repetition.unwrap() {
            Repetition::OPTIONAL => {
                value = ValueSchema::Option(Box::new(OptionSchema(value)));
            }
            Repetition::REPEATED => {
                value = ValueSchema::List(Box::new(ListSchema(value, ListSchemaType::Repeated)));
            }
            Repetition::REQUIRED => (),
        }

        Ok((schema.name().to_owned(), value))
    }

    fn reader(
        schema: &Self::Schema,
        path: &mut Vec<String>,
        def_level: i16,
        rep_level: i16,
        paths: &mut HashMap<ColumnPath, ColumnReader>,
        batch_size: usize,
    ) -> Self::Reader {
        match *schema {
            ValueSchema::Bool(ref schema) => ValueReader::Bool(<bool as Deserialize>::reader(
                schema, path, def_level, rep_level, paths, batch_size,
            )),
            ValueSchema::U8(ref schema) => ValueReader::U8(<u8 as Deserialize>::reader(
                schema, path, def_level, rep_level, paths, batch_size,
            )),
            ValueSchema::I8(ref schema) => ValueReader::I8(<i8 as Deserialize>::reader(
                schema, path, def_level, rep_level, paths, batch_size,
            )),
            ValueSchema::U16(ref schema) => ValueReader::U16(<u16 as Deserialize>::reader(
                schema, path, def_level, rep_level, paths, batch_size,
            )),
            ValueSchema::I16(ref schema) => ValueReader::I16(<i16 as Deserialize>::reader(
                schema, path, def_level, rep_level, paths, batch_size,
            )),
            ValueSchema::U32(ref schema) => ValueReader::U32(<u32 as Deserialize>::reader(
                schema, path, def_level, rep_level, paths, batch_size,
            )),
            ValueSchema::I32(ref schema) => ValueReader::I32(<i32 as Deserialize>::reader(
                schema, path, def_level, rep_level, paths, batch_size,
            )),
            ValueSchema::U64(ref schema) => ValueReader::U64(<u64 as Deserialize>::reader(
                schema, path, def_level, rep_level, paths, batch_size,
            )),
            ValueSchema::I64(ref schema) => ValueReader::I64(<i64 as Deserialize>::reader(
                schema, path, def_level, rep_level, paths, batch_size,
            )),
            ValueSchema::F32(ref schema) => ValueReader::F32(<f32 as Deserialize>::reader(
                schema, path, def_level, rep_level, paths, batch_size,
            )),
            ValueSchema::F64(ref schema) => ValueReader::F64(<f64 as Deserialize>::reader(
                schema, path, def_level, rep_level, paths, batch_size,
            )),
            ValueSchema::Date(ref schema) => ValueReader::Date(<Date as Deserialize>::reader(
                schema, path, def_level, rep_level, paths, batch_size,
            )),
            ValueSchema::Time(ref schema) => ValueReader::Time(<Time as Deserialize>::reader(
                schema, path, def_level, rep_level, paths, batch_size,
            )),
            ValueSchema::Timestamp(ref schema) => {
                ValueReader::Timestamp(<Timestamp as Deserialize>::reader(
                    schema, path, def_level, rep_level, paths, batch_size,
                ))
            }
            ValueSchema::Decimal(ref schema) => {
                ValueReader::Decimal(<Decimal as Deserialize>::reader(
                    schema, path, def_level, rep_level, paths, batch_size,
                ))
            }
            ValueSchema::Array(ref schema) => ValueReader::Array(<Vec<u8> as Deserialize>::reader(
                schema, path, def_level, rep_level, paths, batch_size,
            )),
            ValueSchema::Bson(ref schema) => ValueReader::Bson(<Bson as Deserialize>::reader(
                schema, path, def_level, rep_level, paths, batch_size,
            )),
            ValueSchema::String(ref schema) => {
                ValueReader::String(<String as Deserialize>::reader(
                    schema, path, def_level, rep_level, paths, batch_size,
                ))
            }
            ValueSchema::Json(ref schema) => ValueReader::Json(<Json as Deserialize>::reader(
                schema, path, def_level, rep_level, paths, batch_size,
            )),
            ValueSchema::Enum(ref schema) => ValueReader::Enum(<Enum as Deserialize>::reader(
                schema, path, def_level, rep_level, paths, batch_size,
            )),
            ValueSchema::List(ref schema) => {
                ValueReader::List(Box::new(<List<Value> as Deserialize>::reader(
                    schema, path, def_level, rep_level, paths, batch_size,
                )))
            }
            ValueSchema::Map(ref schema) => {
                ValueReader::Map(Box::new(<Map<Value, Value> as Deserialize>::reader(
                    schema, path, def_level, rep_level, paths, batch_size,
                )))
            }
            ValueSchema::Group(ref schema) => ValueReader::Group(<Group as Deserialize>::reader(
                schema, path, def_level, rep_level, paths, batch_size,
            )),
            ValueSchema::Option(ref schema) => {
                ValueReader::Option(Box::new(<Option<Value> as Deserialize>::reader(
                    schema, path, def_level, rep_level, paths, batch_size,
                )))
            }
        }
    }
}
