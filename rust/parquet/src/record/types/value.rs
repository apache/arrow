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
            BoolSchema, BsonSchema, ByteArraySchema, DateSchema, DecimalSchema,
            EnumSchema, F32Schema, F64Schema, GroupSchema, I16Schema, I32Schema,
            I64Schema, I8Schema, JsonSchema, ListSchema, ListSchemaType, OptionSchema,
            StringSchema, TimeSchema, TimestampSchema, U16Schema, U32Schema, U64Schema,
            U8Schema, ValueSchema,
        },
        types::{
            list::parse_list, map::parse_map, Bson, Date, Downcast, Enum, Group, Json,
            List, Map, Time, Timestamp, ValueRequired,
        },
        Deserialize,
    },
    schema::types::{ColumnPath, Type},
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
    ByteArray(Vec<u8>),
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
    Option(Option<ValueRequired>),
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
            Value::ByteArray(value) => {
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

    /// Returns true if the `Value` is an ByteArray. Returns false otherwise.
    pub fn is_byte_array(&self) -> bool {
        if let Value::ByteArray(_) = self {
            true
        } else {
            false
        }
    }

    /// If the `Value` is an ByteArray, return a reference to it. Returns Err otherwise.
    pub fn as_byte_array(&self) -> Result<&Vec<u8>, ParquetError> {
        if let Value::ByteArray(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as byte_array",
                self
            )))
        }
    }

    /// If the `Value` is an ByteArray, return it. Returns Err otherwise.
    pub fn into_byte_array(self) -> Result<Vec<u8>, ParquetError> {
        if let Value::ByteArray(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as byte_array",
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
    fn as_option(&self) -> Result<&Option<ValueRequired>, ParquetError> {
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
            Ok(ret.map(Into::into))
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as option",
                self
            )))
        }
    }
}

impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Value::Bool(value)
    }
}
impl From<u8> for Value {
    fn from(value: u8) -> Self {
        Value::U8(value)
    }
}
impl From<i8> for Value {
    fn from(value: i8) -> Self {
        Value::I8(value)
    }
}
impl From<u16> for Value {
    fn from(value: u16) -> Self {
        Value::U16(value)
    }
}
impl From<i16> for Value {
    fn from(value: i16) -> Self {
        Value::I16(value)
    }
}
impl From<u32> for Value {
    fn from(value: u32) -> Self {
        Value::U32(value)
    }
}
impl From<i32> for Value {
    fn from(value: i32) -> Self {
        Value::I32(value)
    }
}
impl From<u64> for Value {
    fn from(value: u64) -> Self {
        Value::U64(value)
    }
}
impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Value::I64(value)
    }
}
impl From<f32> for Value {
    fn from(value: f32) -> Self {
        Value::F32(value)
    }
}
impl From<f64> for Value {
    fn from(value: f64) -> Self {
        Value::F64(value)
    }
}
impl From<Date> for Value {
    fn from(value: Date) -> Self {
        Value::Date(value)
    }
}
impl From<Time> for Value {
    fn from(value: Time) -> Self {
        Value::Time(value)
    }
}
impl From<Timestamp> for Value {
    fn from(value: Timestamp) -> Self {
        Value::Timestamp(value)
    }
}
impl From<Decimal> for Value {
    fn from(value: Decimal) -> Self {
        Value::Decimal(value)
    }
}
impl From<Vec<u8>> for Value {
    fn from(value: Vec<u8>) -> Self {
        Value::ByteArray(value)
    }
}
impl From<Bson> for Value {
    fn from(value: Bson) -> Self {
        Value::Bson(value)
    }
}
impl From<String> for Value {
    fn from(value: String) -> Self {
        Value::String(value)
    }
}
impl From<Json> for Value {
    fn from(value: Json) -> Self {
        Value::Json(value)
    }
}
impl From<Enum> for Value {
    fn from(value: Enum) -> Self {
        Value::Enum(value)
    }
}
impl<T> From<List<T>> for Value
where
    Value: From<T>,
{
    default fn from(value: List<T>) -> Self {
        Value::List(List(value.0.into_iter().map(Into::into).collect()))
    }
}
impl From<List<Value>> for Value {
    fn from(value: List<Value>) -> Self {
        Value::List(value)
    }
}
impl<K, V> From<Map<K, V>> for Value
where
    Value: From<K> + From<V>,
    K: Hash + Eq,
{
    default fn from(value: Map<K, V>) -> Self {
        Value::Map(Map(value
            .0
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect()))
    }
}
impl From<Map<Value, Value>> for Value {
    fn from(value: Map<Value, Value>) -> Self {
        Value::Map(value)
    }
}
impl From<Group> for Value {
    fn from(value: Group) -> Self {
        Value::Group(value)
    }
}
impl<T> From<Option<T>> for Value
where
    Value: From<T>,
{
    default fn from(value: Option<T>) -> Self {
        Value::Option(
            value
                .map(Into::into)
                .map(|x| <Option<ValueRequired> as From<Value>>::from(x).unwrap()),
        )
    }
}
impl From<Option<Value>> for Value {
    fn from(value: Option<Value>) -> Self {
        Value::Option(value.map(|x| <Option<ValueRequired>>::from(x).unwrap()))
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
        self.into_byte_array()
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
        self.into_list().and_then(|list| {
            list.0
                .into_iter()
                .map(Downcast::downcast)
                .collect::<Result<Vec<_>, _>>()
                .map(List)
        })
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
        self.into_map().and_then(|map| {
            map.0
                .into_iter()
                .map(|(k, v)| Ok((k.downcast()?, v.downcast()?)))
                .collect::<Result<HashMap<_, _>, _>>()
                .map(Map)
        })
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
        match self.into_option()? {
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

impl PartialEq<bool> for Value {
    fn eq(&self, other: &bool) -> bool {
        self.as_bool().map(|bool| &bool == other).unwrap_or(false)
    }
}
impl PartialEq<u8> for Value {
    fn eq(&self, other: &u8) -> bool {
        self.as_u8().map(|u8| &u8 == other).unwrap_or(false)
    }
}
impl PartialEq<i8> for Value {
    fn eq(&self, other: &i8) -> bool {
        self.as_i8().map(|i8| &i8 == other).unwrap_or(false)
    }
}
impl PartialEq<u16> for Value {
    fn eq(&self, other: &u16) -> bool {
        self.as_u16().map(|u16| &u16 == other).unwrap_or(false)
    }
}
impl PartialEq<i16> for Value {
    fn eq(&self, other: &i16) -> bool {
        self.as_i16().map(|i16| &i16 == other).unwrap_or(false)
    }
}
impl PartialEq<u32> for Value {
    fn eq(&self, other: &u32) -> bool {
        self.as_u32().map(|u32| &u32 == other).unwrap_or(false)
    }
}
impl PartialEq<i32> for Value {
    fn eq(&self, other: &i32) -> bool {
        self.as_i32().map(|i32| &i32 == other).unwrap_or(false)
    }
}
impl PartialEq<u64> for Value {
    fn eq(&self, other: &u64) -> bool {
        self.as_u64().map(|u64| &u64 == other).unwrap_or(false)
    }
}
impl PartialEq<i64> for Value {
    fn eq(&self, other: &i64) -> bool {
        self.as_i64().map(|i64| &i64 == other).unwrap_or(false)
    }
}
impl PartialEq<f32> for Value {
    fn eq(&self, other: &f32) -> bool {
        self.as_f32().map(|f32| &f32 == other).unwrap_or(false)
    }
}
impl PartialEq<f64> for Value {
    fn eq(&self, other: &f64) -> bool {
        self.as_f64().map(|f64| &f64 == other).unwrap_or(false)
    }
}
impl PartialEq<Date> for Value {
    fn eq(&self, other: &Date) -> bool {
        self.as_date().map(|date| date == other).unwrap_or(false)
    }
}
impl PartialEq<Time> for Value {
    fn eq(&self, other: &Time) -> bool {
        self.as_time().map(|time| time == other).unwrap_or(false)
    }
}
impl PartialEq<Timestamp> for Value {
    fn eq(&self, other: &Timestamp) -> bool {
        self.as_timestamp()
            .map(|timestamp| timestamp == other)
            .unwrap_or(false)
    }
}
impl PartialEq<Decimal> for Value {
    fn eq(&self, other: &Decimal) -> bool {
        self.as_decimal()
            .map(|decimal| decimal == other)
            .unwrap_or(false)
    }
}
impl PartialEq<Vec<u8>> for Value {
    fn eq(&self, other: &Vec<u8>) -> bool {
        self.as_byte_array()
            .map(|byte_array| byte_array == other)
            .unwrap_or(false)
    }
}
impl PartialEq<Bson> for Value {
    fn eq(&self, other: &Bson) -> bool {
        self.as_bson().map(|bson| bson == other).unwrap_or(false)
    }
}
impl PartialEq<String> for Value {
    fn eq(&self, other: &String) -> bool {
        self.as_string()
            .map(|string| string == other)
            .unwrap_or(false)
    }
}
impl PartialEq<Json> for Value {
    fn eq(&self, other: &Json) -> bool {
        self.as_json().map(|json| json == other).unwrap_or(false)
    }
}
impl PartialEq<Enum> for Value {
    fn eq(&self, other: &Enum) -> bool {
        self.as_enum().map(|enum_| enum_ == other).unwrap_or(false)
    }
}
impl<T> PartialEq<List<T>> for Value
where
    Value: PartialEq<T>,
{
    fn eq(&self, other: &List<T>) -> bool {
        self.as_list().map(|list| list == other).unwrap_or(false)
    }
}
impl<K, V> PartialEq<Map<K, V>> for Value
where
    Value: PartialEq<K> + PartialEq<V>,
    K: Hash + Eq + Clone + Into<Value>,
{
    fn eq(&self, other: &Map<K, V>) -> bool {
        self.as_map()
            .map(|map| {
                if map.0.len() != other.0.len() {
                    return false;
                }

                let other = other
                    .0
                    .iter()
                    .map(|(k, v)| (k.clone().into(), v))
                    .collect::<HashMap<Value, _>>();

                map.0
                    .iter()
                    .all(|(key, value)| other.get(key).map_or(false, |v| value == *v))
            })
            .unwrap_or(false)
    }
}
impl PartialEq<Group> for Value {
    fn eq(&self, other: &Group) -> bool {
        self.as_group().map(|group| group == other).unwrap_or(false)
    }
}
impl<T> PartialEq<Option<T>> for Value
where
    Value: PartialEq<T>,
{
    fn eq(&self, other: &Option<T>) -> bool {
        self.as_option()
            .map(|option| match (&option, other) {
                (Some(a), Some(b)) if a.eq(b) => true,
                (None, None) => true,
                _ => false,
            })
            .unwrap_or(false)
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
                    (PhysicalType::BOOLEAN, LogicalType::NONE) => {
                        ValueSchema::Bool(BoolSchema)
                    }
                    (PhysicalType::INT32, LogicalType::UINT_8) => {
                        ValueSchema::U8(U8Schema)
                    }
                    (PhysicalType::INT32, LogicalType::INT_8) => {
                        ValueSchema::I8(I8Schema)
                    }
                    (PhysicalType::INT32, LogicalType::UINT_16) => {
                        ValueSchema::U16(U16Schema)
                    }
                    (PhysicalType::INT32, LogicalType::INT_16) => {
                        ValueSchema::I16(I16Schema)
                    }
                    (PhysicalType::INT32, LogicalType::UINT_32) => {
                        ValueSchema::U32(U32Schema)
                    }
                    (PhysicalType::INT32, LogicalType::INT_32)
                    | (PhysicalType::INT32, LogicalType::NONE) => {
                        ValueSchema::I32(I32Schema)
                    }
                    (PhysicalType::INT32, LogicalType::DATE) => {
                        ValueSchema::Date(DateSchema)
                    }
                    (PhysicalType::INT32, LogicalType::TIME_MILLIS) => {
                        ValueSchema::Time(TimeSchema::Millis)
                    }
                    (PhysicalType::INT32, LogicalType::DECIMAL) => {
                        let (precision, scale) =
                            (schema.get_precision(), schema.get_scale());
                        let (precision, scale) =
                            (precision.try_into().unwrap(), scale.try_into().unwrap());
                        ValueSchema::Decimal(DecimalSchema::Int32 { precision, scale })
                    }
                    (PhysicalType::INT64, LogicalType::UINT_64) => {
                        ValueSchema::U64(U64Schema)
                    }
                    (PhysicalType::INT64, LogicalType::INT_64)
                    | (PhysicalType::INT64, LogicalType::NONE) => {
                        ValueSchema::I64(I64Schema)
                    }
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
                        let (precision, scale) =
                            (schema.get_precision(), schema.get_scale());
                        let (precision, scale) =
                            (precision.try_into().unwrap(), scale.try_into().unwrap());
                        ValueSchema::Decimal(DecimalSchema::Int64 { precision, scale })
                    }
                    (PhysicalType::INT96, LogicalType::NONE) => {
                        ValueSchema::Timestamp(TimestampSchema::Int96)
                    }
                    (PhysicalType::FLOAT, LogicalType::NONE) => {
                        ValueSchema::F32(F32Schema)
                    }
                    (PhysicalType::DOUBLE, LogicalType::NONE) => {
                        ValueSchema::F64(F64Schema)
                    }
                    (PhysicalType::BYTE_ARRAY, LogicalType::UTF8)
                    | (PhysicalType::FIXED_LEN_BYTE_ARRAY, LogicalType::UTF8) => {
                        ValueSchema::String(StringSchema(ByteArraySchema(
                            if schema.get_physical_type()
                                == PhysicalType::FIXED_LEN_BYTE_ARRAY
                            {
                                Some(schema.get_type_length().try_into().unwrap())
                            } else {
                                None
                            },
                        )))
                    }
                    (PhysicalType::BYTE_ARRAY, LogicalType::JSON)
                    | (PhysicalType::FIXED_LEN_BYTE_ARRAY, LogicalType::JSON) => {
                        ValueSchema::Json(JsonSchema(StringSchema(ByteArraySchema(
                            if schema.get_physical_type()
                                == PhysicalType::FIXED_LEN_BYTE_ARRAY
                            {
                                Some(schema.get_type_length().try_into().unwrap())
                            } else {
                                None
                            },
                        ))))
                    }
                    (PhysicalType::BYTE_ARRAY, LogicalType::ENUM)
                    | (PhysicalType::FIXED_LEN_BYTE_ARRAY, LogicalType::ENUM) => {
                        ValueSchema::Enum(EnumSchema(StringSchema(ByteArraySchema(
                            if schema.get_physical_type()
                                == PhysicalType::FIXED_LEN_BYTE_ARRAY
                            {
                                Some(schema.get_type_length().try_into().unwrap())
                            } else {
                                None
                            },
                        ))))
                    }
                    (PhysicalType::BYTE_ARRAY, LogicalType::NONE)
                    | (PhysicalType::FIXED_LEN_BYTE_ARRAY, LogicalType::NONE) => {
                        ValueSchema::ByteArray(ByteArraySchema(
                            if schema.get_physical_type()
                                == PhysicalType::FIXED_LEN_BYTE_ARRAY
                            {
                                Some(schema.get_type_length().try_into().unwrap())
                            } else {
                                None
                            },
                        ))
                    }
                    (PhysicalType::BYTE_ARRAY, LogicalType::BSON)
                    | (PhysicalType::FIXED_LEN_BYTE_ARRAY, LogicalType::BSON) => {
                        ValueSchema::Bson(BsonSchema(ByteArraySchema(
                            if schema.get_physical_type()
                                == PhysicalType::FIXED_LEN_BYTE_ARRAY
                            {
                                Some(schema.get_type_length().try_into().unwrap())
                            } else {
                                None
                            },
                        )))
                    }
                    (PhysicalType::BYTE_ARRAY, LogicalType::DECIMAL)
                    | (PhysicalType::FIXED_LEN_BYTE_ARRAY, LogicalType::DECIMAL) => {
                        let (precision, scale) =
                            (schema.get_precision(), schema.get_scale());
                        let (precision, scale) =
                            (precision.try_into().unwrap(), scale.try_into().unwrap());
                        ValueSchema::Decimal(DecimalSchema::Array { precision, scale })
                    }
                    (PhysicalType::BYTE_ARRAY, LogicalType::INTERVAL)
                    | (PhysicalType::FIXED_LEN_BYTE_ARRAY, LogicalType::INTERVAL) => {
                        unimplemented!("Interval logical type not yet implemented")
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
            let mut lookup = HashMap::with_capacity(schema.get_fields().len());
            value = Some(ValueSchema::Group(GroupSchema(
                schema
                    .get_fields()
                    .iter()
                    .map(|schema| {
                        Value::parse(&*schema, Some(schema.get_basic_info().repetition()))
                            .map(|(name, schema)| {
                                let x = lookup.insert(name, lookup.len());
                                assert!(x.is_none());
                                schema
                            })
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                lookup,
            )));
        }

        let mut value = value.ok_or_else(|| {
            ParquetError::General(format!("Can't parse value {:?}", schema))
        })?;

        match repetition.unwrap() {
            Repetition::OPTIONAL => {
                value = ValueSchema::Option(Box::new(OptionSchema(value)));
            }
            Repetition::REPEATED => {
                value = ValueSchema::List(Box::new(ListSchema(
                    value,
                    ListSchemaType::Repeated,
                )));
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
            ValueSchema::Bool(ref schema) => {
                ValueReader::Bool(<bool as Deserialize>::reader(
                    schema, path, def_level, rep_level, paths, batch_size,
                ))
            }
            ValueSchema::U8(ref schema) => ValueReader::U8(<u8 as Deserialize>::reader(
                schema, path, def_level, rep_level, paths, batch_size,
            )),
            ValueSchema::I8(ref schema) => ValueReader::I8(<i8 as Deserialize>::reader(
                schema, path, def_level, rep_level, paths, batch_size,
            )),
            ValueSchema::U16(ref schema) => {
                ValueReader::U16(<u16 as Deserialize>::reader(
                    schema, path, def_level, rep_level, paths, batch_size,
                ))
            }
            ValueSchema::I16(ref schema) => {
                ValueReader::I16(<i16 as Deserialize>::reader(
                    schema, path, def_level, rep_level, paths, batch_size,
                ))
            }
            ValueSchema::U32(ref schema) => {
                ValueReader::U32(<u32 as Deserialize>::reader(
                    schema, path, def_level, rep_level, paths, batch_size,
                ))
            }
            ValueSchema::I32(ref schema) => {
                ValueReader::I32(<i32 as Deserialize>::reader(
                    schema, path, def_level, rep_level, paths, batch_size,
                ))
            }
            ValueSchema::U64(ref schema) => {
                ValueReader::U64(<u64 as Deserialize>::reader(
                    schema, path, def_level, rep_level, paths, batch_size,
                ))
            }
            ValueSchema::I64(ref schema) => {
                ValueReader::I64(<i64 as Deserialize>::reader(
                    schema, path, def_level, rep_level, paths, batch_size,
                ))
            }
            ValueSchema::F32(ref schema) => {
                ValueReader::F32(<f32 as Deserialize>::reader(
                    schema, path, def_level, rep_level, paths, batch_size,
                ))
            }
            ValueSchema::F64(ref schema) => {
                ValueReader::F64(<f64 as Deserialize>::reader(
                    schema, path, def_level, rep_level, paths, batch_size,
                ))
            }
            ValueSchema::Date(ref schema) => {
                ValueReader::Date(<Date as Deserialize>::reader(
                    schema, path, def_level, rep_level, paths, batch_size,
                ))
            }
            ValueSchema::Time(ref schema) => {
                ValueReader::Time(<Time as Deserialize>::reader(
                    schema, path, def_level, rep_level, paths, batch_size,
                ))
            }
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
            ValueSchema::ByteArray(ref schema) => {
                ValueReader::ByteArray(<Vec<u8> as Deserialize>::reader(
                    schema, path, def_level, rep_level, paths, batch_size,
                ))
            }
            ValueSchema::Bson(ref schema) => {
                ValueReader::Bson(<Bson as Deserialize>::reader(
                    schema, path, def_level, rep_level, paths, batch_size,
                ))
            }
            ValueSchema::String(ref schema) => {
                ValueReader::String(<String as Deserialize>::reader(
                    schema, path, def_level, rep_level, paths, batch_size,
                ))
            }
            ValueSchema::Json(ref schema) => {
                ValueReader::Json(<Json as Deserialize>::reader(
                    schema, path, def_level, rep_level, paths, batch_size,
                ))
            }
            ValueSchema::Enum(ref schema) => {
                ValueReader::Enum(<Enum as Deserialize>::reader(
                    schema, path, def_level, rep_level, paths, batch_size,
                ))
            }
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
            ValueSchema::Group(ref schema) => {
                ValueReader::Group(<Group as Deserialize>::reader(
                    schema, path, def_level, rep_level, paths, batch_size,
                ))
            }
            ValueSchema::Option(ref schema) => {
                ValueReader::Option(Box::new(<Option<Value> as Deserialize>::reader(
                    schema, path, def_level, rep_level, paths, batch_size,
                )))
            }
        }
    }
}
