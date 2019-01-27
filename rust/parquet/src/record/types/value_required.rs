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
            BoolSchema, BsonSchema, ByteArraySchema, DateSchema, DecimalSchema, EnumSchema,
            F32Schema, F64Schema, GroupSchema, I16Schema, I32Schema, I64Schema, I8Schema,
            JsonSchema, ListSchema, ListSchemaType, OptionSchema, StringSchema, TimeSchema,
            TimestampSchema, U16Schema, U32Schema, U64Schema, U8Schema, ValueSchema,
        },
        types::{
            list::parse_list, map::parse_map, Bson, Date, Downcast, Enum, Group, Json, List, Map,
            Time, Timestamp, Value,
        },
        Deserialize,
    },
    schema::types::{ColumnDescPtr, ColumnPath, Type},
};

/// Represents any valid Parquet value.
#[derive(Clone, PartialEq, Debug)]
pub enum ValueRequired {
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
}

#[allow(clippy::derive_hash_xor_eq)]
impl Hash for ValueRequired {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            ValueRequired::Bool(value) => {
                0u8.hash(state);
                value.hash(state);
            }
            ValueRequired::U8(value) => {
                1u8.hash(state);
                value.hash(state);
            }
            ValueRequired::I8(value) => {
                2u8.hash(state);
                value.hash(state);
            }
            ValueRequired::U16(value) => {
                3u8.hash(state);
                value.hash(state);
            }
            ValueRequired::I16(value) => {
                4u8.hash(state);
                value.hash(state);
            }
            ValueRequired::U32(value) => {
                5u8.hash(state);
                value.hash(state);
            }
            ValueRequired::I32(value) => {
                6u8.hash(state);
                value.hash(state);
            }
            ValueRequired::U64(value) => {
                7u8.hash(state);
                value.hash(state);
            }
            ValueRequired::I64(value) => {
                8u8.hash(state);
                value.hash(state);
            }
            ValueRequired::F32(_value) => {
                9u8.hash(state);
            }
            ValueRequired::F64(_value) => {
                10u8.hash(state);
            }
            ValueRequired::Date(value) => {
                11u8.hash(state);
                value.hash(state);
            }
            ValueRequired::Time(value) => {
                12u8.hash(state);
                value.hash(state);
            }
            ValueRequired::Timestamp(value) => {
                13u8.hash(state);
                value.hash(state);
            }
            ValueRequired::Decimal(_value) => {
                14u8.hash(state);
            }
            ValueRequired::ByteArray(value) => {
                15u8.hash(state);
                value.hash(state);
            }
            ValueRequired::Bson(value) => {
                16u8.hash(state);
                value.hash(state);
            }
            ValueRequired::String(value) => {
                17u8.hash(state);
                value.hash(state);
            }
            ValueRequired::Json(value) => {
                18u8.hash(state);
                value.hash(state);
            }
            ValueRequired::Enum(value) => {
                19u8.hash(state);
                value.hash(state);
            }
            ValueRequired::List(value) => {
                20u8.hash(state);
                value.hash(state);
            }
            ValueRequired::Map(_value) => {
                21u8.hash(state);
            }
            ValueRequired::Group(_value) => {
                22u8.hash(state);
            }
        }
    }
}
impl Eq for ValueRequired {}

impl ValueRequired {
    pub(in super::super) fn eq<T>(&self, other: &T) -> bool
    where
        Value: PartialEq<T>,
    {
        let self_ = unsafe { std::ptr::read(self) };
        let self_: Value = self_.into();
        let ret = &self_ == other;
        std::mem::forget(self_);
        ret
    }
}

impl From<ValueRequired> for Value {
    fn from(value: ValueRequired) -> Self {
        match value {
            ValueRequired::Bool(value) => Value::Bool(value),
            ValueRequired::U8(value) => Value::U8(value),
            ValueRequired::I8(value) => Value::I8(value),
            ValueRequired::U16(value) => Value::U16(value),
            ValueRequired::I16(value) => Value::I16(value),
            ValueRequired::U32(value) => Value::U32(value),
            ValueRequired::I32(value) => Value::I32(value),
            ValueRequired::U64(value) => Value::U64(value),
            ValueRequired::I64(value) => Value::I64(value),
            ValueRequired::F32(value) => Value::F32(value),
            ValueRequired::F64(value) => Value::F64(value),
            ValueRequired::Date(value) => Value::Date(value),
            ValueRequired::Time(value) => Value::Time(value),
            ValueRequired::Timestamp(value) => Value::Timestamp(value),
            ValueRequired::Decimal(value) => Value::Decimal(value),
            ValueRequired::ByteArray(value) => Value::ByteArray(value),
            ValueRequired::Bson(value) => Value::Bson(value),
            ValueRequired::String(value) => Value::String(value),
            ValueRequired::Json(value) => Value::Json(value),
            ValueRequired::Enum(value) => Value::Enum(value),
            ValueRequired::List(value) => Value::List(value),
            ValueRequired::Map(value) => Value::Map(value),
            ValueRequired::Group(value) => Value::Group(value),
        }
    }
}

impl From<Value> for Option<ValueRequired> {
    fn from(value: Value) -> Self {
        Some(match value {
            Value::Bool(value) => ValueRequired::Bool(value),
            Value::U8(value) => ValueRequired::U8(value),
            Value::I8(value) => ValueRequired::I8(value),
            Value::U16(value) => ValueRequired::U16(value),
            Value::I16(value) => ValueRequired::I16(value),
            Value::U32(value) => ValueRequired::U32(value),
            Value::I32(value) => ValueRequired::I32(value),
            Value::U64(value) => ValueRequired::U64(value),
            Value::I64(value) => ValueRequired::I64(value),
            Value::F32(value) => ValueRequired::F32(value),
            Value::F64(value) => ValueRequired::F64(value),
            Value::Date(value) => ValueRequired::Date(value),
            Value::Time(value) => ValueRequired::Time(value),
            Value::Timestamp(value) => ValueRequired::Timestamp(value),
            Value::Decimal(value) => ValueRequired::Decimal(value),
            Value::ByteArray(value) => ValueRequired::ByteArray(value),
            Value::Bson(value) => ValueRequired::Bson(value),
            Value::String(value) => ValueRequired::String(value),
            Value::Json(value) => ValueRequired::Json(value),
            Value::Enum(value) => ValueRequired::Enum(value),
            Value::List(value) => ValueRequired::List(value),
            Value::Map(value) => ValueRequired::Map(value),
            Value::Group(value) => ValueRequired::Group(value),
            Value::Option(_) => return None,
        })
    }
}
