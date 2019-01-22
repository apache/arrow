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
    fmt::{self, Debug, Display},
    marker::PhantomData,
    mem,
    str::FromStr,
};

use super::{
    types::{Downcast, Root},
    Deserialize, DisplayDisplaySchema, DisplaySchema,
};
use crate::{basic::Repetition, errors::ParquetError, schema::parser::parse_message_type};

#[derive(Debug)]
pub struct BoolSchema;
impl DisplaySchema for BoolSchema {
    fn fmt(&self, r: Repetition, name: &str, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.write_fmt(format_args!("{} bool {};", r, name))
    }
    fn fmt_type(r: Repetition, name: &str, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.write_fmt(format_args!("{} bool {};", r, name))
    }
}

#[derive(Debug)]
pub struct U8Schema;
impl DisplaySchema for U8Schema {
    fn fmt(&self, r: Repetition, name: &str, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.write_fmt(format_args!("{} int32 {} (UINT_8);", r, name))
    }
    fn fmt_type(r: Repetition, name: &str, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.write_fmt(format_args!("{} int32 {} (UINT_8);", r, name))
    }
}

#[derive(Debug)]
pub struct I8Schema;
impl DisplaySchema for I8Schema {
    fn fmt(&self, r: Repetition, name: &str, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.write_fmt(format_args!("{} int32 {} (INT_8);", r, name))
    }
    fn fmt_type(r: Repetition, name: &str, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.write_fmt(format_args!("{} int32 {} (INT_8);", r, name))
    }
}

#[derive(Debug)]
pub struct U16Schema;
impl DisplaySchema for U16Schema {
    fn fmt(&self, r: Repetition, name: &str, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.write_fmt(format_args!("{} int32 {} (UINT_16);", r, name))
    }
    fn fmt_type(r: Repetition, name: &str, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.write_fmt(format_args!("{} int32 {} (UINT_16);", r, name))
    }
}

#[derive(Debug)]
pub struct I16Schema;
impl DisplaySchema for I16Schema {
    fn fmt(&self, r: Repetition, name: &str, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.write_fmt(format_args!("{} int32 {} (INT_16);", r, name))
    }
    fn fmt_type(r: Repetition, name: &str, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.write_fmt(format_args!("{} int32 {} (INT_16);", r, name))
    }
}

#[derive(Debug)]
pub struct U32Schema;
impl DisplaySchema for U32Schema {
    fn fmt(&self, r: Repetition, name: &str, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.write_fmt(format_args!("{} int32 {} (UINT_32);", r, name))
    }
    fn fmt_type(r: Repetition, name: &str, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.write_fmt(format_args!("{} int32 {} (UINT_32);", r, name))
    }
}

#[derive(Debug)]
pub struct I32Schema;
impl DisplaySchema for I32Schema {
    fn fmt(&self, r: Repetition, name: &str, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.write_fmt(format_args!("{} int32 {} (INT_32);", r, name))
    }
    fn fmt_type(r: Repetition, name: &str, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.write_fmt(format_args!("{} int32 {} (INT_32);", r, name))
    }
}

#[derive(Debug)]
pub struct U64Schema;
impl DisplaySchema for U64Schema {
    fn fmt(&self, r: Repetition, name: &str, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.write_fmt(format_args!("{} int64 {} (UINT_64);", r, name))
    }
    fn fmt_type(r: Repetition, name: &str, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.write_fmt(format_args!("{} int64 {} (UINT_64);", r, name))
    }
}

#[derive(Debug)]
pub struct I64Schema;
impl DisplaySchema for I64Schema {
    fn fmt(&self, r: Repetition, name: &str, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.write_fmt(format_args!("{} int64 {} (INT_64);", r, name))
    }
    fn fmt_type(r: Repetition, name: &str, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.write_fmt(format_args!("{} int64 {} (INT_64);", r, name))
    }
}

#[derive(Debug)]
pub struct F32Schema;
impl DisplaySchema for F32Schema {
    fn fmt(&self, r: Repetition, name: &str, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.write_fmt(format_args!("{} float {};", r, name))
    }
    fn fmt_type(r: Repetition, name: &str, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.write_fmt(format_args!("{} float {};", r, name))
    }
}

#[derive(Debug)]
pub struct F64Schema;
impl DisplaySchema for F64Schema {
    fn fmt(&self, r: Repetition, name: &str, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.write_fmt(format_args!("{} double {};", r, name))
    }
    fn fmt_type(r: Repetition, name: &str, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.write_fmt(format_args!("{} double {};", r, name))
    }
}

#[derive(Debug)]
pub struct VecSchema(pub(super) Option<u32>);
impl DisplaySchema for VecSchema {
    fn fmt(&self, r: Repetition, name: &str, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        if let Some(len) = self.0 {
            f.write_fmt(format_args!(
                "{} fixed_len_byte_array({}) {};",
                r, len, name
            ))
        } else {
            f.write_fmt(format_args!("{} byte_array {};", r, name))
        }
    }
    fn fmt_type(r: Repetition, name: &str, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.write_fmt(format_args!("{} byte_array {};", r, name))
    }
}

pub struct ArraySchema<T>(pub(super) PhantomData<fn(T)>);
impl<T> Debug for ArraySchema<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.debug_tuple("ArraySchema").finish()
    }
}
impl<T> DisplaySchema for ArraySchema<T> {
    fn fmt(&self, r: Repetition, name: &str, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.write_fmt(format_args!(
            "{} fixed_len_byte_array({}) {};",
            r,
            mem::size_of::<T>(),
            name
        ))
    }
    fn fmt_type(r: Repetition, name: &str, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.write_fmt(format_args!(
            "{} fixed_len_byte_array({}) {};",
            r,
            mem::size_of::<T>(),
            name
        ))
    }
}

#[derive(Debug)]
pub struct BsonSchema(pub(super) Option<u32>);
impl DisplaySchema for BsonSchema {
    fn fmt(&self, r: Repetition, name: &str, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        if let Some(len) = self.0 {
            f.write_fmt(format_args!(
                "{} fixed_len_byte_array({}) {} (BSON);",
                r, len, name
            ))
        } else {
            f.write_fmt(format_args!("{} byte_array {} (BSON);", r, name))
        }
    }
    fn fmt_type(r: Repetition, name: &str, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.write_fmt(format_args!("{} byte_array {} (BSON);", r, name))
    }
}

#[derive(Debug)]
pub struct StringSchema;
impl DisplaySchema for StringSchema {
    fn fmt(&self, r: Repetition, name: &str, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.write_fmt(format_args!("{} byte_array {} (UTF8);", r, name))
    }
    fn fmt_type(r: Repetition, name: &str, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.write_fmt(format_args!("{} byte_array {} (UTF8);", r, name))
    }
}

#[derive(Debug)]
pub struct JsonSchema;
impl DisplaySchema for JsonSchema {
    fn fmt(&self, r: Repetition, name: &str, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.write_fmt(format_args!("{} byte_array {} (JSON);", r, name))
    }
    fn fmt_type(r: Repetition, name: &str, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.write_fmt(format_args!("{} byte_array {} (JSON);", r, name))
    }
}

#[derive(Debug)]
pub struct EnumSchema;
impl DisplaySchema for EnumSchema {
    fn fmt(&self, r: Repetition, name: &str, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.write_fmt(format_args!("{} byte_array {} (ENUM);", r, name))
    }
    fn fmt_type(r: Repetition, name: &str, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.write_fmt(format_args!("{} byte_array {} (ENUM);", r, name))
    }
}

#[derive(Debug)]
pub struct DateSchema;
impl DisplaySchema for DateSchema {
    fn fmt(&self, r: Repetition, name: &str, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.write_fmt(format_args!("{} int32 {} (DATE);", r, name))
    }
    fn fmt_type(r: Repetition, name: &str, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.write_fmt(format_args!("{} int32 {} (DATE);", r, name))
    }
}

#[derive(Debug)]
pub enum TimeSchema {
    Millis,
    Micros,
}
impl DisplaySchema for TimeSchema {
    fn fmt(&self, r: Repetition, name: &str, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match self {
            TimeSchema::Millis => f.write_fmt(format_args!("{} int32 {} (TIME_MILLIS);", r, name)),
            TimeSchema::Micros => f.write_fmt(format_args!("{} int64 {} (TIME_MICROS);", r, name)),
        }
    }
    fn fmt_type(r: Repetition, name: &str, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.write_fmt(format_args!("{} int64 {} (TIME_MICROS);", r, name))
    }
}

#[derive(Debug)]
pub enum TimestampSchema {
    Int96,
    Millis,
    Micros,
}
impl DisplaySchema for TimestampSchema {
    fn fmt(&self, r: Repetition, name: &str, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match self {
            TimestampSchema::Int96 => f.write_fmt(format_args!("{} int96 {};", r, name)),
            TimestampSchema::Millis => {
                f.write_fmt(format_args!("{} int64 {} (TIMESTAMP_MILLIS);", r, name))
            }
            TimestampSchema::Micros => {
                f.write_fmt(format_args!("{} int64 {} (TIMESTAMP_MICROS);", r, name))
            }
        }
    }
    fn fmt_type(r: Repetition, name: &str, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.write_fmt(format_args!("{} int96 {};", r, name))
    }
}

#[derive(Debug)]
pub enum DecimalSchema {
    Int32 { precision: u8, scale: u8 },
    Int64 { precision: u8, scale: u8 },
    Array { precision: u32, scale: u32 },
}
impl DisplaySchema for DecimalSchema {
    fn fmt(&self, r: Repetition, name: &str, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        // For decimal type we should print precision and scale if they are > 0, e.g. DECIMAL(9, 2) - DECIMAL(9) - DECIMAL
        // let precision_scale = match (precision, scale) {
        //     (p, s) if p > 0 && s > 0 => format!(" ({}, {})", p, s),
        //     (p, 0) if p > 0 => format!(" ({})", p),
        //     _ => format!(""),
        // };
        match self {
            DecimalSchema::Int32 { precision, scale } => {
                f.write_fmt(format_args!("{} int32 {} (DECIMAL);", r, name))
            }
            DecimalSchema::Int64 { precision, scale } => {
                f.write_fmt(format_args!("{} int64 {} (DECIMAL);", r, name))
            }
            DecimalSchema::Array { precision, scale } => {
                f.write_fmt(format_args!("{} byte_array {} (DECIMAL);", r, name))
            }
        }
    }
    fn fmt_type(r: Repetition, name: &str, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.write_fmt(format_args!("{} int64 {} (DECIMAL);", r, name))
    }
}

#[derive(Debug)]
pub struct MapSchema<K, V>(
    pub(super) K,
    pub(super) V,
    pub(super) Option<String>,
    pub(super) Option<String>,
    pub(super) Option<String>,
);
impl<K, V> DisplaySchema for MapSchema<K, V>
where
    K: DisplaySchema,
    V: DisplaySchema,
{
    fn fmt(&self, r: Repetition, name: &str, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.write_fmt(format_args!(
            "{} group {} (MAP) {{\n    repeated group key_value {{\n        {}\n        {}\n    }}\n}}",
            r, name, "", ""
        ))
    }
    fn fmt_type(r: Repetition, name: &str, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.write_fmt(format_args!(
            "{} group {} (MAP) {{\n    repeated group key_value {{\n        {}\n        {}\n    }}\n}}",
            r, name, "", ""
        ))
    }
}

#[derive(Debug)]
pub struct OptionSchema<T>(pub(super) T);
impl<T> DisplaySchema for OptionSchema<T>
where
    T: DisplaySchema,
{
    fn fmt(&self, r: Repetition, name: &str, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        assert_eq!(r, Repetition::REQUIRED);
        self.0.fmt(Repetition::OPTIONAL, name, f)
    }
    fn fmt_type(r: Repetition, name: &str, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        assert_eq!(r, Repetition::REQUIRED);
        T::fmt_type(Repetition::OPTIONAL, name, f)
    }
}

#[derive(Debug)]
pub struct ListSchema<T>(pub(super) T, pub(super) ListSchemaType);
#[derive(Debug)]
pub(super) enum ListSchemaType {
    List(Option<String>, Option<String>),
    ListCompat(String),
    Repeated,
}
impl<T> DisplaySchema for ListSchema<T>
where
    T: DisplaySchema,
{
    fn fmt(&self, r: Repetition, name: &str, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.write_fmt(format_args!(
            "{} group {} (LIST) {{\n    repeated group list {{\n        {}\n    }}\n}}",
            r, name, ""
        ))
    }
    fn fmt_type(r: Repetition, name: &str, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.write_fmt(format_args!(
            "{} group {} (LIST) {{\n    repeated group list {{\n        {}\n    }}\n}}",
            r, name, ""
        ))
    }
}

#[derive(Debug)]
pub struct GroupSchema(
    pub(super) Vec<ValueSchema>,
    pub(super) HashMap<String, usize>,
);
impl DisplaySchema for GroupSchema {
    fn fmt(&self, r: Repetition, name: &str, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.write_fmt(format_args!("{} group {} {{\n    ...\n}}", r, name))
    }
    fn fmt_type(r: Repetition, name: &str, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.write_fmt(format_args!("{} group {} {{\n    ...\n}}", r, name))
    }
}

#[derive(Debug)]
pub enum ValueSchema {
    Bool(BoolSchema),
    U8(U8Schema),
    I8(I8Schema),
    U16(U16Schema),
    I16(I16Schema),
    U32(U32Schema),
    I32(I32Schema),
    U64(U64Schema),
    I64(I64Schema),
    F32(F32Schema),
    F64(F64Schema),
    Date(DateSchema),
    Time(TimeSchema),
    Timestamp(TimestampSchema),
    Decimal(DecimalSchema),
    Array(VecSchema),
    Bson(BsonSchema),
    String(StringSchema),
    Json(JsonSchema),
    Enum(EnumSchema),
    List(Box<ListSchema<ValueSchema>>),
    Map(Box<MapSchema<ValueSchema, ValueSchema>>),
    Group(GroupSchema),
    Option(Box<OptionSchema<ValueSchema>>),
}
impl DisplaySchema for ValueSchema {
    fn fmt(&self, r: Repetition, name: &str, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match self {
            ValueSchema::Bool(schema) => DisplaySchema::fmt(schema, r, name, f),
            ValueSchema::U8(schema) => DisplaySchema::fmt(schema, r, name, f),
            ValueSchema::I8(schema) => DisplaySchema::fmt(schema, r, name, f),
            ValueSchema::U16(schema) => DisplaySchema::fmt(schema, r, name, f),
            ValueSchema::I16(schema) => DisplaySchema::fmt(schema, r, name, f),
            ValueSchema::U32(schema) => DisplaySchema::fmt(schema, r, name, f),
            ValueSchema::I32(schema) => DisplaySchema::fmt(schema, r, name, f),
            ValueSchema::U64(schema) => DisplaySchema::fmt(schema, r, name, f),
            ValueSchema::I64(schema) => DisplaySchema::fmt(schema, r, name, f),
            ValueSchema::F32(schema) => DisplaySchema::fmt(schema, r, name, f),
            ValueSchema::F64(schema) => DisplaySchema::fmt(schema, r, name, f),
            ValueSchema::Date(schema) => DisplaySchema::fmt(schema, r, name, f),
            ValueSchema::Time(schema) => DisplaySchema::fmt(schema, r, name, f),
            ValueSchema::Timestamp(schema) => DisplaySchema::fmt(schema, r, name, f),
            ValueSchema::Decimal(schema) => DisplaySchema::fmt(schema, r, name, f),
            ValueSchema::Array(schema) => DisplaySchema::fmt(schema, r, name, f),
            ValueSchema::Bson(schema) => DisplaySchema::fmt(schema, r, name, f),
            ValueSchema::String(schema) => DisplaySchema::fmt(schema, r, name, f),
            ValueSchema::Json(schema) => DisplaySchema::fmt(schema, r, name, f),
            ValueSchema::Enum(schema) => DisplaySchema::fmt(schema, r, name, f),
            ValueSchema::List(schema) => DisplaySchema::fmt(&**schema, r, name, f),
            ValueSchema::Map(schema) => DisplaySchema::fmt(&**schema, r, name, f),
            ValueSchema::Group(schema) => DisplaySchema::fmt(schema, r, name, f),
            ValueSchema::Option(schema) => DisplaySchema::fmt(&**schema, r, name, f),
        }
    }
    fn fmt_type(r: Repetition, name: &str, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.write_fmt(format_args!("{} ... {};", r, name))
    }
}
impl ValueSchema {
    pub fn is_bool(&self) -> bool {
        if let ValueSchema::Bool(_) = self {
            true
        } else {
            false
        }
    }

    pub fn as_bool(&self) -> Result<&BoolSchema, ParquetError> {
        if let ValueSchema::Bool(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as bool",
                self
            )))
        }
    }

    pub fn into_bool(self) -> Result<BoolSchema, ParquetError> {
        if let ValueSchema::Bool(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as bool",
                self
            )))
        }
    }

    pub fn is_u8(&self) -> bool {
        if let ValueSchema::U8(_) = self {
            true
        } else {
            false
        }
    }

    pub fn as_u8(&self) -> Result<&U8Schema, ParquetError> {
        if let ValueSchema::U8(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as u8",
                self
            )))
        }
    }

    pub fn into_u8(self) -> Result<U8Schema, ParquetError> {
        if let ValueSchema::U8(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as u8",
                self
            )))
        }
    }

    pub fn is_i8(&self) -> bool {
        if let ValueSchema::I8(_) = self {
            true
        } else {
            false
        }
    }

    pub fn as_i8(&self) -> Result<&I8Schema, ParquetError> {
        if let ValueSchema::I8(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as i8",
                self
            )))
        }
    }

    pub fn into_i8(self) -> Result<I8Schema, ParquetError> {
        if let ValueSchema::I8(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as i8",
                self
            )))
        }
    }

    pub fn is_u16(&self) -> bool {
        if let ValueSchema::U16(_) = self {
            true
        } else {
            false
        }
    }

    pub fn as_u16(&self) -> Result<&U16Schema, ParquetError> {
        if let ValueSchema::U16(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as u16",
                self
            )))
        }
    }

    pub fn into_u16(self) -> Result<U16Schema, ParquetError> {
        if let ValueSchema::U16(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as u16",
                self
            )))
        }
    }

    pub fn is_i16(&self) -> bool {
        if let ValueSchema::I16(_) = self {
            true
        } else {
            false
        }
    }

    pub fn as_i16(&self) -> Result<&I16Schema, ParquetError> {
        if let ValueSchema::I16(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as i16",
                self
            )))
        }
    }

    pub fn into_i16(self) -> Result<I16Schema, ParquetError> {
        if let ValueSchema::I16(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as i16",
                self
            )))
        }
    }

    pub fn is_u32(&self) -> bool {
        if let ValueSchema::U32(_) = self {
            true
        } else {
            false
        }
    }

    pub fn as_u32(&self) -> Result<&U32Schema, ParquetError> {
        if let ValueSchema::U32(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as u32",
                self
            )))
        }
    }

    pub fn into_u32(self) -> Result<U32Schema, ParquetError> {
        if let ValueSchema::U32(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as u32",
                self
            )))
        }
    }

    pub fn is_i32(&self) -> bool {
        if let ValueSchema::I32(_) = self {
            true
        } else {
            false
        }
    }

    pub fn as_i32(&self) -> Result<&I32Schema, ParquetError> {
        if let ValueSchema::I32(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as i32",
                self
            )))
        }
    }

    pub fn into_i32(self) -> Result<I32Schema, ParquetError> {
        if let ValueSchema::I32(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as i32",
                self
            )))
        }
    }

    pub fn is_u64(&self) -> bool {
        if let ValueSchema::U64(_) = self {
            true
        } else {
            false
        }
    }

    pub fn as_u64(&self) -> Result<&U64Schema, ParquetError> {
        if let ValueSchema::U64(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as u64",
                self
            )))
        }
    }

    pub fn into_u64(self) -> Result<U64Schema, ParquetError> {
        if let ValueSchema::U64(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as u64",
                self
            )))
        }
    }

    pub fn is_i64(&self) -> bool {
        if let ValueSchema::I64(_) = self {
            true
        } else {
            false
        }
    }

    pub fn as_i64(&self) -> Result<&I64Schema, ParquetError> {
        if let ValueSchema::I64(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as i64",
                self
            )))
        }
    }

    pub fn into_i64(self) -> Result<I64Schema, ParquetError> {
        if let ValueSchema::I64(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as i64",
                self
            )))
        }
    }

    pub fn is_f32(&self) -> bool {
        if let ValueSchema::F32(_) = self {
            true
        } else {
            false
        }
    }

    pub fn as_f32(&self) -> Result<&F32Schema, ParquetError> {
        if let ValueSchema::F32(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as f32",
                self
            )))
        }
    }

    pub fn into_f32(self) -> Result<F32Schema, ParquetError> {
        if let ValueSchema::F32(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as f32",
                self
            )))
        }
    }

    pub fn is_f64(&self) -> bool {
        if let ValueSchema::F64(_) = self {
            true
        } else {
            false
        }
    }

    pub fn as_f64(&self) -> Result<&F64Schema, ParquetError> {
        if let ValueSchema::F64(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as f64",
                self
            )))
        }
    }

    pub fn into_f64(self) -> Result<F64Schema, ParquetError> {
        if let ValueSchema::F64(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as f64",
                self
            )))
        }
    }

    pub fn is_date(&self) -> bool {
        if let ValueSchema::Date(_) = self {
            true
        } else {
            false
        }
    }

    pub fn as_date(&self) -> Result<&DateSchema, ParquetError> {
        if let ValueSchema::Date(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as date",
                self
            )))
        }
    }

    pub fn into_date(self) -> Result<DateSchema, ParquetError> {
        if let ValueSchema::Date(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as date",
                self
            )))
        }
    }

    pub fn is_time(&self) -> bool {
        if let ValueSchema::Time(_) = self {
            true
        } else {
            false
        }
    }

    pub fn as_time(&self) -> Result<&TimeSchema, ParquetError> {
        if let ValueSchema::Time(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as time",
                self
            )))
        }
    }

    pub fn into_time(self) -> Result<TimeSchema, ParquetError> {
        if let ValueSchema::Time(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as time",
                self
            )))
        }
    }

    pub fn is_timestamp(&self) -> bool {
        if let ValueSchema::Timestamp(_) = self {
            true
        } else {
            false
        }
    }

    pub fn as_timestamp(&self) -> Result<&TimestampSchema, ParquetError> {
        if let ValueSchema::Timestamp(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as timestamp",
                self
            )))
        }
    }

    pub fn into_timestamp(self) -> Result<TimestampSchema, ParquetError> {
        if let ValueSchema::Timestamp(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as timestamp",
                self
            )))
        }
    }

    pub fn is_decimal(&self) -> bool {
        if let ValueSchema::Decimal(_) = self {
            true
        } else {
            false
        }
    }

    pub fn as_decimal(&self) -> Result<&DecimalSchema, ParquetError> {
        if let ValueSchema::Decimal(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as decimal",
                self
            )))
        }
    }

    pub fn into_decimal(self) -> Result<DecimalSchema, ParquetError> {
        if let ValueSchema::Decimal(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as decimal",
                self
            )))
        }
    }

    pub fn is_array(&self) -> bool {
        if let ValueSchema::Array(_) = self {
            true
        } else {
            false
        }
    }

    pub fn as_array(&self) -> Result<&VecSchema, ParquetError> {
        if let ValueSchema::Array(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as array",
                self
            )))
        }
    }

    pub fn into_array(self) -> Result<VecSchema, ParquetError> {
        if let ValueSchema::Array(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as array",
                self
            )))
        }
    }

    pub fn is_bson(&self) -> bool {
        if let ValueSchema::Bson(_) = self {
            true
        } else {
            false
        }
    }

    pub fn as_bson(&self) -> Result<&BsonSchema, ParquetError> {
        if let ValueSchema::Bson(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as bson",
                self
            )))
        }
    }

    pub fn into_bson(self) -> Result<BsonSchema, ParquetError> {
        if let ValueSchema::Bson(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as bson",
                self
            )))
        }
    }

    pub fn is_string(&self) -> bool {
        if let ValueSchema::String(_) = self {
            true
        } else {
            false
        }
    }

    pub fn as_string(&self) -> Result<&StringSchema, ParquetError> {
        if let ValueSchema::String(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as string",
                self
            )))
        }
    }

    pub fn into_string(self) -> Result<StringSchema, ParquetError> {
        if let ValueSchema::String(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as string",
                self
            )))
        }
    }

    pub fn is_json(&self) -> bool {
        if let ValueSchema::Json(_) = self {
            true
        } else {
            false
        }
    }

    pub fn as_json(&self) -> Result<&JsonSchema, ParquetError> {
        if let ValueSchema::Json(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as json",
                self
            )))
        }
    }

    pub fn into_json(self) -> Result<JsonSchema, ParquetError> {
        if let ValueSchema::Json(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as json",
                self
            )))
        }
    }

    pub fn is_enum(&self) -> bool {
        if let ValueSchema::Enum(_) = self {
            true
        } else {
            false
        }
    }

    pub fn as_enum(&self) -> Result<&EnumSchema, ParquetError> {
        if let ValueSchema::Enum(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as enum",
                self
            )))
        }
    }

    pub fn into_enum(self) -> Result<EnumSchema, ParquetError> {
        if let ValueSchema::Enum(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as enum",
                self
            )))
        }
    }

    pub fn is_list(&self) -> bool {
        if let ValueSchema::List(_) = self {
            true
        } else {
            false
        }
    }

    pub fn as_list(&self) -> Result<&ListSchema<ValueSchema>, ParquetError> {
        if let ValueSchema::List(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as list",
                self
            )))
        }
    }

    pub fn into_list(self) -> Result<ListSchema<ValueSchema>, ParquetError> {
        if let ValueSchema::List(ret) = self {
            Ok(*ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as list",
                self
            )))
        }
    }

    pub fn is_map(&self) -> bool {
        if let ValueSchema::Map(_) = self {
            true
        } else {
            false
        }
    }

    pub fn as_map(&self) -> Result<&MapSchema<ValueSchema, ValueSchema>, ParquetError> {
        if let ValueSchema::Map(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as map",
                self
            )))
        }
    }

    pub fn into_map(self) -> Result<MapSchema<ValueSchema, ValueSchema>, ParquetError> {
        if let ValueSchema::Map(ret) = self {
            Ok(*ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as map",
                self
            )))
        }
    }

    pub fn is_group(&self) -> bool {
        if let ValueSchema::Group(_) = self {
            true
        } else {
            false
        }
    }

    pub fn as_group(&self) -> Result<&GroupSchema, ParquetError> {
        if let ValueSchema::Group(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as group",
                self
            )))
        }
    }

    pub fn into_group(self) -> Result<GroupSchema, ParquetError> {
        if let ValueSchema::Group(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as group",
                self
            )))
        }
    }

    pub fn is_option(&self) -> bool {
        if let ValueSchema::Option(_) = self {
            true
        } else {
            false
        }
    }

    pub fn as_option(&self) -> Result<&OptionSchema<ValueSchema>, ParquetError> {
        if let ValueSchema::Option(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as option",
                self
            )))
        }
    }

    pub fn into_option(self) -> Result<OptionSchema<ValueSchema>, ParquetError> {
        if let ValueSchema::Option(ret) = self {
            Ok(*ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as option",
                self
            )))
        }
    }
}

impl Downcast<ValueSchema> for ValueSchema {
    fn downcast(self) -> Result<ValueSchema, ParquetError> {
        Ok(self)
    }
}
impl Downcast<BoolSchema> for ValueSchema {
    fn downcast(self) -> Result<BoolSchema, ParquetError> {
        self.into_bool()
    }
}
impl Downcast<U8Schema> for ValueSchema {
    fn downcast(self) -> Result<U8Schema, ParquetError> {
        self.into_u8()
    }
}
impl Downcast<I8Schema> for ValueSchema {
    fn downcast(self) -> Result<I8Schema, ParquetError> {
        self.into_i8()
    }
}
impl Downcast<U16Schema> for ValueSchema {
    fn downcast(self) -> Result<U16Schema, ParquetError> {
        self.into_u16()
    }
}
impl Downcast<I16Schema> for ValueSchema {
    fn downcast(self) -> Result<I16Schema, ParquetError> {
        self.into_i16()
    }
}
impl Downcast<U32Schema> for ValueSchema {
    fn downcast(self) -> Result<U32Schema, ParquetError> {
        self.into_u32()
    }
}
impl Downcast<I32Schema> for ValueSchema {
    fn downcast(self) -> Result<I32Schema, ParquetError> {
        self.into_i32()
    }
}
impl Downcast<U64Schema> for ValueSchema {
    fn downcast(self) -> Result<U64Schema, ParquetError> {
        self.into_u64()
    }
}
impl Downcast<I64Schema> for ValueSchema {
    fn downcast(self) -> Result<I64Schema, ParquetError> {
        self.into_i64()
    }
}
impl Downcast<F32Schema> for ValueSchema {
    fn downcast(self) -> Result<F32Schema, ParquetError> {
        self.into_f32()
    }
}
impl Downcast<F64Schema> for ValueSchema {
    fn downcast(self) -> Result<F64Schema, ParquetError> {
        self.into_f64()
    }
}
impl Downcast<DateSchema> for ValueSchema {
    fn downcast(self) -> Result<DateSchema, ParquetError> {
        self.into_date()
    }
}
impl Downcast<TimeSchema> for ValueSchema {
    fn downcast(self) -> Result<TimeSchema, ParquetError> {
        self.into_time()
    }
}
impl Downcast<TimestampSchema> for ValueSchema {
    fn downcast(self) -> Result<TimestampSchema, ParquetError> {
        self.into_timestamp()
    }
}
impl Downcast<DecimalSchema> for ValueSchema {
    fn downcast(self) -> Result<DecimalSchema, ParquetError> {
        self.into_decimal()
    }
}
impl Downcast<VecSchema> for ValueSchema {
    fn downcast(self) -> Result<VecSchema, ParquetError> {
        self.into_array()
    }
}
impl Downcast<BsonSchema> for ValueSchema {
    fn downcast(self) -> Result<BsonSchema, ParquetError> {
        self.into_bson()
    }
}
impl Downcast<StringSchema> for ValueSchema {
    fn downcast(self) -> Result<StringSchema, ParquetError> {
        self.into_string()
    }
}
impl Downcast<JsonSchema> for ValueSchema {
    fn downcast(self) -> Result<JsonSchema, ParquetError> {
        self.into_json()
    }
}
impl Downcast<EnumSchema> for ValueSchema {
    fn downcast(self) -> Result<EnumSchema, ParquetError> {
        self.into_enum()
    }
}
impl<T> Downcast<ListSchema<T>> for ValueSchema
where
    ValueSchema: Downcast<T>,
{
    default fn downcast(self) -> Result<ListSchema<T>, ParquetError> {
        let ret = self.into_list()?;
        Ok(ListSchema(ret.0.downcast()?, ret.1))
    }
}
impl Downcast<ListSchema<ValueSchema>> for ValueSchema {
    fn downcast(self) -> Result<ListSchema<ValueSchema>, ParquetError> {
        self.into_list()
    }
}
impl<K, V> Downcast<MapSchema<K, V>> for ValueSchema
where
    ValueSchema: Downcast<K> + Downcast<V>,
{
    default fn downcast(self) -> Result<MapSchema<K, V>, ParquetError> {
        let ret = self.into_map()?;
        Ok(MapSchema(
            ret.0.downcast()?,
            ret.1.downcast()?,
            ret.2,
            ret.3,
            ret.4,
        ))
    }
}
impl Downcast<MapSchema<ValueSchema, ValueSchema>> for ValueSchema {
    fn downcast(self) -> Result<MapSchema<ValueSchema, ValueSchema>, ParquetError> {
        self.into_map()
    }
}
impl Downcast<GroupSchema> for ValueSchema {
    fn downcast(self) -> Result<GroupSchema, ParquetError> {
        self.into_group()
    }
}
impl<T> Downcast<OptionSchema<T>> for ValueSchema
where
    ValueSchema: Downcast<T>,
{
    default fn downcast(self) -> Result<OptionSchema<T>, ParquetError> {
        let ret = self.into_option()?;
        ret.0.downcast().map(OptionSchema)
    }
}
impl Downcast<OptionSchema<ValueSchema>> for ValueSchema {
    fn downcast(self) -> Result<OptionSchema<ValueSchema>, ParquetError> {
        self.into_option()
    }
}

pub struct RootSchema<T, S>(pub String, pub S, pub PhantomData<fn(T)>);
impl<T, S> Debug for RootSchema<T, S>
where
    S: Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.debug_tuple("RootSchema")
            .field(&self.0)
            .field(&self.1)
            .finish()
    }
}
impl<T, S> DisplaySchema for RootSchema<T, S>
where
    S: DisplaySchema,
{
    fn fmt(&self, r: Repetition, name: &str, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        assert_eq!(r, Repetition::REQUIRED);
        f.write_fmt(format_args!("message {}", ""))
    }
    fn fmt_type(r: Repetition, name: &str, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        assert_eq!(r, Repetition::REQUIRED);
        f.write_fmt(format_args!("message {}", ""))
    }
}
impl<T, S> FromStr for RootSchema<T, S>
where
    Root<T>: Deserialize<Schema = Self>,
    S: DisplaySchema,
{
    type Err = ParquetError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse_message_type(s)
            .and_then(|x| {
                <Root<T> as Deserialize>::parse(&x).map_err(|err| {
                    // let schema: Type = <Root<T> as Deserialize>::render("", &<Root<T> as
                    // Deserialize>::placeholder());
                    let mut b = Vec::new();
                    crate::schema::printer::print_schema(&mut b, &x);
                    // let mut a = Vec::new();
                    // print_schema(&mut a, &schema);

                    ParquetError::General(format!(
            "Types don't match schema.\nSchema is:\n{}\nBut types require:\n{}\nError: \
             {}",
            String::from_utf8(b).unwrap(),
            // String::from_utf8(a).unwrap(),
            DisplayDisplaySchema::<<Root<T> as Deserialize>::Schema>::new(),
            err
          ))

                    // // let x: Type = <Root<($($t,)*)> as Deserialize>::render("",
                    // &<Root<($($t,)*)> as Deserialize>::placeholder()); let a = Vec::
                    // new(); // print_schema(&mut a, &x);
                    // ParquetError::General(format!(
                    //   "Types don't match schema.\nSchema is:\n{}\nBut types require:\n{}\nError:
                    // {}",   s,
                    //   String::from_utf8(a).unwrap(),
                    //   err
                    // ))
                })
            })
            .map(|x| x.1)
    }
}

pub struct TupleSchema<T>(pub(super) T);

#[cfg(test)]
mod tests {
    use super::*;

    // #[test]
    // fn schema_printing() {
    //     let file_name = "alltypes_dictionary.parquet";
    //     let file = get_test_file(file_name);
    //     let file_reader: SerializedFileReader<_> = SerializedFileReader::new(file)?;
    // }
}
