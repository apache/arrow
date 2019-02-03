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

//! Structs to represent types of fields in a Parquet schema.
//!
//! These structs should be sufficient to represent any valid Parquet schema.
//!
//! They can be created from [`Type`](crate::schema::types::Type) with [`Record::parse`],
//! or from a Parquet schema string with [`str::parse`].
//!
//! They can be printed as a Parquet schema string with [`Schema::fmt`], or more
//! conveniently [`RootSchema`](super::RootSchema) also implements [`Display`].
//!
//! ```
//! # use parquet::errors::Result;
//! use parquet::record::{RootSchema, types::Value};
//!
//! #
//! # fn main() -> Result<()> {
//! let schema_str =
//! "message spark_schema {
//!     REQUIRED double c;
//!     REQUIRED int32 b (INT_32);
//! }";
//!
//! let schema: RootSchema<Value> = schema_str.parse()?;
//!
//! let schema_str2 = schema.to_string();
//!
//! assert_eq!(schema_str, schema_str2);
//! # Ok(())
//! # }
//! ```

use std::{
    collections::HashMap,
    fmt::{self, Debug, Display},
    marker::PhantomData,
    mem,
    str::FromStr,
};

use super::{
    display::{DisplayFmt, DisplaySchemaGroup},
    types::{Downcast, Root},
    Record, Schema,
};
use crate::basic::{LogicalType, Repetition};
use crate::errors::{ParquetError, Result};
use crate::schema::parser::parse_message_type;

#[derive(Default, Debug)]
pub struct BoolSchema;
impl Schema for BoolSchema {
    fn fmt(
        _self_: Option<&Self>,
        r: Option<Repetition>,
        name: Option<&str>,
        f: &mut fmt::Formatter,
    ) -> fmt::Result {
        f.write_fmt(format_args!(
            "{} boolean {};",
            r.unwrap(),
            name.unwrap_or("<name>")
        ))
    }
}

#[derive(Default, Debug)]
pub struct U8Schema;
impl Schema for U8Schema {
    fn fmt(
        _self_: Option<&Self>,
        r: Option<Repetition>,
        name: Option<&str>,
        f: &mut fmt::Formatter,
    ) -> fmt::Result {
        f.write_fmt(format_args!(
            "{} int32 {} (UINT_8);",
            r.unwrap(),
            name.unwrap_or("<name>")
        ))
    }
}

#[derive(Default, Debug)]
pub struct I8Schema;
impl Schema for I8Schema {
    fn fmt(
        _self_: Option<&Self>,
        r: Option<Repetition>,
        name: Option<&str>,
        f: &mut fmt::Formatter,
    ) -> fmt::Result {
        f.write_fmt(format_args!(
            "{} int32 {} (INT_8);",
            r.unwrap(),
            name.unwrap_or("<name>")
        ))
    }
}

#[derive(Default, Debug)]
pub struct U16Schema;
impl Schema for U16Schema {
    fn fmt(
        _self_: Option<&Self>,
        r: Option<Repetition>,
        name: Option<&str>,
        f: &mut fmt::Formatter,
    ) -> fmt::Result {
        f.write_fmt(format_args!(
            "{} int32 {} (UINT_16);",
            r.unwrap(),
            name.unwrap_or("<name>")
        ))
    }
}

#[derive(Default, Debug)]
pub struct I16Schema;
impl Schema for I16Schema {
    fn fmt(
        _self_: Option<&Self>,
        r: Option<Repetition>,
        name: Option<&str>,
        f: &mut fmt::Formatter,
    ) -> fmt::Result {
        f.write_fmt(format_args!(
            "{} int32 {} (INT_16);",
            r.unwrap(),
            name.unwrap_or("<name>")
        ))
    }
}

#[derive(Default, Debug)]
pub struct U32Schema;
impl Schema for U32Schema {
    fn fmt(
        _self_: Option<&Self>,
        r: Option<Repetition>,
        name: Option<&str>,
        f: &mut fmt::Formatter,
    ) -> fmt::Result {
        f.write_fmt(format_args!(
            "{} int32 {} (UINT_32);",
            r.unwrap(),
            name.unwrap_or("<name>")
        ))
    }
}

#[derive(Default, Debug)]
pub struct I32Schema;
impl Schema for I32Schema {
    fn fmt(
        _self_: Option<&Self>,
        r: Option<Repetition>,
        name: Option<&str>,
        f: &mut fmt::Formatter,
    ) -> fmt::Result {
        f.write_fmt(format_args!(
            "{} int32 {} (INT_32);",
            r.unwrap(),
            name.unwrap_or("<name>")
        ))
    }
}

#[derive(Default, Debug)]
pub struct U64Schema;
impl Schema for U64Schema {
    fn fmt(
        _self_: Option<&Self>,
        r: Option<Repetition>,
        name: Option<&str>,
        f: &mut fmt::Formatter,
    ) -> fmt::Result {
        f.write_fmt(format_args!(
            "{} int64 {} (UINT_64);",
            r.unwrap(),
            name.unwrap_or("<name>")
        ))
    }
}

#[derive(Default, Debug)]
pub struct I64Schema;
impl Schema for I64Schema {
    fn fmt(
        _self_: Option<&Self>,
        r: Option<Repetition>,
        name: Option<&str>,
        f: &mut fmt::Formatter,
    ) -> fmt::Result {
        f.write_fmt(format_args!(
            "{} int64 {} (INT_64);",
            r.unwrap(),
            name.unwrap_or("<name>")
        ))
    }
}

#[derive(Default, Debug)]
pub struct F32Schema;
impl Schema for F32Schema {
    fn fmt(
        _self_: Option<&Self>,
        r: Option<Repetition>,
        name: Option<&str>,
        f: &mut fmt::Formatter,
    ) -> fmt::Result {
        f.write_fmt(format_args!(
            "{} float {};",
            r.unwrap(),
            name.unwrap_or("<name>")
        ))
    }
}

#[derive(Default, Debug)]
pub struct F64Schema;
impl Schema for F64Schema {
    fn fmt(
        _self_: Option<&Self>,
        r: Option<Repetition>,
        name: Option<&str>,
        f: &mut fmt::Formatter,
    ) -> fmt::Result {
        f.write_fmt(format_args!(
            "{} double {};",
            r.unwrap(),
            name.unwrap_or("<name>")
        ))
    }
}

#[derive(Default, Debug)]
pub struct ByteArraySchema(pub(super) Option<u32>);
impl Schema for ByteArraySchema {
    fn fmt(
        self_: Option<&Self>,
        r: Option<Repetition>,
        name: Option<&str>,
        f: &mut fmt::Formatter,
    ) -> fmt::Result {
        if let Some(ByteArraySchema(Some(len))) = self_ {
            f.write_fmt(format_args!(
                "{} fixed_len_byte_array({}) {};",
                r.unwrap(),
                len,
                name.unwrap_or("<name>")
            ))
        } else {
            f.write_fmt(format_args!(
                "{} byte_array {};",
                r.unwrap(),
                name.unwrap_or("<name>")
            ))
        }
    }
}

pub struct FixedByteArraySchema<T>(pub(super) PhantomData<fn(T)>);
impl<T> Default for FixedByteArraySchema<T> {
    fn default() -> Self {
        Self(PhantomData)
    }
}
impl<T> Debug for FixedByteArraySchema<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_tuple("FixedByteArraySchema").finish()
    }
}
impl<T> Schema for FixedByteArraySchema<T> {
    fn fmt(
        _self_: Option<&Self>,
        r: Option<Repetition>,
        name: Option<&str>,
        f: &mut fmt::Formatter,
    ) -> fmt::Result {
        f.write_fmt(format_args!(
            "{} fixed_len_byte_array({}) {};",
            r.unwrap(),
            mem::size_of::<T>(),
            name.unwrap_or("<name>")
        ))
    }
}

#[derive(Default, Debug)]
pub struct BsonSchema(pub(super) ByteArraySchema);
impl Schema for BsonSchema {
    fn fmt(
        self_: Option<&Self>,
        r: Option<Repetition>,
        name: Option<&str>,
        f: &mut fmt::Formatter,
    ) -> fmt::Result {
        if let Some(BsonSchema(ByteArraySchema(Some(len)))) = self_ {
            f.write_fmt(format_args!(
                "{} fixed_len_byte_array({}) {} (BSON);",
                r.unwrap(),
                len,
                name.unwrap_or("<name>")
            ))
        } else {
            f.write_fmt(format_args!(
                "{} byte_array {} (BSON);",
                r.unwrap(),
                name.unwrap_or("<name>")
            ))
        }
    }
}

#[derive(Default, Debug)]
pub struct StringSchema(pub(super) ByteArraySchema);
impl Schema for StringSchema {
    fn fmt(
        self_: Option<&Self>,
        r: Option<Repetition>,
        name: Option<&str>,
        f: &mut fmt::Formatter,
    ) -> fmt::Result {
        if let Some(StringSchema(ByteArraySchema(Some(len)))) = self_ {
            f.write_fmt(format_args!(
                "{} fixed_len_byte_array({}) {} (UTF8);",
                r.unwrap(),
                len,
                name.unwrap_or("<name>")
            ))
        } else {
            f.write_fmt(format_args!(
                "{} byte_array {} (UTF8);",
                r.unwrap(),
                name.unwrap_or("<name>")
            ))
        }
    }
}

#[derive(Default, Debug)]
pub struct JsonSchema(pub(super) StringSchema);
impl Schema for JsonSchema {
    fn fmt(
        self_: Option<&Self>,
        r: Option<Repetition>,
        name: Option<&str>,
        f: &mut fmt::Formatter,
    ) -> fmt::Result {
        if let Some(JsonSchema(StringSchema(ByteArraySchema(Some(len))))) = self_ {
            f.write_fmt(format_args!(
                "{} fixed_len_byte_array({}) {} (JSON);",
                r.unwrap(),
                len,
                name.unwrap_or("<name>")
            ))
        } else {
            f.write_fmt(format_args!(
                "{} byte_array {} (JSON);",
                r.unwrap(),
                name.unwrap_or("<name>")
            ))
        }
    }
}

#[derive(Default, Debug)]
pub struct EnumSchema(pub(super) StringSchema);
impl Schema for EnumSchema {
    fn fmt(
        self_: Option<&Self>,
        r: Option<Repetition>,
        name: Option<&str>,
        f: &mut fmt::Formatter,
    ) -> fmt::Result {
        if let Some(EnumSchema(StringSchema(ByteArraySchema(Some(len))))) = self_ {
            f.write_fmt(format_args!(
                "{} fixed_len_byte_array({}) {} (ENUM);",
                r.unwrap(),
                len,
                name.unwrap_or("<name>")
            ))
        } else {
            f.write_fmt(format_args!(
                "{} byte_array {} (ENUM);",
                r.unwrap(),
                name.unwrap_or("<name>")
            ))
        }
    }
}

#[derive(Default, Debug)]
pub struct DateSchema;
impl Schema for DateSchema {
    fn fmt(
        _self_: Option<&Self>,
        r: Option<Repetition>,
        name: Option<&str>,
        f: &mut fmt::Formatter,
    ) -> fmt::Result {
        f.write_fmt(format_args!(
            "{} int32 {} (DATE);",
            r.unwrap(),
            name.unwrap_or("<name>")
        ))
    }
}

#[derive(Debug)]
pub enum TimeSchema {
    Millis,
    Micros,
}
impl Default for TimeSchema {
    fn default() -> Self {
        TimeSchema::Micros
    }
}
impl Schema for TimeSchema {
    fn fmt(
        self_: Option<&Self>,
        r: Option<Repetition>,
        name: Option<&str>,
        f: &mut fmt::Formatter,
    ) -> fmt::Result {
        match self_ {
            Some(TimeSchema::Millis) => f.write_fmt(format_args!(
                "{} int32 {} (TIME_MILLIS);",
                r.unwrap(),
                name.unwrap_or("<name>")
            )),
            Some(TimeSchema::Micros) => f.write_fmt(format_args!(
                "{} int64 {} (TIME_MICROS);",
                r.unwrap(),
                name.unwrap_or("<name>")
            )),
            None => f.write_fmt(format_args!(
                "{} int32|int64 {} (TIME_MILLIS|TIME_MICROS);",
                r.unwrap(),
                name.unwrap_or("<name>")
            )),
        }
    }
}

#[derive(Debug)]
pub enum TimestampSchema {
    Int96,
    Millis,
    Micros,
}
impl Default for TimestampSchema {
    fn default() -> Self {
        TimestampSchema::Int96
    }
}
impl Schema for TimestampSchema {
    fn fmt(
        self_: Option<&Self>,
        r: Option<Repetition>,
        name: Option<&str>,
        f: &mut fmt::Formatter,
    ) -> fmt::Result {
        match self_ {
            Some(TimestampSchema::Int96) => f.write_fmt(format_args!(
                "{} int96 {};",
                r.unwrap(),
                name.unwrap_or("<name>")
            )),
            Some(TimestampSchema::Millis) => f.write_fmt(format_args!(
                "{} int64 {} (TIMESTAMP_MILLIS);",
                r.unwrap(),
                name.unwrap_or("<name>")
            )),
            Some(TimestampSchema::Micros) => f.write_fmt(format_args!(
                "{} int64 {} (TIMESTAMP_MICROS);",
                r.unwrap(),
                name.unwrap_or("<name>")
            )),
            None => f.write_fmt(format_args!(
                "{} int64|int96 {} (TIMESTAMP_MILLIS|TIMESTAMP_MICROS);",
                r.unwrap(),
                name.unwrap_or("<name>")
            )),
        }
    }
}

#[derive(Debug)]
pub enum DecimalSchema {
    Int32 {
        precision: u8,
        scale: u8,
    },
    Int64 {
        precision: u8,
        scale: u8,
    },
    Array {
        byte_array_schema: ByteArraySchema,
        precision: u32,
        scale: u32,
    },
}
impl Schema for DecimalSchema {
    fn fmt(
        self_: Option<&Self>,
        r: Option<Repetition>,
        name: Option<&str>,
        f: &mut fmt::Formatter,
    ) -> fmt::Result {
        let decimal = |precision: u32, scale: u32| {
            DisplayFmt::new(move |fmt| match (precision, scale) {
                (p, 0) => fmt.write_fmt(format_args!(" ({})", p)),
                (p, s) => fmt.write_fmt(format_args!(" ({}, {})", p, s)),
            })
        };
        match self_ {
            Some(DecimalSchema::Int32 { precision, scale }) => f.write_fmt(format_args!(
                "{} int32 {} (DECIMAL{});",
                r.unwrap(),
                name.unwrap_or("<name>"),
                decimal(*precision as u32, *scale as u32)
            )),
            Some(DecimalSchema::Int64 { precision, scale }) => f.write_fmt(format_args!(
                "{} int64 {} (DECIMAL{});",
                r.unwrap(),
                name.unwrap_or("<name>"),
                decimal(*precision as u32, *scale as u32)
            )),
            Some(DecimalSchema::Array {
                byte_array_schema: ByteArraySchema(Some(len)),
                precision,
                scale,
            }) => f.write_fmt(format_args!(
                "{} fixed_len_byte_array({}) {} (DECIMAL{});",
                r.unwrap(),
                len,
                name.unwrap_or("<name>"),
                decimal(*precision as u32, *scale as u32)
            )),
            Some(DecimalSchema::Array {
                byte_array_schema: ByteArraySchema(None),
                precision,
                scale,
            }) => f.write_fmt(format_args!(
                "{} byte_array {} (DECIMAL{});",
                r.unwrap(),
                name.unwrap_or("<name>"),
                decimal(*precision as u32, *scale as u32)
            )),
            None => f.write_fmt(format_args!(
                "{} int32|int64|byte_array {} (DECIMAL(precision,scale));",
                r.unwrap(),
                name.unwrap_or("<name>")
            )),
        }
    }
}

/// Schema for the [Map logical type](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#maps).
#[derive(Default, Debug)]
pub struct MapSchema<K, V>(
    /// Schema for the keys
    pub(super) K,
    /// Schema for the values
    pub(super) V,
    /// Name of the repeated field; None = "key_value"
    pub(super) Option<String>,
    /// Name of the key field; None = "key"
    pub(super) Option<String>,
    /// Name of the value field; None = "value"
    pub(super) Option<String>,
);
impl<K, V> Schema for MapSchema<K, V>
where
    K: Schema,
    V: Schema,
{
    fn fmt(
        self_: Option<&Self>,
        r: Option<Repetition>,
        name: Option<&str>,
        f: &mut fmt::Formatter,
    ) -> fmt::Result {
        #[derive(Debug)]
        struct KeyValue<'a, K, V>(Option<(&'a K, &'a V)>, String, String);
        impl<'a, K, V> Schema for KeyValue<'a, K, V>
        where
            K: Schema,
            V: Schema,
        {
            fn fmt(
                self_: Option<&Self>,
                _r: Option<Repetition>,
                name: Option<&str>,
                f: &mut fmt::Formatter,
            ) -> fmt::Result {
                let self_ = self_.unwrap();
                let mut printer =
                    DisplaySchemaGroup::new(Some(Repetition::REPEATED), name, None, f);
                printer.field(Some(&self_.1), self_.0.map(|(k, _v)| k));
                printer.field(Some(&self_.2), self_.0.map(|(_k, v)| v));
                printer.finish()
            }
        }
        let mut printer = DisplaySchemaGroup::new(r, name, Some(LogicalType::MAP), f);
        printer.field(
            Some(
                &self_
                    .and_then(|self_| self_.2.clone())
                    .unwrap_or_else(|| String::from("key_value")),
            ),
            Some(&KeyValue(
                self_.map(|self_| (&self_.0, &self_.1)),
                self_
                    .and_then(|self_| self_.3.clone())
                    .unwrap_or_else(|| String::from("key")),
                self_
                    .and_then(|self_| self_.4.clone())
                    .unwrap_or_else(|| String::from("value")),
            )),
        );
        printer.finish()
    }
}

/// Schema for fields marked as "optional".
#[derive(Default, Debug)]
pub struct OptionSchema<T>(pub(super) T);
impl<T> Schema for OptionSchema<T>
where
    T: Schema,
{
    fn fmt(
        self_: Option<&Self>,
        r: Option<Repetition>,
        name: Option<&str>,
        f: &mut fmt::Formatter,
    ) -> fmt::Result {
        assert_eq!(r.unwrap(), Repetition::REQUIRED);
        <T as Schema>::fmt(
            self_.map(|self_| &self_.0),
            Some(Repetition::OPTIONAL),
            name,
            f,
        )
    }
}

/// Schema for the [List logical type](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#lists) and unannotated repeated elements.
#[derive(Default, Debug)]
pub struct ListSchema<T>(
    /// Schema of elements
    pub(super) T,
    /// Kind of List
    pub(super) ListSchemaType,
);
#[derive(Debug)]
pub(super) enum ListSchemaType {
    /// List logical type.
    List(
        /// Name of the repeated group; None = "list"
        Option<String>,
        /// Name of the element; None = "element"
        Option<String>,
    ),
    /// Legacy List logical type
    ListCompat(
        /// Name of the element
        String,
    ),
    /// Unannotated repeated field
    Repeated,
}
impl Default for ListSchemaType {
    fn default() -> Self {
        ListSchemaType::List(None, None)
    }
}
impl<T> Schema for ListSchema<T>
where
    T: Schema,
{
    fn fmt(
        self_: Option<&Self>,
        r: Option<Repetition>,
        name: Option<&str>,
        f: &mut fmt::Formatter,
    ) -> fmt::Result {
        match self_ {
            self_ @ Some(ListSchema(_, ListSchemaType::List(_, _))) | self_ @ None => {
                let (self_, list_name, element_name) = match self_ {
                    Some(ListSchema(
                        self_,
                        ListSchemaType::List(list_name, element_name),
                    )) => (Some(self_), list_name.clone(), element_name.clone()),
                    None => (None, None, None),
                    _ => unreachable!(),
                };
                #[derive(Debug)]
                struct List<'a, T>(Option<&'a T>, String);
                impl<'a, T> Schema for List<'a, T>
                where
                    T: Schema,
                {
                    fn fmt(
                        self_: Option<&Self>,
                        _r: Option<Repetition>,
                        name: Option<&str>,
                        f: &mut fmt::Formatter,
                    ) -> fmt::Result {
                        let self_ = self_.unwrap();
                        let mut printer = DisplaySchemaGroup::new(
                            Some(Repetition::REPEATED),
                            name,
                            None,
                            f,
                        );
                        printer.field(Some(&self_.1), self_.0);
                        printer.finish()
                    }
                }
                let mut printer =
                    DisplaySchemaGroup::new(r, name, Some(LogicalType::LIST), f);
                printer.field(
                    Some(&list_name.clone().unwrap_or_else(|| String::from("list"))),
                    Some(&List(
                        self_,
                        element_name
                            .clone()
                            .unwrap_or_else(|| String::from("element")),
                    )),
                );
                printer.finish()
            }
            Some(ListSchema(self_, ListSchemaType::ListCompat(element_name))) => {
                let mut printer =
                    DisplaySchemaGroup::new(r, name, Some(LogicalType::LIST), f);
                printer.field(Some(&element_name.clone()), Some(self_));
                printer.finish()
            }
            Some(ListSchema(self_, ListSchemaType::Repeated)) => {
                assert_eq!(r, Some(Repetition::REQUIRED));
                <T as Schema>::fmt(Some(self_), Some(Repetition::REPEATED), name, f)
            }
        }
    }
}

/// Schema for groups
#[derive(Debug)]
pub struct GroupSchema(
    /// Vec of schemas for the fields in the group
    pub(super) Vec<ValueSchema>,
    /// Map of field names to index in the Vec
    pub(super) HashMap<String, usize>,
);
impl Schema for GroupSchema {
    fn fmt(
        self_: Option<&Self>,
        r: Option<Repetition>,
        name: Option<&str>,
        f: &mut fmt::Formatter,
    ) -> fmt::Result {
        let mut printer = DisplaySchemaGroup::new(r, name, None, f);
        if let Some(self_) = self_ {
            let fields = self_.0.iter();
            let mut names = vec![None; self_.1.len()];
            for (name, &index) in self_.1.iter() {
                names[index].replace(name);
            }
            let names = names.into_iter().map(Option::unwrap);
            for (name, field) in names.zip(fields) {
                printer.field(Some(name), Some(field));
            }
        }
        printer.finish()
    }
}

/// Schema for values, i.e. any valid Parquet type
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
    ByteArray(ByteArraySchema),
    Bson(BsonSchema),
    String(StringSchema),
    Json(JsonSchema),
    Enum(EnumSchema),
    List(Box<ListSchema<ValueSchema>>),
    Map(Box<MapSchema<ValueSchema, ValueSchema>>),
    Group(GroupSchema),
    Option(Box<OptionSchema<ValueSchema>>),
}
impl Schema for ValueSchema {
    fn fmt(
        self_: Option<&Self>,
        r: Option<Repetition>,
        name: Option<&str>,
        f: &mut fmt::Formatter,
    ) -> fmt::Result {
        if let Some(self_) = self_ {
            match self_ {
                ValueSchema::Bool(schema) => Schema::fmt(Some(schema), r, name, f),
                ValueSchema::U8(schema) => Schema::fmt(Some(schema), r, name, f),
                ValueSchema::I8(schema) => Schema::fmt(Some(schema), r, name, f),
                ValueSchema::U16(schema) => Schema::fmt(Some(schema), r, name, f),
                ValueSchema::I16(schema) => Schema::fmt(Some(schema), r, name, f),
                ValueSchema::U32(schema) => Schema::fmt(Some(schema), r, name, f),
                ValueSchema::I32(schema) => Schema::fmt(Some(schema), r, name, f),
                ValueSchema::U64(schema) => Schema::fmt(Some(schema), r, name, f),
                ValueSchema::I64(schema) => Schema::fmt(Some(schema), r, name, f),
                ValueSchema::F32(schema) => Schema::fmt(Some(schema), r, name, f),
                ValueSchema::F64(schema) => Schema::fmt(Some(schema), r, name, f),
                ValueSchema::Date(schema) => Schema::fmt(Some(schema), r, name, f),
                ValueSchema::Time(schema) => Schema::fmt(Some(schema), r, name, f),
                ValueSchema::Timestamp(schema) => Schema::fmt(Some(schema), r, name, f),
                ValueSchema::Decimal(schema) => Schema::fmt(Some(schema), r, name, f),
                ValueSchema::ByteArray(schema) => Schema::fmt(Some(schema), r, name, f),
                ValueSchema::Bson(schema) => Schema::fmt(Some(schema), r, name, f),
                ValueSchema::String(schema) => Schema::fmt(Some(schema), r, name, f),
                ValueSchema::Json(schema) => Schema::fmt(Some(schema), r, name, f),
                ValueSchema::Enum(schema) => Schema::fmt(Some(schema), r, name, f),
                ValueSchema::List(schema) => Schema::fmt(Some(&**schema), r, name, f),
                ValueSchema::Map(schema) => Schema::fmt(Some(&**schema), r, name, f),
                ValueSchema::Group(schema) => Schema::fmt(Some(schema), r, name, f),
                ValueSchema::Option(schema) => Schema::fmt(Some(&**schema), r, name, f),
            }
        } else {
            f.write_str("<any repetition> <any type> <any name>")
        }
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

    pub fn as_bool(&self) -> Result<&BoolSchema> {
        if let ValueSchema::Bool(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as bool",
                self
            )))
        }
    }

    pub fn into_bool(self) -> Result<BoolSchema> {
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

    pub fn as_u8(&self) -> Result<&U8Schema> {
        if let ValueSchema::U8(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as u8",
                self
            )))
        }
    }

    pub fn into_u8(self) -> Result<U8Schema> {
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

    pub fn as_i8(&self) -> Result<&I8Schema> {
        if let ValueSchema::I8(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as i8",
                self
            )))
        }
    }

    pub fn into_i8(self) -> Result<I8Schema> {
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

    pub fn as_u16(&self) -> Result<&U16Schema> {
        if let ValueSchema::U16(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as u16",
                self
            )))
        }
    }

    pub fn into_u16(self) -> Result<U16Schema> {
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

    pub fn as_i16(&self) -> Result<&I16Schema> {
        if let ValueSchema::I16(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as i16",
                self
            )))
        }
    }

    pub fn into_i16(self) -> Result<I16Schema> {
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

    pub fn as_u32(&self) -> Result<&U32Schema> {
        if let ValueSchema::U32(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as u32",
                self
            )))
        }
    }

    pub fn into_u32(self) -> Result<U32Schema> {
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

    pub fn as_i32(&self) -> Result<&I32Schema> {
        if let ValueSchema::I32(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as i32",
                self
            )))
        }
    }

    pub fn into_i32(self) -> Result<I32Schema> {
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

    pub fn as_u64(&self) -> Result<&U64Schema> {
        if let ValueSchema::U64(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as u64",
                self
            )))
        }
    }

    pub fn into_u64(self) -> Result<U64Schema> {
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

    pub fn as_i64(&self) -> Result<&I64Schema> {
        if let ValueSchema::I64(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as i64",
                self
            )))
        }
    }

    pub fn into_i64(self) -> Result<I64Schema> {
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

    pub fn as_f32(&self) -> Result<&F32Schema> {
        if let ValueSchema::F32(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as f32",
                self
            )))
        }
    }

    pub fn into_f32(self) -> Result<F32Schema> {
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

    pub fn as_f64(&self) -> Result<&F64Schema> {
        if let ValueSchema::F64(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as f64",
                self
            )))
        }
    }

    pub fn into_f64(self) -> Result<F64Schema> {
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

    pub fn as_date(&self) -> Result<&DateSchema> {
        if let ValueSchema::Date(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as date",
                self
            )))
        }
    }

    pub fn into_date(self) -> Result<DateSchema> {
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

    pub fn as_time(&self) -> Result<&TimeSchema> {
        if let ValueSchema::Time(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as time",
                self
            )))
        }
    }

    pub fn into_time(self) -> Result<TimeSchema> {
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

    pub fn as_timestamp(&self) -> Result<&TimestampSchema> {
        if let ValueSchema::Timestamp(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as timestamp",
                self
            )))
        }
    }

    pub fn into_timestamp(self) -> Result<TimestampSchema> {
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

    pub fn as_decimal(&self) -> Result<&DecimalSchema> {
        if let ValueSchema::Decimal(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as decimal",
                self
            )))
        }
    }

    pub fn into_decimal(self) -> Result<DecimalSchema> {
        if let ValueSchema::Decimal(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as decimal",
                self
            )))
        }
    }

    pub fn is_byte_array(&self) -> bool {
        if let ValueSchema::ByteArray(_) = self {
            true
        } else {
            false
        }
    }

    pub fn as_byte_array(&self) -> Result<&ByteArraySchema> {
        if let ValueSchema::ByteArray(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as byte_array",
                self
            )))
        }
    }

    pub fn into_byte_array(self) -> Result<ByteArraySchema> {
        if let ValueSchema::ByteArray(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as byte_array",
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

    pub fn as_bson(&self) -> Result<&BsonSchema> {
        if let ValueSchema::Bson(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as bson",
                self
            )))
        }
    }

    pub fn into_bson(self) -> Result<BsonSchema> {
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

    pub fn as_string(&self) -> Result<&StringSchema> {
        if let ValueSchema::String(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as string",
                self
            )))
        }
    }

    pub fn into_string(self) -> Result<StringSchema> {
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

    pub fn as_json(&self) -> Result<&JsonSchema> {
        if let ValueSchema::Json(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as json",
                self
            )))
        }
    }

    pub fn into_json(self) -> Result<JsonSchema> {
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

    pub fn as_enum(&self) -> Result<&EnumSchema> {
        if let ValueSchema::Enum(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as enum",
                self
            )))
        }
    }

    pub fn into_enum(self) -> Result<EnumSchema> {
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

    pub fn as_list(&self) -> Result<&ListSchema<ValueSchema>> {
        if let ValueSchema::List(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as list",
                self
            )))
        }
    }

    pub fn into_list(self) -> Result<ListSchema<ValueSchema>> {
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

    pub fn as_map(&self) -> Result<&MapSchema<ValueSchema, ValueSchema>> {
        if let ValueSchema::Map(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as map",
                self
            )))
        }
    }

    pub fn into_map(self) -> Result<MapSchema<ValueSchema, ValueSchema>> {
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

    pub fn as_group(&self) -> Result<&GroupSchema> {
        if let ValueSchema::Group(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as group",
                self
            )))
        }
    }

    pub fn into_group(self) -> Result<GroupSchema> {
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

    pub fn as_option(&self) -> Result<&OptionSchema<ValueSchema>> {
        if let ValueSchema::Option(ret) = self {
            Ok(ret)
        } else {
            Err(ParquetError::General(format!(
                "Cannot access {:?} as option",
                self
            )))
        }
    }

    pub fn into_option(self) -> Result<OptionSchema<ValueSchema>> {
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
    fn downcast(self) -> Result<ValueSchema> {
        Ok(self)
    }
}
impl Downcast<BoolSchema> for ValueSchema {
    fn downcast(self) -> Result<BoolSchema> {
        self.into_bool()
    }
}
impl Downcast<U8Schema> for ValueSchema {
    fn downcast(self) -> Result<U8Schema> {
        self.into_u8()
    }
}
impl Downcast<I8Schema> for ValueSchema {
    fn downcast(self) -> Result<I8Schema> {
        self.into_i8()
    }
}
impl Downcast<U16Schema> for ValueSchema {
    fn downcast(self) -> Result<U16Schema> {
        self.into_u16()
    }
}
impl Downcast<I16Schema> for ValueSchema {
    fn downcast(self) -> Result<I16Schema> {
        self.into_i16()
    }
}
impl Downcast<U32Schema> for ValueSchema {
    fn downcast(self) -> Result<U32Schema> {
        self.into_u32()
    }
}
impl Downcast<I32Schema> for ValueSchema {
    fn downcast(self) -> Result<I32Schema> {
        self.into_i32()
    }
}
impl Downcast<U64Schema> for ValueSchema {
    fn downcast(self) -> Result<U64Schema> {
        self.into_u64()
    }
}
impl Downcast<I64Schema> for ValueSchema {
    fn downcast(self) -> Result<I64Schema> {
        self.into_i64()
    }
}
impl Downcast<F32Schema> for ValueSchema {
    fn downcast(self) -> Result<F32Schema> {
        self.into_f32()
    }
}
impl Downcast<F64Schema> for ValueSchema {
    fn downcast(self) -> Result<F64Schema> {
        self.into_f64()
    }
}
impl Downcast<DateSchema> for ValueSchema {
    fn downcast(self) -> Result<DateSchema> {
        self.into_date()
    }
}
impl Downcast<TimeSchema> for ValueSchema {
    fn downcast(self) -> Result<TimeSchema> {
        self.into_time()
    }
}
impl Downcast<TimestampSchema> for ValueSchema {
    fn downcast(self) -> Result<TimestampSchema> {
        self.into_timestamp()
    }
}
impl Downcast<DecimalSchema> for ValueSchema {
    fn downcast(self) -> Result<DecimalSchema> {
        self.into_decimal()
    }
}
impl Downcast<ByteArraySchema> for ValueSchema {
    fn downcast(self) -> Result<ByteArraySchema> {
        self.into_byte_array()
    }
}
impl Downcast<BsonSchema> for ValueSchema {
    fn downcast(self) -> Result<BsonSchema> {
        self.into_bson()
    }
}
impl Downcast<StringSchema> for ValueSchema {
    fn downcast(self) -> Result<StringSchema> {
        self.into_string()
    }
}
impl Downcast<JsonSchema> for ValueSchema {
    fn downcast(self) -> Result<JsonSchema> {
        self.into_json()
    }
}
impl Downcast<EnumSchema> for ValueSchema {
    fn downcast(self) -> Result<EnumSchema> {
        self.into_enum()
    }
}
impl<T> Downcast<ListSchema<T>> for ValueSchema
where
    ValueSchema: Downcast<T>,
{
    default fn downcast(self) -> Result<ListSchema<T>> {
        let ret = self.into_list()?;
        Ok(ListSchema(ret.0.downcast()?, ret.1))
    }
}
impl Downcast<ListSchema<ValueSchema>> for ValueSchema {
    fn downcast(self) -> Result<ListSchema<ValueSchema>> {
        self.into_list()
    }
}
impl<K, V> Downcast<MapSchema<K, V>> for ValueSchema
where
    ValueSchema: Downcast<K> + Downcast<V>,
{
    default fn downcast(self) -> Result<MapSchema<K, V>> {
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
    fn downcast(self) -> Result<MapSchema<ValueSchema, ValueSchema>> {
        self.into_map()
    }
}
impl Downcast<GroupSchema> for ValueSchema {
    fn downcast(self) -> Result<GroupSchema> {
        self.into_group()
    }
}
impl<T> Downcast<OptionSchema<T>> for ValueSchema
where
    ValueSchema: Downcast<T>,
{
    default fn downcast(self) -> Result<OptionSchema<T>> {
        let ret = self.into_option()?;
        ret.0.downcast().map(OptionSchema)
    }
}
impl Downcast<OptionSchema<ValueSchema>> for ValueSchema {
    fn downcast(self) -> Result<OptionSchema<ValueSchema>> {
        self.into_option()
    }
}

#[derive(Default, Debug)]
pub struct BoxSchema<T>(pub(super) T);
impl<T> Schema for BoxSchema<T>
where
    T: Schema,
{
    fn fmt(
        self_: Option<&Self>,
        r: Option<Repetition>,
        name: Option<&str>,
        f: &mut fmt::Formatter,
    ) -> fmt::Result {
        <T as Schema>::fmt(self_.map(|self_| &self_.0), r, name, f)
    }
}

/// A root Parquet schema.
///
/// It implements [`FromStr`] and [`Display`] so it can be converted from and to a Parquet
/// schema string like so:
///
/// ```
/// # use parquet::errors::Result;
/// use parquet::record::{RootSchema, types::Value};
///
/// #
/// # fn main() -> Result<()> {
/// let schema_str =
/// "message spark_schema {
///     REQUIRED double c;
///     REQUIRED int32 b (INT_32);
/// }";
///
/// let schema: RootSchema<Value> = schema_str.parse()?;
///
/// let schema_str2 = schema.to_string();
///
/// assert_eq!(schema_str, schema_str2);
/// # Ok(())
/// # }
/// ```
pub struct RootSchema<T>(pub String, pub T::Schema, pub PhantomData<fn(T)>)
where
    T: Record;
impl<T> Default for RootSchema<T>
where
    T: Record,
    T::Schema: Default,
{
    fn default() -> Self {
        RootSchema(
            String::from("parquet_rust_schema"),
            Default::default(),
            PhantomData,
        )
    }
}
impl<T> Debug for RootSchema<T>
where
    T: Record,
    T::Schema: Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_tuple("RootSchema")
            .field(&self.0)
            .field(&self.1)
            .finish()
    }
}
impl<T> Schema for RootSchema<T>
where
    T: Record,
{
    fn fmt(
        self_: Option<&Self>,
        r: Option<Repetition>,
        _name: Option<&str>,
        f: &mut fmt::Formatter,
    ) -> fmt::Result {
        assert_eq!(r, None);
        <T::Schema as Schema>::fmt(
            self_.map(|self_| &self_.1),
            None,
            self_.map(|self_| &*self_.0),
            f,
        )
    }
}
impl<T> FromStr for RootSchema<T>
where
    T: Record,
{
    type Err = ParquetError;

    fn from_str(s: &str) -> Result<Self> {
        parse_message_type(s)
            .and_then(|x| <Root<T> as Record>::parse(&x, None))
            .map(|x| x.1)
    }
}
impl<T> Display for RootSchema<T>
where
    T: Record,
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        <Self as Schema>::fmt(Some(&self), None, None, fmt)
    }
}

/// Schema for a tuple. See types/tuple.rs for the implementations over lengths up to 32.
pub struct TupleSchema<T>(pub(super) T);

#[cfg(test)]
mod tests {
    use super::*;

    use crate::record::types::Value;

    #[test]
    fn schema_printing() {
        let _schema: RootSchema<Value> = "message org.apache.impala.ComplexTypesTbl {
            REQUIRED int64 ID (INT_64);
            REQUIRED group Int_Array (LIST) {
                REPEATED group list {
                    REQUIRED int32 element (INT_32);
                }
            }
            REQUIRED group int_array_array (LIST) {
                REPEATED group list {
                    REQUIRED group element (LIST) {
                        REPEATED group list {
                            REQUIRED int32 element (INT_32);
                        }
                    }
                }
            }
            REQUIRED group Int_Map (MAP) {
                REPEATED group map {
                    REQUIRED byte_array key (UTF8);
                    REQUIRED int32 value (INT_32);
                }
            }
            REQUIRED group int_map_array (LIST) {
                REPEATED group list {
                    REQUIRED group element (MAP_KEY_VALUE) {
                        REPEATED group map {
                            REQUIRED byte_array key (UTF8);
                            REQUIRED int32 value (INT_32);
                        }
                    }
                }
            }
            OPTIONAL group nested_Struct {
                REQUIRED int32 a (INT_32);
                REQUIRED group B (LIST) {
                    REPEATED group list {
                        REQUIRED int32 element (INT_32);
                    }
                }
                REQUIRED group c {
                    REQUIRED group D (LIST) {
                        REPEATED group list {
                            REQUIRED group element (LIST) {
                                REPEATED group list {
                                    OPTIONAL group element {
                                        REQUIRED int96 e;
                                        REQUIRED byte_array f (UTF8);
                                    }
                                }
                            }
                        }
                    }
                }
                REQUIRED group G (MAP) {
                    REPEATED group key_value {
                        REQUIRED byte_array yek (UTF8);
                        OPTIONAL group eulav {
                            REQUIRED group h {
                                REQUIRED group i (LIST) {
                                    REPEATED group list {
                                        REQUIRED double element;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            REQUIRED group decimals {
                REPEATED int32 j (decimal(2,1));
                OPTIONAL int64 k (decimal(2,0));
                REQUIRED byte_array l (decimal(2));
                REPEATED fixed_len_byte_array(100) m (decimal(20,19));
            }
            REQUIRED group legacy_list (LIST) {
                REPEATED group n_tuple {
                    OPTIONAL fixed_len_byte_array(10) o;
                }
            }
            REQUIRED group times (LIST) {
                REPEATED group q {
                    REQUIRED int64 r (TIMESTAMP_MICROS);
                    REQUIRED int64 s (TIMESTAMP_MILLIS);
                    REQUIRED int32 t (TIME_MILLIS);
                    REQUIRED int64 u (TIME_MICROS);
                    REQUIRED int32 v (DATE);
                }
            }
            REQUIRED group strings {
                REPEATED byte_array j;
                REPEATED byte_array k (BSON);
                REPEATED byte_array l (UTF8);
                REPEATED byte_array m (ENUM);
                REPEATED byte_array n (JSON);
            }
            REQUIRED group numbers {
                REPEATED int32 i8 (INT_8);
                REPEATED int32 u8 (UINT_8);
                REPEATED int32 i16 (INT_16);
                REPEATED int32 u16 (UINT_16);
                REPEATED int32 i32 (INT_32);
                REPEATED int32 u32 (UINT_32);
                REPEATED int64 i64 (INT_64);
                REPEATED int64 u64 (UINT_64);
            }
        }"
        .parse()
        .unwrap();
    }

    #[test]
    fn invalid_map_type() {
        let schema = "message spark_schema {
            OPTIONAL group a (MAP) {
                REPEATED group key_value {
                    REQUIRED BYTE_ARRAY key (UTF8);
                    OPTIONAL group value (MAP) {
                        REPEATED group key_value {
                            REQUIRED INT32 key;
                        }
                    }
                }
            }
        }"
        .parse::<RootSchema<Value>>()
        .unwrap()
        .to_string();

        let schema2 = "message spark_schema {
    OPTIONAL group a (MAP) {
        REPEATED group key_value {
            REQUIRED byte_array key (UTF8);
            OPTIONAL group value {
                REPEATED group key_value {
                    REQUIRED int32 key (INT_32);
                }
            }
        }
    }
}";
        assert_eq!(schema, schema2);
    }

}
