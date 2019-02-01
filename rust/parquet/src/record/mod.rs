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

//! Contains record-based API for reading Parquet files.

mod display;
mod reader;
mod schemas;
mod triplet;
pub mod types;

use std::{
    collections::HashMap,
    fmt::{self, Debug},
};

use crate::{
    basic::Repetition,
    column::reader::ColumnReader,
    errors::Result,
    schema::types::{ColumnPath, Type},
};

#[doc(inline)]
pub use parquet_derive::Record;
pub use reader::RowIter;
pub use schemas::RootSchema;
pub use triplet::{TripletIter, TypedTripletIter};
#[doc(hidden)]
pub mod _private {
    pub use super::display::DisplaySchemaGroup;
}
mod predicate {
    pub struct Predicate;
}
pub(crate) use self::predicate::Predicate;

/// This trait is implemented on all types that can be read from/written to Parquet files.
///
/// It is implemented on the following types:
///
/// | Rust type | Parquet Physical Type | Parquet Logical Type |
/// |---|---|---|
/// | `bool` | boolean | none |
/// | `u8` | int32 | uint_8 |
/// | `i8` | int32 | int_8 |
/// | `u16` | int32 | uint_16 |
/// | `i16` | int32 | int_16 |
/// | `u32` | int32 | uint_32 |
/// | `i32` | int32 | int_32 |
/// | `u64` | int64 | uint_64 |
/// | `i64` | int64 | int_64 |
/// | `f32` | float | none |
/// | `f64` | double | none |
/// | [`Date`](self::types::Date) | int32 | [date](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#date) |
/// | [`Time`](self::types::Time) | int32 | [time_millis](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#time) |
/// | [`Time`](self::types::Time) | int64 | [time_micros](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#time) |
/// | [`Timestamp`](self::types::Timestamp) | int64 | [timestamp_millis](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#timestamp) |
/// | [`Timestamp`](self::types::Timestamp) | int64 | [timestamp_micros](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#timestamp) |
/// | [`Timestamp`](self::types::Timestamp) | int96 | [none](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#timestamp) |
/// | [`Decimal`](self::types::Decimal) | int32 | [decimal](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#decimal) |
/// | [`Decimal`](self::types::Decimal) | int64 | [decimal](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#decimal) |
/// | [`Decimal`](self::types::Decimal) | byte_array | [decimal](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#decimal) |
/// | `Vec<u8>` | byte_array | none |
/// | `[u8; N]` | fixed_len_byte_array | none |
/// | [`Bson`](self::types::Bson) | byte_array | [bson](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#bson) |
/// | `String` | byte_array | [utf8](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#string) |
/// | [`Json`](self::types::Json) | byte_array | [json](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#json) |
/// | [`Enum`](self::types::Enum) | byte_array | [enum](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#enum) |
/// | [`List<T>`](self::types::List) | group | [list](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#lists) |
/// | [`Map<K,V>`](self::types::Map) | group | [map](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#maps) |
/// | [`Group`](self::types::Group) | group | none |
/// | `(T, U, …)` | group | none |
/// | `Option<T>` | – | – |
/// | [`Value`](self::types::Value) | * | * |
///
/// `Option<T>` corresponds to a field marked as "optional".
///
/// [`List<T>`](self::types::List) corresponds to either [annotated List logical types](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#lists), or unannotated fields marked as "repeated".
///
/// [`Value`](self::types::Value) corresponds to any valid Parquet type, and is useful
/// when the type is not known at compile time.
///
/// "byte_array" is interchangeable with "fixed_len_byte_array" for the purposes of the
/// above correspondance.
///
/// The implementation for tuples is only for those up to length 32. The implementation
/// for arrays is only for common array lengths. See [`Record`] for more details.
///
/// ## `#[derive(Record)]`
///
/// The easiest way to implement `Record` on a new type is using `#[derive(Record)]`:
///
/// ```
/// use parquet::record::{types::Timestamp, Record};
///
/// #[derive(Record, Debug)]
/// struct MyRow {
///     id: u64,
///     time: Timestamp,
///     event: String,
/// }
/// ```
///
/// If the Rust field name and the Parquet field name differ, say if the latter is not an idiomatic or valid identifier in Rust, then an automatic rename can be made like so:
///
/// ```
/// # use parquet::record::{Record, types::Timestamp};
/// #[derive(Record, Debug)]
/// struct MyRow {
///     #[parquet(rename = "ID")]
///     id: u64,
///     time: Timestamp,
///     event: String,
/// }
/// ```
pub trait Record: Sized {
    type Schema: Schema;
    type Reader: Reader<Item = Self>;

    /// Parse a [`Type`] into `Self::Schema`.
    fn parse(
        schema: &Type,
        repetition: Option<Repetition>,
    ) -> Result<(String, Self::Schema)>;

    /// Builds tree of readers for the specified schema recursively.
    fn reader(
        schema: &Self::Schema,
        path: &mut Vec<String>,
        def_level: i16,
        rep_level: i16,
        paths: &mut HashMap<ColumnPath, ColumnReader>,
        batch_size: usize,
    ) -> Self::Reader;
}

pub trait Schema: Debug {
    fn fmt(
        self_: Option<&Self>,
        r: Option<Repetition>,
        name: Option<&str>,
        f: &mut fmt::Formatter,
    ) -> fmt::Result;
}

pub trait Reader {
    type Item;
    fn read(&mut self, def_level: i16, rep_level: i16) -> Result<Self::Item>;
    fn advance_columns(&mut self) -> Result<()>;
    fn has_next(&self) -> bool;
    fn current_def_level(&self) -> i16;
    fn current_rep_level(&self) -> i16;
}
