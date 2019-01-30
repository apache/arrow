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
pub mod reader;
pub mod schemas;
mod triplet;
pub mod types;

use std::{
    collections::HashMap,
    fmt::{self, Debug},
};

use self::reader::Reader;
use crate::{
    basic::Repetition,
    column::reader::ColumnReader,
    errors::ParquetError,
    schema::types::{ColumnPath, Type},
};

#[doc(inline)]
pub use parquet_derive::Deserialize;
pub use triplet::{TripletIter, TypedTripletIter};
#[doc(hidden)]
pub mod _private {
    pub use super::display::DisplaySchemaGroup;
}

pub trait Schema: Debug {
    fn fmt(
        self_: Option<&Self>,
        r: Option<Repetition>,
        name: Option<&str>,
        f: &mut fmt::Formatter,
    ) -> Result<(), fmt::Error>;
}

pub trait Deserialize: Sized {
    type Schema: Schema;
    type Reader: Reader<Item = Self>;

    /// Parse a [`Type`] into `Self::Schema`.
    fn parse(
        schema: &Type,
        repetition: Option<Repetition>,
    ) -> Result<(String, Self::Schema), ParquetError>;

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
