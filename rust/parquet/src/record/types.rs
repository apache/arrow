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

//! Implementations of Rust types that correspond to Parquet logical types.
//! [`Record`](super::Record) is implemented for each of them.

mod array;
mod boxed;
mod decimal;
mod group;
mod list;
mod map;
mod numbers;
mod option;
mod root;
mod time;
mod tuple;
mod value;
mod value_required;

use super::schemas::ValueSchema;
use crate::errors::Result;

pub use self::{
    array::{Bson, Enum, Json},
    group::{Group, Row},
    list::List,
    map::Map,
    root::Root,
    time::{Date, Time, Timestamp},
    value::Value,
    value_required::ValueRequired,
};

/// This trait lets one downcast a generic type like [`Value`] to a specific type like
/// `u64`.
///
/// It exists, rather than for example using [`TryInto`](std::convert::TryInto), due to
/// coherence issues with downcasting to foreign types like `Option<T>`.
pub trait Downcast<T> {
    fn downcast(self) -> Result<T>;
}

fn downcast<T>((name, schema): (String, ValueSchema)) -> Result<(String, T)>
where
    ValueSchema: Downcast<T>,
{
    schema.downcast().map(|schema| (name, schema))
}
