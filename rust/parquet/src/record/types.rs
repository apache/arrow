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

use std::{collections::HashMap, marker::PhantomData};

use super::{
    display::DisplayFmt,
    reader::{BoxReader, RootReader},
    schemas::{BoxSchema, RootSchema, ValueSchema},
    Deserialize, Schema,
};
use crate::{
    basic::Repetition,
    column::reader::ColumnReader,
    errors::ParquetError,
    schema::types::{ColumnPath, Type},
};

pub use self::{
    array::*, boxed::*, decimal::*, group::*, list::*, map::*, numbers::*, option::*,
    root::*, time::*, tuple::*, value::*, value_required::*,
};

/// Due to downcasting from Value -> Option<T>
pub trait Downcast<T> {
    fn downcast(self) -> Result<T, ParquetError>;
}

fn downcast<T>((name, schema): (String, ValueSchema)) -> Result<(String, T), ParquetError>
where
    ValueSchema: Downcast<T>,
{
    schema.downcast().map(|schema| (name, schema))
}
