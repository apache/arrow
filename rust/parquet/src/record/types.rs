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
mod time;
mod tuple;
mod value;
mod value_required;

use std::{collections::HashMap, marker::PhantomData};

use super::{
    reader::{BoxReader, RootReader},
    schemas::{BoxSchema, RootSchema, ValueSchema},
    Deserialize,
};
use crate::{
    basic::Repetition,
    column::reader::ColumnReader,
    errors::ParquetError,
    schema::types::{ColumnDescPtr, ColumnPath, Type},
};

pub use self::{
    array::*, boxed::*, decimal::*, group::*, list::*, map::*, numbers::*, option::*, time::*,
    tuple::*, value::*, value_required::*,
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

#[derive(Clone, Hash, PartialEq, Eq, Debug)]
pub struct Root<T>(pub T);

impl<T> Deserialize for Root<T>
where
    T: Deserialize,
{
    type Reader = RootReader<T::Reader>;
    type Schema = RootSchema<T, T::Schema>;

    fn parse(
        schema: &Type,
        repetition: Option<Repetition>,
    ) -> Result<(String, Self::Schema), ParquetError> {
        assert!(repetition.is_none());
        if schema.is_schema() {
            T::parse(schema, Some(Repetition::REQUIRED))
                .map(|(name, schema)| (String::from(""), RootSchema(name, schema, PhantomData)))
        } else {
            Err(ParquetError::General(format!(
                "Can't parse Root {:?}",
                schema
            )))
        }
    }

    fn reader(
        schema: &Self::Schema,
        path: &mut Vec<String>,
        def_level: i16,
        rep_level: i16,
        paths: &mut HashMap<ColumnPath, ColumnReader>,
        batch_size: usize,
    ) -> Self::Reader {
        RootReader(T::reader(
            &schema.1, path, def_level, rep_level, paths, batch_size,
        ))
    }
}
