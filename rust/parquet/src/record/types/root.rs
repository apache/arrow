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

//! Implement [`Record`] for `Root<T> where T: Record`.

use std::{collections::HashMap, marker::PhantomData};

#[cfg(debug_assertions)]
use crate::schema::parser::parse_message_type;
use crate::{
    basic::Repetition,
    column::reader::ColumnReader,
    errors::{ParquetError, Result},
    record::{
        display::DisplayFmt, reader::RootReader, schemas::RootSchema, types::Value,
        Record, Schema,
    },
    schema::types::{ColumnPath, Type},
};

/// `Root<T>` corresponds to the root of the schema, i.e. what is marked as "message" in a
/// Parquet schema string.
#[derive(Clone, Hash, PartialEq, Eq, Debug)]
pub struct Root<T>(pub T);

impl<T> Record for Root<T>
where
    T: Record,
{
    type Reader = RootReader<T::Reader>;
    type Schema = RootSchema<T>;

    fn parse(
        schema: &Type,
        repetition: Option<Repetition>,
    ) -> Result<(String, Self::Schema)> {
        assert!(repetition.is_none());
        if schema.is_schema() {
            T::parse(schema, Some(Repetition::REQUIRED))
                .map(|(name, schema_)| (String::from(""), RootSchema(name, schema_, PhantomData)))
                .map_err(|err| {
                    let actual_schema = Value::parse(schema, Some(Repetition::REQUIRED))
                        .map(|(name, schema_)| RootSchema(name, schema_, PhantomData));
                    let actual_schema = match actual_schema {
                        Ok(actual_schema) => actual_schema,
                        Err(err) => return err,
                    };
                    let actual_schema = DisplayFmt::new(|fmt| {
                        <<Root<Value> as Record>::Schema>::fmt(Some(&actual_schema), None, None, fmt)
                    });
                    let schema_ = DisplayFmt::new(|fmt| <<Root<T> as Record>::Schema>::fmt(None, None, None, fmt));
                    ParquetError::General(format!(
                        "Types don't match schema.\nSchema is:\n{}\nBut types require:\n{}\nError: {}",
                        actual_schema,
                        schema_,
                        err
                    ))
                })
            .map(|(name,schema_)| {
                #[cfg(debug_assertions)] {
                    // Check parsing and printing by round-tripping both typed and untyped and checking correctness.

                    let printed = format!("{}", schema_);
                    let schema_2 = parse_message_type(&printed).unwrap();

                    let schema2 = T::parse(&schema_2, Some(Repetition::REQUIRED))
                        .map(|(name, schema_)| RootSchema::<T>(name, schema_, PhantomData)).unwrap();
                    let printed2 = format!("{}", schema2);
                    assert_eq!(printed, printed2, "{:#?}", schema);

                    let schema3 = Value::parse(&schema_2, Some(Repetition::REQUIRED))
                        .map(|(name, schema_)| RootSchema::<Value>(name, schema_, PhantomData)).unwrap();
                    let printed3 = format!("{}", schema3);
                    assert_eq!(printed, printed3, "{:#?}", schema);
                }
                (name, schema_)
            })
        } else {
            Err(ParquetError::General(format!(
                "Not a valid root schema {:?}",
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
