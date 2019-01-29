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

use std::{collections::HashMap, marker::PhantomData};

use crate::{
    basic::Repetition,
    column::reader::ColumnReader,
    data_type::{ByteArrayType, Decimal, FixedLenByteArrayType, Int32Type, Int64Type},
    errors::ParquetError,
    record::{
        display::DisplayFmt,
        reader::RootReader,
        schemas::RootSchema,
        triplet::TypedTripletIter,
        types::{downcast, Value},
        Deserialize, Schema,
    },
    schema::{
        parser::parse_message_type,
        types::{ColumnPath, Type},
    },
};

#[derive(Clone, Hash, PartialEq, Eq, Debug)]
pub struct Root<T>(pub T);

impl<T> Deserialize for Root<T>
where
    T: Deserialize,
{
    type Reader = RootReader<T::Reader>;
    type Schema = RootSchema<T>;

    fn parse(
        schema: &Type,
        repetition: Option<Repetition>,
    ) -> Result<(String, Self::Schema), ParquetError> {
        assert!(repetition.is_none());
        let schema_ = schema;
        if schema.is_schema() {
            T::parse(schema, Some(Repetition::REQUIRED))
                .map(|(name, schema)| (String::from(""), RootSchema(name, schema, PhantomData)))
                .map_err(|err| {
                    let actual_schema = Value::parse(schema, Some(Repetition::REQUIRED))
                        .map(|(name, schema)| RootSchema(name, schema, PhantomData));
                    let actual_schema = match actual_schema {
                        Ok(actual_schema) => actual_schema,
                        Err(err) => return err,
                    };
                    let actual_schema = DisplayFmt::new(|fmt| {
                        <<Root<Value> as Deserialize>::Schema>::fmt(Some(&actual_schema), None, None, fmt)
                    });
                    let schema = DisplayFmt::new(|fmt| <<Root<T> as Deserialize>::Schema>::fmt(None, None, None, fmt));
                    ParquetError::General(format!(
                        "Types don't match schema.\nSchema is:\n{}\nBut types require:\n{}\nError: {}",
                        actual_schema,
                        schema,
                        err
                    ))
                })
                .map(|(name,schema)| {
                    #[cfg(test)] {
                        let printed = format!("{}", DisplayFmt::new(|fmt| {
                            <<Root<T> as Deserialize>::Schema>::fmt(Some(&schema), None, None, fmt)
                        }));
                        let schema_2 = parse_message_type(&printed).unwrap();
                        let (name2,schema2) = T::parse(&schema_2, Some(Repetition::REQUIRED))
                            .map(|(name, schema)| (String::from(""), RootSchema(name, schema, PhantomData))).unwrap();
                        let printed2 = format!("{}", DisplayFmt::new(|fmt| {
                            <<Root<T> as Deserialize>::Schema>::fmt(Some(&schema2), None, Some(&name2), fmt)
                        }));
                        assert_eq!(printed, printed2, "{:#?}", schema_);

                        let (name3,schema3) = Value::parse(&schema_2, Some(Repetition::REQUIRED))
                            .map(|(name, schema)| (String::from(""), RootSchema(name, schema, PhantomData))).unwrap();
                        let printed3 = format!("{}", DisplayFmt::new(|fmt| {
                            <<Root<Value> as Deserialize>::Schema>::fmt(Some(&schema3), None, Some(&name3), fmt)
                        }));
                        assert_eq!(printed, printed3, "{:#?}", schema_);
                    }
                    (name, schema)
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
