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

use std::collections::HashMap;

use crate::{
    basic::Repetition,
    column::reader::ColumnReader,
    errors::ParquetError,
    record::{reader::OptionReader, schemas::OptionSchema, Deserialize},
    schema::types::{ColumnDescPtr, ColumnPath, Type},
};

impl<T> Deserialize for Option<T>
where
    T: Deserialize,
{
    type Reader = OptionReader<T::Reader>;
    type Schema = OptionSchema<T::Schema>;

    fn parse(schema: &Type) -> Result<(String, Self::Schema), ParquetError> {
        // <Value as Deserialize>::parse(schema).and_then(|(name, schema)| {
        //   Ok((name, OptionSchema(schema.as_option()?.0.downcast()?)))
        // })
        if schema.get_basic_info().repetition() == Repetition::OPTIONAL {
            let mut schema2: Type = schema.clone();
            let basic_info = match schema2 {
                Type::PrimitiveType {
                    ref mut basic_info, ..
                } => basic_info,
                Type::GroupType {
                    ref mut basic_info, ..
                } => basic_info,
            };
            basic_info.set_repetition(Some(Repetition::REQUIRED));
            return Ok((
                schema.name().to_owned(),
                OptionSchema(T::parse(&schema2)?.1),
            ));
        }
        Err(ParquetError::General(String::from(
            "Couldn't parse Option<T>",
        )))
    }

    fn reader(
        schema: &Self::Schema,
        path: &mut Vec<String>,
        curr_def_level: i16,
        curr_rep_level: i16,
        paths: &mut HashMap<ColumnPath, (ColumnDescPtr, ColumnReader)>,
        batch_size: usize,
    ) -> Self::Reader {
        OptionReader {
            def_level: curr_def_level,
            reader: <T as Deserialize>::reader(
                &schema.0,
                path,
                curr_def_level + 1,
                curr_rep_level,
                paths,
                batch_size,
            ),
        }
    }
}
