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

//! Implement [`Record`] for `Option<T> where T: Record`.

use std::collections::HashMap;

use crate::{
    basic::Repetition,
    column::reader::ColumnReader,
    errors::{ParquetError, Result},
    record::{reader::OptionReader, schemas::OptionSchema, Record},
    schema::types::{ColumnPath, Type},
};

// `Option<T>` corresponds to Parquet fields marked as "optional".
impl<T> Record for Option<T>
where
    T: Record,
{
    type Schema = OptionSchema<T::Schema>;
    type Reader = OptionReader<T::Reader>;

    fn parse(
        schema: &Type,
        repetition: Option<Repetition>,
    ) -> Result<(String, Self::Schema)> {
        if repetition == Some(Repetition::OPTIONAL) {
            return T::parse(&schema, Some(Repetition::REQUIRED))
                .map(|(name, schema)| (name, OptionSchema(schema)));
        }
        Err(ParquetError::General(String::from(
            "Couldn't parse Option<T>",
        )))
    }

    fn reader(
        schema: &Self::Schema,
        path: &mut Vec<String>,
        def_level: i16,
        rep_level: i16,
        paths: &mut HashMap<ColumnPath, ColumnReader>,
        batch_size: usize,
    ) -> Self::Reader {
        OptionReader {
            reader: <T as Record>::reader(
                &schema.0,
                path,
                def_level + 1,
                rep_level,
                paths,
                batch_size,
            ),
        }
    }
}
