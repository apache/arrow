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

use std::{
    collections::{hash_map, HashMap},
    fmt::{self, Debug},
    hash::Hash,
};

use crate::{
    basic::{LogicalType, Repetition},
    column::reader::ColumnReader,
    errors::ParquetError,
    record::{
        reader::{KeyValueReader, MapReader, Reader},
        schemas::MapSchema,
        Deserialize,
    },
    schema::types::{ColumnPath, Type},
};

// https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#backward-compatibility-rules
pub(super) fn parse_map<K: Deserialize, V: Deserialize>(
    schema: &Type,
) -> Result<MapSchema<K::Schema, V::Schema>, ParquetError> {
    if schema.is_group()
        && (schema.get_basic_info().logical_type() == LogicalType::MAP
            || schema.get_basic_info().logical_type() == LogicalType::MAP_KEY_VALUE)
        && schema.get_fields().len() == 1
    {
        let sub_schema = schema.get_fields().into_iter().nth(0).unwrap();
        if sub_schema.is_group()
            && sub_schema.get_basic_info().repetition() == Repetition::REPEATED
            && sub_schema.get_fields().len() == 2
        {
            let mut fields = sub_schema.get_fields().into_iter();
            let (key, value) = (fields.next().unwrap(), fields.next().unwrap());
            let key_value_name = if sub_schema.name() == "key_value" {
                None
            } else {
                Some(sub_schema.name().to_owned())
            };
            let key_name = if key.name() == "key" {
                None
            } else {
                Some(key.name().to_owned())
            };
            let value_name = if value.name() == "value" {
                None
            } else {
                Some(value.name().to_owned())
            };
            return Ok(MapSchema(
                K::parse(&*key, Some(key.get_basic_info().repetition()))?.1,
                V::parse(&*value, Some(value.get_basic_info().repetition()))?.1,
                key_value_name,
                key_name,
                value_name,
            ));
        }
    }
    Err(ParquetError::General(String::from(
        "Couldn't parse Map<K,V>",
    )))
}

#[derive(Clone, Eq)]
pub struct Map<K: Hash + Eq, V>(pub(in super::super) HashMap<K, V>);

impl<K, V> Deserialize for Map<K, V>
where
    K: Deserialize + Hash + Eq,
    V: Deserialize,
{
    existential type Reader: Reader<Item = Self>;
    type Schema = MapSchema<K::Schema, V::Schema>;

    fn parse(
        schema: &Type,
        repetition: Option<Repetition>,
    ) -> Result<(String, Self::Schema), ParquetError> {
        if repetition == Some(Repetition::REQUIRED) {
            return parse_map::<K, V>(schema).map(|schema2| (schema.name().to_owned(), schema2));
        }
        Err(ParquetError::General(String::from(
            "Couldn't parse Map<K,V>",
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
        let key_value_name = schema.2.as_ref().map(|x| &**x).unwrap_or("key_value");
        let key_name = schema.3.as_ref().map(|x| &**x).unwrap_or("key");
        let value_name = schema.4.as_ref().map(|x| &**x).unwrap_or("value");

        path.push(key_value_name.to_owned());
        path.push(key_name.to_owned());
        let keys_reader = K::reader(
            &schema.0,
            path,
            def_level + 1,
            rep_level + 1,
            paths,
            batch_size,
        );
        path.pop().unwrap();
        path.push(value_name.to_owned());
        let values_reader = V::reader(
            &schema.1,
            path,
            def_level + 1,
            rep_level + 1,
            paths,
            batch_size,
        );
        path.pop().unwrap();
        path.pop().unwrap();

        MapReader(
            KeyValueReader {
                keys_reader,
                values_reader,
            },
            |x: Vec<_>| Ok(Map(x.into_iter().collect())),
        )
    }
}

impl<K, V> Map<K, V>
where
    K: Hash + Eq,
{
    pub fn iter(&self) -> hash_map::Iter<'_, K, V> {
        self.0.iter()
    }

    pub fn into_iter(self) -> hash_map::IntoIter<K, V> {
        self.0.into_iter()
    }
}
impl<K, V> From<HashMap<K, V>> for Map<K, V>
where
    K: Hash + Eq,
{
    fn from(hashmap: HashMap<K, V>) -> Self {
        Map(hashmap)
    }
}
impl<K, V> Into<HashMap<K, V>> for Map<K, V>
where
    K: Hash + Eq,
{
    fn into(self) -> HashMap<K, V> {
        self.0
    }
}
impl<K, V, V1> PartialEq<Map<K, V1>> for Map<K, V>
where
    K: Eq + Hash,
    V: PartialEq<V1>,
{
    fn eq(&self, other: &Map<K, V1>) -> bool {
        if self.0.len() != other.0.len() {
            return false;
        }

        self.0
            .iter()
            .all(|(key, value)| other.0.get(key).map_or(false, |v| *value == *v))
    }
}
impl<K, V> Debug for Map<K, V>
where
    K: Hash + Eq + Debug,
    V: Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.debug_map().entries(self.iter()).finish()
    }
}
