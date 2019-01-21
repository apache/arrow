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
        reader::{KeyValueReader, MapReader},
        schemas::MapSchema,
        Deserialize,
    },
    schema::types::{ColumnDescPtr, ColumnPath, Type},
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
            && !sub_schema.is_schema()
            && sub_schema.get_basic_info().repetition() == Repetition::REPEATED
            && sub_schema.get_fields().len() == 2
        {
            let mut fields = sub_schema.get_fields().into_iter();
            let (key, value_) = (fields.next().unwrap(), fields.next().unwrap());
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
            let value_name = if value_.name() == "value" {
                None
            } else {
                Some(value_.name().to_owned())
            };
            return Ok(MapSchema(
                K::parse(&*key)?.1,
                V::parse(&*value_)?.1,
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

#[derive(Clone, PartialEq, Eq)]
pub struct Map<K: Hash + Eq, V>(pub(in super::super) HashMap<K, V>);

impl<K, V> Deserialize for Map<K, V>
where
    K: Deserialize + Hash + Eq,
    V: Deserialize,
{
    // existential type Reader: Reader<Item = Self>;
    type Reader = MapReader<
        KeyValueReader<K::Reader, V::Reader>,
        fn(Vec<(K, V)>) -> Result<Self, ParquetError>,
    >;
    type Schema = MapSchema<K::Schema, V::Schema>;

    fn parse(schema: &Type) -> Result<(String, Self::Schema), ParquetError> {
        if !schema.is_schema() && schema.get_basic_info().repetition() == Repetition::REQUIRED {
            return parse_map::<K, V>(schema).map(|schema2| (schema.name().to_owned(), schema2));
        }
        Err(ParquetError::General(String::from(
            "Couldn't parse Map<K,V>",
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
        let key_value_name = schema.2.as_ref().map(|x| &**x).unwrap_or("key_value");
        let key_name = schema.3.as_ref().map(|x| &**x).unwrap_or("key");
        let value_name = schema.4.as_ref().map(|x| &**x).unwrap_or("value");

        path.push(key_value_name.to_owned());
        path.push(key_name.to_owned());
        let keys_reader = K::reader(
            &schema.0,
            path,
            curr_def_level + 1,
            curr_rep_level + 1,
            paths,
            batch_size,
        );
        path.pop().unwrap();
        path.push(value_name.to_owned());
        let values_reader = V::reader(
            &schema.1,
            path,
            curr_def_level + 1,
            curr_rep_level + 1,
            paths,
            batch_size,
        );
        path.pop().unwrap();
        path.pop().unwrap();

        MapReader(
            KeyValueReader {
                def_level: curr_def_level,
                rep_level: curr_rep_level,
                keys_reader,
                values_reader,
            },
            (|x: Vec<_>| Ok(Map(x.into_iter().collect()))) as fn(_) -> _,
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
impl<K, V> Debug for Map<K, V>
where
    K: Hash + Eq + Debug,
    V: Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.debug_map().entries(self.iter()).finish()
    }
}
