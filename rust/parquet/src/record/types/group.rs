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

//! Implement [`Record`] for [`Group`] aka [`Row`].

use fxhash::FxBuildHasher;
use linked_hash_map::LinkedHashMap;
use std::{
    collections::HashMap,
    fmt::{self, Debug},
    ops::Index,
    slice::SliceIndex,
    str,
    sync::Arc,
};

use crate::{
    basic::Repetition,
    column::reader::ColumnReader,
    errors::{ParquetError, Result},
    record::{
        reader::GroupReader,
        schemas::{GroupSchema, ValueSchema},
        types::Value,
        Record,
    },
    schema::types::{ColumnPath, Type},
};

/// A Rust type corresponding to Parquet groups of fields.
#[derive(Clone, PartialEq)]
pub struct Group(
    pub(crate) Vec<Value>,
    pub(crate) Arc<LinkedHashMap<String, usize, FxBuildHasher>>,
);
/// [`Row`] is identical to [`Group`] in every way; this alias exists as arguably reading
/// rows into a type called `Row` is more idiomatic than into a type called `Group`.
pub type Row = Group;

impl Record for Group {
    type Reader = GroupReader;
    type Schema = GroupSchema;

    fn parse(
        schema: &Type,
        repetition: Option<Repetition>,
    ) -> Result<(String, Self::Schema)> {
        if schema.is_group() && repetition == Some(Repetition::REQUIRED) {
            let mut map = LinkedHashMap::with_capacity_and_hasher(
                schema.get_fields().len(),
                Default::default(),
            );
            let fields = schema
                .get_fields()
                .iter()
                .enumerate()
                .map(|(i, field)| {
                    let (name, schema) = <Value as Record>::parse(
                        &**field,
                        Some(field.get_basic_info().repetition()),
                    )?;
                    let x = map.insert(name, i);
                    assert!(x.is_none());
                    Ok(schema)
                })
                .collect::<Result<Vec<ValueSchema>>>()?;
            let schema_ = GroupSchema(fields, map);
            return Ok((schema.name().to_owned(), schema_));
        }
        Err(ParquetError::General(format!(
            "Can't parse Group {:?}",
            schema
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
        let readers = schema
            .1
            .iter()
            .map(|(name, _index)| name)
            .zip(schema.0.iter())
            .map(|(name, field)| {
                path.push(name.clone());
                let ret =
                    Value::reader(field, path, def_level, rep_level, paths, batch_size);
                path.pop().unwrap();
                ret
            })
            .collect();
        GroupReader {
            readers,
            fields: Arc::new(schema.1.clone()),
        }
    }
}

impl Group {
    #[doc(hidden)]
    pub fn new(
        fields: Vec<Value>,
        field_names: Arc<LinkedHashMap<String, usize, FxBuildHasher>>,
    ) -> Self {
        Group(fields, field_names)
    }
    /// Get a reference to the value belonging to a particular field name. Returns `None`
    /// if the field name doesn't exist.
    pub fn get(&self, k: &str) -> Option<&Value> {
        self.1.get(k).map(|&offset| &self.0[offset])
    }
    #[doc(hidden)]
    pub fn into_fields(self) -> Vec<Value> {
        self.0
    }
    #[doc(hidden)]
    pub fn field_names(&self) -> Arc<LinkedHashMap<String, usize, FxBuildHasher>> {
        self.1.clone()
    }
}
impl<I> Index<I> for Group
where
    I: SliceIndex<[Value]>,
{
    type Output = <I as SliceIndex<[Value]>>::Output;

    fn index(&self, index: I) -> &Self::Output {
        self.0.index(index)
    }
}
impl Debug for Group {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut printer = f.debug_struct("Group");
        for (name, field) in self.1.iter().map(|(name, _index)| name).zip(self.0.iter()) {
            printer.field(name, field);
        }
        printer.finish()
    }
}
impl From<LinkedHashMap<String, Value, FxBuildHasher>> for Group {
    fn from(hashmap: LinkedHashMap<String, Value, FxBuildHasher>) -> Self {
        let mut keys =
            LinkedHashMap::with_capacity_and_hasher(hashmap.len(), Default::default());
        Group(
            hashmap
                .into_iter()
                .map(|(key, value)| {
                    keys.insert(key, keys.len());
                    value
                })
                .collect(),
            Arc::new(keys),
        )
    }
}
impl From<Group> for LinkedHashMap<String, Value, FxBuildHasher> {
    fn from(group: Group) -> Self {
        group
            .1
            .iter()
            .map(|(name, _index)| name.clone())
            .zip(group.0)
            .collect()
    }
}
