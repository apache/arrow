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
    collections::HashMap,
    fmt::{self, Debug},
    marker::PhantomData,
    ops::Index,
    rc::Rc,
    slice::SliceIndex,
    str, vec,
};

use crate::{
    basic::Repetition,
    column::reader::ColumnReader,
    errors::ParquetError,
    record::{
        reader::GroupReader,
        schemas::{GroupSchema, ValueSchema},
        types::Value,
        Deserialize,
    },
    schema::types::{ColumnPath, Type},
};

#[derive(Clone, PartialEq)]
pub struct Group(pub(crate) Vec<Value>, pub(crate) Rc<HashMap<String, usize>>);
pub type Row = Group;

impl Deserialize for Group {
    type Reader = GroupReader;
    type Schema = GroupSchema;

    fn parse(
        schema: &Type,
        repetition: Option<Repetition>,
    ) -> Result<(String, Self::Schema), ParquetError> {
        if schema.is_group() && repetition == Some(Repetition::REQUIRED) {
            let mut map = HashMap::with_capacity(schema.get_fields().len());
            let fields = schema
                .get_fields()
                .iter()
                .enumerate()
                .map(|(i, field)| {
                    let (name, schema) = <Value as Deserialize>::parse(
                        &**field,
                        Some(field.get_basic_info().repetition()),
                    )?;
                    let x = map.insert(name, i);
                    assert!(x.is_none());
                    Ok(schema)
                })
                .collect::<Result<Vec<ValueSchema>, ParquetError>>()?;
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
        let mut names_ = vec![None; schema.0.len()];
        for (name, &index) in schema.1.iter() {
            names_[index].replace(name.to_owned());
        }
        let readers = schema
            .0
            .iter()
            .enumerate()
            .map(|(i, field)| {
                path.push(names_[i].take().unwrap());
                let ret = Value::reader(field, path, def_level, rep_level, paths, batch_size);
                path.pop().unwrap();
                ret
            })
            .collect();
        GroupReader {
            readers,
            fields: Rc::new(schema.1.clone()),
        }
    }
}

impl Group {
    pub fn get(&self, k: &str) -> Option<&Value> {
        self.1.get(k).map(|&offset| &self.0[offset])
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
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        let mut printer = f.debug_struct("Group");
        let fields = self.0.iter();
        let mut names = vec![None; self.1.len()];
        for (name, &index) in self.1.iter() {
            names[index].replace(name);
        }
        let names = names.into_iter().map(Option::unwrap);
        for (name, field) in names.zip(fields) {
            printer.field(name, field);
        }
        printer.finish()
    }
}
impl From<HashMap<String, Value>> for Group {
    fn from(hashmap: HashMap<String, Value>) -> Self {
        let mut keys = HashMap::with_capacity(hashmap.len());
        Group(
            hashmap
                .into_iter()
                .map(|(key, value)| {
                    keys.insert(key, keys.len());
                    value
                })
                .collect(),
            Rc::new(keys),
        )
    }
}
impl Into<HashMap<String, Value>> for Group {
    fn into(self) -> HashMap<String, Value> {
        let fields = self.0.into_iter();
        let mut names = vec![None; self.1.len()];
        for (name, &index) in self.1.iter() {
            names[index].replace(name.to_owned());
        }
        let names = names.into_iter().map(Option::unwrap);
        names.zip(fields).collect()
    }
}
