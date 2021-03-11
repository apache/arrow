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
use std::default::Default;
use std::fmt;

use serde_derive::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::error::{ArrowError, Result};

use super::Field;

/// Describes the meta-data of an ordered sequence of relative types.
///
/// Note that this information is only part of the meta-data and not part of the physical
/// memory layout.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Schema {
    pub(crate) fields: Vec<Field>,
    /// A map of key-value pairs containing additional meta data.
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub(crate) metadata: HashMap<String, String>,
}

impl Schema {
    /// Creates an empty `Schema`
    pub fn empty() -> Self {
        Self {
            fields: vec![],
            metadata: HashMap::new(),
        }
    }

    /// Creates a new `Schema` from a sequence of `Field` values.
    ///
    /// # Example
    ///
    /// ```
    /// # extern crate arrow;
    /// # use arrow::datatypes::{Field, DataType, Schema};
    /// let field_a = Field::new("a", DataType::Int64, false);
    /// let field_b = Field::new("b", DataType::Boolean, false);
    ///
    /// let schema = Schema::new(vec![field_a, field_b]);
    /// ```
    pub fn new(fields: Vec<Field>) -> Self {
        Self::new_with_metadata(fields, HashMap::new())
    }

    /// Creates a new `Schema` from a sequence of `Field` values
    /// and adds additional metadata in form of key value pairs.
    ///
    /// # Example
    ///
    /// ```
    /// # extern crate arrow;
    /// # use arrow::datatypes::{Field, DataType, Schema};
    /// # use std::collections::HashMap;
    /// let field_a = Field::new("a", DataType::Int64, false);
    /// let field_b = Field::new("b", DataType::Boolean, false);
    ///
    /// let mut metadata: HashMap<String, String> = HashMap::new();
    /// metadata.insert("row_count".to_string(), "100".to_string());
    ///
    /// let schema = Schema::new_with_metadata(vec![field_a, field_b], metadata);
    /// ```
    #[inline]
    pub const fn new_with_metadata(
        fields: Vec<Field>,
        metadata: HashMap<String, String>,
    ) -> Self {
        Self { fields, metadata }
    }

    /// Merge schema into self if it is compatible. Struct fields will be merged recursively.
    ///
    /// Example:
    ///
    /// ```
    /// use arrow::datatypes::*;
    ///
    /// let merged = Schema::try_merge(vec![
    ///     Schema::new(vec![
    ///         Field::new("c1", DataType::Int64, false),
    ///         Field::new("c2", DataType::Utf8, false),
    ///     ]),
    ///     Schema::new(vec![
    ///         Field::new("c1", DataType::Int64, true),
    ///         Field::new("c2", DataType::Utf8, false),
    ///         Field::new("c3", DataType::Utf8, false),
    ///     ]),
    /// ]).unwrap();
    ///
    /// assert_eq!(
    ///     merged,
    ///     Schema::new(vec![
    ///         Field::new("c1", DataType::Int64, true),
    ///         Field::new("c2", DataType::Utf8, false),
    ///         Field::new("c3", DataType::Utf8, false),
    ///     ]),
    /// );
    /// ```
    pub fn try_merge(schemas: impl IntoIterator<Item = Self>) -> Result<Self> {
        schemas
            .into_iter()
            .try_fold(Self::empty(), |mut merged, schema| {
                let Schema { metadata, fields } = schema;
                for (key, value) in metadata.into_iter() {
                    // merge metadata
                    if let Some(old_val) = merged.metadata.get(&key) {
                        if old_val != &value {
                            return Err(ArrowError::SchemaError(
                                "Fail to merge schema due to conflicting metadata."
                                    .to_string(),
                            ));
                        }
                    }
                    merged.metadata.insert(key, value);
                }
                // merge fields
                for field in fields.into_iter() {
                    let mut new_field = true;
                    for merged_field in &mut merged.fields {
                        if field.name() != merged_field.name() {
                            continue;
                        }
                        new_field = false;
                        merged_field.try_merge(&field)?
                    }
                    // found a new field, add to field list
                    if new_field {
                        merged.fields.push(field);
                    }
                }
                Ok(merged)
            })
    }

    /// Returns an immutable reference of the vector of `Field` instances.
    #[inline]
    pub const fn fields(&self) -> &Vec<Field> {
        &self.fields
    }

    /// Returns an immutable reference of a specific `Field` instance selected using an
    /// offset within the internal `fields` vector.
    pub fn field(&self, i: usize) -> &Field {
        &self.fields[i]
    }

    /// Returns an immutable reference of a specific `Field` instance selected by name.
    pub fn field_with_name(&self, name: &str) -> Result<&Field> {
        Ok(&self.fields[self.index_of(name)?])
    }

    /// Returns a vector of immutable references to all `Field` instances selected by
    /// the dictionary ID they use.
    pub fn fields_with_dict_id(&self, dict_id: i64) -> Vec<&Field> {
        self.fields
            .iter()
            .filter(|f| f.dict_id() == Some(dict_id))
            .collect()
    }

    /// Find the index of the column with the given name.
    pub fn index_of(&self, name: &str) -> Result<usize> {
        for i in 0..self.fields.len() {
            if self.fields[i].name() == name {
                return Ok(i);
            }
        }
        let valid_fields: Vec<String> =
            self.fields.iter().map(|f| f.name().clone()).collect();
        Err(ArrowError::InvalidArgumentError(format!(
            "Unable to get field named \"{}\". Valid fields: {:?}",
            name, valid_fields
        )))
    }

    /// Returns an immutable reference to the Map of custom metadata key-value pairs.
    #[inline]
    pub const fn metadata(&self) -> &HashMap<String, String> {
        &self.metadata
    }

    /// Look up a column by name and return a immutable reference to the column along with
    /// its index.
    pub fn column_with_name(&self, name: &str) -> Option<(usize, &Field)> {
        self.fields
            .iter()
            .enumerate()
            .find(|&(_, c)| c.name() == name)
    }

    /// Generate a JSON representation of the `Schema`.
    pub fn to_json(&self) -> Value {
        json!({
            "fields": self.fields.iter().map(|field| field.to_json()).collect::<Vec<Value>>(),
            "metadata": serde_json::to_value(&self.metadata).unwrap()
        })
    }

    /// Parse a `Schema` definition from a JSON representation.
    pub fn from(json: &Value) -> Result<Self> {
        match *json {
            Value::Object(ref schema) => {
                let fields = if let Some(Value::Array(fields)) = schema.get("fields") {
                    fields
                        .iter()
                        .map(|f| Field::from(f))
                        .collect::<Result<_>>()?
                } else {
                    return Err(ArrowError::ParseError(
                        "Schema fields should be an array".to_string(),
                    ));
                };

                let metadata = if let Some(value) = schema.get("metadata") {
                    Self::from_metadata(value)?
                } else {
                    HashMap::default()
                };

                Ok(Self { fields, metadata })
            }
            _ => Err(ArrowError::ParseError(
                "Invalid json value type for schema".to_string(),
            )),
        }
    }

    /// Parse a `metadata` definition from a JSON representation.
    /// The JSON can either be an Object or an Array of Objects.
    fn from_metadata(json: &Value) -> Result<HashMap<String, String>> {
        match json {
            Value::Array(_) => {
                let mut hashmap = HashMap::new();
                let values: Vec<MetadataKeyValue> = serde_json::from_value(json.clone())
                    .map_err(|_| {
                        ArrowError::JsonError(
                            "Unable to parse object into key-value pair".to_string(),
                        )
                    })?;
                for meta in values {
                    hashmap.insert(meta.key.clone(), meta.value);
                }
                Ok(hashmap)
            }
            Value::Object(md) => md
                .iter()
                .map(|(k, v)| {
                    if let Value::String(v) = v {
                        Ok((k.to_string(), v.to_string()))
                    } else {
                        Err(ArrowError::ParseError(
                            "metadata `value` field must be a string".to_string(),
                        ))
                    }
                })
                .collect::<Result<_>>(),
            _ => Err(ArrowError::ParseError(
                "`metadata` field must be an object".to_string(),
            )),
        }
    }

    /// Check to see if `self` is a superset of `other` schema. Here are the comparision rules:
    ///
    /// * `self` and `other` should contain the same number of fields
    /// * for every field `f` in `other`, the field in `self` with corresponding index should be a
    /// superset of `f`.
    /// * self.metadata is a superset of other.metadata
    ///
    /// In other words, any record conforms to `other` should also conform to `self`.
    pub fn contains(&self, other: &Schema) -> bool {
        if self.fields.len() != other.fields.len() {
            return false;
        }

        for (i, field) in other.fields.iter().enumerate() {
            if !self.fields[i].contains(field) {
                return false;
            }
        }

        // make sure self.metadata is a superset of other.metadata
        for (k, v) in &other.metadata {
            match self.metadata.get(k) {
                Some(s) => {
                    if s != v {
                        return false;
                    }
                }
                None => {
                    return false;
                }
            }
        }

        true
    }
}

impl fmt::Display for Schema {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(
            &self
                .fields
                .iter()
                .map(|c| c.to_string())
                .collect::<Vec<String>>()
                .join(", "),
        )
    }
}

#[derive(Deserialize)]
struct MetadataKeyValue {
    key: String,
    value: String,
}
