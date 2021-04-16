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

use std::collections::BTreeMap;

use serde_derive::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::error::{ArrowError, Result};

use super::DataType;

/// Contains the meta-data for a single relative type.
///
/// The `Schema` object is an ordered collection of `Field` objects.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Field {
    name: String,
    data_type: DataType,
    nullable: bool,
    dict_id: i64,
    dict_is_ordered: bool,
    /// A map of key-value pairs containing additional custom meta data.
    #[serde(skip_serializing_if = "Option::is_none")]
    metadata: Option<BTreeMap<String, String>>,
}

impl Field {
    /// Creates a new field
    pub fn new(name: &str, data_type: DataType, nullable: bool) -> Self {
        Field {
            name: name.to_string(),
            data_type,
            nullable,
            dict_id: 0,
            dict_is_ordered: false,
            metadata: None,
        }
    }

    /// Creates a new field
    pub fn new_dict(
        name: &str,
        data_type: DataType,
        nullable: bool,
        dict_id: i64,
        dict_is_ordered: bool,
    ) -> Self {
        Field {
            name: name.to_string(),
            data_type,
            nullable,
            dict_id,
            dict_is_ordered,
            metadata: None,
        }
    }

    /// Sets the `Field`'s optional custom metadata.
    /// The metadata is set as `None` for empty map.
    #[inline]
    pub fn set_metadata(&mut self, metadata: Option<BTreeMap<String, String>>) {
        // To make serde happy, convert Some(empty_map) to None.
        self.metadata = None;
        if let Some(v) = metadata {
            if !v.is_empty() {
                self.metadata = Some(v);
            }
        }
    }

    /// Returns the immutable reference to the `Field`'s optional custom metadata.
    #[inline]
    pub const fn metadata(&self) -> &Option<BTreeMap<String, String>> {
        &self.metadata
    }

    /// Returns an immutable reference to the `Field`'s name.
    #[inline]
    pub const fn name(&self) -> &String {
        &self.name
    }

    /// Returns an immutable reference to the `Field`'s  data-type.
    #[inline]
    pub const fn data_type(&self) -> &DataType {
        &self.data_type
    }

    /// Indicates whether this `Field` supports null values.
    #[inline]
    pub const fn is_nullable(&self) -> bool {
        self.nullable
    }

    /// Returns the dictionary ID, if this is a dictionary type.
    #[inline]
    pub const fn dict_id(&self) -> Option<i64> {
        match self.data_type {
            DataType::Dictionary(_, _) => Some(self.dict_id),
            _ => None,
        }
    }

    /// Returns whether this `Field`'s dictionary is ordered, if this is a dictionary type.
    #[inline]
    pub const fn dict_is_ordered(&self) -> Option<bool> {
        match self.data_type {
            DataType::Dictionary(_, _) => Some(self.dict_is_ordered),
            _ => None,
        }
    }

    /// Parse a `Field` definition from a JSON representation.
    pub fn from(json: &Value) -> Result<Self> {
        match *json {
            Value::Object(ref map) => {
                let name = match map.get("name") {
                    Some(&Value::String(ref name)) => name.to_string(),
                    _ => {
                        return Err(ArrowError::ParseError(
                            "Field missing 'name' attribute".to_string(),
                        ));
                    }
                };
                let nullable = match map.get("nullable") {
                    Some(&Value::Bool(b)) => b,
                    _ => {
                        return Err(ArrowError::ParseError(
                            "Field missing 'nullable' attribute".to_string(),
                        ));
                    }
                };
                let data_type = match map.get("type") {
                    Some(t) => DataType::from(t)?,
                    _ => {
                        return Err(ArrowError::ParseError(
                            "Field missing 'type' attribute".to_string(),
                        ));
                    }
                };

                // Referenced example file: testing/data/arrow-ipc-stream/integration/1.0.0-littleendian/generated_custom_metadata.json.gz
                let metadata = match map.get("metadata") {
                    Some(&Value::Array(ref values)) => {
                        let mut res: BTreeMap<String, String> = BTreeMap::new();
                        for value in values {
                            match value.as_object() {
                                Some(map) => {
                                    if map.len() != 2 {
                                        return Err(ArrowError::ParseError(
                                            "Field 'metadata' must have exact two entries for each key-value map".to_string(),
                                        ));
                                    }
                                    if let (Some(k), Some(v)) =
                                        (map.get("key"), map.get("value"))
                                    {
                                        if let (Some(k_str), Some(v_str)) =
                                            (k.as_str(), v.as_str())
                                        {
                                            res.insert(
                                                k_str.to_string().clone(),
                                                v_str.to_string().clone(),
                                            );
                                        } else {
                                            return Err(ArrowError::ParseError("Field 'metadata' must have map value of string type".to_string()));
                                        }
                                    } else {
                                        return Err(ArrowError::ParseError("Field 'metadata' lacks map keys named \"key\" or \"value\"".to_string()));
                                    }
                                }
                                _ => {
                                    return Err(ArrowError::ParseError(
                                        "Field 'metadata' contains non-object key-value pair".to_string(),
                                    ));
                                }
                            }
                        }
                        Some(res)
                    }
                    // We also support map format, because Schema's metadata supports this.
                    // See https://github.com/apache/arrow/pull/5907
                    Some(&Value::Object(ref values)) => {
                        let mut res: BTreeMap<String, String> = BTreeMap::new();
                        for (k, v) in values {
                            if let Some(str_value) = v.as_str() {
                                res.insert(k.clone(), str_value.to_string().clone());
                            } else {
                                return Err(ArrowError::ParseError(
                                    format!("Field 'metadata' contains non-string value for key {}", k),
                                ));
                            }
                        }
                        Some(res)
                    }
                    Some(_) => {
                        return Err(ArrowError::ParseError(
                            "Field `metadata` is not json array".to_string(),
                        ));
                    }
                    _ => None,
                };

                // if data_type is a struct or list, get its children
                let data_type = match data_type {
                    DataType::List(_)
                    | DataType::LargeList(_)
                    | DataType::FixedSizeList(_, _) => match map.get("children") {
                        Some(Value::Array(values)) => {
                            if values.len() != 1 {
                                return Err(ArrowError::ParseError(
                                    "Field 'children' must have one element for a list data type".to_string(),
                                ));
                            }
                            match data_type {
                                    DataType::List(_) => {
                                        DataType::List(Box::new(Self::from(&values[0])?))
                                    }
                                    DataType::LargeList(_) => {
                                        DataType::LargeList(Box::new(Self::from(&values[0])?))
                                    }
                                    DataType::FixedSizeList(_, int) => DataType::FixedSizeList(
                                        Box::new(Self::from(&values[0])?),
                                        int,
                                    ),
                                    _ => unreachable!(
                                        "Data type should be a list, largelist or fixedsizelist"
                                    ),
                                }
                        }
                        Some(_) => {
                            return Err(ArrowError::ParseError(
                                "Field 'children' must be an array".to_string(),
                            ))
                        }
                        None => {
                            return Err(ArrowError::ParseError(
                                "Field missing 'children' attribute".to_string(),
                            ));
                        }
                    },
                    DataType::Struct(mut fields) => match map.get("children") {
                        Some(Value::Array(values)) => {
                            let struct_fields: Result<Vec<Field>> =
                                values.iter().map(|v| Field::from(v)).collect();
                            fields.append(&mut struct_fields?);
                            DataType::Struct(fields)
                        }
                        Some(_) => {
                            return Err(ArrowError::ParseError(
                                "Field 'children' must be an array".to_string(),
                            ))
                        }
                        None => {
                            return Err(ArrowError::ParseError(
                                "Field missing 'children' attribute".to_string(),
                            ));
                        }
                    },
                    _ => data_type,
                };

                let mut dict_id = 0;
                let mut dict_is_ordered = false;

                let data_type = match map.get("dictionary") {
                    Some(dictionary) => {
                        let index_type = match dictionary.get("indexType") {
                            Some(t) => DataType::from(t)?,
                            _ => {
                                return Err(ArrowError::ParseError(
                                    "Field missing 'indexType' attribute".to_string(),
                                ));
                            }
                        };
                        dict_id = match dictionary.get("id") {
                            Some(Value::Number(n)) => n.as_i64().unwrap(),
                            _ => {
                                return Err(ArrowError::ParseError(
                                    "Field missing 'id' attribute".to_string(),
                                ));
                            }
                        };
                        dict_is_ordered = match dictionary.get("isOrdered") {
                            Some(&Value::Bool(n)) => n,
                            _ => {
                                return Err(ArrowError::ParseError(
                                    "Field missing 'isOrdered' attribute".to_string(),
                                ));
                            }
                        };
                        DataType::Dictionary(Box::new(index_type), Box::new(data_type))
                    }
                    _ => data_type,
                };
                Ok(Field {
                    name,
                    data_type,
                    nullable,
                    dict_id,
                    dict_is_ordered,
                    metadata,
                })
            }
            _ => Err(ArrowError::ParseError(
                "Invalid json value type for field".to_string(),
            )),
        }
    }

    /// Generate a JSON representation of the `Field`.
    pub fn to_json(&self) -> Value {
        let children: Vec<Value> = match self.data_type() {
            DataType::Struct(fields) => fields.iter().map(|f| f.to_json()).collect(),
            DataType::List(field) => vec![field.to_json()],
            DataType::LargeList(field) => vec![field.to_json()],
            DataType::FixedSizeList(field, _) => vec![field.to_json()],
            _ => vec![],
        };
        match self.data_type() {
            DataType::Dictionary(ref index_type, ref value_type) => json!({
                "name": self.name,
                "nullable": self.nullable,
                "type": value_type.to_json(),
                "children": children,
                "dictionary": {
                    "id": self.dict_id,
                    "indexType": index_type.to_json(),
                    "isOrdered": self.dict_is_ordered
                }
            }),
            _ => json!({
                "name": self.name,
                "nullable": self.nullable,
                "type": self.data_type.to_json(),
                "children": children
            }),
        }
    }

    /// Merge field into self if it is compatible. Struct will be merged recursively.
    /// NOTE: `self` may be updated to unexpected state in case of merge failure.
    ///
    /// Example:
    ///
    /// ```
    /// use arrow::datatypes::*;
    ///
    /// let mut field = Field::new("c1", DataType::Int64, false);
    /// assert!(field.try_merge(&Field::new("c1", DataType::Int64, true)).is_ok());
    /// assert!(field.is_nullable());
    /// ```
    pub fn try_merge(&mut self, from: &Field) -> Result<()> {
        // merge metadata
        match (self.metadata(), from.metadata()) {
            (Some(self_metadata), Some(from_metadata)) => {
                let mut merged = self_metadata.clone();
                for (key, from_value) in from_metadata {
                    if let Some(self_value) = self_metadata.get(key) {
                        if self_value != from_value {
                            return Err(ArrowError::SchemaError(format!(
                                "Fail to merge field due to conflicting metadata data value for key {}", key),
                            ));
                        }
                    } else {
                        merged.insert(key.clone(), from_value.clone());
                    }
                }
                self.set_metadata(Some(merged));
            }
            (None, Some(from_metadata)) => {
                self.set_metadata(Some(from_metadata.clone()));
            }
            _ => {}
        }
        if from.dict_id != self.dict_id {
            return Err(ArrowError::SchemaError(
                "Fail to merge schema Field due to conflicting dict_id".to_string(),
            ));
        }
        if from.dict_is_ordered != self.dict_is_ordered {
            return Err(ArrowError::SchemaError(
                "Fail to merge schema Field due to conflicting dict_is_ordered"
                    .to_string(),
            ));
        }
        match &mut self.data_type {
            DataType::Struct(nested_fields) => match &from.data_type {
                DataType::Struct(from_nested_fields) => {
                    for from_field in from_nested_fields {
                        let mut is_new_field = true;
                        for self_field in nested_fields.iter_mut() {
                            if self_field.name != from_field.name {
                                continue;
                            }
                            is_new_field = false;
                            self_field.try_merge(&from_field)?;
                        }
                        if is_new_field {
                            nested_fields.push(from_field.clone());
                        }
                    }
                }
                _ => {
                    return Err(ArrowError::SchemaError(
                        "Fail to merge schema Field due to conflicting datatype"
                            .to_string(),
                    ));
                }
            },
            DataType::Union(nested_fields) => match &from.data_type {
                DataType::Union(from_nested_fields) => {
                    for from_field in from_nested_fields {
                        let mut is_new_field = true;
                        for self_field in nested_fields.iter_mut() {
                            if from_field == self_field {
                                is_new_field = false;
                                break;
                            }
                        }
                        if is_new_field {
                            nested_fields.push(from_field.clone());
                        }
                    }
                }
                _ => {
                    return Err(ArrowError::SchemaError(
                        "Fail to merge schema Field due to conflicting datatype"
                            .to_string(),
                    ));
                }
            },
            DataType::Null
            | DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Timestamp(_, _)
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Duration(_)
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::Interval(_)
            | DataType::LargeList(_)
            | DataType::List(_)
            | DataType::Dictionary(_, _)
            | DataType::FixedSizeList(_, _)
            | DataType::FixedSizeBinary(_)
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Decimal(_, _) => {
                if self.data_type != from.data_type {
                    return Err(ArrowError::SchemaError(
                        "Fail to merge schema Field due to conflicting datatype"
                            .to_string(),
                    ));
                }
            }
        }
        if from.nullable {
            self.nullable = from.nullable;
        }

        Ok(())
    }

    /// Check to see if `self` is a superset of `other` field. Superset is defined as:
    ///
    /// * if nullability doesn't match, self needs to be nullable
    /// * self.metadata is a superset of other.metadata
    /// * all other fields are equal
    pub fn contains(&self, other: &Field) -> bool {
        if self.name != other.name
            || self.data_type != other.data_type
            || self.dict_id != other.dict_id
            || self.dict_is_ordered != other.dict_is_ordered
        {
            return false;
        }

        if self.nullable != other.nullable && !self.nullable {
            return false;
        }

        // make sure self.metadata is a superset of other.metadata
        match (&self.metadata, &other.metadata) {
            (None, Some(_)) => {
                return false;
            }
            (Some(self_meta), Some(other_meta)) => {
                for (k, v) in other_meta.iter() {
                    match self_meta.get(k) {
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
            }
            _ => {}
        }

        true
    }
}

// TODO: improve display with crate https://crates.io/crates/derive_more ?
impl std::fmt::Display for Field {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}
