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

//! DFSchema is an extended schema struct that DataFusion uses to provide support for
//! fields with optional relation names.

use crate::error::{DataFusionError, Result};
use arrow::datatypes::{Field, Schema};
use std::collections::HashSet;

/// DFSchema wraps an Arrow schema and adds relation names
#[derive(Debug, Clone)]
pub struct DFSchema {
    /// Fields
    fields: Vec<DFField>,
}

impl DFSchema {
    /// Create a DFSChema
    pub fn new(fields: Vec<DFField>) -> Self {
        // TODO assert that field names are unique (taking qualifier into account)
        Self { fields }
    }

    /// Create a DFSChema from an Arrow schema
    pub fn from(schema: &Schema) -> Self {
        Self {
            fields: schema
                .fields()
                .iter()
                .map(|f| DFField {
                    field: f.clone(),
                    qualifier: None,
                })
                .collect(),
        }
    }

    /// Create a DFSChema from an Arrow schema
    pub fn from_qualified(qualifier: &str, schema: &Schema) -> Self {
        Self {
            fields: schema
                .fields()
                .iter()
                .map(|f| DFField {
                    field: f.clone(),
                    qualifier: Some(qualifier.to_owned()),
                })
                .collect(),
        }
    }

    /// Combine two schemas
    pub fn join(&self, schema: &DFSchema) -> Result<Self> {
        let mut fields = self.fields.clone();
        fields.extend_from_slice(schema.fields().as_slice());

        let mut qualified_names: HashSet<(&str, &str)> = HashSet::new();
        let mut unqualified_names = HashSet::new();
        for field in &fields {
            if let Some(qualifier) = field.qualifier() {
                if !qualified_names.insert((qualifier, field.name())) {
                    return Err(DataFusionError::Plan(format!(
                        "Joined schema would contain duplicate qualified field name '{}'",
                        field.qualified_name()
                    )));
                }
            } else {
                if !unqualified_names.insert(field.name()) {
                    return Err(DataFusionError::Plan(
                        format!("Joined schema would contain duplicate unqualified field name '{}'",
                                field.name())
                    ));
                }
            }
        }

        // check for mix of qualified and unqualified field with same unqualified name
        // note that we need to sort the contents of the HashSet first so that errors are
        // deterministic
        let mut qualified_names: Vec<&(&str, &str)> = qualified_names.iter().collect();
        qualified_names.sort_by(|a, b| {
            let a = format!("{}.{}", a.0, a.1);
            let b = format!("{}.{}", b.0, b.1);
            a.cmp(&b)
        });
        for (qualifier, name) in &qualified_names {
            if unqualified_names.contains(name) {
                return Err(DataFusionError::Plan(format!(
                    "Joined schema would contain qualified field name '{}.{}' \
                    and unqualified field name '{}' which would be ambiguous",
                    qualifier, name, name
                )));
            }
        }

        Ok(Self { fields })
    }

    /// Get a list of fields
    pub fn fields(&self) -> &Vec<DFField> {
        &self.fields
    }

    /// Find the field with the given name
    pub fn field_with_name(
        &self,
        relation_name: Option<&str>,
        name: &str,
    ) -> Result<DFField> {
        if let Some(relation_name) = relation_name {
            self.field_with_qualified_name(relation_name, name)
        } else {
            self.field_with_unqualified_name(name)
        }
    }

    /// Find the field with the given name
    pub fn field_with_unqualified_name(&self, name: &str) -> Result<DFField> {
        let matches: Vec<&DFField> = self
            .fields
            .iter()
            .filter(|field| field.name() == name)
            .collect();
        match matches.len() {
            0 => Err(DataFusionError::Plan(format!("No field named '{}'", name))),
            1 => Ok(matches[0].to_owned()),
            _ => Err(DataFusionError::Plan(format!(
                "Ambiguous reference to field named '{}'",
                name
            ))),
        }
    }

    /// Find the field with the given qualified name
    pub fn field_with_qualified_name(
        &self,
        relation_name: &str,
        name: &str,
    ) -> Result<DFField> {
        let matches: Vec<&DFField> = self
            .fields
            .iter()
            .filter(|field| {
                field.qualifier == Some(relation_name.to_string()) && field.name() == name
            })
            .collect();
        match matches.len() {
            0 => Err(DataFusionError::Plan(format!(
                "No field named '{}.{}'",
                relation_name, name
            ))),
            1 => Ok(matches[0].to_owned()),
            _ => Err(DataFusionError::Plan(format!(
                "Ambiguous reference to qualified field named '{}.{}'",
                relation_name, name
            ))),
        }
    }

    /// Return a string containing a comma-separated list of fields in the schema
    pub fn to_string(&self) -> String {
        self.fields
            .iter()
            .map(|field| field.qualified_name())
            .collect::<Vec<String>>()
            .join(", ")
    }
}

/// DFField wraps an Arrow field and adds an optional qualifier
#[derive(Debug, Clone)]
pub struct DFField {
    qualifier: Option<String>,
    field: Field,
}

impl DFField {
    /// Create an unqualified field from an existing Arrow field
    fn from(field: Field) -> Self {
        Self {
            qualifier: None,
            field,
        }
    }

    /// Create a qualified field from an existing Arrow field
    fn from_qualified(qualifier: &str, field: Field) -> Self {
        Self {
            qualifier: Some(qualifier.to_owned()),
            field,
        }
    }

    /// Get the unqualified field name
    fn name(&self) -> &str {
        self.field.name()
    }

    /// Get the qualified field name
    fn qualified_name(&self) -> String {
        if let Some(relation_name) = &self.qualifier {
            format!("{}.{}", relation_name, self.field.name())
        } else {
            self.field.name().to_owned()
        }
    }

    /// Get the optional qualifier
    fn qualifier(&self) -> &Option<String> {
        &self.qualifier
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;

    #[test]
    fn from_unqualified_field() {
        let field = Field::new("c0", DataType::Boolean, true);
        let field = DFField::from(field);
        assert_eq!("c0", field.name());
        assert_eq!("c0", field.qualified_name());
    }

    #[test]
    fn from_qualified_field() {
        let field = Field::new("c0", DataType::Boolean, true);
        let field = DFField::from_qualified("t1", field);
        assert_eq!("c0", field.name());
        assert_eq!("t1.c0", field.qualified_name());
    }

    #[test]
    fn from_unqualified_schema() {
        let schema = DFSchema::from(&test_schema_1());
        assert_eq!("c0, c1", schema.to_string());
    }

    #[test]
    fn from_qualified_schema() {
        let schema = DFSchema::from_qualified("t1", &test_schema_1());
        assert_eq!("t1.c0, t1.c1", schema.to_string());
    }

    #[test]
    fn join_qualified() -> Result<()> {
        let left = DFSchema::from_qualified("t1", &test_schema_1());
        let right = DFSchema::from_qualified("t2", &test_schema_1());
        let join = left.join(&right)?;
        assert_eq!("t1.c0, t1.c1, t2.c0, t2.c1", join.to_string());
        // test valid access
        assert!(join.field_with_qualified_name("t1", "c0").is_ok());
        assert!(join.field_with_qualified_name("t2", "c0").is_ok());
        // test invalid access
        assert!(join.field_with_unqualified_name("c0").is_err());
        assert!(join.field_with_unqualified_name("t1.c0").is_err());
        assert!(join.field_with_unqualified_name("t2.c0").is_err());
        Ok(())
    }

    #[test]
    fn join_qualified_duplicate() -> Result<()> {
        let left = DFSchema::from_qualified("t1", &test_schema_1());
        let right = DFSchema::from_qualified("t1", &test_schema_1());
        let join = left.join(&right);
        assert!(join.is_err());
        assert_eq!(
            "Error during planning: Joined schema would contain duplicate \
        qualified field name \'t1.c0\'",
            &format!("{}", join.err().unwrap())
        );
        Ok(())
    }

    #[test]
    fn join_unqualified_duplicate() -> Result<()> {
        let left = DFSchema::from(&test_schema_1());
        let right = DFSchema::from(&test_schema_1());
        let join = left.join(&right);
        assert!(join.is_err());
        assert_eq!(
            "Error during planning: Joined schema would contain duplicate \
        unqualified field name \'c0\'",
            &format!("{}", join.err().unwrap())
        );
        Ok(())
    }

    #[test]
    fn join_mixed() -> Result<()> {
        let left = DFSchema::from_qualified("t1", &test_schema_1());
        let right = DFSchema::from(&test_schema_2());
        let join = left.join(&right)?;
        assert_eq!("t1.c0, t1.c1, c100, c101", join.to_string());
        // test valid access
        assert!(join.field_with_qualified_name("t1", "c0").is_ok());
        assert!(join.field_with_unqualified_name("c0").is_ok());
        assert!(join.field_with_unqualified_name("c100").is_ok());
        assert!(join.field_with_name(None, "c100").is_ok());
        // test invalid access
        assert!(join.field_with_unqualified_name("t1.c0").is_err());
        assert!(join.field_with_unqualified_name("t1.c100").is_err());
        assert!(join.field_with_qualified_name("", "c100").is_err());
        Ok(())
    }

    #[test]
    fn join_mixed_duplicate() -> Result<()> {
        let left = DFSchema::from_qualified("t1", &test_schema_1());
        let right = DFSchema::from(&test_schema_1());
        let join = left.join(&right);
        assert!(join.is_err());
        assert_eq!(
            "Error during planning: Joined schema would contain qualified \
        field name \'t1.c0\' and unqualified field name \'c0\' which would be ambiguous",
            &format!("{}", join.err().unwrap())
        );
        Ok(())
    }

    fn test_schema_1() -> Schema {
        Schema::new(vec![
            Field::new("c0", DataType::Boolean, true),
            Field::new("c1", DataType::Boolean, true),
        ])
    }

    fn test_schema_2() -> Schema {
        Schema::new(vec![
            Field::new("c100", DataType::Boolean, true),
            Field::new("c101", DataType::Boolean, true),
        ])
    }
}
