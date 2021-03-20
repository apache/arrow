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

//! Describes the interface and built-in implementations of schemas,
//! representing collections of named tables.

use crate::datasource::TableProvider;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Represents a schema, comprising a number of named tables.
pub trait SchemaProvider: Sync + Send {
    /// Retrieves the list of available table names in this schema.
    fn table_names(&self) -> Vec<String>;

    /// Retrieves a specific table from the schema by name, provided it exists.
    fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>>;
}

/// Simple in-memory implementation of a schema.
pub struct MemorySchemaProvider {
    tables: RwLock<HashMap<String, Arc<dyn TableProvider>>>,
}

impl MemorySchemaProvider {
    /// Instantiates a new MemorySchemaProvider with an empty collection of tables.
    pub fn new() -> Self {
        Self {
            tables: RwLock::new(HashMap::new()),
        }
    }

    /// Adds a new table to this schema.
    /// If a table of the same name existed before, it is replaced in the schema and returned.
    pub fn register_table(
        &self,
        name: impl Into<String>,
        table: Arc<dyn TableProvider>,
    ) -> Option<Arc<dyn TableProvider>> {
        let mut tables = self.tables.write().unwrap();
        tables.insert(name.into(), table)
    }

    /// Removes an existing table from this schema and returns it.
    /// If no table of that name exists, returns None.
    pub fn deregister_table(
        &self,
        name: impl AsRef<str>,
    ) -> Option<Arc<dyn TableProvider>> {
        let mut tables = self.tables.write().unwrap();
        tables.remove(name.as_ref())
    }
}

impl SchemaProvider for MemorySchemaProvider {
    fn table_names(&self) -> Vec<String> {
        let tables = self.tables.read().unwrap();
        tables.keys().cloned().collect()
    }

    fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        let tables = self.tables.read().unwrap();
        tables.get(name).cloned()
    }
}
