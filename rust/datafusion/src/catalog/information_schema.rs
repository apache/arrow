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

//! Implements the SQL [Information Schema] for DataFusion.
//!
//! Information Schema](https://en.wikipedia.org/wiki/Information_schema)

use std::{any, sync::Arc};

use arrow::{
    array::StringBuilder,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};

use crate::datasource::{MemTable, TableProvider};

use super::{
    catalog::{CatalogList, CatalogProvider},
    schema::SchemaProvider,
};

const INFORMATION_SCHEMA: &str = "information_schema";
const TABLES: &str = "tables";

/// Wraps another [`CatalogProvider`] and adds a "information_schema"
/// schema that can introspect on tables in the catalog_list
pub(crate) struct CatalogWithInformationSchema {
    catalog_list: Arc<dyn CatalogList>,
    /// wrapped provider
    inner: Arc<dyn CatalogProvider>,
}

impl CatalogWithInformationSchema {
    pub(crate) fn new(
        catalog_list: Arc<dyn CatalogList>,
        inner: Arc<dyn CatalogProvider>,
    ) -> Self {
        Self {
            catalog_list,
            inner,
        }
    }
}

impl CatalogProvider for CatalogWithInformationSchema {
    fn as_any(&self) -> &dyn any::Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.inner
            .schema_names()
            .into_iter()
            .chain(std::iter::once(INFORMATION_SCHEMA.to_string()))
            .collect::<Vec<String>>()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        if name.eq_ignore_ascii_case(INFORMATION_SCHEMA) {
            Some(Arc::new(InformationSchemaProvider {
                catalog_list: self.catalog_list.clone(),
            }))
        } else {
            self.inner.schema(name)
        }
    }
}

/// Implements the `information_schema` virtual schema and tables
///
/// The underlying tables in the `information_schema` are created on
/// demand. This means that if more tables are added to the underlying
/// providers, they will appear the next time the `information_schema`
/// table is queried.
struct InformationSchemaProvider {
    catalog_list: Arc<dyn CatalogList>,
}

impl SchemaProvider for InformationSchemaProvider {
    fn as_any(&self) -> &(dyn any::Any + 'static) {
        self
    }

    fn table_names(&self) -> Vec<String> {
        vec![TABLES.to_string()]
    }

    fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        if name.eq_ignore_ascii_case("tables") {
            // create a mem table with the names of tables
            let mut builder = InformationSchemaTablesBuilder::new();

            for catalog_name in self.catalog_list.catalog_names() {
                let catalog = self.catalog_list.catalog(&catalog_name).unwrap();

                for schema_name in catalog.schema_names() {
                    if schema_name != INFORMATION_SCHEMA {
                        let schema = catalog.schema(&schema_name).unwrap();
                        for table_name in schema.table_names() {
                            builder.add_base_table(
                                &catalog_name,
                                &schema_name,
                                table_name,
                            )
                        }
                    }
                }

                // Add a final list for the information schema tables themselves
                builder.add_system_table(&catalog_name, INFORMATION_SCHEMA, TABLES);
            }

            let mem_table = builder.build();

            Some(Arc::new(mem_table))
        } else {
            None
        }
    }
}

/// Builds the `information_schema.TABLE` table row by row

struct InformationSchemaTablesBuilder {
    catalog_names: StringBuilder,
    schema_names: StringBuilder,
    table_names: StringBuilder,
    table_types: StringBuilder,
}

impl InformationSchemaTablesBuilder {
    fn new() -> Self {
        // StringBuilder requires providing an initial capacity, so
        // pick 10 here arbitrarily as this is not performance
        // critical code and the number of tables is unavailable here.
        let default_capacity = 10;
        Self {
            catalog_names: StringBuilder::new(default_capacity),
            schema_names: StringBuilder::new(default_capacity),
            table_names: StringBuilder::new(default_capacity),
            table_types: StringBuilder::new(default_capacity),
        }
    }

    fn add_base_table(
        &mut self,
        catalog_name: impl AsRef<str>,
        schema_name: impl AsRef<str>,
        table_name: impl AsRef<str>,
    ) {
        // Note: append_value is actually infallable.
        self.catalog_names
            .append_value(catalog_name.as_ref())
            .unwrap();
        self.schema_names
            .append_value(schema_name.as_ref())
            .unwrap();
        self.table_names.append_value(table_name.as_ref()).unwrap();
        self.table_types.append_value("BASE TABLE").unwrap();
    }

    fn add_system_table(
        &mut self,
        catalog_name: impl AsRef<str>,
        schema_name: impl AsRef<str>,
        table_name: impl AsRef<str>,
    ) {
        // Note: append_value is actually infallable.
        self.catalog_names
            .append_value(catalog_name.as_ref())
            .unwrap();
        self.schema_names
            .append_value(schema_name.as_ref())
            .unwrap();
        self.table_names.append_value(table_name.as_ref()).unwrap();
        self.table_types.append_value("VIEW").unwrap();
    }

    fn build(self) -> MemTable {
        let schema = Schema::new(vec![
            Field::new("table_catalog", DataType::Utf8, false),
            Field::new("table_schema", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("table_type", DataType::Utf8, false),
        ]);

        let Self {
            mut catalog_names,
            mut schema_names,
            mut table_names,
            mut table_types,
        } = self;

        let schema = Arc::new(schema);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(catalog_names.finish()),
                Arc::new(schema_names.finish()),
                Arc::new(table_names.finish()),
                Arc::new(table_types.finish()),
            ],
        )
        .unwrap();

        MemTable::try_new(schema, vec![vec![batch]]).unwrap()
    }
}
