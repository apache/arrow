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
    array::{StringBuilder, UInt64Builder},
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
const COLUMNS: &str = "columns";

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

impl InformationSchemaProvider {
    /// Construct the `information_schema.tables` virtual table
    fn make_tables(&self) -> Arc<dyn TableProvider> {
        // create a mem table with the names of tables
        let mut builder = InformationSchemaTablesBuilder::new();

        for catalog_name in self.catalog_list.catalog_names() {
            let catalog = self.catalog_list.catalog(&catalog_name).unwrap();

            for schema_name in catalog.schema_names() {
                if schema_name != INFORMATION_SCHEMA {
                    let schema = catalog.schema(&schema_name).unwrap();
                    for table_name in schema.table_names() {
                        builder.add_base_table(&catalog_name, &schema_name, table_name)
                    }
                }
            }

            // Add a final list for the information schema tables themselves
            builder.add_system_table(&catalog_name, INFORMATION_SCHEMA, TABLES);
            builder.add_system_table(&catalog_name, INFORMATION_SCHEMA, COLUMNS);
        }

        let mem_table: MemTable = builder.into();

        Arc::new(mem_table)
    }

    /// Construct the `information_schema.columns` virtual table
    fn make_columns(&self) -> Arc<dyn TableProvider> {
        let mut builder = InformationSchemaColumnsBuilder::new();

        for catalog_name in self.catalog_list.catalog_names() {
            let catalog = self.catalog_list.catalog(&catalog_name).unwrap();

            for schema_name in catalog.schema_names() {
                if schema_name != INFORMATION_SCHEMA {
                    let schema = catalog.schema(&schema_name).unwrap();
                    for table_name in schema.table_names() {
                        let table = schema.table(&table_name).unwrap();
                        for (i, field) in table.schema().fields().iter().enumerate() {
                            builder.add_column(
                                &catalog_name,
                                &schema_name,
                                &table_name,
                                field.name(),
                                i,
                                field.is_nullable(),
                                field.data_type(),
                            )
                        }
                    }
                }
            }
        }

        let mem_table: MemTable = builder.into();

        Arc::new(mem_table)
    }
}

impl SchemaProvider for InformationSchemaProvider {
    fn as_any(&self) -> &(dyn any::Any + 'static) {
        self
    }

    fn table_names(&self) -> Vec<String> {
        vec![TABLES.to_string(), COLUMNS.to_string()]
    }

    fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        if name.eq_ignore_ascii_case("tables") {
            Some(self.make_tables())
        } else if name.eq_ignore_ascii_case("columns") {
            Some(self.make_columns())
        } else {
            None
        }
    }
}

/// Builds the `information_schema.TABLE` table row by row
///
/// Columns are based on https://www.postgresql.org/docs/current/infoschema-columns.html
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
}

impl From<InformationSchemaTablesBuilder> for MemTable {
    fn from(value: InformationSchemaTablesBuilder) -> MemTable {
        let schema = Schema::new(vec![
            Field::new("table_catalog", DataType::Utf8, false),
            Field::new("table_schema", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("table_type", DataType::Utf8, false),
        ]);

        let InformationSchemaTablesBuilder {
            mut catalog_names,
            mut schema_names,
            mut table_names,
            mut table_types,
        } = value;

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

/// Builds the `information_schema.COLUMNS` table row by row
///
/// Columns are based on https://www.postgresql.org/docs/current/infoschema-columns.html
struct InformationSchemaColumnsBuilder {
    catalog_names: StringBuilder,
    schema_names: StringBuilder,
    table_names: StringBuilder,
    column_names: StringBuilder,
    ordinal_positions: UInt64Builder,
    column_defaults: StringBuilder,
    is_nullables: StringBuilder,
    data_types: StringBuilder,
    character_maximum_lengths: UInt64Builder,
    character_octet_lengths: UInt64Builder,
    numeric_precisions: UInt64Builder,
    numeric_precision_radixes: UInt64Builder,
    numeric_scales: UInt64Builder,
    datetime_precisions: UInt64Builder,
    interval_types: StringBuilder,
}

impl InformationSchemaColumnsBuilder {
    fn new() -> Self {
        // StringBuilder requires providing an initial capacity, so
        // pick 10 here arbitrarily as this is not performance
        // critical code and the number of tables is unavailable here.
        let default_capacity = 10;
        Self {
            catalog_names: StringBuilder::new(default_capacity),
            schema_names: StringBuilder::new(default_capacity),
            table_names: StringBuilder::new(default_capacity),
            column_names: StringBuilder::new(default_capacity),
            ordinal_positions: UInt64Builder::new(default_capacity),
            column_defaults: StringBuilder::new(default_capacity),
            is_nullables: StringBuilder::new(default_capacity),
            data_types: StringBuilder::new(default_capacity),
            character_maximum_lengths: UInt64Builder::new(default_capacity),
            character_octet_lengths: UInt64Builder::new(default_capacity),
            numeric_precisions: UInt64Builder::new(default_capacity),
            numeric_precision_radixes: UInt64Builder::new(default_capacity),
            numeric_scales: UInt64Builder::new(default_capacity),
            datetime_precisions: UInt64Builder::new(default_capacity),
            interval_types: StringBuilder::new(default_capacity),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn add_column(
        &mut self,
        catalog_name: impl AsRef<str>,
        schema_name: impl AsRef<str>,
        table_name: impl AsRef<str>,
        column_name: impl AsRef<str>,
        column_position: usize,
        is_nullable: bool,
        data_type: &DataType,
    ) {
        use DataType::*;

        // Note: append_value is actually infallable.
        self.catalog_names
            .append_value(catalog_name.as_ref())
            .unwrap();
        self.schema_names
            .append_value(schema_name.as_ref())
            .unwrap();
        self.table_names.append_value(table_name.as_ref()).unwrap();

        self.column_names
            .append_value(column_name.as_ref())
            .unwrap();

        self.ordinal_positions
            .append_value(column_position as u64)
            .unwrap();

        // DataFusion does not support column default values, so null
        self.column_defaults.append_null().unwrap();

        // "YES if the column is possibly nullable, NO if it is known not nullable. "
        let nullable_str = if is_nullable { "YES" } else { "NO" };
        self.is_nullables.append_value(nullable_str).unwrap();

        // "System supplied type" --> Use debug format of the datatype
        self.data_types
            .append_value(format!("{:?}", data_type))
            .unwrap();

        // "If data_type identifies a character or bit string type, the
        // declared maximum length; null for all other data types or
        // if no maximum length was declared."
        //
        // Arrow has no equivalent of VARCHAR(20), so we leave this as Null
        let max_chars = None;
        self.character_maximum_lengths
            .append_option(max_chars)
            .unwrap();

        // "Maximum length, in bytes, for binary data, character data,
        // or text and image data."
        let char_len: Option<u64> = match data_type {
            Utf8 | Binary => Some(i32::MAX as u64),
            LargeBinary | LargeUtf8 => Some(i64::MAX as u64),
            _ => None,
        };
        self.character_octet_lengths
            .append_option(char_len)
            .unwrap();

        // numeric_precision: "If data_type identifies a numeric type, this column
        // contains the (declared or implicit) precision of the type
        // for this column. The precision indicates the number of
        // significant digits. It can be expressed in decimal (base
        // 10) or binary (base 2) terms, as specified in the column
        // numeric_precision_radix. For all other data types, this
        // column is null."
        //
        // numeric_radix: If data_type identifies a numeric type, this
        // column indicates in which base the values in the columns
        // numeric_precision and numeric_scale are expressed. The
        // value is either 2 or 10. For all other data types, this
        // column is null.
        //
        // numeric_scale: If data_type identifies an exact numeric
        // type, this column contains the (declared or implicit) scale
        // of the type for this column. The scale indicates the number
        // of significant digits to the right of the decimal point. It
        // can be expressed in decimal (base 10) or binary (base 2)
        // terms, as specified in the column
        // numeric_precision_radix. For all other data types, this
        // column is null.
        let (numeric_precision, numeric_radix, numeric_scale) = match data_type {
            Int8 | UInt8 => (Some(8), Some(2), None),
            Int16 | UInt16 => (Some(16), Some(2), None),
            Int32 | UInt32 => (Some(32), Some(2), None),
            // From max value of 65504 as explained on
            // https://en.wikipedia.org/wiki/Half-precision_floating-point_format#Exponent_encoding
            Float16 => (Some(15), Some(2), None),
            // Numbers from postgres `real` type
            Float32 => (Some(24), Some(2), None),
            // Numbers from postgres `double` type
            Float64 => (Some(24), Some(2), None),
            Decimal(precision, scale) => {
                (Some(*precision as u64), Some(10), Some(*scale as u64))
            }
            _ => (None, None, None),
        };

        self.numeric_precisions
            .append_option(numeric_precision)
            .unwrap();
        self.numeric_precision_radixes
            .append_option(numeric_radix)
            .unwrap();
        self.numeric_scales.append_option(numeric_scale).unwrap();

        self.datetime_precisions.append_option(None).unwrap();
        self.interval_types.append_null().unwrap();
    }
}

impl From<InformationSchemaColumnsBuilder> for MemTable {
    fn from(value: InformationSchemaColumnsBuilder) -> MemTable {
        let schema = Schema::new(vec![
            Field::new("table_catalog", DataType::Utf8, false),
            Field::new("table_schema", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("column_name", DataType::Utf8, false),
            Field::new("ordinal_position", DataType::UInt64, false),
            Field::new("column_default", DataType::Utf8, false),
            Field::new("is_nullable", DataType::Utf8, false),
            Field::new("data_type", DataType::Utf8, false),
            Field::new("character_maximum_length", DataType::UInt64, false),
            Field::new("character_octet_length", DataType::UInt64, false),
            Field::new("numeric_precision", DataType::UInt64, false),
            Field::new("numeric_precision_radix", DataType::UInt64, false),
            Field::new("numeric_scale", DataType::UInt64, false),
            Field::new("datetime_precision", DataType::UInt64, false),
            Field::new("interval_type", DataType::Utf8, false),
        ]);

        let InformationSchemaColumnsBuilder {
            mut catalog_names,
            mut schema_names,
            mut table_names,
            mut column_names,
            mut ordinal_positions,
            mut column_defaults,
            mut is_nullables,
            mut data_types,
            mut character_maximum_lengths,
            mut character_octet_lengths,
            mut numeric_precisions,
            mut numeric_precision_radixes,
            mut numeric_scales,
            mut datetime_precisions,
            mut interval_types,
        } = value;

        let schema = Arc::new(schema);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(catalog_names.finish()),
                Arc::new(schema_names.finish()),
                Arc::new(table_names.finish()),
                Arc::new(column_names.finish()),
                Arc::new(ordinal_positions.finish()),
                Arc::new(column_defaults.finish()),
                Arc::new(is_nullables.finish()),
                Arc::new(data_types.finish()),
                Arc::new(character_maximum_lengths.finish()),
                Arc::new(character_octet_lengths.finish()),
                Arc::new(numeric_precisions.finish()),
                Arc::new(numeric_precision_radixes.finish()),
                Arc::new(numeric_scales.finish()),
                Arc::new(datetime_precisions.finish()),
                Arc::new(interval_types.finish()),
            ],
        )
        .unwrap();

        MemTable::try_new(schema, vec![vec![batch]]).unwrap()
    }
}
