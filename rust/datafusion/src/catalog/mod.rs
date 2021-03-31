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

//! This module contains interfaces and default implementations
//! of table namespacing concepts, including catalogs and schemas.

pub mod catalog;
pub mod information_schema;
pub mod schema;

use crate::error::DataFusionError;
use std::convert::TryFrom;

/// Represents a resolved path to a table of the form "catalog.schema.table"
#[derive(Clone, Copy)]
pub struct ResolvedTableReference<'a> {
    /// The catalog (aka database) containing the table
    pub catalog: &'a str,
    /// The schema containing the table
    pub schema: &'a str,
    /// The table name
    pub table: &'a str,
}

/// Represents a path to a table that may require further resolution
#[derive(Clone, Copy)]
pub enum TableReference<'a> {
    /// An unqualified table reference, e.g. "table"
    Bare {
        /// The table name
        table: &'a str,
    },
    /// A partially resolved table reference, e.g. "schema.table"
    Partial {
        /// The schema containing the table
        schema: &'a str,
        /// The table name
        table: &'a str,
    },
    /// A fully resolved table reference, e.g. "catalog.schema.table"
    Full {
        /// The catalog (aka database) containing the table
        catalog: &'a str,
        /// The schema containing the table
        schema: &'a str,
        /// The table name
        table: &'a str,
    },
}

impl<'a> TableReference<'a> {
    /// Retrieve the actual table name, regardless of qualification
    pub fn table(&self) -> &str {
        match self {
            Self::Full { table, .. }
            | Self::Partial { table, .. }
            | Self::Bare { table } => table,
        }
    }

    /// Given a default catalog and schema, ensure this table reference is fully resolved
    pub fn resolve(
        self,
        default_catalog: &'a str,
        default_schema: &'a str,
    ) -> ResolvedTableReference<'a> {
        match self {
            Self::Full {
                catalog,
                schema,
                table,
            } => ResolvedTableReference {
                catalog,
                schema,
                table,
            },
            Self::Partial { schema, table } => ResolvedTableReference {
                catalog: default_catalog,
                schema,
                table,
            },
            Self::Bare { table } => ResolvedTableReference {
                catalog: default_catalog,
                schema: default_schema,
                table,
            },
        }
    }
}

impl<'a> From<&'a str> for TableReference<'a> {
    fn from(s: &'a str) -> Self {
        Self::Bare { table: s }
    }
}

impl<'a> From<ResolvedTableReference<'a>> for TableReference<'a> {
    fn from(resolved: ResolvedTableReference<'a>) -> Self {
        Self::Full {
            catalog: resolved.catalog,
            schema: resolved.schema,
            table: resolved.table,
        }
    }
}

impl<'a> TryFrom<&'a sqlparser::ast::ObjectName> for TableReference<'a> {
    type Error = DataFusionError;

    fn try_from(value: &'a sqlparser::ast::ObjectName) -> Result<Self, Self::Error> {
        let idents = &value.0;

        match idents.len() {
            1 => Ok(Self::Bare {
                table: &idents[0].value,
            }),
            2 => Ok(Self::Partial {
                schema: &idents[0].value,
                table: &idents[1].value,
            }),
            3 => Ok(Self::Full {
                catalog: &idents[0].value,
                schema: &idents[1].value,
                table: &idents[2].value,
            }),
            _ => Err(DataFusionError::Plan(format!(
                "invalid table reference: {}",
                value
            ))),
        }
    }
}
