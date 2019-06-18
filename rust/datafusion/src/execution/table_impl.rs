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

//! Implementation of Table API

use std::sync::Arc;

use crate::arrow::datatypes::{Field, Schema};
use crate::error::{ExecutionError, Result};
use crate::logicalplan::Expr::Literal;
use crate::logicalplan::ScalarValue;
use crate::logicalplan::{Expr, LogicalPlan};
use crate::table::*;

/// Implementation of Table API
pub struct TableImpl {
    plan: Arc<LogicalPlan>,
}

impl TableImpl {
    /// Create a new Table based on an existing logical plan
    pub fn new(plan: Arc<LogicalPlan>) -> Self {
        Self { plan }
    }
}

impl Table for TableImpl {
    /// Apply a projection based on a list of column names
    fn select_columns(&self, columns: Vec<&str>) -> Result<Arc<Table>> {
        let schema = self.plan.schema();
        let mut projection_index: Vec<usize> = Vec::with_capacity(columns.len());
        let mut expr: Vec<Expr> = Vec::with_capacity(columns.len());

        for column in columns {
            match schema.column_with_name(column) {
                Some((i, _)) => {
                    projection_index.push(i);
                    expr.push(Expr::Column(i));
                }
                _ => {
                    return Err(ExecutionError::InvalidColumn(format!(
                        "No column named '{}'",
                        column
                    )));
                }
            }
        }

        Ok(Arc::new(TableImpl::new(Arc::new(
            LogicalPlan::Projection {
                expr,
                input: self.plan.clone(),
                schema: projection(&schema, &projection_index)?,
            },
        ))))
    }

    /// Limit the number of rows
    fn limit(&self, n: usize) -> Result<Arc<Table>> {
        Ok(Arc::new(TableImpl::new(Arc::new(LogicalPlan::Limit {
            expr: Literal(ScalarValue::UInt32(n as u32)),
            input: self.plan.clone(),
            schema: self.plan.schema().clone(),
        }))))
    }

    /// Convert to logical plan
    fn to_logical_plan(&self) -> Arc<LogicalPlan> {
        self.plan.clone()
    }
}

/// Create a new schema by applying a projection to this schema's fields
fn projection(schema: &Schema, projection: &Vec<usize>) -> Result<Arc<Schema>> {
    let mut fields: Vec<Field> = Vec::with_capacity(projection.len());
    for i in projection {
        if *i < schema.fields().len() {
            fields.push(schema.field(*i).clone());
        } else {
            return Err(ExecutionError::InvalidColumn(format!(
                "Invalid column index {} in projection",
                i
            )));
        }
    }
    Ok(Arc::new(Schema::new(fields)))
}
