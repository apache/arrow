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

use crate::execution::error::{ExecutionError, Result};
use crate::logicalplan::{Expr, LogicalPlan};
use crate::table::*;

use crate::logicalplan::Expr::Literal;
use crate::logicalplan::ScalarValue;

pub struct TableImpl {
    plan: Arc<LogicalPlan>,
}

impl TableImpl {
    pub fn new(plan: Arc<LogicalPlan>) -> Self {
        Self { plan }
    }
}

impl Table for TableImpl {
    fn select_columns(&self, columns: Vec<&str>) -> Result<Arc<Table>> {
        let schema = self.plan.schema();
        let mut projection: Vec<usize> = Vec::with_capacity(columns.len());
        let mut expr: Vec<Expr> = Vec::with_capacity(columns.len());

        for column in columns {
            match schema.column_with_name(column) {
                Some((i, _)) => {
                    projection.push(i);
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
                schema: schema.projection(&projection)?,
            },
        ))))
    }

    fn limit(&self, n: usize) -> Result<Arc<Table>> {
        Ok(Arc::new(TableImpl::new(Arc::new(LogicalPlan::Limit {
            expr: Literal(ScalarValue::UInt32(n as u32)),
            input: self.plan.clone(),
            schema: self.plan.schema().clone(),
        }))))
    }

    fn to_logical_plan(&self) -> Arc<LogicalPlan> {
        self.plan.clone()
    }
}
