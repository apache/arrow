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

//! Execution of a filter (predicate)

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use arrow::array::*;
use arrow::compute::array_ops::filter;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

use super::error::{ExecutionError, Result};
use super::expression::RuntimeExpr;
use super::relation::Relation;

pub struct FilterRelation {
    schema: Arc<Schema>,
    input: Rc<RefCell<Relation>>,
    expr: RuntimeExpr,
}

impl FilterRelation {
    pub fn new(
        input: Rc<RefCell<Relation>>,
        expr: RuntimeExpr,
        schema: Arc<Schema>,
    ) -> Self {
        Self {
            schema,
            input,
            expr,
        }
    }
}

impl Relation for FilterRelation {
    fn next(&mut self) -> Result<Option<RecordBatch>> {
        match self.input.borrow_mut().next()? {
            Some(batch) => {
                // evaluate the filter expression against the batch
                match self.expr.get_func()?(&batch)?
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                {
                    Some(filter_bools) => {
                        let filtered_columns: Result<Vec<ArrayRef>> = (0..batch
                            .num_columns())
                            .map(|i| {
                                match filter(batch.column(i).as_ref(), &filter_bools) {
                                    Ok(result) => Ok(result),
                                    Err(error) => Err(ExecutionError::from(error)),
                                }
                            })
                            .collect();

                        let filtered_batch: RecordBatch =
                            RecordBatch::try_new(self.schema.clone(), filtered_columns?)?;

                        Ok(Some(filtered_batch))
                    }
                    _ => Err(ExecutionError::ExecutionError(
                        "Filter expression did not evaluate to boolean".to_string(),
                    )),
                }
            }
            None => Ok(None),
        }
    }

    fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }
}
