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

//! Execution of a limit (predicate)

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use arrow::array::*;
use arrow::compute::array_ops::limit;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

use super::error::{ExecutionError, Result};
use super::relation::Relation;

pub struct LimitRelation {
    input: Rc<RefCell<Relation>>,
    schema: Arc<Schema>,
    limit: usize,
    num_consumed_rows: usize,
}

impl LimitRelation {
    pub fn new(input: Rc<RefCell<Relation>>, limit: usize, schema: Arc<Schema>) -> Self {
        Self {
            input,
            schema,
            limit,
            num_consumed_rows: 0,
        }
    }
}

impl Relation for LimitRelation {
    fn next(&mut self) -> Result<Option<RecordBatch>> {
        match self.input.borrow_mut().next()? {
            Some(batch) => {
                let capacity = self.limit - self.num_consumed_rows;

                if capacity <= 0 {
                    return Ok(None);
                }

                if batch.num_rows() >= capacity {
                    let limited_columns: Result<Vec<ArrayRef>> = (0..batch.num_columns())
                        .map(|i| match limit(batch.column(i).as_ref(), capacity) {
                            Ok(result) => Ok(result),
                            Err(error) => Err(ExecutionError::from(error)),
                        })
                        .collect();

                    let limited_batch: RecordBatch =
                        RecordBatch::new(self.schema.clone(), limited_columns?);
                    self.num_consumed_rows += capacity;

                    Ok(Some(limited_batch))
                } else {
                    self.num_consumed_rows += batch.num_rows();
                    Ok(Some(batch))
                }
            }
            None => Ok(None),
        }
    }

    fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }
}
