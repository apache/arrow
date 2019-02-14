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
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;

use super::error::{ExecutionError, Result};
use super::expression::RuntimeExpr;
use super::relation::Relation;

pub struct LimitRelation {
    input: Rc<RefCell<Relation>>,
    expr: RuntimeExpr,
    schema: Arc<Schema>,
    num_consumed_rows: i64,
}

impl LimitRelation {
    pub fn new(
        input: Rc<RefCell<Relation>>,
        expr: RuntimeExpr,
        schema: Arc<Schema>,
    ) -> Self {
        Self {
            input,
            expr,
            schema,
            num_consumed_rows: 0,
        }
    }
}

impl Relation for LimitRelation {
    fn next(&mut self) -> Result<Option<RecordBatch>> {
        match self.input.borrow_mut().next()? {
            Some(batch) => {
                match self.expr.get_func()(&batch)?
                    .as_any()
                    .downcast_ref::<Int64Array>() {
                    Some(limits) => {
                        if limits.len() == 0 {
                            return Ok(None)
                        }
                        let limit_value: i64 = limits.value(0);
                        let capacity = limit_value - self.num_consumed_rows;

                        if capacity <= 0 {
                            return Ok(None)
                        }

                        let num_batch_rows = batch.num_rows() as i64;
                        if num_batch_rows >= capacity {
                            let num_rows_to_read = limit_value - self.num_consumed_rows;
                            let limited_columns: Result<Vec<ArrayRef>> = (0..batch
                                .num_columns())
                                .map(|i| filter(batch.column(i).as_ref(), num_rows_to_read))
                                .collect();

                            let filtered_batch: RecordBatch = RecordBatch::new(
                                Arc::new(Schema::empty()),
                                limited_columns?,
                            );
                            self.num_consumed_rows += capacity;

                            Ok(Some(filtered_batch))
                        } else {
                            self.num_consumed_rows += num_batch_rows;
                            Ok(Some(batch))
                        }
                    }
                    None => Err(ExecutionError::ExecutionError(
                        "Limit expression could not be evaluated".to_string(),
                    )),
                }
            }
            None => Ok(None)
        }
    }

    fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }
}

//TODO: move into Arrow array_ops
fn filter(a: &Array, num_rows_to_read: i64) -> Result<ArrayRef> {
    //TODO use macros
    match a.data_type() {
        DataType::UInt8 => {
            let b = a.as_any().downcast_ref::<UInt8Array>().unwrap();
            let mut builder = UInt8Array::builder(num_rows_to_read as usize);
            for i in 0..num_rows_to_read {
                builder.append_value(b.value(i as usize))?;
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::UInt16 => {
            let b = a.as_any().downcast_ref::<UInt16Array>().unwrap();
            let mut builder = UInt16Array::builder(num_rows_to_read as usize);
            for i in 0..num_rows_to_read {
                builder.append_value(b.value(i as usize))?;
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::UInt32 => {
            let b = a.as_any().downcast_ref::<UInt32Array>().unwrap();
            let mut builder = UInt32Array::builder(num_rows_to_read as usize);
            for i in 0..num_rows_to_read {
                builder.append_value(b.value(i as usize))?;
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::UInt64 => {
            let b = a.as_any().downcast_ref::<UInt64Array>().unwrap();
            let mut builder = UInt64Array::builder(num_rows_to_read as usize);
            for i in 0..num_rows_to_read {
                builder.append_value(b.value(i as usize))?;
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int8 => {
            let b = a.as_any().downcast_ref::<Int8Array>().unwrap();
            let mut builder = Int8Array::builder(num_rows_to_read as usize);
            for i in 0..num_rows_to_read {
                builder.append_value(b.value(i as usize))?;
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int16 => {
            let b = a.as_any().downcast_ref::<Int16Array>().unwrap();
            let mut builder = Int16Array::builder(num_rows_to_read as usize);
            for i in 0..num_rows_to_read {
                builder.append_value(b.value(i as usize))?;
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int32 => {
            let b = a.as_any().downcast_ref::<Int32Array>().unwrap();
            let mut builder = Int32Array::builder(num_rows_to_read as usize);
            for i in 0..num_rows_to_read {
                builder.append_value(b.value(i as usize))?;
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int64 => {
            let b = a.as_any().downcast_ref::<Int64Array>().unwrap();
            let mut builder = Int64Array::builder(num_rows_to_read as usize);
            for i in 0..num_rows_to_read {
                builder.append_value(b.value(i as usize))?;
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Float32 => {
            let b = a.as_any().downcast_ref::<Float32Array>().unwrap();
            let mut builder = Float32Array::builder(num_rows_to_read as usize);
            for i in 0..num_rows_to_read {
                builder.append_value(b.value(i as usize))?;
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Float64 => {
            let b = a.as_any().downcast_ref::<Float64Array>().unwrap();
            let mut builder = Float64Array::builder(num_rows_to_read as usize);
            for i in 0..num_rows_to_read {
                builder.append_value(b.value(i as usize))?;
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Utf8 => {
            //TODO: this is inefficient and we should improve the Arrow impl to help make this more concise
            let b = a.as_any().downcast_ref::<BinaryArray>().unwrap();
            let mut values: Vec<String> = Vec::with_capacity(num_rows_to_read as usize);
            for i in 0..num_rows_to_read {
                values.push(b.get_string(i as usize));
            }
            let tmp: Vec<&str> = values.iter().map(|s| s.as_str()).collect();
            Ok(Arc::new(BinaryArray::from(tmp)))
        }
        other => Err(ExecutionError::ExecutionError(format!(
            "filter not supported for {:?}",
            other
        ))),
    }
}
