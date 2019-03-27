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

//! SUM aggregate function

use crate::arrow::array::*;
use crate::arrow::compute;
use crate::arrow::datatypes::DataType;

use crate::error::{ExecutionError, Result};
use crate::execution::aggregate::AggregateFunction;
use crate::logicalplan::ScalarValue;

/// Implementation of SUM aggregate function
#[derive(Debug)]
pub(super) struct SumFunction {
    data_type: DataType,
    value: Option<ScalarValue>,
}

impl SumFunction {
    pub fn new(data_type: &DataType) -> Self {
        Self {
            data_type: data_type.clone(),
            value: None,
        }
    }
}

impl AggregateFunction for SumFunction {
    fn name(&self) -> &str {
        "sum"
    }

    fn accumulate_scalar(
        &mut self,
        value: &Option<ScalarValue>,
        _rollup: bool,
    ) -> Result<()> {
        if self.value.is_none() {
            self.value = value.clone();
        } else if value.is_some() {
            self.value = match (&self.value, value) {
                (Some(ScalarValue::UInt8(a)), Some(ScalarValue::UInt8(b))) => {
                    Some(ScalarValue::UInt8(*a + b))
                }
                (Some(ScalarValue::UInt16(a)), Some(ScalarValue::UInt16(b))) => {
                    Some(ScalarValue::UInt16(*a + b))
                }
                (Some(ScalarValue::UInt32(a)), Some(ScalarValue::UInt32(b))) => {
                    Some(ScalarValue::UInt32(*a + b))
                }
                (Some(ScalarValue::UInt64(a)), Some(ScalarValue::UInt64(b))) => {
                    Some(ScalarValue::UInt64(*a + b))
                }
                (Some(ScalarValue::Int8(a)), Some(ScalarValue::Int8(b))) => {
                    Some(ScalarValue::Int8(*a + b))
                }
                (Some(ScalarValue::Int16(a)), Some(ScalarValue::Int16(b))) => {
                    Some(ScalarValue::Int16(*a + b))
                }
                (Some(ScalarValue::Int32(a)), Some(ScalarValue::Int32(b))) => {
                    Some(ScalarValue::Int32(*a + b))
                }
                (Some(ScalarValue::Int64(a)), Some(ScalarValue::Int64(b))) => {
                    Some(ScalarValue::Int64(*a + b))
                }
                (Some(ScalarValue::Float32(a)), Some(ScalarValue::Float32(b))) => {
                    Some(ScalarValue::Float32(a + *b))
                }
                (Some(ScalarValue::Float64(a)), Some(ScalarValue::Float64(b))) => {
                    Some(ScalarValue::Float64(a + *b))
                }
                _ => {
                    return Err(ExecutionError::ExecutionError(
                        "unsupported data type for SUM".to_string(),
                    ));
                }
            }
        }
        Ok(())
    }

    fn accumulate_array(&mut self, array: ArrayRef) -> Result<()> {
        let scalar = match array.data_type() {
            DataType::UInt8 => {
                match compute::sum(array.as_any().downcast_ref::<UInt8Array>().unwrap()) {
                    Some(n) => Ok(Some(ScalarValue::UInt8(n))),
                    None => Ok(None),
                }
            }
            DataType::UInt16 => {
                match compute::sum(array.as_any().downcast_ref::<UInt16Array>().unwrap())
                {
                    Some(n) => Ok(Some(ScalarValue::UInt16(n))),
                    None => Ok(None),
                }
            }
            DataType::UInt32 => {
                match compute::sum(array.as_any().downcast_ref::<UInt32Array>().unwrap())
                {
                    Some(n) => Ok(Some(ScalarValue::UInt32(n))),
                    None => Ok(None),
                }
            }
            DataType::UInt64 => {
                match compute::sum(array.as_any().downcast_ref::<UInt64Array>().unwrap())
                {
                    Some(n) => Ok(Some(ScalarValue::UInt64(n))),
                    None => Ok(None),
                }
            }
            DataType::Int8 => {
                match compute::sum(array.as_any().downcast_ref::<Int8Array>().unwrap()) {
                    Some(n) => Ok(Some(ScalarValue::Int8(n))),
                    None => Ok(None),
                }
            }
            DataType::Int16 => {
                match compute::sum(array.as_any().downcast_ref::<Int16Array>().unwrap()) {
                    Some(n) => Ok(Some(ScalarValue::Int16(n))),
                    None => Ok(None),
                }
            }
            DataType::Int32 => {
                match compute::sum(array.as_any().downcast_ref::<Int32Array>().unwrap()) {
                    Some(n) => Ok(Some(ScalarValue::Int32(n))),
                    None => Ok(None),
                }
            }
            DataType::Int64 => {
                match compute::sum(array.as_any().downcast_ref::<Int64Array>().unwrap()) {
                    Some(n) => Ok(Some(ScalarValue::Int64(n))),
                    None => Ok(None),
                }
            }
            DataType::Float32 => {
                match compute::sum(array.as_any().downcast_ref::<Float32Array>().unwrap())
                {
                    Some(n) => Ok(Some(ScalarValue::Float32(n))),
                    None => Ok(None),
                }
            }
            DataType::Float64 => {
                match compute::sum(array.as_any().downcast_ref::<Float64Array>().unwrap())
                {
                    Some(n) => Ok(Some(ScalarValue::Float64(n))),
                    None => Ok(None),
                }
            }
            _ => Err(ExecutionError::ExecutionError(
                "Unsupported data type for SUM".to_string(),
            )),
        }?;
        self.accumulate_scalar(&scalar, true)
    }

    fn result(&self) -> Option<ScalarValue> {
        self.value.clone()
    }

    fn data_type(&self) -> &DataType {
        &self.data_type
    }
}
