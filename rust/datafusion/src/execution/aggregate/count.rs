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

//! COUNT aggregate function. Counts the number of non-null values for an expression.

use crate::arrow::array::*;
use crate::arrow::datatypes::DataType;

use crate::error::Result;
use crate::execution::aggregate::AggregateFunction;
use crate::logicalplan::ScalarValue;

/// Implementation of COUNT aggregate function
#[derive(Debug)]
pub(super) struct CountFunction {
    value: Option<u64>,
}

impl CountFunction {
    pub fn new() -> Self {
        Self { value: None }
    }
}

impl AggregateFunction for CountFunction {
    fn name(&self) -> &str {
        "count"
    }

    fn accumulate_scalar(
        &mut self,
        value: &Option<ScalarValue>,
        rollup: bool,
    ) -> Result<()> {
        if rollup {
            if let Some(ScalarValue::UInt64(n)) = value {
                if self.value.is_none() {
                    self.value = Some(*n);
                } else {
                    self.value = Some(self.value.unwrap() + *n);
                }
            }
        } else {
            if value.is_some() {
                if self.value.is_none() {
                    self.value = Some(1);
                } else {
                    self.value = Some(self.value.unwrap() + 1);
                }
            }
        }
        Ok(())
    }

    fn accumulate_array(&mut self, array: ArrayRef) -> Result<()> {
        self.accumulate_scalar(
            &Some(ScalarValue::UInt64(
                (array.len() - array.null_count()) as u64,
            )),
            true,
        )
    }

    fn result(&self) -> Option<ScalarValue> {
        match self.value {
            Some(n) => Some(ScalarValue::UInt64(n)),
            None => None,
        }
    }

    fn data_type(&self) -> &DataType {
        &DataType::UInt64
    }
}
