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

//! UDF support

use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field, Schema};

use crate::error::Result;
use crate::execution::physical_plan::PhysicalExpr;

use arrow::record_batch::RecordBatch;
use std::sync::Arc;

/// Scalar UDF
pub type ScalarUdf = fn(input: &Vec<ArrayRef>) -> Result<ArrayRef>;

/// Scalar UDF Expression
#[derive(Clone)]
pub struct ScalarFunction {
    /// Function name
    pub name: String,
    /// Function argument meta-data
    pub args: Vec<Field>,
    /// Return type
    pub return_type: DataType,
    /// UDF implementation
    pub fun: ScalarUdf,
}

impl ScalarFunction {
    /// Create a new ScalarFunction
    pub fn new(
        name: &str,
        args: Vec<Field>,
        return_type: DataType,
        fun: ScalarUdf,
    ) -> Self {
        Self {
            name: name.to_owned(),
            args,
            return_type,
            fun,
        }
    }
}
/// Scalar UDF Expression
pub struct ScalarFunctionExpr {
    name: String,
    fun: Box<ScalarUdf>,
    args: Vec<Arc<dyn PhysicalExpr>>,
    return_type: DataType,
}

impl ScalarFunctionExpr {
    /// Create a new Scalar function
    pub fn new(
        name: String,
        fun: Box<ScalarUdf>,
        args: Vec<Arc<dyn PhysicalExpr>>,
        return_type: DataType,
    ) -> Self {
        Self {
            name,
            fun,
            args,
            return_type,
        }
    }
}

impl PhysicalExpr for ScalarFunctionExpr {
    fn name(&self) -> String {
        self.name.clone()
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(self.return_type.clone())
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        let inputs = self
            .args
            .iter()
            .map(|e| e.evaluate(batch))
            .collect::<Result<Vec<_>>>()?;

        let fun = self.fun.as_ref();
        (fun)(&inputs)
    }
}
