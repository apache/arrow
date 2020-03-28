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

//! Math expressions

use crate::error::ExecutionError;
use crate::execution::context::ExecutionContext;
use crate::execution::physical_plan::udf::ScalarFunction;

use arrow::array::{Array, ArrayRef, Float64Array, Float64Builder};
use arrow::datatypes::{DataType, Field};

use std::sync::Arc;

/// Register math scalar functions with the context
pub fn register_math_functions(ctx: &mut ExecutionContext) {
    ctx.register_udf(sqrt_fn());
}

fn sqrt_fn() -> ScalarFunction {
    ScalarFunction::new(
        "sqrt",
        vec![Field::new("n", DataType::Float64, true)],
        DataType::Float64,
        |args: &Vec<ArrayRef>| {
            let n = &args[0].as_any().downcast_ref::<Float64Array>();

            match n {
                Some(array) => {
                    let mut builder = Float64Builder::new(array.len());
                    for i in 0..array.len() {
                        if array.is_null(i) {
                            builder.append_null()?;
                        } else {
                            builder.append_value(array.value(i).sqrt())?;
                        }
                    }
                    Ok(Arc::new(builder.finish()))
                }
                _ => Err(ExecutionError::General(
                    "Invalid data type for sqrt".to_owned(),
                )),
            }
        },
    )
}
