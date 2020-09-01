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

use crate::error::{ExecutionError, Result};

use arrow::array::{Array, ArrayRef, Float64Array, Float64Builder};

use std::sync::Arc;

macro_rules! math_unary_function {
    ($NAME:expr, $FUNC:ident) => {
        /// mathematical function
        pub fn $FUNC(args: &[ArrayRef]) -> Result<ArrayRef> {
            let n = &args[0].as_any().downcast_ref::<Float64Array>();
            match n {
                Some(array) => {
                    let mut builder = Float64Builder::new(array.len());
                    for i in 0..array.len() {
                        if array.is_null(i) {
                            builder.append_null()?;
                        } else {
                            builder.append_value(array.value(i).$FUNC())?;
                        }
                    }
                    Ok(Arc::new(builder.finish()))
                }
                _ => Err(ExecutionError::General(format!(
                    "Invalid data type for {}",
                    $NAME
                ))),
            }
        }
    };
}

math_unary_function!("sqrt", sqrt);
math_unary_function!("sin", sin);
math_unary_function!("cos", cos);
math_unary_function!("tan", tan);
math_unary_function!("asin", asin);
math_unary_function!("acos", acos);
math_unary_function!("atan", atan);
math_unary_function!("floor", floor);
math_unary_function!("ceil", ceil);
math_unary_function!("round", round);
math_unary_function!("trunc", trunc);
math_unary_function!("abs", abs);
math_unary_function!("signum", signum);
math_unary_function!("exp", exp);
math_unary_function!("log", ln);
math_unary_function!("log2", log2);
math_unary_function!("log10", log10);
