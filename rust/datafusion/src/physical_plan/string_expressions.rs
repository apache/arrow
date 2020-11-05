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

//! String expressions

use crate::error::{ExecutionError, Result};
use arrow::array::{Array, ArrayRef, StringArray, StringBuilder};

macro_rules! downcast_vec {
    ($ARGS:expr, $ARRAY_TYPE:ident) => {{
        $ARGS
            .iter()
            .map(|e| match e.as_any().downcast_ref::<$ARRAY_TYPE>() {
                Some(array) => Ok(array),
                _ => Err(ExecutionError::General("failed to downcast".to_string())),
            })
    }};
}

/// concatenate string columns together.
pub fn concatenate(args: &[ArrayRef]) -> Result<StringArray> {
    // downcast all arguments to strings
    let args = downcast_vec!(args, StringArray).collect::<Result<Vec<&StringArray>>>()?;
    // do not accept 0 arguments.
    if args.len() == 0 {
        return Err(ExecutionError::InternalError(
            "Concatenate was called with 0 arguments. It requires at least one."
                .to_string(),
        ));
    }

    let mut builder = StringBuilder::new(args.len());
    // for each entry in the array
    for index in 0..args[0].len() {
        let mut owned_string: String = "".to_owned();

        // if any is null, the result is null
        let mut is_null = false;
        for arg in &args {
            if arg.is_null(index) {
                is_null = true;
                break; // short-circuit as we already know the result
            } else {
                owned_string.push_str(&arg.value(index));
            }
        }
        if is_null {
            builder.append_null()?;
        } else {
            builder.append_value(&owned_string)?;
        }
    }
    Ok(builder.finish())
}
