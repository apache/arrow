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

use std::sync::Arc;

use arrow::array::{make_array, Array, ArrayData, ArrayRef, Float32Array, Float64Array};
use arrow::buffer::Buffer;
use arrow::datatypes::{DataType, ToByteSlice};

use crate::error::{ExecutionError, Result};

macro_rules! compute_op {
    ($ARRAY:expr, $FUNC:ident, $TYPE:ident) => {{
        let len = $ARRAY.len();
        let result = (0..len)
            .map(|i| $ARRAY.value(i).$FUNC() as f64)
            .collect::<Vec<f64>>();
        let data = ArrayData::new(
            DataType::Float64,
            len,
            Some($ARRAY.null_count()),
            $ARRAY.data().null_buffer().cloned(),
            0,
            vec![Buffer::from(result.to_byte_slice())],
            vec![],
        );
        Ok(make_array(Arc::new(data)))
    }};
}

macro_rules! downcast_compute_op {
    ($ARRAY:expr, $NAME:expr, $FUNC:ident, $TYPE:ident) => {{
        let n = $ARRAY.as_any().downcast_ref::<$TYPE>();
        match n {
            Some(array) => compute_op!(array, $FUNC, $TYPE),
            _ => Err(ExecutionError::General(format!(
                "Invalid data type for {}",
                $NAME
            ))),
        }
    }};
}

macro_rules! unary_primitive_array_op {
    ($ARRAY:expr, $NAME:expr, $FUNC:ident) => {{
        match ($ARRAY).data_type() {
            DataType::Float32 => downcast_compute_op!($ARRAY, $NAME, $FUNC, Float32Array),
            DataType::Float64 => downcast_compute_op!($ARRAY, $NAME, $FUNC, Float64Array),
            other => Err(ExecutionError::General(format!(
                "Unsupported data type {:?} for function {}",
                other, $NAME,
            ))),
        }
    }};
}

macro_rules! math_unary_function {
    ($NAME:expr, $FUNC:ident) => {
        /// mathematical function that accepts f32 or f64 and returns f64
        pub fn $FUNC(args: &[ArrayRef]) -> Result<ArrayRef> {
            unary_primitive_array_op!(args[0], $NAME, $FUNC)
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
