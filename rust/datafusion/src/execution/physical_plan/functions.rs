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

//! Declaration of built-in (scalar) functions.
//! This module contains built-in functions' enumeration and metadata.
//!
//! Generally, a function has:
//! * a set of valid argument types
//! * a return type function of an incoming argument types
//! * the computation valid for all sets of valid argument types
//!
//! * Argument types: the number of arguments and set of valid types. For example, [[f32, f32], [f64, f64]] is a function of two arguments only accepting f32 or f64 on each of its arguments.
//! * Return type: a function `(arg_types) -> return_type`. E.g. for sqrt, ([f32]) -> f32, ([f64]) -> f64.
//!
//! Currently, this implementation supports only a single argument and a single signature.
//!
//! This module also has a set of coercion rules to improve user experience: if an argument i32 is passed
//! to a function that supports f64, it is coerced to f64.

use super::{
    expressions::{cast, is_numeric},
    PhysicalExpr,
};
use crate::error::{ExecutionError, Result};
use crate::execution::physical_plan::math_expressions;
use crate::execution::physical_plan::udf;
use arrow::{
    compute::kernels::length::length,
    datatypes::{DataType, Schema},
};
use std::{fmt, str::FromStr, sync::Arc};
use udf::ScalarUdf;

/// Enum of all built-in scalar functions
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScalarFunction {
    /// sqrt
    Sqrt,
    /// sin
    Sin,
    /// cos
    Cos,
    /// tan
    Tan,
    /// asin
    Asin,
    /// acos
    Acos,
    /// atan
    Atan,
    /// exp
    Exp,
    /// log, also known as ln
    Log,
    /// log2
    Log2,
    /// log10
    Log10,
    /// floor
    Floor,
    /// ceil
    Ceil,
    /// round
    Round,
    /// trunc
    Trunc,
    /// abs
    Abs,
    /// signum
    Signum,
    /// length
    Length,
}

impl fmt::Display for ScalarFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // lowercase of the debug.
        write!(f, "{}", format!("{:?}", self).to_lowercase())
    }
}

impl FromStr for ScalarFunction {
    type Err = ExecutionError;
    fn from_str(name: &str) -> Result<ScalarFunction> {
        Ok(match name {
            "sqrt" => ScalarFunction::Sqrt,
            "sin" => ScalarFunction::Sin,
            "cos" => ScalarFunction::Cos,
            "tan" => ScalarFunction::Tan,
            "asin" => ScalarFunction::Asin,
            "acos" => ScalarFunction::Acos,
            "atan" => ScalarFunction::Atan,
            "exp" => ScalarFunction::Exp,
            "log" => ScalarFunction::Log,
            "log2" => ScalarFunction::Log2,
            "log10" => ScalarFunction::Log10,
            "floor" => ScalarFunction::Floor,
            "ceil" => ScalarFunction::Ceil,
            "round" => ScalarFunction::Round,
            "truc" => ScalarFunction::Trunc,
            "abs" => ScalarFunction::Abs,
            "signum" => ScalarFunction::Signum,
            "length" => ScalarFunction::Length,
            _ => {
                return Err(ExecutionError::General(format!(
                    "There is no built-in function named {}",
                    name
                )))
            }
        })
    }
}

/// Returns the datatype of the scalar function
pub fn return_type(fun: &ScalarFunction, arg_types: &Vec<DataType>) -> Result<DataType> {
    // Note that this function *must* return the same type that the respective physical expression returns
    // or the execution panics.

    if arg_types.len() != 1 {
        // for now, every function expects a single argument, and thus this is enough
        return Err(ExecutionError::General(format!(
            "The function \"{}\" expected 1 argument, but received \"{}\"",
            fun,
            arg_types.len()
        )));
    }

    // verify that this is a valid type for this function
    coerce(fun, &arg_types[0])?;

    // the return type after coercion.
    // for now, this is type-independent, but there will be built-in functions whose return type
    // depends on the incoming type.
    match fun {
        ScalarFunction::Length => Ok(DataType::UInt32),
        _ => Ok(DataType::Float64),
    }
}

/// Create a physical (function) expression.
/// This function errors when `args`' can't be coerced to a valid argument type of the function.
pub fn function(
    fun: &ScalarFunction,
    args: &Vec<Arc<dyn PhysicalExpr>>,
    input_schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    let fun_expr: ScalarUdf = Arc::new(match fun {
        ScalarFunction::Sqrt => math_expressions::sqrt,
        ScalarFunction::Sin => math_expressions::sin,
        ScalarFunction::Cos => math_expressions::cos,
        ScalarFunction::Tan => math_expressions::tan,
        ScalarFunction::Asin => math_expressions::asin,
        ScalarFunction::Acos => math_expressions::acos,
        ScalarFunction::Atan => math_expressions::atan,
        ScalarFunction::Exp => math_expressions::exp,
        ScalarFunction::Log => math_expressions::ln,
        ScalarFunction::Log2 => math_expressions::log2,
        ScalarFunction::Log10 => math_expressions::log10,
        ScalarFunction::Floor => math_expressions::floor,
        ScalarFunction::Ceil => math_expressions::ceil,
        ScalarFunction::Round => math_expressions::round,
        ScalarFunction::Trunc => math_expressions::trunc,
        ScalarFunction::Abs => math_expressions::abs,
        ScalarFunction::Signum => math_expressions::signum,
        ScalarFunction::Length => |args| Ok(Arc::new(length(args[0].as_ref())?)),
    });
    let data_types = args
        .iter()
        .map(|e| e.data_type(input_schema))
        .collect::<Result<Vec<_>>>()?;

    // coerce type
    // for now, this supports a single type.
    assert!(args.len() == 1);
    assert!(data_types.len() == 1);
    let arg_type = coerce(fun, &data_types[0])?;
    let args = vec![cast(args[0].clone(), input_schema, arg_type.clone())?];

    Ok(Arc::new(udf::ScalarFunctionExpr::new(
        &format!("{}", fun),
        fun_expr,
        args,
        &return_type(&fun, &vec![arg_type])?,
    )))
}

/// the type supported by the function `fun`.
fn valid_type(fun: &ScalarFunction) -> DataType {
    // note: the physical expression must accept the type returned by this function or the execution panics.

    // for now, the list is small, as we do not have many built-in functions.
    match fun {
        ScalarFunction::Length => DataType::Utf8,
        // math expressions expect f64
        _ => DataType::Float64,
    }
}

/// coercion rules for all built-in functions.
/// Returns a DataType coerced from `arg_type` that is accepted by `fun`.
/// Errors when `arg_type` can't be coerced to a valid return type of `fun`.
fn coerce(fun: &ScalarFunction, arg_type: &DataType) -> Result<DataType> {
    let valid_type = valid_type(fun);
    if *arg_type == valid_type {
        // same type => all good
        return Ok(arg_type.clone());
    }
    match fun {
        ScalarFunction::Length => match arg_type {
            // cast does not support LargeUtf8 -> Utf8 yet: every type errors.
            _ => Err(ExecutionError::General(
                format!(
                    "The function '{}' can't evaluate type '{:?}'",
                    fun, arg_type
                )
                .to_string(),
            )),
        },
        // all other numerical types: float64
        _ => {
            if is_numeric(arg_type) {
                Ok(DataType::Float64)
            } else {
                Err(ExecutionError::General(
                    format!(
                        "The function '{}' can't evaluate type '{:?}'",
                        fun, arg_type
                    )
                    .to_string(),
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        error::Result, execution::physical_plan::expressions::lit,
        logicalplan::ScalarValue,
    };
    use arrow::{
        array::{ArrayRef, Float64Array, Int32Array},
        datatypes::Field,
        record_batch::RecordBatch,
    };

    #[test]
    fn test_coerce_math() -> Result<()> {
        // valid cases
        let cases = vec![
            // (observed, expected coercion to)
            (DataType::Float64, DataType::Float64),
            (DataType::Float32, DataType::Float64),
            (DataType::Int8, DataType::Float64),
            (DataType::UInt32, DataType::Float64),
        ];
        for case in cases {
            let result = coerce(&ScalarFunction::Sqrt, &case.0)?;
            assert_eq!(result, case.1)
        }
        // invalid math types
        let cases = vec![DataType::Utf8, DataType::Boolean, DataType::LargeUtf8];
        for case in cases {
            let result = coerce(&ScalarFunction::Sqrt, &case);
            assert!(result.is_err())
        }
        Ok(())
    }

    fn generic_test_math(value: ScalarValue) -> Result<()> {
        // any type works here: we evaluate against a literal of `value`
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let columns: Vec<ArrayRef> = vec![Arc::new(Int32Array::from(vec![1]))];

        let arg = lit(value);

        let expr = function(&ScalarFunction::Exp, &vec![arg], &schema)?;

        // type is correct
        assert_eq!(expr.data_type(&schema)?, DataType::Float64);

        // evaluate works
        let result =
            expr.evaluate(&RecordBatch::try_new(Arc::new(schema.clone()), columns)?)?;

        // downcast works
        let result = result.as_any().downcast_ref::<Float64Array>().unwrap();

        // value is correct
        assert_eq!(format!("{}", result.value(0)), "2.718281828459045"); // = exp(1)

        Ok(())
    }

    #[test]
    fn test_math_function() -> Result<()> {
        generic_test_math(ScalarValue::Int32(1i32))?;
        generic_test_math(ScalarValue::UInt32(1u32))?;
        generic_test_math(ScalarValue::Float64(1f64))?;
        generic_test_math(ScalarValue::Float32(1f32))?;
        Ok(())
    }
}
