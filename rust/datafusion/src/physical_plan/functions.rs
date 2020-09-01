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
//! * a signature
//! * a return type, that is a function of the incoming argument's types
//! * the computation, that must accept each valid signature
//!
//! * Signature: see `Signature`
//! * Return type: a function `(arg_types) -> return_type`. E.g. for sqrt, ([f32]) -> f32, ([f64]) -> f64.
//!
//! This module also has a set of coercion rules to improve user experience: if an argument i32 is passed
//! to a function that supports f64, it is coerced to f64.

use super::{
    type_coercion::{coerce, data_types},
    PhysicalExpr,
};
use crate::error::{ExecutionError, Result};
use crate::physical_plan::math_expressions;
use crate::physical_plan::udf;
use arrow::{
    compute::kernels::length::length,
    datatypes::{DataType, Schema},
};
use std::{fmt, str::FromStr, sync::Arc};
use udf::ScalarUdf;

/// A function's signature, which defines the function's supported argument types.
#[derive(Debug)]
pub enum Signature {
    /// arbitrary number of arguments of an common type out of a list of valid types
    // A function such as `concat` is `Variadic(vec![DataType::Utf8, DataType::LargeUtf8])`
    Variadic(Vec<DataType>),
    /// arbitrary number of arguments of an arbitrary but equal type
    // A function such as `array` is `VariadicEqual`
    // The first argument decides the type used for coercion
    VariadicEqual,
    /// fixed number of arguments of an arbitrary but equal type out of a list of valid types
    // A function of one argument of f64 is `Uniform(1, vec![DataType::Float64])`
    // A function of two arguments of f64 or f32 is `Uniform(1, vec![DataType::Float32, DataType::Float64])`
    Uniform(usize, Vec<DataType>),
}

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

    // verify that this is a valid set of data types for this function
    data_types(&arg_types, &signature(fun))?;

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
pub fn create_physical_expr(
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
    // coerce
    let args = coerce(args, input_schema, &signature(fun))?;

    let arg_types = args
        .iter()
        .map(|e| e.data_type(input_schema))
        .collect::<Result<Vec<_>>>()?;

    Ok(Arc::new(udf::ScalarFunctionExpr::new(
        &format!("{}", fun),
        fun_expr,
        args,
        &return_type(&fun, &arg_types)?,
    )))
}

/// the signatures supported by the function `fun`.
fn signature(fun: &ScalarFunction) -> Signature {
    // note: the physical expression must accept the type returned by this function or the execution panics.

    // for now, the list is small, as we do not have many built-in functions.
    match fun {
        ScalarFunction::Length => Signature::Uniform(1, vec![DataType::Utf8]),
        // math expressions expect 1 argument of type f64
        _ => Signature::Uniform(1, vec![DataType::Float64]),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        error::Result, logical_plan::ScalarValue, physical_plan::expressions::lit,
    };
    use arrow::{
        array::{ArrayRef, Float64Array, Int32Array},
        datatypes::Field,
        record_batch::RecordBatch,
    };

    fn generic_test_math(value: ScalarValue, expected: &str) -> Result<()> {
        // any type works here: we evaluate against a literal of `value`
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let columns: Vec<ArrayRef> = vec![Arc::new(Int32Array::from(vec![1]))];

        let arg = lit(value);

        let expr = create_physical_expr(&ScalarFunction::Exp, &vec![arg], &schema)?;

        // type is correct
        assert_eq!(expr.data_type(&schema)?, DataType::Float64);

        // evaluate works
        let result =
            expr.evaluate(&RecordBatch::try_new(Arc::new(schema.clone()), columns)?)?;

        // downcast works
        let result = result.as_any().downcast_ref::<Float64Array>().unwrap();

        // value is correct
        assert_eq!(format!("{}", result.value(0)), expected); // = exp(1)

        Ok(())
    }

    #[test]
    fn test_math_function() -> Result<()> {
        let exp_f64 = "2.718281828459045";
        generic_test_math(ScalarValue::Int32(1i32), exp_f64)?;
        generic_test_math(ScalarValue::UInt32(1u32), exp_f64)?;
        generic_test_math(ScalarValue::Float64(1f64), exp_f64)?;
        generic_test_math(ScalarValue::Float32(1f32), exp_f64)?;
        Ok(())
    }
}
