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
use crate::physical_plan::string_expressions;
use arrow::{
    array::ArrayRef,
    compute::kernels::length::length,
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};
use fmt::{Debug, Formatter};
use std::{fmt, str::FromStr, sync::Arc};

/// A function's signature, which defines the function's supported argument types.
#[derive(Debug, Clone)]
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
    /// exact number of arguments of an exact type
    Exact(Vec<DataType>),
}

/// Scalar function
pub type ScalarFunctionImplementation =
    Arc<dyn Fn(&[ArrayRef]) -> Result<ArrayRef> + Send + Sync>;

/// A function's return type
pub type ReturnTypeFunction =
    Arc<dyn Fn(&[DataType]) -> Result<Arc<DataType>> + Send + Sync>;

/// Enum of all built-in scalar functions
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BuiltinFunction {
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
    /// concat
    Concat,
}

impl fmt::Display for BuiltinFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // lowercase of the debug.
        write!(f, "{}", format!("{:?}", self).to_lowercase())
    }
}

impl FromStr for BuiltinFunction {
    type Err = ExecutionError;
    fn from_str(name: &str) -> Result<BuiltinFunction> {
        Ok(match name {
            "sqrt" => BuiltinFunction::Sqrt,
            "sin" => BuiltinFunction::Sin,
            "cos" => BuiltinFunction::Cos,
            "tan" => BuiltinFunction::Tan,
            "asin" => BuiltinFunction::Asin,
            "acos" => BuiltinFunction::Acos,
            "atan" => BuiltinFunction::Atan,
            "exp" => BuiltinFunction::Exp,
            "log" => BuiltinFunction::Log,
            "log2" => BuiltinFunction::Log2,
            "log10" => BuiltinFunction::Log10,
            "floor" => BuiltinFunction::Floor,
            "ceil" => BuiltinFunction::Ceil,
            "round" => BuiltinFunction::Round,
            "truc" => BuiltinFunction::Trunc,
            "abs" => BuiltinFunction::Abs,
            "signum" => BuiltinFunction::Signum,
            "length" => BuiltinFunction::Length,
            "concat" => BuiltinFunction::Concat,
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
pub fn return_type(fun: &BuiltinFunction, arg_types: &Vec<DataType>) -> Result<DataType> {
    // Note that this function *must* return the same type that the respective physical expression returns
    // or the execution panics.

    // verify that this is a valid set of data types for this function
    data_types(&arg_types, &signature(fun))?;

    if arg_types.len() == 0 {
        // functions currently cannot be evaluated without arguments, as they can't
        // know the number of rows to return.
        return Err(ExecutionError::General(
            format!("Function '{}' requires at least one argument", fun).to_string(),
        ));
    }

    // the return type after coercion.
    // for now, this is type-independent, but there will be built-in functions whose return type
    // depends on the incoming type.
    match fun {
        BuiltinFunction::Length => Ok(DataType::UInt32),
        BuiltinFunction::Concat => Ok(DataType::Utf8),
        _ => Ok(DataType::Float64),
    }
}

/// Create a physical (function) expression.
/// This function errors when `args`' can't be coerced to a valid argument type of the function.
pub fn create_physical_expr(
    fun: &BuiltinFunction,
    args: &Vec<Arc<dyn PhysicalExpr>>,
    input_schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    let fun_expr: ScalarFunctionImplementation = Arc::new(match fun {
        BuiltinFunction::Sqrt => math_expressions::sqrt,
        BuiltinFunction::Sin => math_expressions::sin,
        BuiltinFunction::Cos => math_expressions::cos,
        BuiltinFunction::Tan => math_expressions::tan,
        BuiltinFunction::Asin => math_expressions::asin,
        BuiltinFunction::Acos => math_expressions::acos,
        BuiltinFunction::Atan => math_expressions::atan,
        BuiltinFunction::Exp => math_expressions::exp,
        BuiltinFunction::Log => math_expressions::ln,
        BuiltinFunction::Log2 => math_expressions::log2,
        BuiltinFunction::Log10 => math_expressions::log10,
        BuiltinFunction::Floor => math_expressions::floor,
        BuiltinFunction::Ceil => math_expressions::ceil,
        BuiltinFunction::Round => math_expressions::round,
        BuiltinFunction::Trunc => math_expressions::trunc,
        BuiltinFunction::Abs => math_expressions::abs,
        BuiltinFunction::Signum => math_expressions::signum,
        BuiltinFunction::Length => |args| Ok(Arc::new(length(args[0].as_ref())?)),
        BuiltinFunction::Concat => {
            |args| Ok(Arc::new(string_expressions::concatenate(args)?))
        }
    });
    // coerce
    let args = coerce(args, input_schema, &signature(fun))?;

    let arg_types = args
        .iter()
        .map(|e| e.data_type(input_schema))
        .collect::<Result<Vec<_>>>()?;

    Ok(Arc::new(ScalarFunctionExpr::new(
        &format!("{}", fun),
        fun_expr,
        args,
        &return_type(&fun, &arg_types)?,
    )))
}

/// the signatures supported by the function `fun`.
fn signature(fun: &BuiltinFunction) -> Signature {
    // note: the physical expression must accept the type returned by this function or the execution panics.

    // for now, the list is small, as we do not have many built-in functions.
    match fun {
        BuiltinFunction::Length => Signature::Uniform(1, vec![DataType::Utf8]),
        BuiltinFunction::Concat => Signature::Variadic(vec![DataType::Utf8]),
        // math expressions expect 1 argument of type f64 or f32
        // priority is given to f64 because e.g. `sqrt(1i32)` is in IR (real numbers) and thus we
        // return the best approximation for it (in f64).
        // We accept f32 because in this case it is clear that the best approximation
        // will be as good as the number of digits in the number
        _ => Signature::Uniform(1, vec![DataType::Float64, DataType::Float32]),
    }
}

/// Physical expression of a scalar function
pub struct ScalarFunctionExpr {
    fun: ScalarFunctionImplementation,
    name: String,
    args: Vec<Arc<dyn PhysicalExpr>>,
    return_type: DataType,
}

impl Debug for ScalarFunctionExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScalarFunctionExpr")
            .field("fun", &"<FUNC>")
            .field("name", &self.name)
            .field("args", &self.args)
            .field("return_type", &self.return_type)
            .finish()
    }
}

impl ScalarFunctionExpr {
    /// Create a new Scalar function
    pub fn new(
        name: &str,
        fun: ScalarFunctionImplementation,
        args: Vec<Arc<dyn PhysicalExpr>>,
        return_type: &DataType,
    ) -> Self {
        Self {
            fun,
            name: name.to_owned(),
            args,
            return_type: return_type.clone(),
        }
    }
}

impl fmt::Display for ScalarFunctionExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}({})",
            self.name,
            self.args
                .iter()
                .map(|e| format!("{}", e))
                .collect::<Vec<String>>()
                .join(", ")
        )
    }
}

impl PhysicalExpr for ScalarFunctionExpr {
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(self.return_type.clone())
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(true)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        // evaluate the arguments
        let inputs = self
            .args
            .iter()
            .map(|e| e.evaluate(batch))
            .collect::<Result<Vec<_>>>()?;

        // evaluate the function
        let fun = self.fun.as_ref();
        (fun)(&inputs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        error::Result, logical_plan::ScalarValue, physical_plan::expressions::lit,
    };
    use arrow::{
        array::{ArrayRef, Float64Array, Int32Array, StringArray},
        datatypes::Field,
        record_batch::RecordBatch,
    };

    fn generic_test_math(value: ScalarValue, expected: &str) -> Result<()> {
        // any type works here: we evaluate against a literal of `value`
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let columns: Vec<ArrayRef> = vec![Arc::new(Int32Array::from(vec![1]))];

        let arg = lit(value);

        let expr = create_physical_expr(&BuiltinFunction::Exp, &vec![arg], &schema)?;

        // type is correct
        assert_eq!(expr.data_type(&schema)?, DataType::Float64);

        // evaluate works
        let result =
            expr.evaluate(&RecordBatch::try_new(Arc::new(schema.clone()), columns)?)?;

        // downcast works
        let result = result.as_any().downcast_ref::<Float64Array>().unwrap();

        // value is correct
        assert_eq!(format!("{}", result.value(0)), expected);

        Ok(())
    }

    #[test]
    fn test_math_function() -> Result<()> {
        // 2.71828182845904523536... : https://oeis.org/A001113
        let exp_f64 = "2.718281828459045";
        let exp_f32 = "2.7182817459106445";
        generic_test_math(ScalarValue::Int32(1i32), exp_f64)?;
        generic_test_math(ScalarValue::UInt32(1u32), exp_f64)?;
        generic_test_math(ScalarValue::UInt64(1u64), exp_f64)?;
        generic_test_math(ScalarValue::Float64(1f64), exp_f64)?;
        generic_test_math(ScalarValue::Float32(1f32), exp_f32)?;
        Ok(())
    }

    fn test_concat(value: ScalarValue, expected: &str) -> Result<()> {
        // any type works here: we evaluate against a literal of `value`
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let columns: Vec<ArrayRef> = vec![Arc::new(Int32Array::from(vec![1]))];

        // concat(value, value)
        let expr = create_physical_expr(
            &BuiltinFunction::Concat,
            &vec![lit(value.clone()), lit(value)],
            &schema,
        )?;

        // type is correct
        assert_eq!(expr.data_type(&schema)?, DataType::Utf8);

        // evaluate works
        let result =
            expr.evaluate(&RecordBatch::try_new(Arc::new(schema.clone()), columns)?)?;

        // downcast works
        let result = result.as_any().downcast_ref::<StringArray>().unwrap();

        // value is correct
        assert_eq!(format!("{}", result.value(0)), expected);

        Ok(())
    }

    #[test]
    fn test_concat_utf8() -> Result<()> {
        test_concat(ScalarValue::Utf8("aa".to_string()), "aaaa")
    }

    #[test]
    fn test_concat_error() -> Result<()> {
        let result = return_type(&BuiltinFunction::Concat, &vec![]);
        if let Ok(_) = result {
            Err(ExecutionError::General(
                "Function 'concat' cannot accept zero arguments".to_string(),
            ))
        } else {
            Ok(())
        }
    }
}
