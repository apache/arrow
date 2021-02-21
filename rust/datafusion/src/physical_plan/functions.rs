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
    ColumnarValue, PhysicalExpr,
};
use crate::physical_plan::array_expressions;
use crate::physical_plan::crypto_expressions;
use crate::physical_plan::datetime_expressions;
use crate::physical_plan::expressions::{nullif_func, SUPPORTED_NULLIF_TYPES};
use crate::physical_plan::math_expressions;
use crate::physical_plan::string_expressions;
use crate::{
    error::{DataFusionError, Result},
    scalar::ScalarValue,
};
use arrow::{
    array::ArrayRef,
    compute::kernels::length::{bit_length, length},
    datatypes::TimeUnit,
    datatypes::{DataType, Field, Int32Type, Int64Type, Schema},
    record_batch::RecordBatch,
};
use fmt::{Debug, Formatter};
use std::{any::Any, fmt, str::FromStr, sync::Arc};

/// A function's signature, which defines the function's supported argument types.
#[derive(Debug, Clone, PartialEq)]
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
    // A function of one argument of f64 or f32 is `Uniform(1, vec![DataType::Float32, DataType::Float64])`
    Uniform(usize, Vec<DataType>),
    /// exact number of arguments of an exact type
    Exact(Vec<DataType>),
    /// fixed number of arguments of arbitrary types
    Any(usize),
    /// One of a list of signatures
    OneOf(Vec<Signature>),
}

/// Scalar function
pub type ScalarFunctionImplementation =
    Arc<dyn Fn(&[ColumnarValue]) -> Result<ColumnarValue> + Send + Sync>;

/// A function's return type
pub type ReturnTypeFunction =
    Arc<dyn Fn(&[DataType]) -> Result<Arc<DataType>> + Send + Sync>;

/// Enum of all built-in scalar functions
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BuiltinScalarFunction {
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
    /// concat
    Concat,
    /// lower
    Lower,
    /// upper
    Upper,
    /// trim
    Trim,
    /// trim left
    Ltrim,
    /// trim right
    Rtrim,
    /// to_timestamp
    ToTimestamp,
    /// construct an array from columns
    Array,
    /// SQL NULLIF()
    NullIf,
    /// Date truncate
    DateTrunc,
    /// Date part
    DatePart,
    /// MD5
    MD5,
    /// SHA224
    SHA224,
    /// SHA256,
    SHA256,
    /// SHA384
    SHA384,
    /// SHA512,
    SHA512,
    /// bit_length
    BitLength,
    /// character_length
    CharacterLength,
    /// octet_length
    OctetLength,
}

impl fmt::Display for BuiltinScalarFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // lowercase of the debug.
        write!(f, "{}", format!("{:?}", self).to_lowercase())
    }
}

impl FromStr for BuiltinScalarFunction {
    type Err = DataFusionError;
    fn from_str(name: &str) -> Result<BuiltinScalarFunction> {
        Ok(match name {
            "sqrt" => BuiltinScalarFunction::Sqrt,
            "sin" => BuiltinScalarFunction::Sin,
            "cos" => BuiltinScalarFunction::Cos,
            "tan" => BuiltinScalarFunction::Tan,
            "asin" => BuiltinScalarFunction::Asin,
            "acos" => BuiltinScalarFunction::Acos,
            "atan" => BuiltinScalarFunction::Atan,
            "exp" => BuiltinScalarFunction::Exp,
            "log" => BuiltinScalarFunction::Log,
            "log2" => BuiltinScalarFunction::Log2,
            "log10" => BuiltinScalarFunction::Log10,
            "floor" => BuiltinScalarFunction::Floor,
            "ceil" => BuiltinScalarFunction::Ceil,
            "round" => BuiltinScalarFunction::Round,
            "truc" => BuiltinScalarFunction::Trunc,
            "abs" => BuiltinScalarFunction::Abs,
            "signum" => BuiltinScalarFunction::Signum,
            "concat" => BuiltinScalarFunction::Concat,
            "lower" => BuiltinScalarFunction::Lower,
            "trim" => BuiltinScalarFunction::Trim,
            "ltrim" => BuiltinScalarFunction::Ltrim,
            "rtrim" => BuiltinScalarFunction::Rtrim,
            "upper" => BuiltinScalarFunction::Upper,
            "to_timestamp" => BuiltinScalarFunction::ToTimestamp,
            "date_trunc" => BuiltinScalarFunction::DateTrunc,
            "date_part" => BuiltinScalarFunction::DatePart,
            "array" => BuiltinScalarFunction::Array,
            "nullif" => BuiltinScalarFunction::NullIf,
            "md5" => BuiltinScalarFunction::MD5,
            "sha224" => BuiltinScalarFunction::SHA224,
            "sha256" => BuiltinScalarFunction::SHA256,
            "sha384" => BuiltinScalarFunction::SHA384,
            "sha512" => BuiltinScalarFunction::SHA512,
            "bit_length" => BuiltinScalarFunction::BitLength,
            "octet_length" => BuiltinScalarFunction::OctetLength,
            "length" => BuiltinScalarFunction::CharacterLength,
            "char_length" => BuiltinScalarFunction::CharacterLength,
            "character_length" => BuiltinScalarFunction::CharacterLength,
            _ => {
                return Err(DataFusionError::Plan(format!(
                    "There is no built-in function named {}",
                    name
                )))
            }
        })
    }
}

/// Returns the datatype of the scalar function
pub fn return_type(
    fun: &BuiltinScalarFunction,
    arg_types: &[DataType],
) -> Result<DataType> {
    // Note that this function *must* return the same type that the respective physical expression returns
    // or the execution panics.

    // verify that this is a valid set of data types for this function
    data_types(&arg_types, &signature(fun))?;

    if arg_types.is_empty() {
        // functions currently cannot be evaluated without arguments, as they can't
        // know the number of rows to return.
        return Err(DataFusionError::Plan(format!(
            "Function '{}' requires at least one argument",
            fun
        )));
    }

    // the return type of the built in function.
    // Some built-in functions' return type depends on the incoming type.
    match fun {
        BuiltinScalarFunction::Concat => Ok(DataType::Utf8),
        BuiltinScalarFunction::Lower => Ok(match arg_types[0] {
            DataType::LargeUtf8 => DataType::LargeUtf8,
            DataType::Utf8 => DataType::Utf8,
            _ => {
                // this error is internal as `data_types` should have captured this.
                return Err(DataFusionError::Internal(
                    "The upper function can only accept strings.".to_string(),
                ));
            }
        }),
        BuiltinScalarFunction::Ltrim => Ok(match arg_types[0] {
            DataType::LargeUtf8 => DataType::LargeUtf8,
            DataType::Utf8 => DataType::Utf8,
            _ => {
                // this error is internal as `data_types` should have captured this.
                return Err(DataFusionError::Internal(
                    "The ltrim function can only accept strings.".to_string(),
                ));
            }
        }),
        BuiltinScalarFunction::Rtrim => Ok(match arg_types[0] {
            DataType::LargeUtf8 => DataType::LargeUtf8,
            DataType::Utf8 => DataType::Utf8,
            _ => {
                // this error is internal as `data_types` should have captured this.
                return Err(DataFusionError::Internal(
                    "The rtrim function can only accept strings.".to_string(),
                ));
            }
        }),
        BuiltinScalarFunction::Trim => Ok(match arg_types[0] {
            DataType::LargeUtf8 => DataType::LargeUtf8,
            DataType::Utf8 => DataType::Utf8,
            _ => {
                // this error is internal as `data_types` should have captured this.
                return Err(DataFusionError::Internal(
                    "The trim function can only accept strings.".to_string(),
                ));
            }
        }),
        BuiltinScalarFunction::Upper => Ok(match arg_types[0] {
            DataType::LargeUtf8 => DataType::LargeUtf8,
            DataType::Utf8 => DataType::Utf8,
            _ => {
                // this error is internal as `data_types` should have captured this.
                return Err(DataFusionError::Internal(
                    "The upper function can only accept strings.".to_string(),
                ));
            }
        }),
        BuiltinScalarFunction::ToTimestamp => {
            Ok(DataType::Timestamp(TimeUnit::Nanosecond, None))
        }
        BuiltinScalarFunction::DateTrunc => {
            Ok(DataType::Timestamp(TimeUnit::Nanosecond, None))
        }
        BuiltinScalarFunction::DatePart => Ok(DataType::Int32),
        BuiltinScalarFunction::Array => Ok(DataType::FixedSizeList(
            Box::new(Field::new("item", arg_types[0].clone(), true)),
            arg_types.len() as i32,
        )),
        BuiltinScalarFunction::NullIf => {
            // NULLIF has two args and they might get coerced, get a preview of this
            let coerced_types = data_types(arg_types, &signature(fun));
            coerced_types.map(|typs| typs[0].clone())
        }
        BuiltinScalarFunction::MD5 => Ok(match arg_types[0] {
            DataType::LargeUtf8 => DataType::LargeUtf8,
            DataType::Utf8 => DataType::Utf8,
            _ => {
                // this error is internal as `data_types` should have captured this.
                return Err(DataFusionError::Internal(
                    "The md5 function can only accept strings.".to_string(),
                ));
            }
        }),
        BuiltinScalarFunction::SHA224 => Ok(match arg_types[0] {
            DataType::LargeUtf8 => DataType::Binary,
            DataType::Utf8 => DataType::Binary,
            _ => {
                // this error is internal as `data_types` should have captured this.
                return Err(DataFusionError::Internal(
                    "The sha224 function can only accept strings.".to_string(),
                ));
            }
        }),
        BuiltinScalarFunction::SHA256 => Ok(match arg_types[0] {
            DataType::LargeUtf8 => DataType::Binary,
            DataType::Utf8 => DataType::Binary,
            _ => {
                // this error is internal as `data_types` should have captured this.
                return Err(DataFusionError::Internal(
                    "The sha256 function can only accept strings.".to_string(),
                ));
            }
        }),
        BuiltinScalarFunction::SHA384 => Ok(match arg_types[0] {
            DataType::LargeUtf8 => DataType::Binary,
            DataType::Utf8 => DataType::Binary,
            _ => {
                // this error is internal as `data_types` should have captured this.
                return Err(DataFusionError::Internal(
                    "The sha384 function can only accept strings.".to_string(),
                ));
            }
        }),
        BuiltinScalarFunction::SHA512 => Ok(match arg_types[0] {
            DataType::LargeUtf8 => DataType::Binary,
            DataType::Utf8 => DataType::Binary,
            _ => {
                // this error is internal as `data_types` should have captured this.
                return Err(DataFusionError::Internal(
                    "The sha512 function can only accept strings.".to_string(),
                ));
            }
        }),
        BuiltinScalarFunction::BitLength => Ok(match arg_types[0] {
            DataType::LargeUtf8 => DataType::Int64,
            DataType::Utf8 => DataType::Int32,
            _ => {
                // this error is internal as `data_types` should have captured this.
                return Err(DataFusionError::Internal(
                    "The bit_length function can only accept strings.".to_string(),
                ));
            }
        }),
        BuiltinScalarFunction::CharacterLength => Ok(match arg_types[0] {
            DataType::LargeUtf8 => DataType::Int64,
            DataType::Utf8 => DataType::Int32,
            _ => {
                // this error is internal as `data_types` should have captured this.
                return Err(DataFusionError::Internal(
                    "The character_length function can only accept strings.".to_string(),
                ));
            }
        }),
        BuiltinScalarFunction::OctetLength => Ok(match arg_types[0] {
            DataType::LargeUtf8 => DataType::Int64,
            DataType::Utf8 => DataType::Int32,
            _ => {
                // this error is internal as `data_types` should have captured this.
                return Err(DataFusionError::Internal(
                    "The octet_length function can only accept strings.".to_string(),
                ));
            }
        }),
        _ => Ok(DataType::Float64),
    }
}

/// Create a physical (function) expression.
/// This function errors when `args`' can't be coerced to a valid argument type of the function.
pub fn create_physical_expr(
    fun: &BuiltinScalarFunction,
    args: &[Arc<dyn PhysicalExpr>],
    input_schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    let fun_expr: ScalarFunctionImplementation = Arc::new(match fun {
        BuiltinScalarFunction::Sqrt => math_expressions::sqrt,
        BuiltinScalarFunction::Sin => math_expressions::sin,
        BuiltinScalarFunction::Cos => math_expressions::cos,
        BuiltinScalarFunction::Tan => math_expressions::tan,
        BuiltinScalarFunction::Asin => math_expressions::asin,
        BuiltinScalarFunction::Acos => math_expressions::acos,
        BuiltinScalarFunction::Atan => math_expressions::atan,
        BuiltinScalarFunction::Exp => math_expressions::exp,
        BuiltinScalarFunction::Log => math_expressions::ln,
        BuiltinScalarFunction::Log2 => math_expressions::log2,
        BuiltinScalarFunction::Log10 => math_expressions::log10,
        BuiltinScalarFunction::Floor => math_expressions::floor,
        BuiltinScalarFunction::Ceil => math_expressions::ceil,
        BuiltinScalarFunction::Round => math_expressions::round,
        BuiltinScalarFunction::Trunc => math_expressions::trunc,
        BuiltinScalarFunction::Abs => math_expressions::abs,
        BuiltinScalarFunction::Signum => math_expressions::signum,
        BuiltinScalarFunction::NullIf => nullif_func,
        BuiltinScalarFunction::MD5 => crypto_expressions::md5,
        BuiltinScalarFunction::SHA224 => crypto_expressions::sha224,
        BuiltinScalarFunction::SHA256 => crypto_expressions::sha256,
        BuiltinScalarFunction::SHA384 => crypto_expressions::sha384,
        BuiltinScalarFunction::SHA512 => crypto_expressions::sha512,
        BuiltinScalarFunction::Concat => string_expressions::concatenate,
        BuiltinScalarFunction::Lower => string_expressions::lower,
        BuiltinScalarFunction::Trim => string_expressions::trim,
        BuiltinScalarFunction::Ltrim => string_expressions::ltrim,
        BuiltinScalarFunction::Rtrim => string_expressions::rtrim,
        BuiltinScalarFunction::Upper => string_expressions::upper,
        BuiltinScalarFunction::ToTimestamp => datetime_expressions::to_timestamp,
        BuiltinScalarFunction::DateTrunc => datetime_expressions::date_trunc,
        BuiltinScalarFunction::Array => array_expressions::array,
        BuiltinScalarFunction::BitLength => |args| match &args[0] {
            ColumnarValue::Array(v) => Ok(ColumnarValue::Array(bit_length(v.as_ref())?)),
            ColumnarValue::Scalar(v) => match v {
                ScalarValue::Utf8(v) => Ok(ColumnarValue::Scalar(ScalarValue::Int32(
                    v.as_ref().map(|x| (x.len() * 8) as i32),
                ))),
                ScalarValue::LargeUtf8(v) => Ok(ColumnarValue::Scalar(
                    ScalarValue::Int64(v.as_ref().map(|x| (x.len() * 8) as i64)),
                )),
                _ => unreachable!(),
            },
        },
        BuiltinScalarFunction::CharacterLength => |args| match args[0].data_type() {
            DataType::Utf8 => make_scalar_function(
                string_expressions::character_length::<Int32Type>,
            )(args),
            DataType::LargeUtf8 => make_scalar_function(
                string_expressions::character_length::<Int64Type>,
            )(args),
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function character_length",
                other,
            ))),
        },
        BuiltinScalarFunction::OctetLength => |args| match &args[0] {
            ColumnarValue::Array(v) => Ok(ColumnarValue::Array(length(v.as_ref())?)),
            ColumnarValue::Scalar(v) => match v {
                ScalarValue::Utf8(v) => Ok(ColumnarValue::Scalar(ScalarValue::Int32(
                    v.as_ref().map(|x| x.len() as i32),
                ))),
                ScalarValue::LargeUtf8(v) => Ok(ColumnarValue::Scalar(
                    ScalarValue::Int64(v.as_ref().map(|x| x.len() as i64)),
                )),
                _ => unreachable!(),
            },
        },
        BuiltinScalarFunction::DatePart => datetime_expressions::date_part,
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
fn signature(fun: &BuiltinScalarFunction) -> Signature {
    // note: the physical expression must accept the type returned by this function or the execution panics.

    // for now, the list is small, as we do not have many built-in functions.
    match fun {
        BuiltinScalarFunction::Concat => Signature::Variadic(vec![DataType::Utf8]),
        BuiltinScalarFunction::Upper
        | BuiltinScalarFunction::Lower
        | BuiltinScalarFunction::BitLength
        | BuiltinScalarFunction::CharacterLength
        | BuiltinScalarFunction::OctetLength
        | BuiltinScalarFunction::Trim
        | BuiltinScalarFunction::Ltrim
        | BuiltinScalarFunction::Rtrim
        | BuiltinScalarFunction::MD5
        | BuiltinScalarFunction::SHA224
        | BuiltinScalarFunction::SHA256
        | BuiltinScalarFunction::SHA384
        | BuiltinScalarFunction::SHA512 => {
            Signature::Uniform(1, vec![DataType::Utf8, DataType::LargeUtf8])
        }
        BuiltinScalarFunction::ToTimestamp => Signature::Uniform(1, vec![DataType::Utf8]),
        BuiltinScalarFunction::DateTrunc => Signature::Exact(vec![
            DataType::Utf8,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
        ]),
        BuiltinScalarFunction::DatePart => Signature::OneOf(vec![
            Signature::Exact(vec![DataType::Utf8, DataType::Date32]),
            Signature::Exact(vec![DataType::Utf8, DataType::Date64]),
            Signature::Exact(vec![
                DataType::Utf8,
                DataType::Timestamp(TimeUnit::Second, None),
            ]),
            Signature::Exact(vec![
                DataType::Utf8,
                DataType::Timestamp(TimeUnit::Microsecond, None),
            ]),
            Signature::Exact(vec![
                DataType::Utf8,
                DataType::Timestamp(TimeUnit::Millisecond, None),
            ]),
            Signature::Exact(vec![
                DataType::Utf8,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
            ]),
        ]),
        BuiltinScalarFunction::Array => {
            Signature::Variadic(array_expressions::SUPPORTED_ARRAY_TYPES.to_vec())
        }
        BuiltinScalarFunction::NullIf => {
            Signature::Uniform(2, SUPPORTED_NULLIF_TYPES.to_vec())
        }
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

    /// Get the scalar function implementation
    pub fn fun(&self) -> &ScalarFunctionImplementation {
        &self.fun
    }

    /// The name for this expression
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Input arguments
    pub fn args(&self) -> &[Arc<dyn PhysicalExpr>] {
        &self.args
    }

    /// Data type produced by this expression
    pub fn return_type(&self) -> &DataType {
        &self.return_type
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
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(self.return_type.clone())
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(true)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
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

/// decorates a function to handle [`ScalarValue`]s by coverting them to arrays before calling the function
/// and vice-versa after evaluation.
pub fn make_scalar_function<F>(inner: F) -> ScalarFunctionImplementation
where
    F: Fn(&[ArrayRef]) -> Result<ArrayRef> + Sync + Send + 'static,
{
    Arc::new(move |args: &[ColumnarValue]| {
        // first, identify if any of the arguments is an Array. If yes, store its `len`,
        // as any scalar will need to be converted to an array of len `len`.
        let len = args
            .iter()
            .fold(Option::<usize>::None, |acc, arg| match arg {
                ColumnarValue::Scalar(_) => acc,
                ColumnarValue::Array(a) => Some(a.len()),
            });

        // to array
        let args = if let Some(len) = len {
            args.iter()
                .map(|arg| arg.clone().into_array(len))
                .collect::<Vec<ArrayRef>>()
        } else {
            args.iter()
                .map(|arg| arg.clone().into_array(1))
                .collect::<Vec<ArrayRef>>()
        };

        let result = (inner)(&args);

        // maybe back to scalar
        if len.is_some() {
            result.map(ColumnarValue::Array)
        } else {
            ScalarValue::try_from_array(&result?, 0).map(ColumnarValue::Scalar)
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        error::Result,
        physical_plan::expressions::{col, lit},
        scalar::ScalarValue,
    };
    use arrow::{
        array::{
            Array, ArrayRef, FixedSizeListArray, Float64Array, Int32Array, StringArray,
            UInt32Array, UInt64Array,
        },
        datatypes::Field,
        record_batch::RecordBatch,
    };

    /// $FUNC function to test
    /// $ARGS arguments (vec) to pass to function
    /// $EXPECTED a Result<Option<$EXPECTED_TYPE>> where Result allows testing errors and Option allows testing Null
    /// $EXPECTED_TYPE is the expected value type
    /// $DATA_TYPE is the function to test result type
    /// $ARRAY_TYPE is the column type after function applied
    macro_rules! test_function {
        ($FUNC:ident, $ARGS:expr, $EXPECTED:expr, $EXPECTED_TYPE:ty, $DATA_TYPE: ident, $ARRAY_TYPE:ident) => {
            // used to provide type annotation
            let expected: Result<Option<$EXPECTED_TYPE>> = $EXPECTED;

            // any type works here: we evaluate against a literal of `value`
            let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
            let columns: Vec<ArrayRef> = vec![Arc::new(Int32Array::from(vec![1]))];

            let expr =
                create_physical_expr(&BuiltinScalarFunction::$FUNC, $ARGS, &schema)?;

            // type is correct
            assert_eq!(expr.data_type(&schema)?, DataType::$DATA_TYPE);

            let batch = RecordBatch::try_new(Arc::new(schema.clone()), columns)?;

            match expected {
                Ok(expected) => {
                    let result = expr.evaluate(&batch)?;
                    let result = result.into_array(batch.num_rows());
                    let result = result.as_any().downcast_ref::<$ARRAY_TYPE>().unwrap();

                    // value is correct
                    match expected {
                        Some(v) => assert_eq!(result.value(0), v),
                        None => assert!(result.is_null(0)),
                    };
                }
                Err(expected_error) => {
                    // evaluate is expected error - cannot use .expect_err() due to Debug not being implemented
                    match expr.evaluate(&batch) {
                        Ok(_) => assert!(false, "expected error"),
                        Err(error) => {
                            assert_eq!(error.to_string(), expected_error.to_string());
                        }
                    }
                }
            };
        };
    }

    #[test]
    fn test_functions() -> Result<()> {
        test_function!(
            CharacterLength,
            &[lit(ScalarValue::Utf8(Some("chars".to_string())))],
            Ok(Some(5)),
            i32,
            Int32,
            Int32Array
        );
        test_function!(
            CharacterLength,
            &[lit(ScalarValue::Utf8(Some("josé".to_string())))],
            Ok(Some(4)),
            i32,
            Int32,
            Int32Array
        );
        test_function!(
            CharacterLength,
            &[lit(ScalarValue::Utf8(Some("".to_string())))],
            Ok(Some(0)),
            i32,
            Int32,
            Int32Array
        );
        test_function!(
            CharacterLength,
            &[lit(ScalarValue::Utf8(None))],
            Ok(None),
            i32,
            Int32,
            Int32Array
        );
        test_function!(
            Exp,
            &[lit(ScalarValue::Int32(Some(1)))],
            Ok(Some((1.0_f64).exp())),
            f64,
            Float64,
            Float64Array
        );
        test_function!(
            Exp,
            &[lit(ScalarValue::UInt32(Some(1)))],
            Ok(Some((1.0_f64).exp())),
            f64,
            Float64,
            Float64Array
        );
        test_function!(
            Exp,
            &[lit(ScalarValue::UInt64(Some(1)))],
            Ok(Some((1.0_f64).exp())),
            f64,
            Float64,
            Float64Array
        );
        test_function!(
            Exp,
            &[lit(ScalarValue::Float64(Some(1.0)))],
            Ok(Some((1.0_f64).exp())),
            f64,
            Float64,
            Float64Array
        );
        test_function!(
            Exp,
            &[lit(ScalarValue::Float32(Some(1.0)))],
            Ok(Some((1.0_f32).exp() as f64)),
            f64,
            Float64,
            Float64Array
        );
        Ok(())
    }

    fn test_concat(value: ScalarValue, expected: &str) -> Result<()> {
        // any type works here: we evaluate against a literal of `value`
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let columns: Vec<ArrayRef> = vec![Arc::new(Int32Array::from(vec![1]))];

        // concat(value, value)
        let expr = create_physical_expr(
            &BuiltinScalarFunction::Concat,
            &[lit(value.clone()), lit(value)],
            &schema,
        )?;

        // type is correct
        assert_eq!(expr.data_type(&schema)?, DataType::Utf8);

        // evaluate works
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), columns)?;
        let result = expr.evaluate(&batch)?.into_array(batch.num_rows());

        // downcast works
        let result = result.as_any().downcast_ref::<StringArray>().unwrap();

        // value is correct
        assert_eq!(result.value(0).to_string(), expected);

        Ok(())
    }

    #[test]
    fn test_concat_utf8() -> Result<()> {
        test_concat(ScalarValue::Utf8(Some("aa".to_string())), "aaaa")
    }

    #[test]
    fn test_concat_error() -> Result<()> {
        let result = return_type(&BuiltinScalarFunction::Concat, &[]);
        if result.is_ok() {
            Err(DataFusionError::Plan(
                "Function 'concat' cannot accept zero arguments".to_string(),
            ))
        } else {
            Ok(())
        }
    }

    fn generic_test_array(
        value1: ArrayRef,
        value2: ArrayRef,
        expected_type: DataType,
        expected: &str,
    ) -> Result<()> {
        // any type works here: we evaluate against a literal of `value`
        let schema = Schema::new(vec![
            Field::new("a", value1.data_type().clone(), false),
            Field::new("b", value2.data_type().clone(), false),
        ]);
        let columns: Vec<ArrayRef> = vec![value1, value2];

        let expr = create_physical_expr(
            &BuiltinScalarFunction::Array,
            &[col("a"), col("b")],
            &schema,
        )?;

        // type is correct
        assert_eq!(
            expr.data_type(&schema)?,
            // type equals to a common coercion
            DataType::FixedSizeList(Box::new(Field::new("item", expected_type, true)), 2)
        );

        // evaluate works
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), columns)?;
        let result = expr.evaluate(&batch)?.into_array(batch.num_rows());

        // downcast works
        let result = result
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap();

        // value is correct
        assert_eq!(format!("{:?}", result.value(0)), expected);

        Ok(())
    }

    #[test]
    fn test_array() -> Result<()> {
        generic_test_array(
            Arc::new(StringArray::from(vec!["aa"])),
            Arc::new(StringArray::from(vec!["bb"])),
            DataType::Utf8,
            "StringArray\n[\n  \"aa\",\n  \"bb\",\n]",
        )?;

        // different types, to validate that casting happens
        generic_test_array(
            Arc::new(UInt32Array::from(vec![1u32])),
            Arc::new(UInt64Array::from(vec![1u64])),
            DataType::UInt64,
            "PrimitiveArray<UInt64>\n[\n  1,\n  1,\n]",
        )?;

        // different types (another order), to validate that casting happens
        generic_test_array(
            Arc::new(UInt64Array::from(vec![1u64])),
            Arc::new(UInt32Array::from(vec![1u32])),
            DataType::UInt64,
            "PrimitiveArray<UInt64>\n[\n  1,\n  1,\n]",
        )
    }
}
