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
    // math functions
    /// abs
    Abs,
    /// acos
    Acos,
    /// asin
    Asin,
    /// atan
    Atan,
    /// ceil
    Ceil,
    /// cos
    Cos,
    /// exp
    Exp,
    /// floor
    Floor,
    /// log, also known as ln
    Log,
    /// log10
    Log10,
    /// log2
    Log2,
    /// round
    Round,
    /// signum
    Signum,
    /// sin
    Sin,
    /// sqrt
    Sqrt,
    /// tan
    Tan,
    /// trunc
    Trunc,

    // string functions
    /// construct an array from columns
    Array,
    /// ascii
    Ascii,
    /// bit_length
    BitLength,
    /// btrim
    Btrim,
    /// character_length
    CharacterLength,
    /// chr
    Chr,
    /// concat
    Concat,
    /// concat_ws
    ConcatWithSeparator,
    /// date_part
    DatePart,
    /// date_trunc
    DateTrunc,
    /// initcap
    InitCap,
    /// left
    Left,
    /// lpad
    Lpad,
    /// lower
    Lower,
    /// ltrim
    Ltrim,
    /// md5
    MD5,
    /// nullif
    NullIf,
    /// octet_length
    OctetLength,
    /// regexp_replace
    RegexpReplace,
    /// repeat
    Repeat,
    /// replace
    Replace,
    /// reverse
    Reverse,
    /// right
    Right,
    /// rpad
    Rpad,
    /// rtrim
    Rtrim,
    /// sha224
    SHA224,
    /// sha256
    SHA256,
    /// sha384
    SHA384,
    /// Sha512
    SHA512,
    /// split_part
    SplitPart,
    /// starts_with
    StartsWith,
    /// strpos
    Strpos,
    /// substr
    Substr,
    /// to_hex
    ToHex,
    /// to_timestamp
    ToTimestamp,
    /// translate
    Translate,
    /// trim
    Trim,
    /// upper
    Upper,
    /// regexp_match
    RegexpMatch,
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
            // math functions
            "abs" => BuiltinScalarFunction::Abs,
            "acos" => BuiltinScalarFunction::Acos,
            "asin" => BuiltinScalarFunction::Asin,
            "atan" => BuiltinScalarFunction::Atan,
            "ceil" => BuiltinScalarFunction::Ceil,
            "cos" => BuiltinScalarFunction::Cos,
            "exp" => BuiltinScalarFunction::Exp,
            "floor" => BuiltinScalarFunction::Floor,
            "log" => BuiltinScalarFunction::Log,
            "log10" => BuiltinScalarFunction::Log10,
            "log2" => BuiltinScalarFunction::Log2,
            "round" => BuiltinScalarFunction::Round,
            "signum" => BuiltinScalarFunction::Signum,
            "sin" => BuiltinScalarFunction::Sin,
            "sqrt" => BuiltinScalarFunction::Sqrt,
            "tan" => BuiltinScalarFunction::Tan,
            "trunc" => BuiltinScalarFunction::Trunc,

            // string functions
            "array" => BuiltinScalarFunction::Array,
            "ascii" => BuiltinScalarFunction::Ascii,
            "bit_length" => BuiltinScalarFunction::BitLength,
            "btrim" => BuiltinScalarFunction::Btrim,
            "char_length" => BuiltinScalarFunction::CharacterLength,
            "character_length" => BuiltinScalarFunction::CharacterLength,
            "concat" => BuiltinScalarFunction::Concat,
            "concat_ws" => BuiltinScalarFunction::ConcatWithSeparator,
            "chr" => BuiltinScalarFunction::Chr,
            "date_part" => BuiltinScalarFunction::DatePart,
            "date_trunc" => BuiltinScalarFunction::DateTrunc,
            "initcap" => BuiltinScalarFunction::InitCap,
            "left" => BuiltinScalarFunction::Left,
            "length" => BuiltinScalarFunction::CharacterLength,
            "lower" => BuiltinScalarFunction::Lower,
            "lpad" => BuiltinScalarFunction::Lpad,
            "ltrim" => BuiltinScalarFunction::Ltrim,
            "md5" => BuiltinScalarFunction::MD5,
            "nullif" => BuiltinScalarFunction::NullIf,
            "octet_length" => BuiltinScalarFunction::OctetLength,
            "regexp_replace" => BuiltinScalarFunction::RegexpReplace,
            "repeat" => BuiltinScalarFunction::Repeat,
            "replace" => BuiltinScalarFunction::Replace,
            "reverse" => BuiltinScalarFunction::Reverse,
            "right" => BuiltinScalarFunction::Right,
            "rpad" => BuiltinScalarFunction::Rpad,
            "rtrim" => BuiltinScalarFunction::Rtrim,
            "sha224" => BuiltinScalarFunction::SHA224,
            "sha256" => BuiltinScalarFunction::SHA256,
            "sha384" => BuiltinScalarFunction::SHA384,
            "sha512" => BuiltinScalarFunction::SHA512,
            "split_part" => BuiltinScalarFunction::SplitPart,
            "starts_with" => BuiltinScalarFunction::StartsWith,
            "strpos" => BuiltinScalarFunction::Strpos,
            "substr" => BuiltinScalarFunction::Substr,
            "to_hex" => BuiltinScalarFunction::ToHex,
            "to_timestamp" => BuiltinScalarFunction::ToTimestamp,
            "translate" => BuiltinScalarFunction::Translate,
            "trim" => BuiltinScalarFunction::Trim,
            "upper" => BuiltinScalarFunction::Upper,
            "regexp_match" => BuiltinScalarFunction::RegexpMatch,
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
        BuiltinScalarFunction::Array => Ok(DataType::FixedSizeList(
            Box::new(Field::new("item", arg_types[0].clone(), true)),
            arg_types.len() as i32,
        )),
        BuiltinScalarFunction::Ascii => Ok(DataType::Int32),
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
        BuiltinScalarFunction::Btrim => Ok(match arg_types[0] {
            DataType::LargeUtf8 => DataType::LargeUtf8,
            DataType::Utf8 => DataType::Utf8,
            _ => {
                // this error is internal as `data_types` should have captured this.
                return Err(DataFusionError::Internal(
                    "The btrim function can only accept strings.".to_string(),
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
        BuiltinScalarFunction::Chr => Ok(DataType::Utf8),
        BuiltinScalarFunction::Concat => Ok(DataType::Utf8),
        BuiltinScalarFunction::ConcatWithSeparator => Ok(DataType::Utf8),
        BuiltinScalarFunction::DatePart => Ok(DataType::Int32),
        BuiltinScalarFunction::DateTrunc => {
            Ok(DataType::Timestamp(TimeUnit::Nanosecond, None))
        }
        BuiltinScalarFunction::InitCap => Ok(match arg_types[0] {
            DataType::LargeUtf8 => DataType::LargeUtf8,
            DataType::Utf8 => DataType::Utf8,
            _ => {
                // this error is internal as `data_types` should have captured this.
                return Err(DataFusionError::Internal(
                    "The initcap function can only accept strings.".to_string(),
                ));
            }
        }),
        BuiltinScalarFunction::Left => Ok(match arg_types[0] {
            DataType::LargeUtf8 => DataType::LargeUtf8,
            DataType::Utf8 => DataType::Utf8,
            _ => {
                // this error is internal as `data_types` should have captured this.
                return Err(DataFusionError::Internal(
                    "The left function can only accept strings.".to_string(),
                ));
            }
        }),
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
        BuiltinScalarFunction::Lpad => Ok(match arg_types[0] {
            DataType::LargeUtf8 => DataType::LargeUtf8,
            DataType::Utf8 => DataType::Utf8,
            _ => {
                // this error is internal as `data_types` should have captured this.
                return Err(DataFusionError::Internal(
                    "The lpad function can only accept strings.".to_string(),
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
        BuiltinScalarFunction::NullIf => {
            // NULLIF has two args and they might get coerced, get a preview of this
            let coerced_types = data_types(arg_types, &signature(fun));
            coerced_types.map(|typs| typs[0].clone())
        }
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
        BuiltinScalarFunction::RegexpReplace => Ok(match arg_types[0] {
            DataType::LargeUtf8 => DataType::LargeUtf8,
            DataType::Utf8 => DataType::Utf8,
            _ => {
                // this error is internal as `data_types` should have captured this.
                return Err(DataFusionError::Internal(
                    "The regexp_replace function can only accept strings.".to_string(),
                ));
            }
        }),
        BuiltinScalarFunction::Repeat => Ok(match arg_types[0] {
            DataType::LargeUtf8 => DataType::LargeUtf8,
            DataType::Utf8 => DataType::Utf8,
            _ => {
                // this error is internal as `data_types` should have captured this.
                return Err(DataFusionError::Internal(
                    "The repeat function can only accept strings.".to_string(),
                ));
            }
        }),
        BuiltinScalarFunction::Replace => Ok(match arg_types[0] {
            DataType::LargeUtf8 => DataType::LargeUtf8,
            DataType::Utf8 => DataType::Utf8,
            _ => {
                // this error is internal as `data_types` should have captured this.
                return Err(DataFusionError::Internal(
                    "The replace function can only accept strings.".to_string(),
                ));
            }
        }),
        BuiltinScalarFunction::Reverse => Ok(match arg_types[0] {
            DataType::LargeUtf8 => DataType::LargeUtf8,
            DataType::Utf8 => DataType::Utf8,
            _ => {
                // this error is internal as `data_types` should have captured this.
                return Err(DataFusionError::Internal(
                    "The reverse function can only accept strings.".to_string(),
                ));
            }
        }),
        BuiltinScalarFunction::Right => Ok(match arg_types[0] {
            DataType::LargeUtf8 => DataType::LargeUtf8,
            DataType::Utf8 => DataType::Utf8,
            _ => {
                // this error is internal as `data_types` should have captured this.
                return Err(DataFusionError::Internal(
                    "The right function can only accept strings.".to_string(),
                ));
            }
        }),
        BuiltinScalarFunction::Rpad => Ok(match arg_types[0] {
            DataType::LargeUtf8 => DataType::LargeUtf8,
            DataType::Utf8 => DataType::Utf8,
            _ => {
                // this error is internal as `data_types` should have captured this.
                return Err(DataFusionError::Internal(
                    "The rpad function can only accept strings.".to_string(),
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
        BuiltinScalarFunction::SplitPart => Ok(match arg_types[0] {
            DataType::LargeUtf8 => DataType::LargeUtf8,
            DataType::Utf8 => DataType::Utf8,
            _ => {
                // this error is internal as `data_types` should have captured this.
                return Err(DataFusionError::Internal(
                    "The split_part function can only accept strings.".to_string(),
                ));
            }
        }),
        BuiltinScalarFunction::StartsWith => Ok(DataType::Boolean),
        BuiltinScalarFunction::Strpos => Ok(match arg_types[0] {
            DataType::LargeUtf8 => DataType::Int64,
            DataType::Utf8 => DataType::Int32,
            _ => {
                // this error is internal as `data_types` should have captured this.
                return Err(DataFusionError::Internal(
                    "The strpos function can only accept strings.".to_string(),
                ));
            }
        }),
        BuiltinScalarFunction::Substr => Ok(match arg_types[0] {
            DataType::LargeUtf8 => DataType::LargeUtf8,
            DataType::Utf8 => DataType::Utf8,
            _ => {
                // this error is internal as `data_types` should have captured this.
                return Err(DataFusionError::Internal(
                    "The substr function can only accept strings.".to_string(),
                ));
            }
        }),
        BuiltinScalarFunction::ToHex => Ok(match arg_types[0] {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                DataType::Utf8
            }
            _ => {
                // this error is internal as `data_types` should have captured this.
                return Err(DataFusionError::Internal(
                    "The to_hex function can only accept integers.".to_string(),
                ));
            }
        }),
        BuiltinScalarFunction::ToTimestamp => {
            Ok(DataType::Timestamp(TimeUnit::Nanosecond, None))
        }
        BuiltinScalarFunction::Translate => Ok(match arg_types[0] {
            DataType::LargeUtf8 => DataType::LargeUtf8,
            DataType::Utf8 => DataType::Utf8,
            _ => {
                // this error is internal as `data_types` should have captured this.
                return Err(DataFusionError::Internal(
                    "The translate function can only accept strings.".to_string(),
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
        BuiltinScalarFunction::RegexpMatch => Ok(match arg_types[0] {
            DataType::LargeUtf8 => {
                DataType::List(Box::new(Field::new("item", DataType::LargeUtf8, true)))
            }
            DataType::Utf8 => {
                DataType::List(Box::new(Field::new("item", DataType::Utf8, true)))
            }
            _ => {
                // this error is internal as `data_types` should have captured this.
                return Err(DataFusionError::Internal(
                    "The regexp_extract function can only accept strings.".to_string(),
                ));
            }
        }),

        BuiltinScalarFunction::Abs
        | BuiltinScalarFunction::Acos
        | BuiltinScalarFunction::Asin
        | BuiltinScalarFunction::Atan
        | BuiltinScalarFunction::Ceil
        | BuiltinScalarFunction::Cos
        | BuiltinScalarFunction::Exp
        | BuiltinScalarFunction::Floor
        | BuiltinScalarFunction::Log
        | BuiltinScalarFunction::Log10
        | BuiltinScalarFunction::Log2
        | BuiltinScalarFunction::Round
        | BuiltinScalarFunction::Signum
        | BuiltinScalarFunction::Sin
        | BuiltinScalarFunction::Sqrt
        | BuiltinScalarFunction::Tan
        | BuiltinScalarFunction::Trunc => Ok(DataType::Float64),
    }
}

#[cfg(feature = "crypto_expressions")]
macro_rules! invoke_if_crypto_expressions_feature_flag {
    ($FUNC:ident, $NAME:expr) => {{
        use crate::physical_plan::crypto_expressions;
        crypto_expressions::$FUNC
    }};
}

#[cfg(not(feature = "crypto_expressions"))]
macro_rules! invoke_if_crypto_expressions_feature_flag {
    ($FUNC:ident, $NAME:expr) => {
        |_: &[ColumnarValue]| -> Result<ColumnarValue> {
            Err(DataFusionError::Internal(format!(
                "function {} requires compilation with feature flag: crypto_expressions.",
                $NAME
            )))
        }
    };
}

#[cfg(feature = "regex_expressions")]
macro_rules! invoke_if_regex_expressions_feature_flag {
    ($FUNC:ident, $T:tt, $NAME:expr) => {{
        use crate::physical_plan::regex_expressions;
        regex_expressions::$FUNC::<$T>
    }};
}

#[cfg(not(feature = "regex_expressions"))]
macro_rules! invoke_if_regex_expressions_feature_flag {
    ($FUNC:ident, $T:tt, $NAME:expr) => {
        |_: &[ArrayRef]| -> Result<ArrayRef> {
            Err(DataFusionError::Internal(format!(
                "function {} requires compilation with feature flag: regex_expressions.",
                $NAME
            )))
        }
    };
}

#[cfg(feature = "unicode_expressions")]
macro_rules! invoke_if_unicode_expressions_feature_flag {
    ($FUNC:ident, $T:tt, $NAME:expr) => {{
        use crate::physical_plan::unicode_expressions;
        unicode_expressions::$FUNC::<$T>
    }};
}

#[cfg(not(feature = "unicode_expressions"))]
macro_rules! invoke_if_unicode_expressions_feature_flag {
    ($FUNC:ident, $T:tt, $NAME:expr) => {
        |_: &[ArrayRef]| -> Result<ArrayRef> {
            Err(DataFusionError::Internal(format!(
                "function {} requires compilation with feature flag: unicode_expressions.",
                $NAME
            )))
        }
    };
}

/// Create a physical (function) expression.
/// This function errors when `args`' can't be coerced to a valid argument type of the function.
pub fn create_physical_expr(
    fun: &BuiltinScalarFunction,
    args: &[Arc<dyn PhysicalExpr>],
    input_schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    let fun_expr: ScalarFunctionImplementation = Arc::new(match fun {
        // math functions
        BuiltinScalarFunction::Abs => math_expressions::abs,
        BuiltinScalarFunction::Acos => math_expressions::acos,
        BuiltinScalarFunction::Asin => math_expressions::asin,
        BuiltinScalarFunction::Atan => math_expressions::atan,
        BuiltinScalarFunction::Ceil => math_expressions::ceil,
        BuiltinScalarFunction::Cos => math_expressions::cos,
        BuiltinScalarFunction::Exp => math_expressions::exp,
        BuiltinScalarFunction::Floor => math_expressions::floor,
        BuiltinScalarFunction::Log => math_expressions::ln,
        BuiltinScalarFunction::Log10 => math_expressions::log10,
        BuiltinScalarFunction::Log2 => math_expressions::log2,
        BuiltinScalarFunction::Round => math_expressions::round,
        BuiltinScalarFunction::Signum => math_expressions::signum,
        BuiltinScalarFunction::Sin => math_expressions::sin,
        BuiltinScalarFunction::Sqrt => math_expressions::sqrt,
        BuiltinScalarFunction::Tan => math_expressions::tan,
        BuiltinScalarFunction::Trunc => math_expressions::trunc,

        // string functions
        BuiltinScalarFunction::Array => array_expressions::array,
        BuiltinScalarFunction::Ascii => |args| match args[0].data_type() {
            DataType::Utf8 => {
                make_scalar_function(string_expressions::ascii::<i32>)(args)
            }
            DataType::LargeUtf8 => {
                make_scalar_function(string_expressions::ascii::<i64>)(args)
            }
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function ascii",
                other,
            ))),
        },
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
        BuiltinScalarFunction::Btrim => |args| match args[0].data_type() {
            DataType::Utf8 => {
                make_scalar_function(string_expressions::btrim::<i32>)(args)
            }
            DataType::LargeUtf8 => {
                make_scalar_function(string_expressions::btrim::<i64>)(args)
            }
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function btrim",
                other,
            ))),
        },
        BuiltinScalarFunction::CharacterLength => |args| match args[0].data_type() {
            DataType::Utf8 => {
                let func = invoke_if_unicode_expressions_feature_flag!(
                    character_length,
                    Int32Type,
                    "character_length"
                );
                make_scalar_function(func)(args)
            }
            DataType::LargeUtf8 => {
                let func = invoke_if_unicode_expressions_feature_flag!(
                    character_length,
                    Int64Type,
                    "character_length"
                );
                make_scalar_function(func)(args)
            }
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function character_length",
                other,
            ))),
        },
        BuiltinScalarFunction::Chr => {
            |args| make_scalar_function(string_expressions::chr)(args)
        }
        BuiltinScalarFunction::Concat => string_expressions::concat,
        BuiltinScalarFunction::ConcatWithSeparator => {
            |args| make_scalar_function(string_expressions::concat_ws)(args)
        }
        BuiltinScalarFunction::DatePart => datetime_expressions::date_part,
        BuiltinScalarFunction::DateTrunc => datetime_expressions::date_trunc,
        BuiltinScalarFunction::InitCap => |args| match args[0].data_type() {
            DataType::Utf8 => {
                make_scalar_function(string_expressions::initcap::<i32>)(args)
            }
            DataType::LargeUtf8 => {
                make_scalar_function(string_expressions::initcap::<i64>)(args)
            }
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function initcap",
                other,
            ))),
        },
        BuiltinScalarFunction::Left => |args| match args[0].data_type() {
            DataType::Utf8 => {
                let func = invoke_if_unicode_expressions_feature_flag!(left, i32, "left");
                make_scalar_function(func)(args)
            }
            DataType::LargeUtf8 => {
                let func = invoke_if_unicode_expressions_feature_flag!(left, i64, "left");
                make_scalar_function(func)(args)
            }
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function left",
                other,
            ))),
        },
        BuiltinScalarFunction::Lower => string_expressions::lower,
        BuiltinScalarFunction::Lpad => |args| match args[0].data_type() {
            DataType::Utf8 => {
                let func = invoke_if_unicode_expressions_feature_flag!(lpad, i32, "lpad");
                make_scalar_function(func)(args)
            }
            DataType::LargeUtf8 => {
                let func = invoke_if_unicode_expressions_feature_flag!(lpad, i64, "lpad");
                make_scalar_function(func)(args)
            }
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function lpad",
                other,
            ))),
        },
        BuiltinScalarFunction::Ltrim => |args| match args[0].data_type() {
            DataType::Utf8 => {
                make_scalar_function(string_expressions::ltrim::<i32>)(args)
            }
            DataType::LargeUtf8 => {
                make_scalar_function(string_expressions::ltrim::<i64>)(args)
            }
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function ltrim",
                other,
            ))),
        },
        BuiltinScalarFunction::MD5 => {
            invoke_if_crypto_expressions_feature_flag!(md5, "md5")
        }
        BuiltinScalarFunction::NullIf => nullif_func,
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
        BuiltinScalarFunction::RegexpMatch => |args| match args[0].data_type() {
            DataType::Utf8 => {
                let func = invoke_if_regex_expressions_feature_flag!(
                    regexp_match,
                    i32,
                    "regexp_match"
                );
                make_scalar_function(func)(args)
            }
            DataType::LargeUtf8 => {
                let func = invoke_if_regex_expressions_feature_flag!(
                    regexp_match,
                    i64,
                    "regexp_match"
                );
                make_scalar_function(func)(args)
            }
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function regexp_match",
                other
            ))),
        },
        BuiltinScalarFunction::RegexpReplace => |args| match args[0].data_type() {
            DataType::Utf8 => {
                let func = invoke_if_regex_expressions_feature_flag!(
                    regexp_replace,
                    i32,
                    "regexp_replace"
                );
                make_scalar_function(func)(args)
            }
            DataType::LargeUtf8 => {
                let func = invoke_if_regex_expressions_feature_flag!(
                    regexp_replace,
                    i64,
                    "regexp_replace"
                );
                make_scalar_function(func)(args)
            }
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function regexp_replace",
                other,
            ))),
        },
        BuiltinScalarFunction::Repeat => |args| match args[0].data_type() {
            DataType::Utf8 => {
                make_scalar_function(string_expressions::repeat::<i32>)(args)
            }
            DataType::LargeUtf8 => {
                make_scalar_function(string_expressions::repeat::<i64>)(args)
            }
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function repeat",
                other,
            ))),
        },
        BuiltinScalarFunction::Replace => |args| match args[0].data_type() {
            DataType::Utf8 => {
                make_scalar_function(string_expressions::replace::<i32>)(args)
            }
            DataType::LargeUtf8 => {
                make_scalar_function(string_expressions::replace::<i64>)(args)
            }
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function replace",
                other,
            ))),
        },
        BuiltinScalarFunction::Reverse => |args| match args[0].data_type() {
            DataType::Utf8 => {
                let func =
                    invoke_if_unicode_expressions_feature_flag!(reverse, i32, "reverse");
                make_scalar_function(func)(args)
            }
            DataType::LargeUtf8 => {
                let func =
                    invoke_if_unicode_expressions_feature_flag!(reverse, i64, "reverse");
                make_scalar_function(func)(args)
            }
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function reverse",
                other,
            ))),
        },
        BuiltinScalarFunction::Right => |args| match args[0].data_type() {
            DataType::Utf8 => {
                let func =
                    invoke_if_unicode_expressions_feature_flag!(right, i32, "right");
                make_scalar_function(func)(args)
            }
            DataType::LargeUtf8 => {
                let func =
                    invoke_if_unicode_expressions_feature_flag!(right, i64, "right");
                make_scalar_function(func)(args)
            }
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function right",
                other,
            ))),
        },
        BuiltinScalarFunction::Rpad => |args| match args[0].data_type() {
            DataType::Utf8 => {
                let func = invoke_if_unicode_expressions_feature_flag!(rpad, i32, "rpad");
                make_scalar_function(func)(args)
            }
            DataType::LargeUtf8 => {
                let func = invoke_if_unicode_expressions_feature_flag!(rpad, i64, "rpad");
                make_scalar_function(func)(args)
            }
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function rpad",
                other,
            ))),
        },
        BuiltinScalarFunction::Rtrim => |args| match args[0].data_type() {
            DataType::Utf8 => {
                make_scalar_function(string_expressions::rtrim::<i32>)(args)
            }
            DataType::LargeUtf8 => {
                make_scalar_function(string_expressions::rtrim::<i64>)(args)
            }
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function rtrim",
                other,
            ))),
        },
        BuiltinScalarFunction::SHA224 => {
            invoke_if_crypto_expressions_feature_flag!(sha224, "sha224")
        }
        BuiltinScalarFunction::SHA256 => {
            invoke_if_crypto_expressions_feature_flag!(sha256, "sha256")
        }
        BuiltinScalarFunction::SHA384 => {
            invoke_if_crypto_expressions_feature_flag!(sha384, "sha384")
        }
        BuiltinScalarFunction::SHA512 => {
            invoke_if_crypto_expressions_feature_flag!(sha512, "sha512")
        }
        BuiltinScalarFunction::SplitPart => |args| match args[0].data_type() {
            DataType::Utf8 => {
                make_scalar_function(string_expressions::split_part::<i32>)(args)
            }
            DataType::LargeUtf8 => {
                make_scalar_function(string_expressions::split_part::<i64>)(args)
            }
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function split_part",
                other,
            ))),
        },
        BuiltinScalarFunction::StartsWith => |args| match args[0].data_type() {
            DataType::Utf8 => {
                make_scalar_function(string_expressions::starts_with::<i32>)(args)
            }
            DataType::LargeUtf8 => {
                make_scalar_function(string_expressions::starts_with::<i64>)(args)
            }
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function starts_with",
                other,
            ))),
        },
        BuiltinScalarFunction::Strpos => |args| match args[0].data_type() {
            DataType::Utf8 => {
                let func = invoke_if_unicode_expressions_feature_flag!(
                    strpos, Int32Type, "strpos"
                );
                make_scalar_function(func)(args)
            }
            DataType::LargeUtf8 => {
                let func = invoke_if_unicode_expressions_feature_flag!(
                    strpos, Int64Type, "strpos"
                );
                make_scalar_function(func)(args)
            }
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function strpos",
                other,
            ))),
        },
        BuiltinScalarFunction::Substr => |args| match args[0].data_type() {
            DataType::Utf8 => {
                let func =
                    invoke_if_unicode_expressions_feature_flag!(substr, i32, "substr");
                make_scalar_function(func)(args)
            }
            DataType::LargeUtf8 => {
                let func =
                    invoke_if_unicode_expressions_feature_flag!(substr, i64, "substr");
                make_scalar_function(func)(args)
            }
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function substr",
                other,
            ))),
        },
        BuiltinScalarFunction::ToHex => |args| match args[0].data_type() {
            DataType::Int32 => {
                make_scalar_function(string_expressions::to_hex::<Int32Type>)(args)
            }
            DataType::Int64 => {
                make_scalar_function(string_expressions::to_hex::<Int64Type>)(args)
            }
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function to_hex",
                other,
            ))),
        },
        BuiltinScalarFunction::ToTimestamp => datetime_expressions::to_timestamp,
        BuiltinScalarFunction::Translate => |args| match args[0].data_type() {
            DataType::Utf8 => {
                let func = invoke_if_unicode_expressions_feature_flag!(
                    translate,
                    i32,
                    "translate"
                );
                make_scalar_function(func)(args)
            }
            DataType::LargeUtf8 => {
                let func = invoke_if_unicode_expressions_feature_flag!(
                    translate,
                    i64,
                    "translate"
                );
                make_scalar_function(func)(args)
            }
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function translate",
                other,
            ))),
        },
        BuiltinScalarFunction::Trim => |args| match args[0].data_type() {
            DataType::Utf8 => {
                make_scalar_function(string_expressions::btrim::<i32>)(args)
            }
            DataType::LargeUtf8 => {
                make_scalar_function(string_expressions::btrim::<i64>)(args)
            }
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function trim",
                other,
            ))),
        },
        BuiltinScalarFunction::Upper => string_expressions::upper,
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
        BuiltinScalarFunction::Array => {
            Signature::Variadic(array_expressions::SUPPORTED_ARRAY_TYPES.to_vec())
        }
        BuiltinScalarFunction::Concat | BuiltinScalarFunction::ConcatWithSeparator => {
            Signature::Variadic(vec![DataType::Utf8])
        }
        BuiltinScalarFunction::Ascii
        | BuiltinScalarFunction::BitLength
        | BuiltinScalarFunction::CharacterLength
        | BuiltinScalarFunction::InitCap
        | BuiltinScalarFunction::Lower
        | BuiltinScalarFunction::MD5
        | BuiltinScalarFunction::OctetLength
        | BuiltinScalarFunction::Reverse
        | BuiltinScalarFunction::SHA224
        | BuiltinScalarFunction::SHA256
        | BuiltinScalarFunction::SHA384
        | BuiltinScalarFunction::SHA512
        | BuiltinScalarFunction::Trim
        | BuiltinScalarFunction::Upper => {
            Signature::Uniform(1, vec![DataType::Utf8, DataType::LargeUtf8])
        }
        BuiltinScalarFunction::Btrim
        | BuiltinScalarFunction::Ltrim
        | BuiltinScalarFunction::Rtrim => Signature::OneOf(vec![
            Signature::Exact(vec![DataType::Utf8]),
            Signature::Exact(vec![DataType::Utf8, DataType::Utf8]),
        ]),
        BuiltinScalarFunction::Chr | BuiltinScalarFunction::ToHex => {
            Signature::Uniform(1, vec![DataType::Int64])
        }
        BuiltinScalarFunction::Lpad | BuiltinScalarFunction::Rpad => {
            Signature::OneOf(vec![
                Signature::Exact(vec![DataType::Utf8, DataType::Int64]),
                Signature::Exact(vec![DataType::LargeUtf8, DataType::Int64]),
                Signature::Exact(vec![DataType::Utf8, DataType::Int64, DataType::Utf8]),
                Signature::Exact(vec![
                    DataType::LargeUtf8,
                    DataType::Int64,
                    DataType::Utf8,
                ]),
                Signature::Exact(vec![
                    DataType::Utf8,
                    DataType::Int64,
                    DataType::LargeUtf8,
                ]),
                Signature::Exact(vec![
                    DataType::LargeUtf8,
                    DataType::Int64,
                    DataType::LargeUtf8,
                ]),
            ])
        }
        BuiltinScalarFunction::Left
        | BuiltinScalarFunction::Repeat
        | BuiltinScalarFunction::Right => Signature::OneOf(vec![
            Signature::Exact(vec![DataType::Utf8, DataType::Int64]),
            Signature::Exact(vec![DataType::LargeUtf8, DataType::Int64]),
        ]),
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
        BuiltinScalarFunction::SplitPart => Signature::OneOf(vec![
            Signature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Int64]),
            Signature::Exact(vec![DataType::LargeUtf8, DataType::Utf8, DataType::Int64]),
            Signature::Exact(vec![DataType::Utf8, DataType::LargeUtf8, DataType::Int64]),
            Signature::Exact(vec![
                DataType::LargeUtf8,
                DataType::LargeUtf8,
                DataType::Int64,
            ]),
        ]),

        BuiltinScalarFunction::Strpos | BuiltinScalarFunction::StartsWith => {
            Signature::OneOf(vec![
                Signature::Exact(vec![DataType::Utf8, DataType::Utf8]),
                Signature::Exact(vec![DataType::Utf8, DataType::LargeUtf8]),
                Signature::Exact(vec![DataType::LargeUtf8, DataType::Utf8]),
                Signature::Exact(vec![DataType::LargeUtf8, DataType::LargeUtf8]),
            ])
        }

        BuiltinScalarFunction::Substr => Signature::OneOf(vec![
            Signature::Exact(vec![DataType::Utf8, DataType::Int64]),
            Signature::Exact(vec![DataType::LargeUtf8, DataType::Int64]),
            Signature::Exact(vec![DataType::Utf8, DataType::Int64, DataType::Int64]),
            Signature::Exact(vec![DataType::LargeUtf8, DataType::Int64, DataType::Int64]),
        ]),

        BuiltinScalarFunction::Replace | BuiltinScalarFunction::Translate => {
            Signature::OneOf(vec![Signature::Exact(vec![
                DataType::Utf8,
                DataType::Utf8,
                DataType::Utf8,
            ])])
        }
        BuiltinScalarFunction::RegexpReplace => Signature::OneOf(vec![
            Signature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Utf8]),
            Signature::Exact(vec![
                DataType::Utf8,
                DataType::Utf8,
                DataType::Utf8,
                DataType::Utf8,
            ]),
        ]),

        BuiltinScalarFunction::NullIf => {
            Signature::Uniform(2, SUPPORTED_NULLIF_TYPES.to_vec())
        }
        BuiltinScalarFunction::RegexpMatch => Signature::OneOf(vec![
            Signature::Exact(vec![DataType::Utf8, DataType::Utf8]),
            Signature::Exact(vec![DataType::LargeUtf8, DataType::Utf8]),
            Signature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Utf8]),
            Signature::Exact(vec![DataType::LargeUtf8, DataType::Utf8, DataType::Utf8]),
        ]),
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
            Array, ArrayRef, BinaryArray, BooleanArray, FixedSizeListArray, Float64Array,
            Int32Array, ListArray, StringArray, UInt32Array, UInt64Array,
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
            Ascii,
            &[lit(ScalarValue::Utf8(Some("x".to_string())))],
            Ok(Some(120)),
            i32,
            Int32,
            Int32Array
        );
        test_function!(
            Ascii,
            &[lit(ScalarValue::Utf8(Some("ésoj".to_string())))],
            Ok(Some(233)),
            i32,
            Int32,
            Int32Array
        );
        test_function!(
            Ascii,
            &[lit(ScalarValue::Utf8(Some("💯".to_string())))],
            Ok(Some(128175)),
            i32,
            Int32,
            Int32Array
        );
        test_function!(
            Ascii,
            &[lit(ScalarValue::Utf8(Some("💯a".to_string())))],
            Ok(Some(128175)),
            i32,
            Int32,
            Int32Array
        );
        test_function!(
            Ascii,
            &[lit(ScalarValue::Utf8(Some("".to_string())))],
            Ok(Some(0)),
            i32,
            Int32,
            Int32Array
        );
        test_function!(
            Ascii,
            &[lit(ScalarValue::Utf8(None))],
            Ok(None),
            i32,
            Int32,
            Int32Array
        );
        test_function!(
            BitLength,
            &[lit(ScalarValue::Utf8(Some("chars".to_string())))],
            Ok(Some(40)),
            i32,
            Int32,
            Int32Array
        );
        test_function!(
            BitLength,
            &[lit(ScalarValue::Utf8(Some("josé".to_string())))],
            Ok(Some(40)),
            i32,
            Int32,
            Int32Array
        );
        test_function!(
            BitLength,
            &[lit(ScalarValue::Utf8(Some("".to_string())))],
            Ok(Some(0)),
            i32,
            Int32,
            Int32Array
        );
        test_function!(
            Btrim,
            &[lit(ScalarValue::Utf8(Some(" trim ".to_string())))],
            Ok(Some("trim")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Btrim,
            &[lit(ScalarValue::Utf8(Some(" trim".to_string())))],
            Ok(Some("trim")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Btrim,
            &[lit(ScalarValue::Utf8(Some("trim ".to_string())))],
            Ok(Some("trim")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Btrim,
            &[lit(ScalarValue::Utf8(Some("\n trim \n".to_string())))],
            Ok(Some("\n trim \n")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Btrim,
            &[
                lit(ScalarValue::Utf8(Some("xyxtrimyyx".to_string()))),
                lit(ScalarValue::Utf8(Some("xyz".to_string()))),
            ],
            Ok(Some("trim")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Btrim,
            &[
                lit(ScalarValue::Utf8(Some("\nxyxtrimyyx\n".to_string()))),
                lit(ScalarValue::Utf8(Some("xyz\n".to_string()))),
            ],
            Ok(Some("trim")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Btrim,
            &[
                lit(ScalarValue::Utf8(None)),
                lit(ScalarValue::Utf8(Some("xyz".to_string()))),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Btrim,
            &[
                lit(ScalarValue::Utf8(Some("xyxtrimyyx".to_string()))),
                lit(ScalarValue::Utf8(None)),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            CharacterLength,
            &[lit(ScalarValue::Utf8(Some("chars".to_string())))],
            Ok(Some(5)),
            i32,
            Int32,
            Int32Array
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            CharacterLength,
            &[lit(ScalarValue::Utf8(Some("josé".to_string())))],
            Ok(Some(4)),
            i32,
            Int32,
            Int32Array
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            CharacterLength,
            &[lit(ScalarValue::Utf8(Some("".to_string())))],
            Ok(Some(0)),
            i32,
            Int32,
            Int32Array
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            CharacterLength,
            &[lit(ScalarValue::Utf8(None))],
            Ok(None),
            i32,
            Int32,
            Int32Array
        );
        #[cfg(not(feature = "unicode_expressions"))]
        test_function!(
            CharacterLength,
            &[lit(ScalarValue::Utf8(Some("josé".to_string())))],
            Err(DataFusionError::Internal(
                "function character_length requires compilation with feature flag: unicode_expressions.".to_string()
            )),
            i32,
            Int32,
            Int32Array
        );
        test_function!(
            Chr,
            &[lit(ScalarValue::Int64(Some(128175)))],
            Ok(Some("💯")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Chr,
            &[lit(ScalarValue::Int64(None))],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Chr,
            &[lit(ScalarValue::Int64(Some(120)))],
            Ok(Some("x")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Chr,
            &[lit(ScalarValue::Int64(Some(128175)))],
            Ok(Some("💯")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Chr,
            &[lit(ScalarValue::Int64(None))],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Chr,
            &[lit(ScalarValue::Int64(Some(0)))],
            Err(DataFusionError::Execution(
                "null character not permitted.".to_string(),
            )),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Chr,
            &[lit(ScalarValue::Int64(Some(i64::MAX)))],
            Err(DataFusionError::Execution(
                "requested character too large for encoding.".to_string(),
            )),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Concat,
            &[
                lit(ScalarValue::Utf8(Some("aa".to_string()))),
                lit(ScalarValue::Utf8(Some("bb".to_string()))),
                lit(ScalarValue::Utf8(Some("cc".to_string()))),
            ],
            Ok(Some("aabbcc")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Concat,
            &[
                lit(ScalarValue::Utf8(Some("aa".to_string()))),
                lit(ScalarValue::Utf8(None)),
                lit(ScalarValue::Utf8(Some("cc".to_string()))),
            ],
            Ok(Some("aacc")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Concat,
            &[lit(ScalarValue::Utf8(None))],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            ConcatWithSeparator,
            &[
                lit(ScalarValue::Utf8(Some("|".to_string()))),
                lit(ScalarValue::Utf8(Some("aa".to_string()))),
                lit(ScalarValue::Utf8(Some("bb".to_string()))),
                lit(ScalarValue::Utf8(Some("cc".to_string()))),
            ],
            Ok(Some("aa|bb|cc")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            ConcatWithSeparator,
            &[
                lit(ScalarValue::Utf8(Some("|".to_string()))),
                lit(ScalarValue::Utf8(None)),
            ],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            ConcatWithSeparator,
            &[
                lit(ScalarValue::Utf8(None)),
                lit(ScalarValue::Utf8(Some("aa".to_string()))),
                lit(ScalarValue::Utf8(Some("bb".to_string()))),
                lit(ScalarValue::Utf8(Some("cc".to_string()))),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            ConcatWithSeparator,
            &[
                lit(ScalarValue::Utf8(Some("|".to_string()))),
                lit(ScalarValue::Utf8(Some("aa".to_string()))),
                lit(ScalarValue::Utf8(None)),
                lit(ScalarValue::Utf8(Some("cc".to_string()))),
            ],
            Ok(Some("aa|cc")),
            &str,
            Utf8,
            StringArray
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
        test_function!(
            InitCap,
            &[lit(ScalarValue::Utf8(Some("hi THOMAS".to_string())))],
            Ok(Some("Hi Thomas")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            InitCap,
            &[lit(ScalarValue::Utf8(Some("".to_string())))],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            InitCap,
            &[lit(ScalarValue::Utf8(Some("".to_string())))],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            InitCap,
            &[lit(ScalarValue::Utf8(None))],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Left,
            &[
                lit(ScalarValue::Utf8(Some("abcde".to_string()))),
                lit(ScalarValue::Int8(Some(2))),
            ],
            Ok(Some("ab")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Left,
            &[
                lit(ScalarValue::Utf8(Some("abcde".to_string()))),
                lit(ScalarValue::Int64(Some(200))),
            ],
            Ok(Some("abcde")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Left,
            &[
                lit(ScalarValue::Utf8(Some("abcde".to_string()))),
                lit(ScalarValue::Int64(Some(-2))),
            ],
            Ok(Some("abc")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Left,
            &[
                lit(ScalarValue::Utf8(Some("abcde".to_string()))),
                lit(ScalarValue::Int64(Some(-200))),
            ],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Left,
            &[
                lit(ScalarValue::Utf8(Some("abcde".to_string()))),
                lit(ScalarValue::Int64(Some(0))),
            ],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Left,
            &[
                lit(ScalarValue::Utf8(None)),
                lit(ScalarValue::Int64(Some(2))),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Left,
            &[
                lit(ScalarValue::Utf8(Some("abcde".to_string()))),
                lit(ScalarValue::Int64(None)),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Left,
            &[
                lit(ScalarValue::Utf8(Some("joséésoj".to_string()))),
                lit(ScalarValue::Int64(Some(5))),
            ],
            Ok(Some("joséé")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Left,
            &[
                lit(ScalarValue::Utf8(Some("joséésoj".to_string()))),
                lit(ScalarValue::Int64(Some(-3))),
            ],
            Ok(Some("joséé")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(not(feature = "unicode_expressions"))]
        test_function!(
            Left,
            &[
                lit(ScalarValue::Utf8(Some("abcde".to_string()))),
                lit(ScalarValue::Int8(Some(2))),
            ],
            Err(DataFusionError::Internal(
                "function left requires compilation with feature flag: unicode_expressions.".to_string()
            )),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Lpad,
            &[
                lit(ScalarValue::Utf8(Some("josé".to_string()))),
                lit(ScalarValue::Int64(Some(5))),
            ],
            Ok(Some(" josé")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Lpad,
            &[
                lit(ScalarValue::Utf8(Some("hi".to_string()))),
                lit(ScalarValue::Int64(Some(5))),
            ],
            Ok(Some("   hi")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Lpad,
            &[
                lit(ScalarValue::Utf8(Some("hi".to_string()))),
                lit(ScalarValue::Int64(Some(0))),
            ],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Lpad,
            &[
                lit(ScalarValue::Utf8(Some("hi".to_string()))),
                lit(ScalarValue::Int64(None)),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Lpad,
            &[
                lit(ScalarValue::Utf8(None)),
                lit(ScalarValue::Int64(Some(5))),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Lpad,
            &[
                lit(ScalarValue::Utf8(Some("hi".to_string()))),
                lit(ScalarValue::Int64(Some(5))),
                lit(ScalarValue::Utf8(Some("xy".to_string()))),
            ],
            Ok(Some("xyxhi")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Lpad,
            &[
                lit(ScalarValue::Utf8(Some("hi".to_string()))),
                lit(ScalarValue::Int64(Some(21))),
                lit(ScalarValue::Utf8(Some("abcdef".to_string()))),
            ],
            Ok(Some("abcdefabcdefabcdefahi")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Lpad,
            &[
                lit(ScalarValue::Utf8(Some("hi".to_string()))),
                lit(ScalarValue::Int64(Some(5))),
                lit(ScalarValue::Utf8(Some(" ".to_string()))),
            ],
            Ok(Some("   hi")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Lpad,
            &[
                lit(ScalarValue::Utf8(Some("hi".to_string()))),
                lit(ScalarValue::Int64(Some(5))),
                lit(ScalarValue::Utf8(Some("".to_string()))),
            ],
            Ok(Some("hi")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Lpad,
            &[
                lit(ScalarValue::Utf8(None)),
                lit(ScalarValue::Int64(Some(5))),
                lit(ScalarValue::Utf8(Some("xy".to_string()))),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Lpad,
            &[
                lit(ScalarValue::Utf8(Some("hi".to_string()))),
                lit(ScalarValue::Int64(None)),
                lit(ScalarValue::Utf8(Some("xy".to_string()))),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Lpad,
            &[
                lit(ScalarValue::Utf8(Some("hi".to_string()))),
                lit(ScalarValue::Int64(Some(5))),
                lit(ScalarValue::Utf8(None)),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Lpad,
            &[
                lit(ScalarValue::Utf8(Some("josé".to_string()))),
                lit(ScalarValue::Int64(Some(10))),
                lit(ScalarValue::Utf8(Some("xy".to_string()))),
            ],
            Ok(Some("xyxyxyjosé")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Lpad,
            &[
                lit(ScalarValue::Utf8(Some("josé".to_string()))),
                lit(ScalarValue::Int64(Some(10))),
                lit(ScalarValue::Utf8(Some("éñ".to_string()))),
            ],
            Ok(Some("éñéñéñjosé")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(not(feature = "unicode_expressions"))]
        test_function!(
            Lpad,
            &[
                lit(ScalarValue::Utf8(Some("josé".to_string()))),
                lit(ScalarValue::Int64(Some(5))),
            ],
            Err(DataFusionError::Internal(
                "function lpad requires compilation with feature flag: unicode_expressions.".to_string()
            )),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Ltrim,
            &[lit(ScalarValue::Utf8(Some(" trim".to_string())))],
            Ok(Some("trim")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Ltrim,
            &[lit(ScalarValue::Utf8(Some(" trim ".to_string())))],
            Ok(Some("trim ")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Ltrim,
            &[lit(ScalarValue::Utf8(Some("trim ".to_string())))],
            Ok(Some("trim ")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Ltrim,
            &[lit(ScalarValue::Utf8(Some("trim".to_string())))],
            Ok(Some("trim")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Ltrim,
            &[lit(ScalarValue::Utf8(Some("\n trim ".to_string())))],
            Ok(Some("\n trim ")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Ltrim,
            &[lit(ScalarValue::Utf8(None))],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "crypto_expressions")]
        test_function!(
            MD5,
            &[lit(ScalarValue::Utf8(Some("tom".to_string())))],
            Ok(Some("34b7da764b21d298ef307d04d8152dc5")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "crypto_expressions")]
        test_function!(
            MD5,
            &[lit(ScalarValue::Utf8(Some("".to_string())))],
            Ok(Some("d41d8cd98f00b204e9800998ecf8427e")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "crypto_expressions")]
        test_function!(
            MD5,
            &[lit(ScalarValue::Utf8(None))],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(not(feature = "crypto_expressions"))]
        test_function!(
            MD5,
            &[lit(ScalarValue::Utf8(Some("tom".to_string())))],
            Err(DataFusionError::Internal(
                "function md5 requires compilation with feature flag: crypto_expressions.".to_string()
            )),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            OctetLength,
            &[lit(ScalarValue::Utf8(Some("chars".to_string())))],
            Ok(Some(5)),
            i32,
            Int32,
            Int32Array
        );
        test_function!(
            OctetLength,
            &[lit(ScalarValue::Utf8(Some("josé".to_string())))],
            Ok(Some(5)),
            i32,
            Int32,
            Int32Array
        );
        test_function!(
            OctetLength,
            &[lit(ScalarValue::Utf8(Some("".to_string())))],
            Ok(Some(0)),
            i32,
            Int32,
            Int32Array
        );
        test_function!(
            OctetLength,
            &[lit(ScalarValue::Utf8(None))],
            Ok(None),
            i32,
            Int32,
            Int32Array
        );
        #[cfg(feature = "regex_expressions")]
        test_function!(
            RegexpReplace,
            &[
                lit(ScalarValue::Utf8(Some("Thomas".to_string()))),
                lit(ScalarValue::Utf8(Some(".[mN]a.".to_string()))),
                lit(ScalarValue::Utf8(Some("M".to_string()))),
            ],
            Ok(Some("ThM")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "regex_expressions")]
        test_function!(
            RegexpReplace,
            &[
                lit(ScalarValue::Utf8(Some("foobarbaz".to_string()))),
                lit(ScalarValue::Utf8(Some("b..".to_string()))),
                lit(ScalarValue::Utf8(Some("X".to_string()))),
            ],
            Ok(Some("fooXbaz")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "regex_expressions")]
        test_function!(
            RegexpReplace,
            &[
                lit(ScalarValue::Utf8(Some("foobarbaz".to_string()))),
                lit(ScalarValue::Utf8(Some("b..".to_string()))),
                lit(ScalarValue::Utf8(Some("X".to_string()))),
                lit(ScalarValue::Utf8(Some("g".to_string()))),
            ],
            Ok(Some("fooXX")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "regex_expressions")]
        test_function!(
            RegexpReplace,
            &[
                lit(ScalarValue::Utf8(Some("foobarbaz".to_string()))),
                lit(ScalarValue::Utf8(Some("b(..)".to_string()))),
                lit(ScalarValue::Utf8(Some("X\\1Y".to_string()))),
                lit(ScalarValue::Utf8(Some("g".to_string()))),
            ],
            Ok(Some("fooXarYXazY")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "regex_expressions")]
        test_function!(
            RegexpReplace,
            &[
                lit(ScalarValue::Utf8(None)),
                lit(ScalarValue::Utf8(Some("b(..)".to_string()))),
                lit(ScalarValue::Utf8(Some("X\\1Y".to_string()))),
                lit(ScalarValue::Utf8(Some("g".to_string()))),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "regex_expressions")]
        test_function!(
            RegexpReplace,
            &[
                lit(ScalarValue::Utf8(Some("foobarbaz".to_string()))),
                lit(ScalarValue::Utf8(None)),
                lit(ScalarValue::Utf8(Some("X\\1Y".to_string()))),
                lit(ScalarValue::Utf8(Some("g".to_string()))),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "regex_expressions")]
        test_function!(
            RegexpReplace,
            &[
                lit(ScalarValue::Utf8(Some("foobarbaz".to_string()))),
                lit(ScalarValue::Utf8(Some("b(..)".to_string()))),
                lit(ScalarValue::Utf8(None)),
                lit(ScalarValue::Utf8(Some("g".to_string()))),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "regex_expressions")]
        test_function!(
            RegexpReplace,
            &[
                lit(ScalarValue::Utf8(Some("foobarbaz".to_string()))),
                lit(ScalarValue::Utf8(Some("b(..)".to_string()))),
                lit(ScalarValue::Utf8(Some("X\\1Y".to_string()))),
                lit(ScalarValue::Utf8(None)),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "regex_expressions")]
        test_function!(
            RegexpReplace,
            &[
                lit(ScalarValue::Utf8(Some("ABCabcABC".to_string()))),
                lit(ScalarValue::Utf8(Some("(abc)".to_string()))),
                lit(ScalarValue::Utf8(Some("X".to_string()))),
                lit(ScalarValue::Utf8(Some("gi".to_string()))),
            ],
            Ok(Some("XXX")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "regex_expressions")]
        test_function!(
            RegexpReplace,
            &[
                lit(ScalarValue::Utf8(Some("ABCabcABC".to_string()))),
                lit(ScalarValue::Utf8(Some("(abc)".to_string()))),
                lit(ScalarValue::Utf8(Some("X".to_string()))),
                lit(ScalarValue::Utf8(Some("i".to_string()))),
            ],
            Ok(Some("XabcABC")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(not(feature = "regex_expressions"))]
        test_function!(
            RegexpReplace,
            &[
                lit(ScalarValue::Utf8(Some("foobarbaz".to_string()))),
                lit(ScalarValue::Utf8(Some("b..".to_string()))),
                lit(ScalarValue::Utf8(Some("X".to_string()))),
            ],
            Err(DataFusionError::Internal(
                "function regexp_replace requires compilation with feature flag: regex_expressions.".to_string()
            )),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Repeat,
            &[
                lit(ScalarValue::Utf8(Some("Pg".to_string()))),
                lit(ScalarValue::Int64(Some(4))),
            ],
            Ok(Some("PgPgPgPg")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Repeat,
            &[
                lit(ScalarValue::Utf8(None)),
                lit(ScalarValue::Int64(Some(4))),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Repeat,
            &[
                lit(ScalarValue::Utf8(Some("Pg".to_string()))),
                lit(ScalarValue::Int64(None)),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Reverse,
            &[lit(ScalarValue::Utf8(Some("abcde".to_string())))],
            Ok(Some("edcba")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Reverse,
            &[lit(ScalarValue::Utf8(Some("loẅks".to_string())))],
            Ok(Some("skẅol")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Reverse,
            &[lit(ScalarValue::Utf8(Some("loẅks".to_string())))],
            Ok(Some("skẅol")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Reverse,
            &[lit(ScalarValue::Utf8(None))],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(not(feature = "unicode_expressions"))]
        test_function!(
            Reverse,
            &[lit(ScalarValue::Utf8(Some("abcde".to_string())))],
            Err(DataFusionError::Internal(
                "function reverse requires compilation with feature flag: unicode_expressions.".to_string()
            )),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Right,
            &[
                lit(ScalarValue::Utf8(Some("abcde".to_string()))),
                lit(ScalarValue::Int8(Some(2))),
            ],
            Ok(Some("de")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Right,
            &[
                lit(ScalarValue::Utf8(Some("abcde".to_string()))),
                lit(ScalarValue::Int64(Some(200))),
            ],
            Ok(Some("abcde")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Right,
            &[
                lit(ScalarValue::Utf8(Some("abcde".to_string()))),
                lit(ScalarValue::Int64(Some(-2))),
            ],
            Ok(Some("cde")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Right,
            &[
                lit(ScalarValue::Utf8(Some("abcde".to_string()))),
                lit(ScalarValue::Int64(Some(-200))),
            ],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Right,
            &[
                lit(ScalarValue::Utf8(Some("abcde".to_string()))),
                lit(ScalarValue::Int64(Some(0))),
            ],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Right,
            &[
                lit(ScalarValue::Utf8(None)),
                lit(ScalarValue::Int64(Some(2))),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Right,
            &[
                lit(ScalarValue::Utf8(Some("abcde".to_string()))),
                lit(ScalarValue::Int64(None)),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Right,
            &[
                lit(ScalarValue::Utf8(Some("joséésoj".to_string()))),
                lit(ScalarValue::Int64(Some(5))),
            ],
            Ok(Some("éésoj")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Right,
            &[
                lit(ScalarValue::Utf8(Some("joséésoj".to_string()))),
                lit(ScalarValue::Int64(Some(-3))),
            ],
            Ok(Some("éésoj")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(not(feature = "unicode_expressions"))]
        test_function!(
            Right,
            &[
                lit(ScalarValue::Utf8(Some("abcde".to_string()))),
                lit(ScalarValue::Int8(Some(2))),
            ],
            Err(DataFusionError::Internal(
                "function right requires compilation with feature flag: unicode_expressions.".to_string()
            )),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Rpad,
            &[
                lit(ScalarValue::Utf8(Some("josé".to_string()))),
                lit(ScalarValue::Int64(Some(5))),
            ],
            Ok(Some("josé ")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Rpad,
            &[
                lit(ScalarValue::Utf8(Some("hi".to_string()))),
                lit(ScalarValue::Int64(Some(5))),
            ],
            Ok(Some("hi   ")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Rpad,
            &[
                lit(ScalarValue::Utf8(Some("hi".to_string()))),
                lit(ScalarValue::Int64(Some(0))),
            ],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Rpad,
            &[
                lit(ScalarValue::Utf8(Some("hi".to_string()))),
                lit(ScalarValue::Int64(None)),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Rpad,
            &[
                lit(ScalarValue::Utf8(None)),
                lit(ScalarValue::Int64(Some(5))),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Rpad,
            &[
                lit(ScalarValue::Utf8(Some("hi".to_string()))),
                lit(ScalarValue::Int64(Some(5))),
                lit(ScalarValue::Utf8(Some("xy".to_string()))),
            ],
            Ok(Some("hixyx")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Rpad,
            &[
                lit(ScalarValue::Utf8(Some("hi".to_string()))),
                lit(ScalarValue::Int64(Some(21))),
                lit(ScalarValue::Utf8(Some("abcdef".to_string()))),
            ],
            Ok(Some("hiabcdefabcdefabcdefa")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Rpad,
            &[
                lit(ScalarValue::Utf8(Some("hi".to_string()))),
                lit(ScalarValue::Int64(Some(5))),
                lit(ScalarValue::Utf8(Some(" ".to_string()))),
            ],
            Ok(Some("hi   ")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Rpad,
            &[
                lit(ScalarValue::Utf8(Some("hi".to_string()))),
                lit(ScalarValue::Int64(Some(5))),
                lit(ScalarValue::Utf8(Some("".to_string()))),
            ],
            Ok(Some("hi")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Rpad,
            &[
                lit(ScalarValue::Utf8(None)),
                lit(ScalarValue::Int64(Some(5))),
                lit(ScalarValue::Utf8(Some("xy".to_string()))),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Rpad,
            &[
                lit(ScalarValue::Utf8(Some("hi".to_string()))),
                lit(ScalarValue::Int64(None)),
                lit(ScalarValue::Utf8(Some("xy".to_string()))),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Rpad,
            &[
                lit(ScalarValue::Utf8(Some("hi".to_string()))),
                lit(ScalarValue::Int64(Some(5))),
                lit(ScalarValue::Utf8(None)),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Rpad,
            &[
                lit(ScalarValue::Utf8(Some("josé".to_string()))),
                lit(ScalarValue::Int64(Some(10))),
                lit(ScalarValue::Utf8(Some("xy".to_string()))),
            ],
            Ok(Some("joséxyxyxy")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Rpad,
            &[
                lit(ScalarValue::Utf8(Some("josé".to_string()))),
                lit(ScalarValue::Int64(Some(10))),
                lit(ScalarValue::Utf8(Some("éñ".to_string()))),
            ],
            Ok(Some("josééñéñéñ")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(not(feature = "unicode_expressions"))]
        test_function!(
            Rpad,
            &[
                lit(ScalarValue::Utf8(Some("josé".to_string()))),
                lit(ScalarValue::Int64(Some(5))),
            ],
            Err(DataFusionError::Internal(
                "function rpad requires compilation with feature flag: unicode_expressions.".to_string()
            )),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Rtrim,
            &[lit(ScalarValue::Utf8(Some("trim ".to_string())))],
            Ok(Some("trim")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Rtrim,
            &[lit(ScalarValue::Utf8(Some(" trim ".to_string())))],
            Ok(Some(" trim")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Rtrim,
            &[lit(ScalarValue::Utf8(Some(" trim \n".to_string())))],
            Ok(Some(" trim \n")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Rtrim,
            &[lit(ScalarValue::Utf8(Some(" trim".to_string())))],
            Ok(Some(" trim")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Rtrim,
            &[lit(ScalarValue::Utf8(Some("trim".to_string())))],
            Ok(Some("trim")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Rtrim,
            &[lit(ScalarValue::Utf8(None))],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "crypto_expressions")]
        test_function!(
            SHA224,
            &[lit(ScalarValue::Utf8(Some("tom".to_string())))],
            Ok(Some(&[
                11u8, 246u8, 203u8, 98u8, 100u8, 156u8, 66u8, 169u8, 174u8, 56u8, 118u8,
                171u8, 111u8, 109u8, 146u8, 173u8, 54u8, 203u8, 84u8, 20u8, 228u8, 149u8,
                248u8, 135u8, 50u8, 146u8, 190u8, 77u8
            ])),
            &[u8],
            Binary,
            BinaryArray
        );
        #[cfg(feature = "crypto_expressions")]
        test_function!(
            SHA224,
            &[lit(ScalarValue::Utf8(Some("".to_string())))],
            Ok(Some(&[
                209u8, 74u8, 2u8, 140u8, 42u8, 58u8, 43u8, 201u8, 71u8, 97u8, 2u8, 187u8,
                40u8, 130u8, 52u8, 196u8, 21u8, 162u8, 176u8, 31u8, 130u8, 142u8, 166u8,
                42u8, 197u8, 179u8, 228u8, 47u8
            ])),
            &[u8],
            Binary,
            BinaryArray
        );
        #[cfg(feature = "crypto_expressions")]
        test_function!(
            SHA224,
            &[lit(ScalarValue::Utf8(None))],
            Ok(None),
            &[u8],
            Binary,
            BinaryArray
        );
        #[cfg(not(feature = "crypto_expressions"))]
        test_function!(
            SHA224,
            &[lit(ScalarValue::Utf8(Some("tom".to_string())))],
            Err(DataFusionError::Internal(
                "function sha224 requires compilation with feature flag: crypto_expressions.".to_string()
            )),
            &[u8],
            Binary,
            BinaryArray
        );
        #[cfg(feature = "crypto_expressions")]
        test_function!(
            SHA256,
            &[lit(ScalarValue::Utf8(Some("tom".to_string())))],
            Ok(Some(&[
                225u8, 96u8, 143u8, 117u8, 197u8, 215u8, 129u8, 63u8, 61u8, 64u8, 49u8,
                203u8, 48u8, 191u8, 183u8, 134u8, 80u8, 125u8, 152u8, 19u8, 117u8, 56u8,
                255u8, 142u8, 18u8, 138u8, 111u8, 247u8, 78u8, 132u8, 230u8, 67u8
            ])),
            &[u8],
            Binary,
            BinaryArray
        );
        #[cfg(feature = "crypto_expressions")]
        test_function!(
            SHA256,
            &[lit(ScalarValue::Utf8(Some("".to_string())))],
            Ok(Some(&[
                227u8, 176u8, 196u8, 66u8, 152u8, 252u8, 28u8, 20u8, 154u8, 251u8, 244u8,
                200u8, 153u8, 111u8, 185u8, 36u8, 39u8, 174u8, 65u8, 228u8, 100u8, 155u8,
                147u8, 76u8, 164u8, 149u8, 153u8, 27u8, 120u8, 82u8, 184u8, 85u8
            ])),
            &[u8],
            Binary,
            BinaryArray
        );
        #[cfg(feature = "crypto_expressions")]
        test_function!(
            SHA256,
            &[lit(ScalarValue::Utf8(None))],
            Ok(None),
            &[u8],
            Binary,
            BinaryArray
        );
        #[cfg(not(feature = "crypto_expressions"))]
        test_function!(
            SHA256,
            &[lit(ScalarValue::Utf8(Some("tom".to_string())))],
            Err(DataFusionError::Internal(
                "function sha256 requires compilation with feature flag: crypto_expressions.".to_string()
            )),
            &[u8],
            Binary,
            BinaryArray
        );
        #[cfg(feature = "crypto_expressions")]
        test_function!(
            SHA384,
            &[lit(ScalarValue::Utf8(Some("tom".to_string())))],
            Ok(Some(&[
                9u8, 111u8, 91u8, 104u8, 170u8, 119u8, 132u8, 142u8, 79u8, 223u8, 92u8,
                28u8, 11u8, 53u8, 13u8, 226u8, 219u8, 250u8, 214u8, 15u8, 253u8, 124u8,
                37u8, 217u8, 234u8, 7u8, 198u8, 193u8, 155u8, 138u8, 77u8, 85u8, 169u8,
                24u8, 126u8, 177u8, 23u8, 197u8, 87u8, 136u8, 63u8, 88u8, 193u8, 109u8,
                250u8, 195u8, 227u8, 67u8
            ])),
            &[u8],
            Binary,
            BinaryArray
        );
        #[cfg(feature = "crypto_expressions")]
        test_function!(
            SHA384,
            &[lit(ScalarValue::Utf8(Some("".to_string())))],
            Ok(Some(&[
                56u8, 176u8, 96u8, 167u8, 81u8, 172u8, 150u8, 56u8, 76u8, 217u8, 50u8,
                126u8, 177u8, 177u8, 227u8, 106u8, 33u8, 253u8, 183u8, 17u8, 20u8, 190u8,
                7u8, 67u8, 76u8, 12u8, 199u8, 191u8, 99u8, 246u8, 225u8, 218u8, 39u8,
                78u8, 222u8, 191u8, 231u8, 111u8, 101u8, 251u8, 213u8, 26u8, 210u8,
                241u8, 72u8, 152u8, 185u8, 91u8
            ])),
            &[u8],
            Binary,
            BinaryArray
        );
        #[cfg(feature = "crypto_expressions")]
        test_function!(
            SHA384,
            &[lit(ScalarValue::Utf8(None))],
            Ok(None),
            &[u8],
            Binary,
            BinaryArray
        );
        #[cfg(not(feature = "crypto_expressions"))]
        test_function!(
            SHA384,
            &[lit(ScalarValue::Utf8(Some("tom".to_string())))],
            Err(DataFusionError::Internal(
                "function sha384 requires compilation with feature flag: crypto_expressions.".to_string()
            )),
            &[u8],
            Binary,
            BinaryArray
        );
        #[cfg(feature = "crypto_expressions")]
        test_function!(
            SHA512,
            &[lit(ScalarValue::Utf8(Some("tom".to_string())))],
            Ok(Some(&[
                110u8, 27u8, 155u8, 63u8, 232u8, 64u8, 104u8, 14u8, 55u8, 5u8, 31u8,
                122u8, 213u8, 233u8, 89u8, 214u8, 243u8, 154u8, 208u8, 248u8, 136u8,
                93u8, 133u8, 81u8, 102u8, 245u8, 92u8, 101u8, 148u8, 105u8, 211u8, 200u8,
                183u8, 129u8, 24u8, 196u8, 74u8, 42u8, 73u8, 199u8, 45u8, 219u8, 72u8,
                28u8, 214u8, 216u8, 115u8, 16u8, 52u8, 225u8, 28u8, 192u8, 48u8, 7u8,
                11u8, 168u8, 67u8, 169u8, 11u8, 52u8, 149u8, 203u8, 141u8, 62u8
            ])),
            &[u8],
            Binary,
            BinaryArray
        );
        #[cfg(feature = "crypto_expressions")]
        test_function!(
            SHA512,
            &[lit(ScalarValue::Utf8(Some("".to_string())))],
            Ok(Some(&[
                207u8, 131u8, 225u8, 53u8, 126u8, 239u8, 184u8, 189u8, 241u8, 84u8, 40u8,
                80u8, 214u8, 109u8, 128u8, 7u8, 214u8, 32u8, 228u8, 5u8, 11u8, 87u8,
                21u8, 220u8, 131u8, 244u8, 169u8, 33u8, 211u8, 108u8, 233u8, 206u8, 71u8,
                208u8, 209u8, 60u8, 93u8, 133u8, 242u8, 176u8, 255u8, 131u8, 24u8, 210u8,
                135u8, 126u8, 236u8, 47u8, 99u8, 185u8, 49u8, 189u8, 71u8, 65u8, 122u8,
                129u8, 165u8, 56u8, 50u8, 122u8, 249u8, 39u8, 218u8, 62u8
            ])),
            &[u8],
            Binary,
            BinaryArray
        );
        #[cfg(feature = "crypto_expressions")]
        test_function!(
            SHA512,
            &[lit(ScalarValue::Utf8(None))],
            Ok(None),
            &[u8],
            Binary,
            BinaryArray
        );
        #[cfg(not(feature = "crypto_expressions"))]
        test_function!(
            SHA512,
            &[lit(ScalarValue::Utf8(Some("tom".to_string())))],
            Err(DataFusionError::Internal(
                "function sha512 requires compilation with feature flag: crypto_expressions.".to_string()
            )),
            &[u8],
            Binary,
            BinaryArray
        );
        test_function!(
            SplitPart,
            &[
                lit(ScalarValue::Utf8(Some("abc~@~def~@~ghi".to_string()))),
                lit(ScalarValue::Utf8(Some("~@~".to_string()))),
                lit(ScalarValue::Int64(Some(2))),
            ],
            Ok(Some("def")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SplitPart,
            &[
                lit(ScalarValue::Utf8(Some("abc~@~def~@~ghi".to_string()))),
                lit(ScalarValue::Utf8(Some("~@~".to_string()))),
                lit(ScalarValue::Int64(Some(20))),
            ],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SplitPart,
            &[
                lit(ScalarValue::Utf8(Some("abc~@~def~@~ghi".to_string()))),
                lit(ScalarValue::Utf8(Some("~@~".to_string()))),
                lit(ScalarValue::Int64(Some(-1))),
            ],
            Err(DataFusionError::Execution(
                "field position must be greater than zero".to_string(),
            )),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            StartsWith,
            &[
                lit(ScalarValue::Utf8(Some("alphabet".to_string()))),
                lit(ScalarValue::Utf8(Some("alph".to_string()))),
            ],
            Ok(Some(true)),
            bool,
            Boolean,
            BooleanArray
        );
        test_function!(
            StartsWith,
            &[
                lit(ScalarValue::Utf8(Some("alphabet".to_string()))),
                lit(ScalarValue::Utf8(Some("blph".to_string()))),
            ],
            Ok(Some(false)),
            bool,
            Boolean,
            BooleanArray
        );
        test_function!(
            StartsWith,
            &[
                lit(ScalarValue::Utf8(None)),
                lit(ScalarValue::Utf8(Some("alph".to_string()))),
            ],
            Ok(None),
            bool,
            Boolean,
            BooleanArray
        );
        test_function!(
            StartsWith,
            &[
                lit(ScalarValue::Utf8(Some("alphabet".to_string()))),
                lit(ScalarValue::Utf8(None)),
            ],
            Ok(None),
            bool,
            Boolean,
            BooleanArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Strpos,
            &[
                lit(ScalarValue::Utf8(Some("abc".to_string()))),
                lit(ScalarValue::Utf8(Some("c".to_string()))),
            ],
            Ok(Some(3)),
            i32,
            Int32,
            Int32Array
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Strpos,
            &[
                lit(ScalarValue::Utf8(Some("josé".to_string()))),
                lit(ScalarValue::Utf8(Some("é".to_string()))),
            ],
            Ok(Some(4)),
            i32,
            Int32,
            Int32Array
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Strpos,
            &[
                lit(ScalarValue::Utf8(Some("joséésoj".to_string()))),
                lit(ScalarValue::Utf8(Some("so".to_string()))),
            ],
            Ok(Some(6)),
            i32,
            Int32,
            Int32Array
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Strpos,
            &[
                lit(ScalarValue::Utf8(Some("joséésoj".to_string()))),
                lit(ScalarValue::Utf8(Some("abc".to_string()))),
            ],
            Ok(Some(0)),
            i32,
            Int32,
            Int32Array
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Strpos,
            &[
                lit(ScalarValue::Utf8(None)),
                lit(ScalarValue::Utf8(Some("abc".to_string()))),
            ],
            Ok(None),
            i32,
            Int32,
            Int32Array
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Strpos,
            &[
                lit(ScalarValue::Utf8(Some("joséésoj".to_string()))),
                lit(ScalarValue::Utf8(None)),
            ],
            Ok(None),
            i32,
            Int32,
            Int32Array
        );
        #[cfg(not(feature = "unicode_expressions"))]
        test_function!(
            Strpos,
            &[
                lit(ScalarValue::Utf8(Some("joséésoj".to_string()))),
                lit(ScalarValue::Utf8(None)),
            ],
            Err(DataFusionError::Internal(
                "function strpos requires compilation with feature flag: unicode_expressions.".to_string()
            )),
            i32,
            Int32,
            Int32Array
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Substr,
            &[
                lit(ScalarValue::Utf8(Some("alphabet".to_string()))),
                lit(ScalarValue::Int64(Some(0))),
            ],
            Ok(Some("alphabet")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Substr,
            &[
                lit(ScalarValue::Utf8(Some("joséésoj".to_string()))),
                lit(ScalarValue::Int64(Some(5))),
            ],
            Ok(Some("ésoj")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Substr,
            &[
                lit(ScalarValue::Utf8(Some("alphabet".to_string()))),
                lit(ScalarValue::Int64(Some(1))),
            ],
            Ok(Some("alphabet")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Substr,
            &[
                lit(ScalarValue::Utf8(Some("alphabet".to_string()))),
                lit(ScalarValue::Int64(Some(2))),
            ],
            Ok(Some("lphabet")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Substr,
            &[
                lit(ScalarValue::Utf8(Some("alphabet".to_string()))),
                lit(ScalarValue::Int64(Some(3))),
            ],
            Ok(Some("phabet")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Substr,
            &[
                lit(ScalarValue::Utf8(Some("alphabet".to_string()))),
                lit(ScalarValue::Int64(Some(-3))),
            ],
            Ok(Some("alphabet")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Substr,
            &[
                lit(ScalarValue::Utf8(Some("alphabet".to_string()))),
                lit(ScalarValue::Int64(Some(30))),
            ],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Substr,
            &[
                lit(ScalarValue::Utf8(Some("alphabet".to_string()))),
                lit(ScalarValue::Int64(None)),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Substr,
            &[
                lit(ScalarValue::Utf8(Some("alphabet".to_string()))),
                lit(ScalarValue::Int64(Some(3))),
                lit(ScalarValue::Int64(Some(2))),
            ],
            Ok(Some("ph")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Substr,
            &[
                lit(ScalarValue::Utf8(Some("alphabet".to_string()))),
                lit(ScalarValue::Int64(Some(3))),
                lit(ScalarValue::Int64(Some(20))),
            ],
            Ok(Some("phabet")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Substr,
            &[
                lit(ScalarValue::Utf8(Some("alphabet".to_string()))),
                lit(ScalarValue::Int64(None)),
                lit(ScalarValue::Int64(Some(20))),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Substr,
            &[
                lit(ScalarValue::Utf8(Some("alphabet".to_string()))),
                lit(ScalarValue::Int64(Some(3))),
                lit(ScalarValue::Int64(None)),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Substr,
            &[
                lit(ScalarValue::Utf8(Some("alphabet".to_string()))),
                lit(ScalarValue::Int64(Some(1))),
                lit(ScalarValue::Int64(Some(-1))),
            ],
            Err(DataFusionError::Execution(
                "negative substring length not allowed".to_string(),
            )),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Substr,
            &[
                lit(ScalarValue::Utf8(Some("joséésoj".to_string()))),
                lit(ScalarValue::Int64(Some(5))),
                lit(ScalarValue::Int64(Some(2))),
            ],
            Ok(Some("és")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(not(feature = "unicode_expressions"))]
        test_function!(
            Substr,
            &[
                lit(ScalarValue::Utf8(Some("alphabet".to_string()))),
                lit(ScalarValue::Int64(Some(0))),
            ],
            Err(DataFusionError::Internal(
                "function substr requires compilation with feature flag: unicode_expressions.".to_string()
            )),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Translate,
            &[
                lit(ScalarValue::Utf8(Some("12345".to_string()))),
                lit(ScalarValue::Utf8(Some("143".to_string()))),
                lit(ScalarValue::Utf8(Some("ax".to_string()))),
            ],
            Ok(Some("a2x5")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Translate,
            &[
                lit(ScalarValue::Utf8(None)),
                lit(ScalarValue::Utf8(Some("143".to_string()))),
                lit(ScalarValue::Utf8(Some("ax".to_string()))),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Translate,
            &[
                lit(ScalarValue::Utf8(Some("12345".to_string()))),
                lit(ScalarValue::Utf8(None)),
                lit(ScalarValue::Utf8(Some("ax".to_string()))),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Translate,
            &[
                lit(ScalarValue::Utf8(Some("12345".to_string()))),
                lit(ScalarValue::Utf8(Some("143".to_string()))),
                lit(ScalarValue::Utf8(None)),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Translate,
            &[
                lit(ScalarValue::Utf8(Some("é2íñ5".to_string()))),
                lit(ScalarValue::Utf8(Some("éñí".to_string()))),
                lit(ScalarValue::Utf8(Some("óü".to_string()))),
            ],
            Ok(Some("ó2ü5")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(not(feature = "unicode_expressions"))]
        test_function!(
            Translate,
            &[
                lit(ScalarValue::Utf8(Some("12345".to_string()))),
                lit(ScalarValue::Utf8(Some("143".to_string()))),
                lit(ScalarValue::Utf8(Some("ax".to_string()))),
            ],
            Err(DataFusionError::Internal(
                "function translate requires compilation with feature flag: unicode_expressions.".to_string()
            )),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Trim,
            &[lit(ScalarValue::Utf8(Some(" trim ".to_string())))],
            Ok(Some("trim")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Trim,
            &[lit(ScalarValue::Utf8(Some("trim ".to_string())))],
            Ok(Some("trim")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Trim,
            &[lit(ScalarValue::Utf8(Some(" trim".to_string())))],
            Ok(Some("trim")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Trim,
            &[lit(ScalarValue::Utf8(None))],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Upper,
            &[lit(ScalarValue::Utf8(Some("upper".to_string())))],
            Ok(Some("UPPER")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Upper,
            &[lit(ScalarValue::Utf8(Some("UPPER".to_string())))],
            Ok(Some("UPPER")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Upper,
            &[lit(ScalarValue::Utf8(None))],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        Ok(())
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

    #[test]
    #[cfg(feature = "regex_expressions")]
    fn test_regexp_match() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Utf8, false)]);

        // concat(value, value)
        let col_value: ArrayRef = Arc::new(StringArray::from(vec!["aaa-555"]));
        let pattern = lit(ScalarValue::Utf8(Some(r".*-(\d*)".to_string())));
        let columns: Vec<ArrayRef> = vec![col_value];
        let expr = create_physical_expr(
            &BuiltinScalarFunction::RegexpMatch,
            &[col("a"), pattern],
            &schema,
        )?;

        // type is correct
        assert_eq!(
            expr.data_type(&schema)?,
            DataType::List(Box::new(Field::new("item", DataType::Utf8, true)))
        );

        // evaluate works
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), columns)?;
        let result = expr.evaluate(&batch)?.into_array(batch.num_rows());

        // downcast works
        let result = result.as_any().downcast_ref::<ListArray>().unwrap();
        let first_row = result.value(0);
        let first_row = first_row.as_any().downcast_ref::<StringArray>().unwrap();

        // value is correct
        let expected = "555".to_string();
        assert_eq!(first_row.value(0), expected);

        Ok(())
    }

    #[test]
    #[cfg(feature = "regex_expressions")]
    fn test_regexp_match_all_literals() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

        // concat(value, value)
        let col_value = lit(ScalarValue::Utf8(Some("aaa-555".to_string())));
        let pattern = lit(ScalarValue::Utf8(Some(r".*-(\d*)".to_string())));
        let columns: Vec<ArrayRef> = vec![Arc::new(Int32Array::from(vec![1]))];
        let expr = create_physical_expr(
            &BuiltinScalarFunction::RegexpMatch,
            &[col_value, pattern],
            &schema,
        )?;

        // type is correct
        assert_eq!(
            expr.data_type(&schema)?,
            DataType::List(Box::new(Field::new("item", DataType::Utf8, true)))
        );

        // evaluate works
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), columns)?;
        let result = expr.evaluate(&batch)?.into_array(batch.num_rows());

        // downcast works
        let result = result.as_any().downcast_ref::<ListArray>().unwrap();
        let first_row = result.value(0);
        let first_row = first_row.as_any().downcast_ref::<StringArray>().unwrap();

        // value is correct
        let expected = "555".to_string();
        assert_eq!(first_row.value(0), expected);

        Ok(())
    }
}
