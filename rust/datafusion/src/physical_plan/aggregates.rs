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

//! Declaration of built-in (aggregate) functions.
//! This module contains built-in aggregates' enumeration and metadata.
//!
//! Generally, an aggregate has:
//! * a signature
//! * a return type, that is a function of the incoming argument's types
//! * the computation, that must accept each valid signature
//!
//! * Signature: see `Signature`
//! * Return type: a function `(arg_types) -> return_type`. E.g. for min, ([f32]) -> f32, ([f64]) -> f64.

use super::{
    functions::Signature,
    type_coercion::{coerce, data_types},
    AggregateExpr, PhysicalExpr,
};
use crate::error::{ExecutionError, Result};
use crate::physical_plan::expressions;
use arrow::datatypes::{DataType, Schema};
use expressions::{avg_return_type, sum_return_type};
use std::{fmt, str::FromStr, sync::Arc};

/// Enum of all built-in scalar functions
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AggregateFunction {
    /// count
    Count,
    /// sum
    Sum,
    /// min
    Min,
    /// max
    Max,
    /// avg
    Avg,
}

impl fmt::Display for AggregateFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // uppercase of the debug.
        write!(f, "{}", format!("{:?}", self).to_uppercase())
    }
}

impl FromStr for AggregateFunction {
    type Err = ExecutionError;
    fn from_str(name: &str) -> Result<AggregateFunction> {
        Ok(match &*name.to_uppercase() {
            "MIN" => AggregateFunction::Min,
            "MAX" => AggregateFunction::Max,
            "COUNT" => AggregateFunction::Count,
            "AVG" => AggregateFunction::Avg,
            "SUM" => AggregateFunction::Sum,
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
pub fn return_type(
    fun: &AggregateFunction,
    arg_types: &Vec<DataType>,
) -> Result<DataType> {
    // Note that this function *must* return the same type that the respective physical expression returns
    // or the execution panics.

    // verify that this is a valid set of data types for this function
    data_types(arg_types, &signature(fun))?;

    match fun {
        AggregateFunction::Count => Ok(DataType::UInt64),
        AggregateFunction::Max | AggregateFunction::Min => Ok(arg_types[0].clone()),
        AggregateFunction::Sum => sum_return_type(&arg_types[0]),
        AggregateFunction::Avg => avg_return_type(&arg_types[0]),
    }
}

/// Create a physical (function) expression.
/// This function errors when `args`' can't be coerced to a valid argument type of the function.
pub fn create_aggregate_expr(
    fun: &AggregateFunction,
    args: &Vec<Arc<dyn PhysicalExpr>>,
    input_schema: &Schema,
) -> Result<Arc<dyn AggregateExpr>> {
    // coerce
    let arg = coerce(args, input_schema, &signature(fun))?[0].clone();

    Ok(match fun {
        AggregateFunction::Count => expressions::count(arg),
        AggregateFunction::Sum => expressions::sum(arg),
        AggregateFunction::Min => expressions::min(arg),
        AggregateFunction::Max => expressions::max(arg),
        AggregateFunction::Avg => expressions::avg(arg),
    })
}

static NUMERICS: &'static [DataType] = &[
    DataType::Int8,
    DataType::Int16,
    DataType::Int32,
    DataType::Int64,
    DataType::UInt8,
    DataType::UInt16,
    DataType::UInt32,
    DataType::UInt64,
    DataType::Float32,
    DataType::Float64,
];

/// the signatures supported by the function `fun`.
fn signature(fun: &AggregateFunction) -> Signature {
    // note: the physical expression must accept the type returned by this function or the execution panics.
    match fun {
        AggregateFunction::Count => Signature::Any(1),
        AggregateFunction::Min | AggregateFunction::Max => {
            let mut valid = vec![DataType::Utf8];
            valid.extend_from_slice(NUMERICS);
            Signature::Uniform(1, valid)
        }
        AggregateFunction::Avg | AggregateFunction::Sum => {
            Signature::Uniform(1, NUMERICS.to_vec())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result;

    #[test]
    fn test_min_max() -> Result<()> {
        let observed = return_type(&AggregateFunction::Min, &vec![DataType::Utf8])?;
        assert_eq!(DataType::Utf8, observed);

        let observed = return_type(&AggregateFunction::Max, &vec![DataType::Int32])?;
        assert_eq!(DataType::Int32, observed);
        Ok(())
    }

    #[test]
    fn test_sum_no_utf8() -> Result<()> {
        let observed = return_type(&AggregateFunction::Sum, &vec![DataType::Utf8]);
        assert!(observed.is_err());
        Ok(())
    }

    #[test]
    fn test_sum_upcasts() -> Result<()> {
        let observed = return_type(&AggregateFunction::Sum, &vec![DataType::UInt32])?;
        assert_eq!(DataType::UInt64, observed);
        Ok(())
    }

    #[test]
    fn test_count_return_type() -> Result<()> {
        let observed = return_type(&AggregateFunction::Count, &vec![DataType::Utf8])?;
        assert_eq!(DataType::UInt64, observed);

        let observed = return_type(&AggregateFunction::Count, &vec![DataType::Int8])?;
        assert_eq!(DataType::UInt64, observed);
        Ok(())
    }

    #[test]
    fn test_avg_return_type() -> Result<()> {
        let observed = return_type(&AggregateFunction::Avg, &vec![DataType::Float32])?;
        assert_eq!(DataType::Float64, observed);

        let observed = return_type(&AggregateFunction::Avg, &vec![DataType::Float64])?;
        assert_eq!(DataType::Float64, observed);
        Ok(())
    }

    #[test]
    fn test_avg_no_utf8() -> Result<()> {
        let observed = return_type(&AggregateFunction::Avg, &vec![DataType::Utf8]);
        assert!(observed.is_err());
        Ok(())
    }
}
