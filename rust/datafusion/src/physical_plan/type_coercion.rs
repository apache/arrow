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

//! Type coercion rules for functions with multiple valid signatures

use std::sync::Arc;

use arrow::datatypes::{DataType, Schema};

use super::{functions::Signature, PhysicalExpr};
use crate::error::{ExecutionError, Result};
use crate::physical_plan::expressions::cast;

/// Returns expressions constructed by casting `expressions` to types compatible with `signatures`.
pub fn coerce(
    expressions: &Vec<Arc<dyn PhysicalExpr>>,
    schema: &Schema,
    signature: &Signature,
) -> Result<Vec<Arc<dyn PhysicalExpr>>> {
    let current_types = expressions
        .iter()
        .map(|e| e.data_type(schema))
        .collect::<Result<Vec<_>>>()?;

    let new_types = data_types(&current_types, signature)?;

    expressions
        .iter()
        .enumerate()
        .map(|(i, expr)| cast(expr.clone(), &schema, new_types[i].clone()))
        .collect::<Result<Vec<_>>>()
}

/// returns the data types that each argument must be casted to match the `signature`.
pub fn data_types(
    current_types: &Vec<DataType>,
    signature: &Signature,
) -> Result<Vec<DataType>> {
    let valid_types = match signature {
        Signature::Variadic(valid_types) => valid_types
            .iter()
            .map(|valid_type| current_types.iter().map(|_| valid_type.clone()).collect())
            .collect(),
        Signature::Uniform(number, valid_types) => valid_types
            .iter()
            .map(|valid_type| (0..*number).map(|_| valid_type.clone()).collect())
            .collect(),
        Signature::VariadicEqual => {
            // one entry with the same len as current_types, whose type is `current_types[0]`.
            vec![current_types
                .iter()
                .map(|_| current_types[0].clone())
                .collect()]
        }
        Signature::Exact(valid_types) => vec![valid_types.clone()],
        Signature::Any(number) => {
            if current_types.len() != *number {
                return Err(ExecutionError::General(format!(
                    "The function expected {} arguments but received {}",
                    number,
                    current_types.len()
                )));
            }
            vec![(0..*number).map(|i| current_types[i].clone()).collect()]
        }
    };

    if valid_types.contains(current_types) {
        return Ok(current_types.clone());
    }

    for valid_types in valid_types {
        if let Some(types) = maybe_data_types(&valid_types, &current_types) {
            return Ok(types);
        }
    }

    // none possible -> Error
    Err(ExecutionError::General(format!(
        "Coercion from {:?} to the signature {:?} failed.",
        current_types, signature
    )))
}

/// Try to coerce current_types into valid_types.
fn maybe_data_types(
    valid_types: &Vec<DataType>,
    current_types: &Vec<DataType>,
) -> Option<Vec<DataType>> {
    if valid_types.len() != current_types.len() {
        return None;
    }

    let mut new_type = Vec::with_capacity(valid_types.len());
    for (i, valid_type) in valid_types.iter().enumerate() {
        let current_type = &current_types[i];

        if current_type == valid_type {
            new_type.push(current_type.clone())
        } else {
            // attempt to coerce
            if can_coerce_from(valid_type, &current_type) {
                new_type.push(valid_type.clone())
            } else {
                // not possible
                return None;
            }
        }
    }
    Some(new_type)
}

/// Verify that the type cast can be performed
pub fn can_coerce_from(type_into: &DataType, type_from: &DataType) -> bool {
    use self::DataType::*;
    match type_into {
        Int8 => match type_from {
            Int8 => true,
            _ => false,
        },
        Int16 => match type_from {
            Int8 | Int16 | UInt8 => true,
            _ => false,
        },
        Int32 => match type_from {
            Int8 | Int16 | Int32 | UInt8 | UInt16 => true,
            _ => false,
        },
        Int64 => match type_from {
            Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 => true,
            _ => false,
        },
        UInt8 => match type_from {
            UInt8 => true,
            _ => false,
        },
        UInt16 => match type_from {
            UInt8 | UInt16 => true,
            _ => false,
        },
        UInt32 => match type_from {
            UInt8 | UInt16 | UInt32 => true,
            _ => false,
        },
        UInt64 => match type_from {
            UInt8 | UInt16 | UInt32 | UInt64 => true,
            _ => false,
        },
        Float32 => match type_from {
            Int8 | Int16 | Int32 | Int64 => true,
            UInt8 | UInt16 | UInt32 | UInt64 => true,
            Float32 => true,
            _ => false,
        },
        Float64 => match type_from {
            Int8 | Int16 | Int32 | Int64 => true,
            UInt8 | UInt16 | UInt32 | UInt64 => true,
            Float32 | Float64 => true,
            _ => false,
        },
        Utf8 => true,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical_plan::expressions::col;
    use arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn test_maybe_data_types() -> Result<()> {
        // this vec contains: arg1, arg2, expected result
        let cases = vec![
            // 2 entries, same values
            (
                vec![DataType::UInt8, DataType::UInt16],
                vec![DataType::UInt8, DataType::UInt16],
                Some(vec![DataType::UInt8, DataType::UInt16]),
            ),
            // 2 entries, can coerse values
            (
                vec![DataType::UInt16, DataType::UInt16],
                vec![DataType::UInt8, DataType::UInt16],
                Some(vec![DataType::UInt16, DataType::UInt16]),
            ),
            // 0 entries, all good
            (vec![], vec![], Some(vec![])),
            // 2 entries, can't coerce
            (
                vec![DataType::Boolean, DataType::UInt16],
                vec![DataType::UInt8, DataType::UInt16],
                None,
            ),
            // u32 -> u16 is possible
            (
                vec![DataType::Boolean, DataType::UInt32],
                vec![DataType::Boolean, DataType::UInt16],
                Some(vec![DataType::Boolean, DataType::UInt32]),
            ),
        ];

        for case in cases {
            assert_eq!(maybe_data_types(&case.0, &case.1), case.2)
        }
        Ok(())
    }

    #[test]
    fn test_coerce() -> Result<()> {
        // create a schema
        let schema = |t: Vec<DataType>| {
            Schema::new(
                t.iter()
                    .enumerate()
                    .map(|(i, t)| Field::new(&*format!("c{}", i), t.clone(), true))
                    .collect(),
            )
        };

        // create a vector of expressions
        let expressions = |t: Vec<DataType>, schema| -> Result<Vec<_>> {
            t.iter()
                .enumerate()
                .map(|(i, t)| cast(col(&format!("c{}", i)), &schema, t.clone()))
                .collect::<Result<Vec<_>>>()
        };

        // create a case: input + expected result
        let case =
            |observed: Vec<DataType>, valid, expected: Vec<DataType>| -> Result<_> {
                let schema = schema(observed.clone());
                let expr = expressions(observed, schema.clone())?;
                let expected = expressions(expected, schema.clone())?;
                Ok((expr.clone(), schema, valid, expected))
            };

        let cases = vec![
            // u16 -> u32
            case(
                vec![DataType::UInt16],
                Signature::Uniform(1, vec![DataType::UInt32]),
                vec![DataType::UInt32],
            )?,
            // same type
            case(
                vec![DataType::UInt32, DataType::UInt32],
                Signature::Uniform(2, vec![DataType::UInt32]),
                vec![DataType::UInt32, DataType::UInt32],
            )?,
            case(
                vec![DataType::UInt32],
                Signature::Uniform(1, vec![DataType::Float32, DataType::Float64]),
                vec![DataType::Float32],
            )?,
            // u32 -> f32
            case(
                vec![DataType::UInt32, DataType::UInt32],
                Signature::Variadic(vec![DataType::Float32]),
                vec![DataType::Float32, DataType::Float32],
            )?,
            // u32 -> f32
            case(
                vec![DataType::Float32, DataType::UInt32],
                Signature::VariadicEqual,
                vec![DataType::Float32, DataType::Float32],
            )?,
            // common type is u64
            case(
                vec![DataType::UInt32, DataType::UInt64],
                Signature::Variadic(vec![DataType::UInt32, DataType::UInt64]),
                vec![DataType::UInt64, DataType::UInt64],
            )?,
            // f32 -> f32
            case(
                vec![DataType::Float32],
                Signature::Any(1),
                vec![DataType::Float32],
            )?,
        ];

        for case in cases {
            let observed = format!("{:?}", coerce(&case.0, &case.1, &case.2)?);
            let expected = format!("{:?}", case.3);
            assert_eq!(observed, expected);
        }

        // now cases that are expected to fail
        let cases = vec![
            // we do not know how to cast bool to UInt16 => fail
            case(
                vec![DataType::Boolean],
                Signature::Uniform(1, vec![DataType::UInt16]),
                vec![],
            )?,
            // u32 and bool are not uniform
            case(
                vec![DataType::UInt32, DataType::Boolean],
                Signature::VariadicEqual,
                vec![],
            )?,
            // bool is not castable to u32
            case(
                vec![DataType::Boolean, DataType::Boolean],
                Signature::Variadic(vec![DataType::UInt32]),
                vec![],
            )?,
            // expected two arguments
            case(vec![DataType::UInt32], Signature::Any(2), vec![])?,
        ];

        for case in cases {
            if let Ok(_) = coerce(&case.0, &case.1, &case.2) {
                return Err(ExecutionError::General(format!(
                    "Error was expected in {:?}",
                    case
                )));
            }
        }

        Ok(())
    }
}
